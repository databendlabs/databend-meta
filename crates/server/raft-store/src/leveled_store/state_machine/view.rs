// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::ops::RangeBounds;

use databend_meta_types::sys_data::SysData;
use futures_util::StreamExt;
use map_api::IOResultStream;
use map_api::MapKey;
use map_api::mvcc;
use map_api::util;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;
use stream_more::KMerge;
use stream_more::StreamMore;

use super::changes::Changes;
use super::read_view::StateMachineReadView;
use crate::leveled_store::leveled_map::LeveledMap;

/// A typed state-machine transaction with one sequence allocator.
pub(crate) struct StateMachineView {
    read_view: StateMachineReadView,
    changes: Changes,
}

impl StateMachineView {
    pub(crate) fn from_leveled_map(store: &LeveledMap) -> Self {
        let sys_data = store.with_sys_data(|sys_data| sys_data.clone());

        Self {
            read_view: StateMachineReadView::new(store),
            changes: Changes::new(sys_data),
        }
    }

    pub(crate) fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        f(&mut self.changes.sys_data.lock().unwrap())
    }

    /// Publish the staged changes to the underlying `LeveledMap`.
    ///
    /// The publish itself is synchronous and infallible; `async` and `Result`
    /// only mirror `StateMachineApi::commit`, which drives this method.
    pub(crate) async fn commit(self) -> Result<(), io::Error> {
        self.read_view.store().commit(self.changes.into_level());
        Ok(())
    }

    async fn get_typed<K>(&self, key: K) -> Result<SeqMarked<K::V>, io::Error>
    where
        K: MapKey,
        StateMachineReadView: mvcc::ViewGet<K>,
        Changes: AsRef<mvcc::Table<K, K::V>>,
    {
        let seq = self.with_sys_data(|sys_data| sys_data.curr_seq());
        let changes: &mvcc::Table<K, K::V> = self.changes.as_ref();
        let changed = changes.get(key.clone(), seq).cloned();
        if !changed.is_not_found() {
            return Ok(changed);
        }

        mvcc::ViewGet::get(&self.read_view, key).await
    }

    async fn range_typed<K, R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where
        K: MapKey,
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
        StateMachineReadView: mvcc::ViewRange<K>,
        Changes: AsRef<mvcc::Table<K, K::V>>,
    {
        let base = mvcc::ViewRange::range(&self.read_view, range.clone()).await?;
        let seq = self.with_sys_data(|sys_data| sys_data.curr_seq());
        let changes: &mvcc::Table<K, K::V> = self.changes.as_ref();
        let changed = changes
            .range(range, seq)
            .map(|(key, value)| Ok((key.clone(), value.cloned())))
            .collect::<Vec<_>>();

        let changed = futures::stream::iter(changed).boxed();
        let merged = KMerge::by(util::by_key_seq).merge(base).merge(changed);
        Ok(merged.coalesce(util::merge_kv_results).boxed())
    }

    fn set_typed<K>(&mut self, key: K, value: Option<K::V>, increment_seq: bool) -> SeqMarked<()>
    where
        K: MapKey,
        Changes: AsMut<mvcc::Table<K, K::V>>,
    {
        let seq = {
            let sys_data = self.changes.sys_data.get_mut().unwrap();
            match (&value, increment_seq) {
                (Some(_), true) => SeqMarked::new_normal(sys_data.next_seq(), ()),
                (Some(_), false) => SeqMarked::new_normal(sys_data.curr_seq(), ()),
                (None, _) => SeqMarked::new_tombstone(sys_data.curr_seq()),
            }
        };

        let table: &mut mvcc::Table<K, K::V> = self.changes.as_mut();
        match value {
            Some(value) => table.insert(key, *seq.internal_seq(), value).unwrap(),
            None => table.insert_tombstone(key, *seq.internal_seq()).unwrap(),
        }
        seq
    }
}

#[async_trait::async_trait]
impl mvcc::ViewGet<UserKey> for StateMachineView {
    async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        self.get_typed(key).await
    }
}

#[async_trait::async_trait]
impl mvcc::ViewRange<UserKey> for StateMachineView {
    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(UserKey, SeqMarked<MetaValue>)>, io::Error>
    where
        R: RangeBounds<UserKey> + Send + Sync + Clone + 'static,
    {
        self.range_typed(range).await
    }
}

impl mvcc::ViewSet<UserKey> for StateMachineView {
    fn set(&mut self, key: UserKey, value: Option<MetaValue>) -> SeqMarked<()> {
        self.set_typed(key, value, true)
    }
}

#[async_trait::async_trait]
impl mvcc::ViewGet<ExpireKey> for StateMachineView {
    async fn get(&self, key: ExpireKey) -> Result<SeqMarked<String>, io::Error> {
        self.get_typed(key).await
    }
}

impl mvcc::ViewSet<ExpireKey> for StateMachineView {
    fn set(&mut self, key: ExpireKey, value: Option<String>) -> SeqMarked<()> {
        self.set_typed(key, value, false)
    }
}

#[async_trait::async_trait]
impl mvcc::ViewRange<ExpireKey> for StateMachineView {
    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(ExpireKey, SeqMarked<String>)>, io::Error>
    where
        R: RangeBounds<ExpireKey> + Send + Sync + Clone + 'static,
    {
        self.range_typed(range).await
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;
    use map_api::mvcc::GetAtSeq;
    use map_api::mvcc::ViewGet;
    use map_api::mvcc::ViewRange;
    use map_api::mvcc::ViewSet;

    use super::*;

    fn user(key: &str) -> UserKey {
        UserKey::new(key)
    }

    fn expire(time_ms: u64, seq: u64) -> ExpireKey {
        ExpireKey::new(time_ms, seq)
    }

    fn value(value: &str) -> MetaValue {
        (None, value.as_bytes().to_vec())
    }

    async fn user_range(
        view: &StateMachineView,
    ) -> anyhow::Result<Vec<(UserKey, SeqMarked<MetaValue>)>> {
        Ok(view.range(user("")..).await?.try_collect().await?)
    }

    async fn expire_range(
        view: &StateMachineView,
    ) -> anyhow::Result<Vec<(ExpireKey, SeqMarked<String>)>> {
        Ok(view.range(..).await?.try_collect().await?)
    }

    #[tokio::test]
    async fn test_view_stages_typed_changes_and_sys_data() -> anyhow::Result<()> {
        let map = LeveledMap::default();
        let mut view = map.to_view();

        assert_eq!(
            view.set(UserKey::new("user"), Some((None, b"value".to_vec()))),
            SeqMarked::new_normal(1, ())
        );
        assert_eq!(
            view.set(ExpireKey::new(100, 1), Some("user".to_string())),
            SeqMarked::new_normal(1, ())
        );
        view.with_sys_data(|sys_data| {
            sys_data.features_mut().insert("feature".to_string());
        });

        assert_eq!(map.curr_seq(), 0);
        assert!(
            map.get_at_seq(UserKey::new("user"), u64::MAX)
                .await?
                .is_not_found()
        );
        assert_eq!(
            view.get(UserKey::new("user")).await?,
            SeqMarked::new_normal(1, (None, b"value".to_vec()))
        );

        view.commit().await?;

        assert_eq!(map.curr_seq(), 1);
        assert_eq!(
            map.get_at_seq(UserKey::new("user"), u64::MAX).await?,
            SeqMarked::new_normal(1, (None, b"value".to_vec()))
        );
        assert_eq!(
            map.get_at_seq(ExpireKey::new(100, 1), u64::MAX).await?,
            SeqMarked::new_normal(1, "user".to_string())
        );
        assert!(map.with_sys_data(|sys_data| sys_data.feature_enabled("feature")));
        Ok(())
    }

    #[tokio::test]
    async fn test_view_uses_one_sequence_allocator() -> anyhow::Result<()> {
        let map = LeveledMap::default();
        let mut view = map.to_view();

        assert_eq!(
            view.set(user("a"), Some(value("a"))),
            SeqMarked::new_normal(1, ())
        );
        assert_eq!(
            view.set(expire(100, 1), Some("a".to_string())),
            SeqMarked::new_normal(1, ())
        );
        assert_eq!(
            view.set(user("b"), Some(value("b"))),
            SeqMarked::new_normal(2, ())
        );
        assert_eq!(
            view.set(expire(200, 2), Some("b".to_string())),
            SeqMarked::new_normal(2, ())
        );
        assert_eq!(view.set(user("a"), None), SeqMarked::new_tombstone(2));
        assert_eq!(view.set(expire(100, 1), None), SeqMarked::new_tombstone(2));

        assert_eq!(map.curr_seq(), 0);
        assert_eq!(view.get(user("a")).await?, SeqMarked::new_tombstone(2));
        assert_eq!(view.get(expire(100, 1)).await?, SeqMarked::new_tombstone(2));

        view.commit().await?;

        assert_eq!(map.curr_seq(), 2);
        assert_eq!(
            map.get_at_seq(user("a"), u64::MAX).await?,
            SeqMarked::new_tombstone(2)
        );
        assert_eq!(
            map.get_at_seq(user("b"), u64::MAX).await?,
            SeqMarked::new_normal(2, value("b"))
        );
        assert_eq!(
            map.get_at_seq(expire(100, 1), u64::MAX).await?,
            SeqMarked::new_tombstone(2)
        );
        assert_eq!(
            map.get_at_seq(expire(200, 2), u64::MAX).await?,
            SeqMarked::new_normal(2, "b".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_view_reads_and_ranges_merge_staged_changes() -> anyhow::Result<()> {
        let map = LeveledMap::default();
        let mut base = map.to_view();
        base.set(user("a"), Some(value("old-a")));
        base.set(user("b"), Some(value("old-b")));
        base.set(user("d"), Some(value("old-d")));
        base.set(expire(100, 1), Some("a".to_string()));
        base.set(expire(200, 2), Some("b".to_string()));
        base.commit().await?;

        let mut view = map.to_view();
        view.set(user("a"), Some(value("new-a")));
        view.set(user("b"), None);
        view.set(user("c"), Some(value("new-c")));
        view.set(expire(100, 1), Some("new-a".to_string()));
        view.set(expire(200, 2), None);
        view.set(expire(300, 3), Some("new-c".to_string()));

        assert_eq!(
            view.get(user("a")).await?,
            SeqMarked::new_normal(4, value("new-a"))
        );
        assert_eq!(view.get(user("b")).await?, SeqMarked::new_tombstone(4));
        assert_eq!(
            view.get(user("d")).await?,
            SeqMarked::new_normal(3, value("old-d"))
        );
        assert_eq!(
            view.get(expire(100, 1)).await?,
            SeqMarked::new_normal(5, "new-a".to_string())
        );
        assert_eq!(view.get(expire(200, 2)).await?, SeqMarked::new_tombstone(5));

        assert_eq!(user_range(&view).await?, vec![
            (user("a"), SeqMarked::new_normal(4, value("new-a"))),
            (user("b"), SeqMarked::new_tombstone(4)),
            (user("c"), SeqMarked::new_normal(5, value("new-c"))),
            (user("d"), SeqMarked::new_normal(3, value("old-d"))),
        ]);
        assert_eq!(expire_range(&view).await?, vec![
            (
                expire(100, 1),
                SeqMarked::new_normal(5, "new-a".to_string())
            ),
            (expire(200, 2), SeqMarked::new_tombstone(5)),
            (
                expire(300, 3),
                SeqMarked::new_normal(5, "new-c".to_string())
            ),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_view_reads_its_original_sequence_boundary() -> anyhow::Result<()> {
        let map = LeveledMap::default();
        let mut initial = map.to_view();
        initial.set(user("key"), Some(value("old")));
        initial.commit().await?;

        let view = map.to_view();
        let mut update = map.to_view();
        update.set(user("key"), Some(value("new")));
        update.commit().await?;

        assert_eq!(
            view.get(user("key")).await?,
            SeqMarked::new_normal(1, value("old"))
        );
        assert_eq!(user_range(&view).await?, vec![(
            user("key"),
            SeqMarked::new_normal(1, value("old"))
        )]);
        assert_eq!(*map.oldest_active_read_seq(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_dropping_view_discards_all_staged_state() -> anyhow::Result<()> {
        let map = LeveledMap::default();
        let mut view = map.to_view();

        view.set(UserKey::new("user"), Some((None, b"value".to_vec())));
        view.set(ExpireKey::new(100, 1), Some("user".to_string()));
        view.with_sys_data(|sys_data| sys_data.update_seq(9));
        drop(view);

        assert_eq!(map.curr_seq(), 0);
        assert!(
            map.get_at_seq(UserKey::new("user"), u64::MAX)
                .await?
                .is_not_found()
        );
        assert!(
            map.get_at_seq(ExpireKey::new(100, 1), u64::MAX)
                .await?
                .is_not_found()
        );
        Ok(())
    }
}
