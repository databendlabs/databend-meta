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

//! Sequence-bounded state-machine reads.

use std::io;
use std::ops::RangeBounds;

use map_api::IOResultStream;
use map_api::mvcc;
use map_api::mvcc::GetAtSeq;
use map_api::mvcc::RangeAtSeq;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_map::ActiveReadGuard;
use crate::leveled_map::LeveledMap;

/// A typed, sequence-bounded read view of the state machine.
#[derive(Clone, Debug)]
pub struct StateMachineReadView {
    store: LeveledMap,
    read_guard: ActiveReadGuard,
}

impl StateMachineReadView {
    pub fn new(store: &LeveledMap) -> Self {
        let read_guard = store.new_active_read_guard();
        Self {
            store: store.clone(),
            read_guard,
        }
    }

    /// Returns the map this read boundary belongs to.
    ///
    /// `StateMachineView` uses it to publish its staged write set after all
    /// reads have used this snapshot boundary.
    pub fn store(&self) -> &LeveledMap {
        &self.store
    }

    /// Returns the fixed sequence boundary for this read view.
    pub fn seq(&self) -> u64 {
        self.read_guard.seq()
    }

    pub async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        self.store.get_at_seq(key, self.seq()).await
    }

    pub async fn get_expire(&self, key: ExpireKey) -> Result<SeqMarked<String>, io::Error> {
        self.store.get_at_seq(key, self.seq()).await
    }
}

#[async_trait::async_trait]
impl mvcc::ViewGet<UserKey> for StateMachineReadView {
    async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        self.get(key).await
    }
}

#[async_trait::async_trait]
impl mvcc::ViewRange<UserKey> for StateMachineReadView {
    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(UserKey, SeqMarked<MetaValue>)>, io::Error>
    where
        R: RangeBounds<UserKey> + Send + Sync + Clone + 'static,
    {
        self.store.range_at_seq(range, self.seq()).await
    }
}

#[async_trait::async_trait]
impl mvcc::ViewGet<ExpireKey> for StateMachineReadView {
    async fn get(&self, key: ExpireKey) -> Result<SeqMarked<String>, io::Error> {
        self.get_expire(key).await
    }
}

#[async_trait::async_trait]
impl mvcc::ViewRange<ExpireKey> for StateMachineReadView {
    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(ExpireKey, SeqMarked<String>)>, io::Error>
    where
        R: RangeBounds<ExpireKey> + Send + Sync + Clone + 'static,
    {
        self.store.range_at_seq(range, self.seq()).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::TryStreamExt;
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
        view: &StateMachineReadView,
    ) -> anyhow::Result<Vec<(UserKey, SeqMarked<MetaValue>)>> {
        Ok(view.range(user("")..).await?.try_collect().await?)
    }

    async fn expire_range(
        view: &StateMachineReadView,
    ) -> anyhow::Result<Vec<(ExpireKey, SeqMarked<String>)>> {
        Ok(view.range(..).await?.try_collect().await?)
    }

    #[tokio::test]
    async fn test_read_views_pin_the_oldest_active_sequence() -> anyhow::Result<()> {
        let map = LeveledMap::default();
        let first = map.to_read_view();
        assert_eq!(first.seq(), 0);

        let mut view = map.to_view();
        view.set(UserKey::new("key"), Some((None, b"value".to_vec())));
        view.commit().await?;

        let second = map.to_read_view();
        assert_eq!(second.seq(), 1);
        assert_eq!(*map.oldest_active_read_seq(), 0);

        drop(first);
        assert_eq!(*map.oldest_active_read_seq(), 1);

        drop(second);
        assert_eq!(*map.oldest_active_read_seq(), u64::MAX);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_view_keeps_its_sequence_boundary() -> anyhow::Result<()> {
        let map = LeveledMap::default();
        let mut base = map.to_view();
        base.set(user("a"), Some(value("old-a")));
        base.set(user("b"), Some(value("old-b")));
        base.set(expire(100, 1), Some("a".to_string()));
        base.set(expire(200, 2), Some("b".to_string()));
        base.commit().await?;

        let read_view = map.to_read_view();
        assert_eq!(read_view.seq(), 2);

        let mut update = map.to_view();
        update.set(user("a"), Some(value("new-a")));
        update.set(user("b"), None);
        update.set(user("c"), Some(value("new-c")));
        update.set(expire(100, 1), Some("new-a".to_string()));
        update.set(expire(200, 2), None);
        update.set(expire(300, 3), Some("new-c".to_string()));
        update.commit().await?;

        assert_eq!(
            read_view.get(user("a")).await?,
            SeqMarked::new_normal(1, value("old-a"))
        );
        assert_eq!(
            read_view.get(user("b")).await?,
            SeqMarked::new_normal(2, value("old-b"))
        );
        assert!(read_view.get(user("c")).await?.is_not_found());
        assert_eq!(
            read_view.get_expire(expire(100, 1)).await?,
            SeqMarked::new_normal(2, "a".to_string())
        );
        assert_eq!(
            read_view.get_expire(expire(200, 2)).await?,
            SeqMarked::new_normal(2, "b".to_string())
        );
        assert!(read_view.get_expire(expire(300, 3)).await?.is_not_found());

        assert_eq!(user_range(&read_view).await?, vec![
            (user("a"), SeqMarked::new_normal(1, value("old-a"))),
            (user("b"), SeqMarked::new_normal(2, value("old-b"))),
        ]);
        assert_eq!(expire_range(&read_view).await?, vec![
            (expire(100, 1), SeqMarked::new_normal(2, "a".to_string())),
            (expire(200, 2), SeqMarked::new_normal(2, "b".to_string())),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_cloned_read_view_keeps_the_registration_alive() -> anyhow::Result<()> {
        let map = LeveledMap::default();
        let read_view = map.to_read_view();
        let clone = read_view.clone();

        drop(read_view);
        assert_eq!(*map.oldest_active_read_seq(), 0);

        drop(clone);
        assert_eq!(*map.oldest_active_read_seq(), u64::MAX);
        Ok(())
    }

    #[tokio::test]
    async fn test_compaction_waits_only_for_reads_before_its_boundary() -> anyhow::Result<()> {
        let map = LeveledMap::default();
        let old_read = map.to_read_view();

        let mut view = map.to_view();
        view.set(user("key"), Some(value("value")));
        view.commit().await?;

        map.freeze_writable_without_permit();

        let current_read = map.to_read_view();
        assert_eq!(current_read.seq(), 1);

        let mut later_write = map.to_view();
        later_write.set(user("later"), Some(value("value")));
        later_write.commit().await?;
        assert_eq!(map.curr_seq(), 2);

        let waiting_map = map.clone();
        let mut waiting = tokio::spawn(async move {
            waiting_map.wait_for_active_reads_before_compaction().await;
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiting)
                .await
                .is_err(),
            "the read at sequence 0 must block compaction through sequence 1",
        );

        drop(old_read);

        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("the remaining read at the compaction boundary is safe")
            .expect("the waiting task must not panic");

        assert_eq!(*map.oldest_active_read_seq(), 1);
        drop(current_read);
        Ok(())
    }
}
