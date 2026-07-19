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

use std::io::Error;
use std::ops::Bound;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use futures_util::TryStreamExt;
use map_api::IOResultStream;
use map_api::MapKey;
use map_api::mvcc;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::UserKey;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::persisted_codec::PersistedCodec;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Value;

#[async_trait::async_trait]
impl<K> mvcc::GetAtSeq<K> for LeveledMap
where
    K: MapKey + MapKeyEncode + MapKeyDecode,
    SeqMarked<K::V>: PersistedCodec<SeqMarked>,
    Level: AsRef<mvcc::Table<K, K::V>>,
{
    async fn get_at_seq(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<K::V>, Error> {
        let immutable = {
            let inner = self.data.lock().unwrap();
            let table: &mvcc::Table<K, K::V> = inner.writable.as_ref();
            let got = table.get(key.clone(), snapshot_seq).cloned();
            if !got.is_not_found() {
                return Ok(got);
            }

            inner.immutable.clone()
        };

        immutable.get_at_seq(key, snapshot_seq).await
    }
}

#[async_trait::async_trait]
impl<K> mvcc::RangeAtSeq<K> for LeveledMap
where
    K: MapKey + MapKeyEncode + MapKeyDecode,
    SeqMarked<K::V>: PersistedCodec<SeqMarked>,
    Level: AsRef<mvcc::Table<K, K::V>>,
    Immutable: AsRef<mvcc::Table<K, K::V>>,
{
    async fn range_at_seq<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        super::impl_scoped_seq_bounded_range::range_at_seq(self, range, snapshot_seq).await
    }
}

#[async_trait::async_trait]
impl mvcc::GetAtSeq<Key> for LeveledMap {
    async fn get_at_seq(&self, key: Key, snapshot_seq: u64) -> Result<SeqMarked<Value>, Error> {
        match key {
            Key::User(key) => {
                let got =
                    <LeveledMap as mvcc::GetAtSeq<UserKey>>::get_at_seq(self, key, snapshot_seq)
                        .await?;
                Ok(got.map(Value::User))
            }
            Key::Expire(key) => {
                let got =
                    <LeveledMap as mvcc::GetAtSeq<ExpireKey>>::get_at_seq(self, key, snapshot_seq)
                        .await?;
                Ok(got.map(Value::Expire))
            }
        }
    }
}

#[async_trait::async_trait]
impl mvcc::RangeAtSeq<Key> for LeveledMap {
    async fn range_at_seq<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(Key, SeqMarked<Value>)>, Error>
    where
        R: RangeBounds<Key> + Send + Sync + Clone + 'static,
    {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        let user = if let Some(range) = user_range(start.clone(), end.clone()) {
            Some(
                <LeveledMap as mvcc::RangeAtSeq<UserKey>>::range_at_seq(self, range, snapshot_seq)
                    .await?
                    .map_ok(|(key, value)| (Key::User(key), value.map(Value::User)))
                    .boxed(),
            )
        } else {
            None
        };

        let expire = if let Some(range) = expire_range(start, end) {
            Some(
                <LeveledMap as mvcc::RangeAtSeq<ExpireKey>>::range_at_seq(
                    self,
                    range,
                    snapshot_seq,
                )
                .await?
                .map_ok(|(key, value)| (Key::Expire(key), value.map(Value::Expire)))
                .boxed(),
            )
        } else {
            None
        };

        let strm = match (user, expire) {
            (Some(user), Some(expire)) => user.chain(expire).boxed(),
            (Some(user), None) => user,
            (None, Some(expire)) => expire,
            (None, None) => futures::stream::empty().boxed(),
        };

        Ok(strm)
    }
}

fn user_range(start: Bound<Key>, end: Bound<Key>) -> Option<(Bound<UserKey>, Bound<UserKey>)> {
    let start = match start {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(Key::User(key)) => Bound::Included(key),
        Bound::Excluded(Key::User(key)) => Bound::Excluded(key),
        Bound::Included(Key::Expire(_)) | Bound::Excluded(Key::Expire(_)) => return None,
    };

    let end = match end {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(Key::User(key)) => Bound::Included(key),
        Bound::Excluded(Key::User(key)) => Bound::Excluded(key),
        Bound::Included(Key::Expire(_)) | Bound::Excluded(Key::Expire(_)) => Bound::Unbounded,
    };

    Some((start, end))
}

fn expire_range(
    start: Bound<Key>,
    end: Bound<Key>,
) -> Option<(Bound<ExpireKey>, Bound<ExpireKey>)> {
    let start = match start {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(Key::User(_)) | Bound::Excluded(Key::User(_)) => Bound::Unbounded,
        Bound::Included(Key::Expire(key)) => Bound::Included(key),
        Bound::Excluded(Key::Expire(key)) => Bound::Excluded(key),
    };

    let end = match end {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(Key::User(_)) | Bound::Excluded(Key::User(_)) => return None,
        Bound::Included(Key::Expire(key)) => Bound::Included(key),
        Bound::Excluded(Key::Expire(key)) => Bound::Excluded(key),
    };

    Some((start, end))
}

#[cfg(test)]
mod tests {
    use map_api::mvcc::GetAtSeq;
    use map_api::mvcc::ViewSet;
    use seq_marked::SeqMarked;
    use state_machine_api::UserKey;

    use crate::leveled_store::leveled_map::LeveledMap;

    fn user_key(s: impl ToString) -> UserKey {
        UserKey::new(s)
    }

    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().as_bytes().to_vec()
    }

    #[tokio::test]
    async fn test_get_from_writable_only() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.set(user_key("b"), Some((None, b("b0"))));
        view.commit().await.unwrap();

        // snapshot_seq covers entries at seq 1 and 2
        let got = lm.get_at_seq(user_key("a"), 10).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(1, (None, b("a0"))));

        let got = lm.get_at_seq(user_key("b"), 10).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(2, (None, b("b0"))));
    }

    #[tokio::test]
    async fn test_get_from_immutable_fallback() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.commit().await.unwrap();

        // Freeze the writable level to create immutable level
        lm.freeze_writable_without_permit();

        // Key "a" is now only in immutable level
        let got = lm.get_at_seq(user_key("a"), 10).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(1, (None, b("a0"))));
    }

    #[tokio::test]
    async fn test_get_writable_wins_over_immutable() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.commit().await.unwrap();

        // Freeze and create new entry for same key
        lm.freeze_writable_without_permit();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a1"))));
        view.commit().await.unwrap();

        // Writable level has seq=2, immutable has seq=1
        // With high snapshot_seq, writable wins
        let got = lm.get_at_seq(user_key("a"), 10).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(2, (None, b("a1"))));
    }

    #[tokio::test]
    async fn test_get_missing_key_returns_not_found() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.commit().await.unwrap();

        let got = lm.get_at_seq(user_key("nonexistent"), 10).await.unwrap();
        assert!(got.is_not_found());
    }

    #[tokio::test]
    async fn test_get_snapshot_seq_filters_entries() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        // Create entries at seq 1, 2, 3
        view.set(user_key("a"), Some((None, b("a0")))); // seq=1
        view.set(user_key("b"), Some((None, b("b0")))); // seq=2
        view.set(user_key("c"), Some((None, b("c0")))); // seq=3
        view.commit().await.unwrap();

        // snapshot_seq=0: nothing visible
        let got = lm.get_at_seq(user_key("a"), 0).await.unwrap();
        assert!(got.is_not_found());

        // snapshot_seq=1: only "a" visible
        let got = lm.get_at_seq(user_key("a"), 1).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(1, (None, b("a0"))));
        let got = lm.get_at_seq(user_key("b"), 1).await.unwrap();
        assert!(got.is_not_found());

        // snapshot_seq=2: "a" and "b" visible
        let got = lm.get_at_seq(user_key("b"), 2).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(2, (None, b("b0"))));

        // snapshot_seq=u64::MAX: all visible
        let got = lm.get_at_seq(user_key("c"), u64::MAX).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(3, (None, b("c0"))));
    }

    #[tokio::test]
    async fn test_get_snapshot_seq_with_multiple_levels() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0")))); // seq=1
        view.commit().await.unwrap();

        lm.freeze_writable_without_permit();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a1")))); // seq=2
        view.commit().await.unwrap();

        // snapshot_seq=1: sees old value from immutable
        let got = lm.get_at_seq(user_key("a"), 1).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(1, (None, b("a0"))));

        // snapshot_seq=2: sees new value from writable
        let got = lm.get_at_seq(user_key("a"), 2).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(2, (None, b("a1"))));
    }

    #[tokio::test]
    async fn test_get_tombstone_handling() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0")))); // seq=1
        view.set(user_key("a"), None); // tombstone, seq is still 1 (same batch)
        view.commit().await.unwrap();

        // Tombstone is visible
        let got = lm.get_at_seq(user_key("a"), 10).await.unwrap();
        assert!(got.is_tombstone());
    }
}
