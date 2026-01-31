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

use map_api::MapKey;
use map_api::mvcc;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use seq_marked::SeqMarked;

use crate::leveled_store::get_sub_table::GetSubTable;
use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::persisted_codec::PersistedCodec;

#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedGet<K, K::V> for LeveledMap
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: PersistedCodec<SeqMarked>,
    Level: GetSubTable<K, K::V>,
    Immutable: mvcc::ScopedSeqBoundedGet<K, K::V>,
{
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<K::V>, io::Error> {
        let immutable = {
            let inner = self.data.lock().unwrap();
            let got = inner
                .writable
                .get_sub_table()
                .get(key.clone(), snapshot_seq)
                .cloned();
            if !got.is_not_found() {
                return Ok(got);
            }

            inner.immutable.clone()
        };

        immutable.get(key, snapshot_seq).await
    }
}

#[cfg(test)]
mod tests {
    use map_api::mvcc::ScopedSeqBoundedGet;
    use map_api::mvcc::ScopedSet;
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
        let got = lm.get(user_key("a"), 10).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(1, (None, b("a0"))));

        let got = lm.get(user_key("b"), 10).await.unwrap();
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
        let got = lm.get(user_key("a"), 10).await.unwrap();
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
        let got = lm.get(user_key("a"), 10).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(2, (None, b("a1"))));
    }

    #[tokio::test]
    async fn test_get_missing_key_returns_not_found() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.commit().await.unwrap();

        let got = lm.get(user_key("nonexistent"), 10).await.unwrap();
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
        let got = lm.get(user_key("a"), 0).await.unwrap();
        assert!(got.is_not_found());

        // snapshot_seq=1: only "a" visible
        let got = lm.get(user_key("a"), 1).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(1, (None, b("a0"))));
        let got = lm.get(user_key("b"), 1).await.unwrap();
        assert!(got.is_not_found());

        // snapshot_seq=2: "a" and "b" visible
        let got = lm.get(user_key("b"), 2).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(2, (None, b("b0"))));

        // snapshot_seq=u64::MAX: all visible
        let got = lm.get(user_key("c"), u64::MAX).await.unwrap();
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
        let got = lm.get(user_key("a"), 1).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(1, (None, b("a0"))));

        // snapshot_seq=2: sees new value from writable
        let got = lm.get(user_key("a"), 2).await.unwrap();
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
        let got = lm.get(user_key("a"), 10).await.unwrap();
        assert!(got.is_tombstone());
    }
}
