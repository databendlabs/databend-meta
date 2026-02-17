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
use std::ops::RangeBounds;
use std::time::Instant;

use futures_util::StreamExt;
use log::debug;
use log::warn;
use map_api::IOResultStream;
use map_api::MapKey;
use map_api::mvcc;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use map_api::util;
use seq_marked::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::get_sub_table::GetSubTable;
use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::persisted_codec::PersistedCodec;

#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedRange<K, K::V> for LeveledMap
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: PersistedCodec<SeqMarked>,
    Level: GetSubTable<K, K::V>,
    Immutable: mvcc::ScopedSeqBoundedRange<K, K::V>,
{
    async fn range<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let mut kmerge = KMerge::by(util::by_key_seq);

        // writable level

        let (vec, immutable) = {
            let start = Instant::now();
            debug!(
                "Level.writable::range(start={:?}, end={:?})",
                range.start_bound(),
                range.end_bound()
            );

            let inner = self.data.lock().unwrap();

            debug!(
                "Level.writable::range(start={:?}, end={:?}) acquired lock, took {:?}, writable: kv.len={}, expire.len={}",
                range.start_bound(),
                range.end_bound(),
                start.elapsed(),
                inner.writable.kv.inner.len(),
                inner.writable.expire.inner.len()
            );

            let it = inner
                .writable
                .get_sub_table()
                .range(range.clone(), snapshot_seq);
            let vec = it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>();

            (vec, inner.immutable.clone())
        };

        if vec.len() > 1000 {
            warn!(
                "Level.writable::range(start={:?}, end={:?}) returns big range of len={}",
                range.start_bound(),
                range.end_bound(),
                vec.len()
            );
        }

        let strm = futures::stream::iter(vec).map(Ok).boxed();
        kmerge = kmerge.merge(strm);

        let strm = immutable.range(range, snapshot_seq).await?;
        kmerge = kmerge.merge(strm);

        // Merge entries with the same key, keep the one with larger internal-seq
        let coalesce = kmerge.coalesce(util::merge_kv_results);

        Ok(coalesce.boxed())
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;
    use map_api::mvcc::ScopedSeqBoundedRange;
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
    async fn test_range_from_writable_only() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.set(user_key("b"), Some((None, b("b0"))));
        view.set(user_key("c"), Some((None, b("c0"))));
        view.commit().await.unwrap();

        let strm = lm.range(user_key("").., 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();

        assert_eq!(got, vec![
            (user_key("a"), SeqMarked::new_normal(1, (None, b("a0")))),
            (user_key("b"), SeqMarked::new_normal(2, (None, b("b0")))),
            (user_key("c"), SeqMarked::new_normal(3, (None, b("c0")))),
        ]);
    }

    #[tokio::test]
    async fn test_range_from_immutable_only() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.set(user_key("b"), Some((None, b("b0"))));
        view.commit().await.unwrap();

        // Freeze - entries now in immutable level
        lm.freeze_writable_without_permit();

        let strm = lm.range(user_key("").., 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();

        assert_eq!(got, vec![
            (user_key("a"), SeqMarked::new_normal(1, (None, b("a0")))),
            (user_key("b"), SeqMarked::new_normal(2, (None, b("b0")))),
        ]);
    }

    #[tokio::test]
    async fn test_range_merged_from_both_levels() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.set(user_key("c"), Some((None, b("c0"))));
        view.commit().await.unwrap();

        lm.freeze_writable_without_permit();
        let mut view = lm.to_view();

        view.set(user_key("b"), Some((None, b("b1"))));
        view.set(user_key("d"), Some((None, b("d1"))));
        view.commit().await.unwrap();

        // Should merge and sort by key
        let strm = lm.range(user_key("").., 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();

        assert_eq!(got, vec![
            (user_key("a"), SeqMarked::new_normal(1, (None, b("a0")))),
            (user_key("b"), SeqMarked::new_normal(3, (None, b("b1")))),
            (user_key("c"), SeqMarked::new_normal(2, (None, b("c0")))),
            (user_key("d"), SeqMarked::new_normal(4, (None, b("d1")))),
        ]);
    }

    #[tokio::test]
    async fn test_range_key_deduplication_writable_wins() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.set(user_key("b"), Some((None, b("b0"))));
        view.commit().await.unwrap();

        lm.freeze_writable_without_permit();
        let mut view = lm.to_view();

        // Override same keys in writable level
        view.set(user_key("a"), Some((None, b("a1"))));
        view.set(user_key("b"), Some((None, b("b1"))));
        view.commit().await.unwrap();

        let strm = lm.range(user_key("").., 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();

        // Writable values (higher seq) win
        assert_eq!(got, vec![
            (user_key("a"), SeqMarked::new_normal(3, (None, b("a1")))),
            (user_key("b"), SeqMarked::new_normal(4, (None, b("b1")))),
        ]);
    }

    #[tokio::test]
    async fn test_range_tombstone_handling() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.set(user_key("b"), Some((None, b("b0"))));
        view.set(user_key("c"), Some((None, b("c0"))));
        view.commit().await.unwrap();

        lm.freeze_writable_without_permit();
        let mut view = lm.to_view();

        // Delete "b" with tombstone
        view.set(user_key("b"), None);
        view.commit().await.unwrap();

        let strm = lm.range(user_key("").., 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();

        // Tombstone is included in range
        assert_eq!(got, vec![
            (user_key("a"), SeqMarked::new_normal(1, (None, b("a0")))),
            (user_key("b"), SeqMarked::new_tombstone(3)),
            (user_key("c"), SeqMarked::new_normal(3, (None, b("c0")))),
        ]);
    }

    #[tokio::test]
    async fn test_range_boundaries() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0"))));
        view.set(user_key("b"), Some((None, b("b0"))));
        view.set(user_key("c"), Some((None, b("c0"))));
        view.set(user_key("d"), Some((None, b("d0"))));
        view.commit().await.unwrap();

        // Range from "b" onwards
        let strm = lm.range(user_key("b").., 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![
            (user_key("b"), SeqMarked::new_normal(2, (None, b("b0")))),
            (user_key("c"), SeqMarked::new_normal(3, (None, b("c0")))),
            (user_key("d"), SeqMarked::new_normal(4, (None, b("d0")))),
        ]);

        // Range "b" to "d" (exclusive)
        let strm = lm.range(user_key("b")..user_key("d"), 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![
            (user_key("b"), SeqMarked::new_normal(2, (None, b("b0")))),
            (user_key("c"), SeqMarked::new_normal(3, (None, b("c0")))),
        ]);

        // Range "b" to "d" (inclusive)
        let strm = lm.range(user_key("b")..=user_key("d"), 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![
            (user_key("b"), SeqMarked::new_normal(2, (None, b("b0")))),
            (user_key("c"), SeqMarked::new_normal(3, (None, b("c0")))),
            (user_key("d"), SeqMarked::new_normal(4, (None, b("d0")))),
        ]);
    }

    #[tokio::test]
    async fn test_range_snapshot_seq_filters_entries() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0")))); // seq=1
        view.set(user_key("b"), Some((None, b("b0")))); // seq=2
        view.set(user_key("c"), Some((None, b("c0")))); // seq=3
        view.commit().await.unwrap();

        // snapshot_seq=0: nothing visible
        let strm = lm.range(user_key("").., 0).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert!(got.is_empty());

        // snapshot_seq=1: only "a" visible
        let strm = lm.range(user_key("").., 1).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![(
            user_key("a"),
            SeqMarked::new_normal(1, (None, b("a0")))
        ),]);

        // snapshot_seq=2: "a" and "b" visible
        let strm = lm.range(user_key("").., 2).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![
            (user_key("a"), SeqMarked::new_normal(1, (None, b("a0")))),
            (user_key("b"), SeqMarked::new_normal(2, (None, b("b0")))),
        ]);

        // snapshot_seq=u64::MAX: all visible
        let strm = lm.range(user_key("").., u64::MAX).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![
            (user_key("a"), SeqMarked::new_normal(1, (None, b("a0")))),
            (user_key("b"), SeqMarked::new_normal(2, (None, b("b0")))),
            (user_key("c"), SeqMarked::new_normal(3, (None, b("c0")))),
        ]);
    }

    #[tokio::test]
    async fn test_range_snapshot_seq_with_multiple_levels() {
        let lm = LeveledMap::default();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a0")))); // seq=1
        view.commit().await.unwrap();

        lm.freeze_writable_without_permit();
        let mut view = lm.to_view();

        view.set(user_key("a"), Some((None, b("a1")))); // seq=2
        view.commit().await.unwrap();

        // snapshot_seq=1: sees old value from immutable
        let strm = lm.range(user_key("").., 1).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![(
            user_key("a"),
            SeqMarked::new_normal(1, (None, b("a0")))
        ),]);

        // snapshot_seq=2: sees new value from writable (dedup by coalesce)
        let strm = lm.range(user_key("").., 2).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![(
            user_key("a"),
            SeqMarked::new_normal(2, (None, b("a1")))
        ),]);
    }

    #[tokio::test]
    async fn test_range_empty_result() {
        let lm = LeveledMap::default();

        // Empty store
        let strm = lm.range(user_key("").., 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert!(got.is_empty());

        // With data but range doesn't match
        let mut view = lm.to_view();
        view.set(user_key("a"), Some((None, b("a0"))));
        view.commit().await.unwrap();

        let strm = lm.range(user_key("z").., 10).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert!(got.is_empty());
    }
}
