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

use databend_meta_types::snapshot_db::DB;
use futures_util::StreamExt;
use map_api::IOResultStream;
use map_api::mvcc;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use rotbl::v001::SeqMarked;

use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::persisted_codec::PersistedCodec;
use crate::leveled_store::rotbl_codec::RotblCodec;

/// A wrapper that implements the `ScopedSnapshot*` trait for the `DB`.
#[derive(Debug, Clone)]
pub struct ScopedSeqBoundedRead<'a>(pub &'a DB);

#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedGet<K, K::V> for ScopedSeqBoundedRead<'_>
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: PersistedCodec<SeqMarked>,
{
    async fn get(&self, key: K, _snapshot_seq: u64) -> Result<SeqMarked<K::V>, io::Error> {
        // TODO: DB does not consider snapshot_seq
        let key = RotblCodec::encode_key(&key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let res = self.0.rotbl.get(&key).await?;

        let Some(seq_marked) = res else {
            return Ok(SeqMarked::new_not_found());
        };

        let marked = SeqMarked::<K::V>::decode_from(seq_marked)?;
        Ok(marked)
    }
}

#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedRange<K, K::V> for ScopedSeqBoundedRead<'_>
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: PersistedCodec<SeqMarked>,
{
    async fn range<R>(
        &self,
        range: R,
        _snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        // TODO: DB does not consider snapshot_seq

        let rng = RotblCodec::encode_range(&range)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let strm = self.0.rotbl.range(rng);

        let strm = strm.map(|res_item: Result<(String, SeqMarked), io::Error>| {
            let (str_k, seq_marked) = res_item?;
            let key = RotblCodec::decode_key(&str_k)?;
            let marked = SeqMarked::decode_from(seq_marked)?;
            Ok((key, marked))
        });

        Ok(strm.boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_meta_types::snapshot_db::DB;
    use futures_util::TryStreamExt;
    use map_api::mvcc::ScopedSeqBoundedGet;
    use map_api::mvcc::ScopedSeqBoundedRange;
    use rotbl::storage::impls::fs::FsStorage;
    use rotbl::v001::Config;
    use rotbl::v001::Rotbl;
    use rotbl::v001::RotblMeta;
    use rotbl::v001::SeqMarked;
    use state_machine_api::MetaValue;
    use state_machine_api::UserKey;

    use super::*;
    use crate::leveled_store::rotbl_codec::RotblCodec;

    fn user_key(s: impl ToString) -> UserKey {
        UserKey::new(s)
    }

    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().as_bytes().to_vec()
    }

    /// Helper to create a DB with test data
    fn create_db_with_data(
        tmp_dir: &tempfile::TempDir,
        test_entries: Vec<(UserKey, SeqMarked<MetaValue>)>,
    ) -> Result<DB, Box<dyn std::error::Error>> {
        let storage = FsStorage::new(tmp_dir.path().to_path_buf());
        let config = Config::default();
        let path = "test_rotbl";

        // Convert test entries to rotbl format using RotblCodec
        let entries: Result<Vec<_>, _> = test_entries
            .iter()
            .map(|(key, seq_marked)| RotblCodec::encode_key_seq_marked(key, seq_marked.clone()))
            .collect();
        let entries = entries?;

        // Find the maximum sequence number from entries
        let max_seq = test_entries
            .iter()
            .map(|(_, seq_marked)| *seq_marked.internal_seq())
            .max()
            .unwrap_or(0);

        let rotbl = Rotbl::create_table(
            storage,
            config,
            path,
            RotblMeta::new(1, "test_data"),
            entries,
        )?;

        // Create sys_data with the correct sequence number
        let mut sys_data = databend_meta_types::sys_data::SysData::default();
        if max_seq > 0 {
            sys_data.update_seq(max_seq);
        }

        let db = DB {
            storage_path: tmp_dir.path().to_string_lossy().to_string(),
            rel_path: path.to_string(),
            meta: Default::default(),
            sys_data,
            rotbl: Arc::new(rotbl),
        };
        Ok(db)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_get_single_key() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries = vec![
            (
                user_key("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (
                user_key("key2"),
                SeqMarked::new_normal(20, (None, b("value2"))),
            ),
        ];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        let got: SeqMarked<MetaValue> = reader.get(user_key("key1"), u64::MAX).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(10, (None, b("value1"))));

        let got: SeqMarked<MetaValue> = reader.get(user_key("key2"), u64::MAX).await.unwrap();
        assert_eq!(got, SeqMarked::new_normal(20, (None, b("value2"))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_get_missing_key() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries = vec![(
            user_key("key1"),
            SeqMarked::new_normal(10, (None, b("value1"))),
        )];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        let got: SeqMarked<MetaValue> =
            reader.get(user_key("nonexistent"), u64::MAX).await.unwrap();
        assert!(got.is_not_found());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_get_empty_db() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries: Vec<(UserKey, SeqMarked<MetaValue>)> = vec![];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        let got: SeqMarked<MetaValue> = reader.get(user_key("any"), u64::MAX).await.unwrap();
        assert!(got.is_not_found());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_get_tombstone() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries = vec![
            (
                user_key("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (user_key("key2"), SeqMarked::new_tombstone(20)),
        ];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        let got: SeqMarked<MetaValue> = reader.get(user_key("key2"), u64::MAX).await.unwrap();
        assert!(got.is_tombstone());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_range_query() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries = vec![
            (
                user_key("a"),
                SeqMarked::new_normal(1, (None, b("value_a"))),
            ),
            (
                user_key("b"),
                SeqMarked::new_normal(2, (None, b("value_b"))),
            ),
            (
                user_key("c"),
                SeqMarked::new_normal(3, (None, b("value_c"))),
            ),
        ];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        let strm = reader.range(user_key("").., u64::MAX).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();

        assert_eq!(got, vec![
            (
                user_key("a"),
                SeqMarked::new_normal(1, (None, b("value_a")))
            ),
            (
                user_key("b"),
                SeqMarked::new_normal(2, (None, b("value_b")))
            ),
            (
                user_key("c"),
                SeqMarked::new_normal(3, (None, b("value_c")))
            ),
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_range_with_boundaries() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries = vec![
            (
                user_key("a"),
                SeqMarked::new_normal(1, (None, b("value_a"))),
            ),
            (
                user_key("b"),
                SeqMarked::new_normal(2, (None, b("value_b"))),
            ),
            (
                user_key("c"),
                SeqMarked::new_normal(3, (None, b("value_c"))),
            ),
            (
                user_key("d"),
                SeqMarked::new_normal(4, (None, b("value_d"))),
            ),
        ];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        // Range from "b" onwards
        let strm = reader.range(user_key("b").., u64::MAX).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![
            (
                user_key("b"),
                SeqMarked::new_normal(2, (None, b("value_b")))
            ),
            (
                user_key("c"),
                SeqMarked::new_normal(3, (None, b("value_c")))
            ),
            (
                user_key("d"),
                SeqMarked::new_normal(4, (None, b("value_d")))
            ),
        ]);

        // Range "b" to "d" (exclusive)
        let strm = reader
            .range(user_key("b")..user_key("d"), u64::MAX)
            .await
            .unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();
        assert_eq!(got, vec![
            (
                user_key("b"),
                SeqMarked::new_normal(2, (None, b("value_b")))
            ),
            (
                user_key("c"),
                SeqMarked::new_normal(3, (None, b("value_c")))
            ),
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_range_empty_db() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries: Vec<(UserKey, SeqMarked<MetaValue>)> = vec![];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        let strm = reader.range(user_key("").., u64::MAX).await.unwrap();
        let got: Vec<(UserKey, SeqMarked<MetaValue>)> = strm.try_collect().await.unwrap();
        assert!(got.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_range_no_match() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries = vec![(
            user_key("a"),
            SeqMarked::new_normal(1, (None, b("value_a"))),
        )];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        // Range starts after all data
        let strm = reader.range(user_key("z").., u64::MAX).await.unwrap();
        let got: Vec<(UserKey, SeqMarked<MetaValue>)> = strm.try_collect().await.unwrap();
        assert!(got.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_range_with_tombstones() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries = vec![
            (
                user_key("a"),
                SeqMarked::new_normal(1, (None, b("value_a"))),
            ),
            (user_key("b"), SeqMarked::new_tombstone(2)),
            (
                user_key("c"),
                SeqMarked::new_normal(3, (None, b("value_c"))),
            ),
        ];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        let strm = reader.range(user_key("").., u64::MAX).await.unwrap();
        let got: Vec<_> = strm.try_collect().await.unwrap();

        assert_eq!(got, vec![
            (
                user_key("a"),
                SeqMarked::new_normal(1, (None, b("value_a")))
            ),
            (user_key("b"), SeqMarked::new_tombstone(2)),
            (
                user_key("c"),
                SeqMarked::new_normal(3, (None, b("value_c")))
            ),
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_key_encoding_special_chars() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let entries = vec![
            (
                user_key("key/with/slashes"),
                SeqMarked::new_normal(1, (None, b("value1"))),
            ),
            (
                user_key("key:with:colons"),
                SeqMarked::new_normal(2, (None, b("value2"))),
            ),
        ];
        let db = create_db_with_data(&tmp_dir, entries).unwrap();
        let reader = ScopedSeqBoundedRead(&db);

        let got: SeqMarked<MetaValue> = reader
            .get(user_key("key/with/slashes"), u64::MAX)
            .await
            .unwrap();
        assert_eq!(got, SeqMarked::new_normal(1, (None, b("value1"))));

        let got: SeqMarked<MetaValue> = reader
            .get(user_key("key:with:colons"), u64::MAX)
            .await
            .unwrap();
        assert_eq!(got, SeqMarked::new_normal(2, (None, b("value2"))));
    }
}
