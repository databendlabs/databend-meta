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

use async_trait::async_trait;
use databend_meta_types::Change;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use databend_meta_types::UpsertKV;
use databend_meta_types::errors;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::debug;

use crate::KVApi;
use crate::KVStream;
use crate::ListOptions;

/// Extend the `KVApi` trait with auto implemented handy methods.
#[async_trait]
pub trait KvApiExt: KVApi {
    /// Update or insert a key-value record.
    async fn upsert_kv(&self, req: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error> {
        let txn = TxnRequest::from_upsert(req);
        let reply = self.transaction(txn).await?;
        let change = reply.into_upsert_reply()?;
        Ok(change)
    }

    /// Get key-values by keys.
    ///
    /// 2024-01-06: since: 1.2.287
    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error> {
        let keys: Vec<Result<String, Self::Error>> = keys.iter().map(|k| Ok(k.clone())).collect();
        let strm = futures_util::stream::iter(keys);
        self.get_many_kv(strm.boxed()).await
    }

    /// Get single key-value record by key.
    async fn get_kv(&self, key: &str) -> Result<Option<SeqV>, Self::Error> {
        let mut strm = self.get_kv_stream(&[key.to_string()]).await?;

        let strm_item = strm
            .next()
            .await
            .ok_or_else(|| errors::IncompleteStream::new(1, 0).context(" while get_kv"))??;

        let reply = strm_item.value.map(SeqV::from);

        // Drain the stream to wait for gRPC trailers before dropping.
        //
        // In gRPC, a response consists of: Headers -> Data* -> Trailers.
        // If we drop the stream after reading data but before trailers arrive,
        // h2 sends RST_STREAM CANCEL. Under high load, this can trigger
        // "too_many_internal_resets" (ENHANCE_YOUR_CALM) errors in h2.
        while strm.next().await.transpose()?.is_some() {}

        Ok(reply)
    }

    /// Get several key-values by keys.
    async fn mget_kv(&self, keys: &[String]) -> Result<Vec<Option<SeqV>>, Self::Error> {
        let n = keys.len();
        let strm = self.get_kv_stream(keys).await?;

        let seq_values: Vec<Option<SeqV>> = strm
            .map_ok(|item| item.value.map(SeqV::from))
            .try_collect()
            .await?;

        if seq_values.len() != n {
            return Err(
                errors::IncompleteStream::new(n as u64, seq_values.len() as u64)
                    .context(" while mget_kv")
                    .into(),
            );
        }

        debug!("mget: keys: {:?}; values: {:?}", keys, seq_values);

        Ok(seq_values)
    }

    /// List key-value starting with the specified prefix and return a [`Vec`]
    ///
    /// Same as [`KVApi::list_kv`] but return a [`Vec`] instead of a stream.
    async fn list_kv_collect(
        &self,
        opts: ListOptions<'_, str>,
    ) -> Result<Vec<(String, SeqV)>, Self::Error> {
        let now = std::time::Instant::now();

        let strm = self.list_kv(opts).await?;

        debug!("list_kv() took {:?}", now.elapsed());

        let key_seqv_list = strm
            .map_ok(|stream_item| {
                // Safe unwrap(): list_kv() does not return None value
                (stream_item.key, SeqV::from(stream_item.value.unwrap()))
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(key_seqv_list)
    }
}

impl<T: KVApi + ?Sized> KvApiExt for T {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io;

    use async_trait::async_trait;
    use databend_meta_types::SeqV;
    use databend_meta_types::TxnReply;
    use databend_meta_types::TxnRequest;
    use databend_meta_types::protobuf;
    use databend_meta_types::protobuf::StreamItem;
    use futures_util::StreamExt;
    use futures_util::TryStreamExt;
    use futures_util::stream::BoxStream;

    use crate as kvapi;
    use crate::KVStream;
    use crate::KvApiExt;
    use crate::ListOptions;
    use crate::fail_fast;
    use crate::limit_stream;

    /// In-memory mock of KVApi backed by a BTreeMap of protobuf SeqV.
    struct MockKVApi {
        data: BTreeMap<String, protobuf::SeqV>,
    }

    impl MockKVApi {
        fn new(entries: &[(&str, u64, &[u8])]) -> Self {
            let data = entries
                .iter()
                .map(|&(k, seq, v)| (k.to_string(), protobuf::SeqV::new(seq, v.to_vec())))
                .collect();
            Self { data }
        }
    }

    #[async_trait]
    impl kvapi::KVApi for MockKVApi {
        type Error = io::Error;

        async fn get_many_kv(
            &self,
            keys: BoxStream<'static, Result<String, Self::Error>>,
        ) -> Result<KVStream<Self::Error>, Self::Error> {
            let data = self.data.clone();
            let strm = fail_fast(keys).and_then(move |key| {
                let val = data.get(&key).cloned();
                let item = StreamItem::new(key, val);
                async move { Ok(item) }
            });
            Ok(strm.boxed())
        }

        async fn list_kv(
            &self,
            opts: ListOptions<'_, str>,
        ) -> Result<KVStream<Self::Error>, Self::Error> {
            let prefix = opts.prefix.to_string();
            let strm = futures_util::stream::iter(
                self.data
                    .range(prefix.clone()..)
                    .take_while(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, v)| Ok(StreamItem::new(k.clone(), Some(v.clone()))))
                    .collect::<Vec<_>>(),
            );
            Ok(limit_stream(strm, opts.limit))
        }

        async fn transaction(&self, _txn: TxnRequest) -> Result<TxnReply, Self::Error> {
            unimplemented!()
        }
    }

    fn seqv(seq: u64, data: &[u8]) -> SeqV {
        SeqV::new(seq, data.to_vec())
    }

    #[tokio::test]
    async fn test_get_kv_found() {
        let api = MockKVApi::new(&[("k1", 1, b"v1")]);
        assert_eq!(api.get_kv("k1").await.unwrap(), Some(seqv(1, b"v1")));
    }

    #[tokio::test]
    async fn test_get_kv_not_found() {
        let api = MockKVApi::new(&[("k1", 1, b"v1")]);
        assert_eq!(api.get_kv("no_such").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_mget_kv() {
        let api = MockKVApi::new(&[("a", 1, b"va"), ("b", 2, b"vb"), ("c", 3, b"vc")]);
        let keys: Vec<String> = vec!["a".into(), "c".into(), "missing".into()];
        assert_eq!(api.mget_kv(&keys).await.unwrap(), vec![
            Some(seqv(1, b"va")),
            Some(seqv(3, b"vc")),
            None,
        ]);
    }

    #[tokio::test]
    async fn test_mget_kv_empty() {
        let api = MockKVApi::new(&[]);
        assert_eq!(api.mget_kv(&[]).await.unwrap(), Vec::<Option<SeqV>>::new());
    }

    #[tokio::test]
    async fn test_get_kv_stream() {
        let api = MockKVApi::new(&[("x", 5, b"vx"), ("y", 6, b"vy")]);
        let keys = vec!["x".into(), "y".into(), "z".into()];
        let strm = api.get_kv_stream(&keys).await.unwrap();
        let items: Vec<StreamItem> = strm.try_collect().await.unwrap();

        assert_eq!(items, vec![
            StreamItem::new("x".into(), Some(protobuf::SeqV::new(5, b"vx".to_vec()))),
            StreamItem::new("y".into(), Some(protobuf::SeqV::new(6, b"vy".to_vec()))),
            StreamItem::new("z".into(), None),
        ]);
    }

    #[tokio::test]
    async fn test_list_kv_collect_unlimited() {
        let api = MockKVApi::new(&[
            ("pfx/a", 1, b"a"),
            ("pfx/b", 2, b"b"),
            ("pfx/c", 3, b"c"),
            ("other", 4, b"o"),
        ]);

        assert_eq!(
            api.list_kv_collect(ListOptions::unlimited("pfx/"))
                .await
                .unwrap(),
            vec![
                ("pfx/a".to_string(), seqv(1, b"a")),
                ("pfx/b".to_string(), seqv(2, b"b")),
                ("pfx/c".to_string(), seqv(3, b"c")),
            ]
        );
    }

    #[tokio::test]
    async fn test_list_kv_collect_with_limit() {
        let api = MockKVApi::new(&[("pfx/a", 1, b"a"), ("pfx/b", 2, b"b"), ("pfx/c", 3, b"c")]);

        assert_eq!(
            api.list_kv_collect(ListOptions::limited("pfx/", 2))
                .await
                .unwrap(),
            vec![
                ("pfx/a".to_string(), seqv(1, b"a")),
                ("pfx/b".to_string(), seqv(2, b"b")),
            ]
        );
    }

    #[tokio::test]
    async fn test_list_kv_collect_no_match() {
        let api = MockKVApi::new(&[("other", 1, b"v")]);

        assert_eq!(
            api.list_kv_collect(ListOptions::unlimited("pfx/"))
                .await
                .unwrap(),
            vec![]
        );
    }
}
