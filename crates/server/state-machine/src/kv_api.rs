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

use std::future;
use std::io;
use std::sync::Arc;

use databend_meta_kvapi as kvapi;
use databend_meta_kvapi::KVStream;
use databend_meta_kvapi::ListOptions;
use databend_meta_kvapi::limit_stream;
use databend_meta_leveled_store::mvcc::read_view::StateMachineReadView;
use databend_meta_leveled_store::util::add_cooperative_yielding;
use databend_meta_types::SeqV;
use databend_meta_types::TxnReply;
use databend_meta_types::TxnRequest;
use databend_meta_types::protobuf::StreamItem;
use futures_util::TryStreamExt;
use futures_util::stream::BoxStream;
use map_api::mvcc::ViewRange;
use seq_marked::SeqValue;
use state_machine_api::UserKey;

use crate::state_machine::StateMachine;
use crate::utils::prefix_right_bound;
use crate::utils::seq_marked_to_seqv;
use crate::utils::since_epoch_millis;

/// A wrapper that implements KVApi **readonly** methods for the state machine.
pub struct StateMachineKVApi<'a> {
    pub(crate) sm: &'a StateMachine,
}

#[async_trait::async_trait]
impl kvapi::KVApi for StateMachineKVApi<'_> {
    type Error = io::Error;

    async fn list_kv(
        &self,
        opts: ListOptions<'_, str>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        let prefix = opts.prefix;
        let limit = opts.limit;
        let local_now_ms = since_epoch_millis();

        // get an unchanging readonly view
        let read_view = self.sm.data().to_read_view();

        let p = prefix.to_string();

        let strm = if let Some(right) = prefix_right_bound(&p) {
            read_view
                .range(UserKey::new(&p)..UserKey::new(right))
                .await?
        } else {
            read_view.range(UserKey::new(&p)..).await?
        };

        let strm = add_cooperative_yielding(strm, format!("StateMachineKVApi::list_kv: {prefix}"))
            // Skip tombstone
            .try_filter_map(|(k, marked)| future::ready(Ok(seq_marked_to_seqv(k, marked))))
            // Skip expired
            .try_filter(move |(_k, v)| future::ready(!v.is_expired(local_now_ms)))
            .map_ok(StreamItem::from);

        Ok(limit_stream(strm, limit))
    }

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, Self::Error>>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        let local_now_ms = since_epoch_millis();
        Ok(state_machine_read_view_get_many_kv(
            self.sm.to_read_view(),
            keys,
            local_now_ms,
        ))
    }

    async fn transaction(&self, _txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        unreachable!("write operation SM2KVApi::transaction is disabled")
    }
}

impl StateMachineKVApi<'_> {
    fn non_expired<V>(seq_value: Option<SeqV<V>>, now_ms: u64) -> Option<SeqV<V>> {
        if seq_value.is_expired(now_ms) {
            None
        } else {
            seq_value
        }
    }
}

/// A helper function that get many keys from a stream of keys.
///
/// The input stream may contain errors; errors are propagated to the output stream.
/// The stream terminates immediately after the first error (fail-fast).
fn state_machine_read_view_get_many_kv(
    read_view: StateMachineReadView,
    keys: BoxStream<'static, Result<String, io::Error>>,
    local_now_ms: u64,
) -> KVStream<io::Error> {
    use databend_meta_kvapi::fail_fast;
    use futures_util::StreamExt;
    use futures_util::TryStreamExt;

    let read_view = Arc::new(read_view);

    fail_fast(keys)
        .and_then(move |key| {
            let read_view = read_view.clone();
            async move {
                let got = read_view.get(UserKey::new(key.clone())).await?;
                let seqv: Option<SeqV> = got.into();
                let non_expired = StateMachineKVApi::non_expired(seqv, local_now_ms);
                Ok(StreamItem::from((key, non_expired)))
            }
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::KVApi;
    use databend_meta_types::KVMeta;
    use databend_meta_types::SeqV;
    use databend_meta_types::UpsertKV;
    use databend_meta_types::normalize_meta::NormalizeMeta;
    use futures_util::StreamExt;
    use futures_util::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn test_get_many_kv_preserves_input_order_and_missing_values() -> anyhow::Result<()> {
        let sm = StateMachine::default();
        let mut applier = sm.new_applier().await;
        applier
            .upsert_kv(&UpsertKV::update("first", b"one"))
            .await?;
        applier
            .upsert_kv(&UpsertKV::update("second", b"two"))
            .await?;
        applier.commit().await?;

        let keys = futures_util::stream::iter(vec![
            Ok("second".to_string()),
            Ok("missing".to_string()),
            Ok("first".to_string()),
        ])
        .boxed();
        let stream = sm.kv_api().get_many_kv(keys).await?;
        let got = stream
            .map_ok(|item| item.into_option_pair())
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|(key, value)| (key, value.without_proposed_at()))
            .collect::<Vec<_>>();

        assert_eq!(got, vec![
            ("second".to_string(), Some(SeqV::new(2, b"two".to_vec()))),
            ("missing".to_string(), None),
            ("first".to_string(), Some(SeqV::new(1, b"one".to_vec()))),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_many_kv_forwards_input_errors() -> anyhow::Result<()> {
        let sm = StateMachine::default();
        let keys =
            futures_util::stream::iter(vec![Err(std::io::Error::other("input failure"))]).boxed();
        let stream = sm.kv_api().get_many_kv(keys).await?;
        let error = stream.try_collect::<Vec<_>>().await.unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(error.to_string(), "input failure");
        Ok(())
    }

    #[test]
    fn test_non_expired_filters_only_expired_values() {
        let expired = SeqV::new_with_meta(1, Some(KVMeta::new_expires_at(1)), b"expired".to_vec());
        let live = SeqV::new_with_meta(2, Some(KVMeta::new_expires_at(3)), b"live".to_vec());

        assert_eq!(StateMachineKVApi::non_expired(Some(expired), 2_000), None);
        assert_eq!(
            StateMachineKVApi::non_expired(Some(live.clone()), 2_000),
            Some(live)
        );
        assert_eq!(StateMachineKVApi::non_expired::<Vec<u8>>(None, 2_000), None);
    }
}
