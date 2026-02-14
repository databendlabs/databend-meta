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
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::KVStream;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_kvapi::kvapi::fail_fast;
use databend_meta_kvapi::kvapi::limit_stream;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_types::Change;
use databend_meta_types::MetaError;
use databend_meta_types::MetaNetworkError;
use databend_meta_types::TxnReply;
use databend_meta_types::TxnRequest;
use databend_meta_types::UpsertKV;
use futures_util::TryStreamExt;
use futures_util::stream::BoxStream;

use crate::ClientHandle;
use crate::grpc_action::StreamedGetMany;

fn status_to_meta_error(status: tonic::Status) -> MetaError {
    MetaNetworkError::from(status).into()
}

#[async_trait]
impl<RT: SpawnApi> kvapi::KVApi for ClientHandle<RT> {
    type Error = MetaError;

    async fn upsert_kv(&self, req: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error> {
        self.upsert_via_txn(req).await.map_err(MetaError::from)
    }

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, Self::Error>>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        self.streamed_get_many(StreamedGetMany { keys })
            .await
            .map_err(MetaError::from)
    }

    async fn list_kv(
        &self,
        opts: ListOptions<'_, str>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        let strm = self.list(opts.prefix).await?;

        let mapped = strm.map_err(status_to_meta_error);
        let limited = limit_stream(fail_fast(mapped), opts.limit);
        Ok(limited)
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        ClientHandle::transaction(self, txn).await
    }
}
