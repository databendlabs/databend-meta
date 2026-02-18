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

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use databend_meta_client::ClientHandle;
use databend_meta_kvapi as kvapi;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_test_harness::MetaSrvTestContext;
use databend_meta_test_harness::make_grpc_client;
use databend_meta_test_harness::start_metasrv;
use databend_meta_test_harness::start_metasrv_cluster;

/// Builds `Arc<ClientHandle<TokioRuntime>>` backed by real metasrv instances.
///
/// Keeps `MetaSrvTestContext`s alive in a shared vec so the servers
/// are not dropped while clients are in use.
#[derive(Clone)]
pub struct MetaSrvBuilder {
    contexts: Arc<Mutex<Vec<MetaSrvTestContext<TokioRuntime>>>>,
}

impl MetaSrvBuilder {
    pub fn new() -> Self {
        Self {
            contexts: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl kvapi::ApiBuilder<Arc<ClientHandle<TokioRuntime>>> for MetaSrvBuilder {
    async fn build(&self) -> Arc<ClientHandle<TokioRuntime>> {
        let (tc, _addr) = start_metasrv::<TokioRuntime>().await.unwrap();
        let client = tc.grpc_client().await.unwrap();
        self.contexts.lock().unwrap().push(tc);
        client
    }

    async fn build_cluster(&self) -> Vec<Arc<ClientHandle<TokioRuntime>>> {
        let tcs = start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2])
            .await
            .unwrap();

        let all_endpoints: Vec<String> = tcs
            .iter()
            .map(|tc| tc.config.grpc.api_address().unwrap())
            .collect();

        // Each client gets all endpoints but starts with its own
        // server as the current endpoint. This ensures some clients
        // initially connect to a follower, testing that RPCs can
        // find the leader.
        let mut clients = Vec::with_capacity(tcs.len());
        for tc in &tcs {
            let current = tc.config.grpc.api_address().unwrap();
            let client = make_grpc_client(all_endpoints.clone()).unwrap();
            client.set_current_endpoint(current);
            clients.push(client);
        }

        self.contexts.lock().unwrap().extend(tcs);
        clients
    }
}
