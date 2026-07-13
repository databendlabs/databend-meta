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
use std::time::Duration;

use async_trait::async_trait;
use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::errors::CreationError;
use databend_meta_kvapi as kvapi;
use databend_meta_runtime_api::RuntimeApi;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_test_harness::MetaSrvTestContext;
use databend_meta_test_harness::start_metasrv;
use databend_meta_test_harness::start_metasrv_cluster;

fn make_grpc_client<R: RuntimeApi>(
    addresses: Vec<String>,
) -> Result<Arc<ClientHandle<R>>, CreationError> {
    let client = MetaGrpcClient::<R>::try_create(
        addresses,
        "root",
        "xxx",
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(10)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;

    Ok(client)
}

async fn grpc_client<R: RuntimeApi>(
    tc: &MetaSrvTestContext<R>,
) -> anyhow::Result<Arc<ClientHandle<R>>> {
    let addr = tc
        .config
        .grpc
        .api_address()
        .ok_or_else(|| anyhow::anyhow!("gRPC port not assigned yet"))?;

    let client = MetaGrpcClient::<R>::try_create(
        vec![addr],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;
    Ok(client)
}

/// Builds `Arc<ClientHandle<TokioRuntime>>` backed by real metasrv instances.
///
/// Keeps the contexts for the current client(s) alive.
/// The test suite uses clients sequentially, so a new build replaces them.
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
        self.contexts.lock().unwrap().clear();

        let (tc, _addr) = start_metasrv::<TokioRuntime>().await.unwrap();
        let client = grpc_client(&tc).await.unwrap();
        self.contexts.lock().unwrap().push(tc);
        client
    }

    async fn build_cluster(&self) -> Vec<Arc<ClientHandle<TokioRuntime>>> {
        self.contexts.lock().unwrap().clear();

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

#[cfg(test)]
mod tests {
    use databend_meta_test_harness::meta_service_test_harness;
    use kvapi::ApiBuilder;
    use test_harness::test;

    use super::*;

    #[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
    async fn test_replaces_unused_context() -> anyhow::Result<()> {
        let builder = MetaSrvBuilder::new();

        let client = builder.build().await;
        assert_eq!(builder.contexts.lock().unwrap().len(), 1);
        drop(client);

        let client = builder.build().await;
        assert_eq!(builder.contexts.lock().unwrap().len(), 1);
        drop(client);

        Ok(())
    }
}
