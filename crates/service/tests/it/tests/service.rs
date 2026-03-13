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
use std::time::Duration;

use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::errors::CreationError;
use databend_meta_runtime_api::RuntimeApi;
// Re-export from test-harness crate
pub use databend_meta_test_harness::MetaSrvTestContext;
pub use databend_meta_test_harness::start_metasrv;
pub use databend_meta_test_harness::start_metasrv_cluster;
pub use databend_meta_test_harness::start_metasrv_with_context;

pub fn make_grpc_client<R: RuntimeApi>(
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

pub async fn grpc_client<R: RuntimeApi>(
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
