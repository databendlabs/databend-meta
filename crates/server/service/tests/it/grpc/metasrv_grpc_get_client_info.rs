// Copyright 2022 Datafuse Labs.
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

use databend_meta_runtime_api::TokioRuntime;
use databend_meta_types::protobuf::Empty;
use databend_meta_types::protobuf::meta_service_client::MetaServiceClient;
use pretty_assertions::assert_eq;
use regex::Regex;
use test_harness::test;
use tonic::Code;

use crate::testing::meta_service_test_harness;
use crate::tests::service::grpc_client;
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_get_client_info() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Get client ip

    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;

    let client = grpc_client(&tc).await?;

    let resp = client.get_client_info().await?;

    let client_addr = resp.client_addr;

    let masked_addr = Regex::new(r"\d+")
        .unwrap()
        .replace_all(&client_addr, "1")
        .to_string();

    assert_eq!("1.1.1.1:1", masked_addr);

    assert!(resp.server_time > Some(1), "server time is returned");
    Ok(())
}

/// Both `Empty`-request info RPCs are covered here because there is no test
/// file for `get_cluster_status` of its own.
///
/// Neither returns key-value data, so what they leak is reconnaissance:
/// `get_cluster_status` gives the node ids, endpoints, versions and raft state
/// of every member, and `get_client_info` confirms the port is a meta service
/// and tells the caller which source address the server sees it as.
///
/// The client is the generated stub rather than `MetaGrpcClient`, since that
/// one handshakes before every call and could not express these requests.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_info_rpcs_refuse_a_client_that_did_not_handshake() -> anyhow::Result<()> {
    let (_tc, addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    let cluster_status = client.get_cluster_status(Empty {}).await.unwrap_err();
    let client_info = client.get_client_info(Empty {}).await.unwrap_err();

    for status in [cluster_status, client_info] {
        assert_eq!(status.code(), Code::Unauthenticated);
        // The message names the missing token, which is what tells this
        // refusal apart from one the handshake version gate would produce.
        assert_eq!(status.message(), "Error auth-token-bin is empty");
    }

    Ok(())
}
