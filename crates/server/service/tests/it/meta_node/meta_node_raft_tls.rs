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

//! The second raft listener, the one that serves the same raft service over
//! TLS.
//!
//! A node configured with a certificate serves raft on two ports at once, and
//! these tests pin down what the second one changes and what it does not: the
//! transport is encrypted, the plaintext port keeps answering, and the shared
//! secret is checked on both alike.

use std::net::TcpListener;

use databend_meta::message::ForwardRequest;
use databend_meta::message::ForwardRequestBody;
use databend_meta::message::ForwardResponse;
use databend_meta::meta_service::MetaNode;
use databend_meta::util::reply_to_api_result;
use databend_meta_raft_config::Secret;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_meta_types::raft_types::NodeId;
use log::info;
use openraft::ServerState;
use test_harness::test;
use tonic::Code;
use tonic::Status;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;

use crate::testing::meta_service_test_harness;
use crate::tests::meta_node::timeout;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::start_metasrv_with_context;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

const SECRET: &str = "cluster-shared-secret";

/// A node that serves raft on its plaintext port and on its TLS port.
///
/// The TLS port itself is assigned by [`MetaSrvTestContext::new`]; what turns
/// the second listener on is the certificate and the key set here.
fn tls_node(id: NodeId) -> MetaSrvTestContext<TokioRuntime> {
    let mut tc = MetaSrvTestContext::new(id);

    tc.config.raft_config.raft_tls_server_cert = Some(TEST_SERVER_CERT.to_string());
    tc.config.raft_config.raft_tls_server_key = Some(TEST_SERVER_KEY.to_string());

    tc
}

fn ping() -> ForwardRequest<ForwardRequestBody> {
    ForwardRequest::new(0, ForwardRequestBody::Ping)
}

/// The address `tc` publishes for its TLS listener.
fn tls_addr(tc: &MetaSrvTestContext<TokioRuntime>) -> String {
    tc.config
        .raft_config
        .raft_tls_advertise_host_string()
        .expect("a node built by tls_node() has a TLS listener")
}

/// Connect to a raft service over TLS, verifying its certificate against the
/// test CA.
async fn tls_raft_client(addr: &str) -> anyhow::Result<RaftServiceClient<Channel>> {
    let ca = tokio::fs::read(TEST_CA_CERT).await?;

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .domain_name(TEST_CN_NAME);

    let channel = Channel::from_shared(format!("https://{}", addr))?
        .tls_config(tls)?
        .connect()
        .await?;

    Ok(RaftServiceClient::new(channel))
}

/// Send one raft RPC in plaintext, connection included.
///
/// Connecting and calling are one step here because a listener that speaks TLS
/// may turn a plaintext caller away at either of them, and which one it is is
/// not what these tests are about.
async fn plaintext_ping(addr: &str) -> anyhow::Result<ForwardResponse> {
    let mut client = RaftServiceClient::connect(format!("http://{}", addr)).await?;

    let reply = client.forward(ping()).await?;
    let response = reply_to_api_result(reply.into_inner())?;

    Ok(response)
}

/// A node with a certificate answers raft on its TLS port, and the plaintext
/// port it already had keeps answering beside it.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_configured_node_serves_raft_on_both_ports() -> anyhow::Result<()> {
    let mut tc = tls_node(0);
    start_metasrv_with_context(&mut tc).await?;

    info!("--- the TLS port completes a handshake and answers a raft RPC");
    {
        let mut client = tls_raft_client(&tls_addr(&tc)).await?;

        let reply = client.forward(ping()).await?;
        let response: ForwardResponse = reply_to_api_result(reply.into_inner())?;

        assert_eq!(response, ForwardResponse::Pong);
    }

    info!("--- the plaintext port answers the same RPC, as it did before");
    {
        let addr = tc
            .config
            .raft_config
            .raft_api_addr::<TokioRuntime>()
            .await?;
        let response = plaintext_ping(&addr.to_string()).await?;

        assert_eq!(response, ForwardResponse::Pong);
    }

    info!("--- and the TLS port is genuinely TLS: a plaintext caller gets nothing out of it");
    {
        let result = plaintext_ping(&tls_addr(&tc)).await;
        assert!(result.is_err(), "the TLS port answered a plaintext caller");
    }

    Ok(())
}

/// Leaving the certificate out leaves the node on one port, whatever
/// `raft_tls_port` says.
///
/// This is the state every node is in before the rollout starts, and the
/// reserved port has to stay closed in it: a listener nobody asked for is one
/// nobody has issued a certificate for.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_node_without_a_certificate_opens_no_tls_port() -> anyhow::Result<()> {
    let mut tc = MetaSrvTestContext::<TokioRuntime>::new(0);
    start_metasrv_with_context(&mut tc).await?;

    assert_eq!(tc.config.raft_config.raft_tls_advertise_host_string(), None);

    let port = tc.config.raft_config.raft_tls_port.unwrap();
    let addr = format!("{}:{}", tc.config.raft_config.raft_advertise_host, port);

    let result = plaintext_ping(&addr).await;
    assert!(result.is_err(), "the reserved TLS port is open");

    Ok(())
}

/// Both ports refuse a caller that sends no secret, and refuse it the same way.
///
/// The check belongs to the service rather than to a listener, so encrypting
/// one of the two ports must not make it the lenient one. The refusals are
/// compared to each other rather than to a literal: what matters is that a
/// caller cannot pick a port and be treated better on it.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_both_ports_refuse_a_caller_with_no_secret_alike() -> anyhow::Result<()> {
    let mut tc = tls_node(0);
    tc.config.raft_config.raft_secret = Some(Secret::new(SECRET));
    tc.config.raft_config.raft_accepted_secrets = vec![Secret::new(SECRET)];
    tc.config.raft_config.raft_secret_strict = Some(true);

    // `start_metasrv_with_context()` probes the raft port with a client that
    // sends no secret, which is what this node refuses, so it is booted here
    // instead.
    let mn = MetaNode::<TokioRuntime>::boot(&tc.config).await?;
    tc.meta_node = Some(mn.clone());

    mn.raft
        .wait(timeout())
        .state(ServerState::Leader, "leader started")
        .await?;

    let plaintext_addr = tc
        .config
        .raft_config
        .raft_api_addr::<TokioRuntime>()
        .await?;

    let mut plaintext_client =
        RaftServiceClient::connect(format!("http://{}", plaintext_addr)).await?;
    let mut tls_client = tls_raft_client(&tls_addr(&tc)).await?;

    let plaintext_status = plaintext_client.forward(ping()).await.unwrap_err();
    let tls_status = tls_client.forward(ping()).await.unwrap_err();

    let plaintext_reason = refusal_reason(&plaintext_status);
    let tls_reason = refusal_reason(&tls_status);

    assert_eq!(plaintext_status.code(), Code::Unauthenticated);
    assert_eq!(plaintext_reason, "raft secret is missing");

    assert_eq!(tls_status.code(), plaintext_status.code());
    assert_eq!(tls_reason, plaintext_reason);

    Ok(())
}

/// A node that cannot start every listener it was configured for starts none
/// of them, whichever of the two fallible steps is the one that fails.
///
/// What makes this worth asserting is that a listener, once spawned, cannot be
/// called back: its service owns an `Arc<MetaNode>`, so the node outlives the
/// failed startup with nobody holding a handle to stop it. The freed plaintext
/// port is the observable proof that no such task was left running.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_node_that_cannot_start_its_tls_listener_starts_no_listener() -> anyhow::Result<()> {
    info!("--- a TLS port already taken by someone else");
    {
        let mut tc = tls_node(0);

        let tls_port = tc.config.raft_config.raft_tls_port.unwrap();
        let occupier = TcpListener::bind(("127.0.0.1", tls_port))?;

        assert_startup_leaves_the_plaintext_port_free(&mut tc).await?;

        drop(occupier);
    }

    info!("--- a certificate that cannot be read");
    {
        let mut tc = tls_node(1);
        tc.config.raft_config.raft_tls_server_cert = Some("/no/such/certificate.pem".to_string());

        assert_startup_leaves_the_plaintext_port_free(&mut tc).await?;
    }

    Ok(())
}

/// Start `tc`, require it to fail, and require its plaintext raft port to be
/// bindable afterwards.
async fn assert_startup_leaves_the_plaintext_port_free(
    tc: &mut MetaSrvTestContext<TokioRuntime>,
) -> anyhow::Result<()> {
    let started = MetaNode::<TokioRuntime>::boot(&tc.config).await;

    let Err(e) = started else {
        // Hand the unexpectedly started node to the test context, so that the
        // ports it holds are released when the context is dropped.
        tc.meta_node = started.ok();
        panic!("startup succeeded while a configured listener could not start");
    };
    info!("--- startup failed as it must: {}", e);

    let raft_port = tc.config.raft_config.raft_api_port;
    let rebound = TcpListener::bind(("127.0.0.1", raft_port));

    assert!(
        rebound.is_ok(),
        "the plaintext raft port is still served by a listener nothing can stop: {:?}",
        rebound.err()
    );

    Ok(())
}

/// The reason a refusal gives, without the address it names the caller by.
///
/// Two clients reach a node from two source ports, so the addresses differ
/// while nothing about the decision does.
fn refusal_reason(status: &Status) -> &str {
    let message = status.message();

    let split = message.split_once(": from:");
    let (reason, _caller) = split.unwrap_or((message, ""));

    reason
}
