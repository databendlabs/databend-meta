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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_meta::message::ForwardRequest;
use databend_meta::message::ForwardRequestBody;
use databend_meta::message::ForwardResponse;
use databend_meta::meta_service::MetaNode;
use databend_meta::raft_client::connect_raft_service;
use databend_meta::raft_transport::RaftPeerTarget;
use databend_meta::util::reply_to_api_result;
use databend_meta_kvapi::KvApiExt;
use databend_meta_raft_config::Secret;
use databend_meta_raft_config::config::RaftConfig;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_types::Cmd;
use databend_meta_types::Endpoint;
use databend_meta_types::LogEntry;
use databend_meta_types::UpsertKV;
use databend_meta_types::node::Node;
use databend_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_meta_types::raft_types::NodeId;
use log::info;
use openraft::ServerState;
use test_harness::test;
use tokio::time::sleep;
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

/// How long a write may take to reach a follower before the test calls it lost.
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Which address an outbound connection dials, over which transport, and under
/// which name it is reported.
///
/// Every case below passes a real address for the one it must choose and
/// [`dead_addr()`] for the one it must ignore, so an RPC that works names the
/// choice on its own rather than leaving both possible. Each case then asserts
/// the resolved target itself, because that one value is what a log line, an
/// error context and the `active_peers` metric all read: a connection reported
/// under an address other than the one it dialed points monitoring at a peer
/// this node is not talking to.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_which_address_an_outbound_connection_dials() -> anyhow::Result<()> {
    let mut tc = tls_node(0);
    start_metasrv_with_context(&mut tc).await?;

    let no_ca = tc.config.raft_config.clone();
    let with_ca = with_root_ca(&no_ca);

    let plaintext_addr = tc
        .config
        .raft_config
        .raft_api_addr::<TokioRuntime>()
        .await?;

    info!("--- a CA here and a TLS address published there: dials the published address");
    {
        let peer = peer_target(dead_addr(), Some(tls_addr(&tc)), &with_ca).await?;

        assert_eq!(peer.address(), tls_addr(&tc));
        assert_eq!(peer.to_string(), format!("https://{}", tls_addr(&tc)));

        assert_eq!(ping_over(&peer, &with_ca).await?, ForwardResponse::Pong);
    }

    info!("--- no CA here: nothing can verify the peer, so the published address goes unused");
    {
        let dead = dead_addr().to_string();
        let peer = peer_target(plaintext_addr.clone(), Some(dead), &no_ca).await?;

        assert_eq!(peer.address(), plaintext_addr.to_string());
        assert_eq!(peer.to_string(), format!("http://{}", plaintext_addr));

        assert_eq!(ping_over(&peer, &no_ca).await?, ForwardResponse::Pong);
    }

    info!("--- a CA here but nothing published there: plaintext, as for any pre-TLS peer");
    {
        let peer = peer_target(plaintext_addr.clone(), None, &with_ca).await?;

        assert_eq!(peer.address(), plaintext_addr.to_string());
        assert_eq!(peer.to_string(), format!("http://{}", plaintext_addr));

        assert_eq!(ping_over(&peer, &with_ca).await?, ForwardResponse::Pong);
    }

    Ok(())
}

/// Where a peer that publishes `endpoint` as its plaintext address and
/// `tls_address` as its TLS one is reached from a node configured by `config`.
///
/// Which of the two addresses that is, and over which transport, is decided by
/// [`RaftPeerTarget::of_node`] out of the peer's record and the local
/// configuration, which is exactly what these cases are about.
async fn peer_target(
    endpoint: Endpoint,
    tls_address: Option<String>,
    config: &RaftConfig,
) -> anyhow::Result<RaftPeerTarget> {
    let node = Node::new(0, endpoint).with_raft_tls_advertise_address(tls_address);
    let peer = RaftPeerTarget::of_node(&node, config).await?;

    Ok(peer)
}

/// Send one raft RPC over a connection to `peer`, the way this node's own raft
/// traffic reaches it.
async fn ping_over(peer: &RaftPeerTarget, config: &RaftConfig) -> anyhow::Result<ForwardResponse> {
    let mut client = connect_raft_service(peer, config).await?;

    let reply = client.forward(ping()).await?;
    let response = reply_to_api_result(reply.into_inner())?;

    Ok(response)
}

/// `raft_tls_client_domain_name` decides the name a peer's certificate is
/// checked against.
///
/// The two halves are what make this an assertion about the name: the same
/// peer, at the same address, is refused under a name its certificate does not
/// carry and accepted when no name is configured at all.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_the_configured_domain_name_is_what_a_peer_is_verified_against() -> anyhow::Result<()>
{
    let mut tc = tls_node(0);
    start_metasrv_with_context(&mut tc).await?;

    let unnamed = with_root_ca(&tc.config.raft_config);

    let mut wrongly_named = unnamed.clone();
    wrongly_named.raft_tls_client_domain_name =
        Some("a-name-the-certificate-does-not-carry".to_string());

    let peer = peer_target(dead_addr(), Some(tls_addr(&tc)), &wrongly_named).await?;
    let refused = ping_over(&peer, &wrongly_named).await;
    assert!(
        refused.is_err(),
        "a certificate issued for other names was accepted"
    );

    info!("--- unset, the peer is verified against the address it was dialed on");
    let peer = peer_target(dead_addr(), Some(tls_addr(&tc)), &unnamed).await?;

    assert_eq!(ping_over(&peer, &unnamed).await?, ForwardResponse::Pong);

    Ok(())
}

/// A two-node cluster in which every raft connection is TLS: it elects a
/// leader, replicates a write made on the leader, and replicates one made on
/// the follower, which reaches the leader through the forwarder.
///
/// Both node records publish a plaintext address nothing listens on, so neither
/// node can reach the other except over TLS. That is what makes this a test of
/// the transport rather than of the cluster.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_tls_cluster_replicates_in_both_directions() -> anyhow::Result<()> {
    let mut tc0 = tls_node(0);
    let mut tc1 = tls_node(1);

    tc0.config.raft_config = with_root_ca(&tc0.config.raft_config);
    tc1.config.raft_config = with_root_ca(&tc1.config.raft_config);

    let leader = MetaNode::<TokioRuntime>::boot(&tc0.config).await?;
    tc0.meta_node = Some(leader.clone());

    leader
        .raft
        .wait(timeout())
        .state(ServerState::Leader, "leader started")
        .await?;

    info!("--- the leader withdraws its own plaintext address, keeping only the TLS one");
    let write_res = leader.write(LogEntry::new(Cmd::AddNode {
        node_id: 0,
        node: reachable_only_over_tls(&tc0),
        overriding: true,
    }));
    write_res.await?;

    info!("--- the follower joins, publishing a TLS address and no reachable plaintext one");
    let follower = MetaNode::<TokioRuntime>::open(&tc1.config.raft_config).await?;
    tc1.meta_node = Some(follower.clone());

    leader.add_node(1, reachable_only_over_tls(&tc1)).await?;

    follower
        .raft
        .wait(timeout())
        .current_leader(0, "follower has leader")
        .await?;

    info!("--- leader to follower: replication reaches a node with no reachable plaintext port");
    write_kv(&leader, "written-on-leader").await?;

    info!("--- follower to leader: the forwarder reaches the leader the same way");
    write_kv(&follower, "written-on-follower").await?;

    for mn in [&leader, &follower] {
        for key in ["written-on-leader", "written-on-follower"] {
            assert_eq!(
                read_replicated(mn, key).await?,
                Some(key.as_bytes().to_vec()),
                "node-{} is missing {}",
                mn.raft_store.id,
                key
            );
        }
    }

    Ok(())
}

/// A node that joins an existing cluster publishes its TLS address, exactly as
/// a node that boots a cluster of its own does.
///
/// Joining is the only way a production node ever enters a cluster, and it is
/// the one path that carries the node record over the wire and rebuilds it on
/// the leader. A record that loses its TLS address there leaves every peer
/// dialing the joined node in plaintext, with nothing to say the TLS listener
/// it is running was ever meant to be used.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_joining_node_publishes_its_tls_address() -> anyhow::Result<()> {
    let mut tc0 = tls_node(0);
    let mut tc1 = tls_node(1);

    let leader_addr = tc0
        .config
        .raft_config
        .raft_api_addr::<TokioRuntime>()
        .await?;

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![leader_addr.to_string()];

    let leader = MetaNode::<TokioRuntime>::boot(&tc0.config).await?;
    tc0.meta_node = Some(leader.clone());

    leader
        .raft
        .wait(timeout())
        .state(ServerState::Leader, "leader started")
        .await?;

    info!("--- the follower joins through the join path, not through add_node()");
    let follower = MetaNode::<TokioRuntime>::start(&tc1.config).await?;
    tc1.meta_node = Some(follower.clone());

    let joined = follower.join_cluster(&tc1.config).await?;
    assert_eq!(Ok(()), joined);

    info!("--- the leader stored the record the follower advertises, TLS address included");
    let stored = leader.raft_store.get_node(&1).await;
    assert_eq!(Some(tc1.config.get_node()), stored);

    let published_tls_address = stored.and_then(|node| node.raft_tls_advertise_address);
    assert_eq!(Some(tls_addr(&tc1)), published_tls_address);

    Ok(())
}

/// An address nothing listens on.
///
/// Handing it to a connection as the half it must not use turns a working RPC
/// into evidence of which half it did use.
fn dead_addr() -> Endpoint {
    Endpoint::new("127.0.0.1", 1)
}

/// `config` plus the CA that issued the test certificate, which is what makes a
/// node willing to dial its peers over TLS.
fn with_root_ca(config: &RaftConfig) -> RaftConfig {
    RaftConfig {
        raft_tls_client_root_ca_cert: Some(TEST_CA_CERT.to_string()),
        ..config.clone()
    }
}

/// The record `tc` publishes, with its plaintext address replaced by one
/// nothing answers on.
///
/// A peer that reaches this node at all therefore reached it over TLS.
fn reachable_only_over_tls(tc: &MetaSrvTestContext<TokioRuntime>) -> Node {
    let mut node = tc.config.get_node();
    node.endpoint = dead_addr();
    node
}

async fn write_kv(mn: &Arc<MetaNode<TokioRuntime>>, key: &str) -> anyhow::Result<()> {
    let upsert = UpsertKV::update(key, key.as_bytes());
    mn.write(LogEntry::new(Cmd::UpsertKV(upsert))).await?;
    Ok(())
}

/// Read `key` out of `mn`'s own state machine, waiting for replication to
/// deliver it.
///
/// A write returns once the leader has applied it, so a follower reaches it
/// only afterwards. Giving up after [`REPLICATION_TIMEOUT`] makes a failure an
/// assertion about the value rather than a hang.
async fn read_replicated(
    mn: &Arc<MetaNode<TokioRuntime>>,
    key: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    let deadline = Instant::now() + REPLICATION_TIMEOUT;

    loop {
        let got = mn
            .raft_store
            .get_state_machine()
            .kv_api()
            .get_kv(key)
            .await?;

        if got.is_some() || Instant::now() >= deadline {
            return Ok(got.map(|seq_v| seq_v.data));
        }

        sleep(Duration::from_millis(50)).await;
    }
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
