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

//! The raft shared secret, across a running cluster.
//!
//! These tests pin down the two phases of the rollout: phase 1 mixes nodes that
//! send the secret with nodes that predate it, and phase 2 turns
//! `raft_secret_strict` on once every node sends it. What makes the order
//! mandatory is the third test: a strict node refuses, and a refusal is not one
//! of the errors the raft client absorbs.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_meta::message::ForwardRequest;
use databend_meta::message::ForwardRequestBody;
use databend_meta::meta_service::MetaNode;
use databend_meta::raft_client::connect_raft_service;
use databend_meta::raft_transport::RaftPeerTarget;
use databend_meta_kvapi::KvApiExt;
use databend_meta_raft_config::Secret;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_types::Cmd;
use databend_meta_types::Endpoint;
use databend_meta_types::LogEntry;
use databend_meta_types::UpsertKV;
use databend_meta_types::node::Node;
use databend_meta_types::protobuf::InstallEntryV004;
use databend_meta_types::raft_types::NodeId;
use futures::stream;
use log::info;
use openraft::ServerState;
use test_harness::test;
use tokio::time::sleep;
use tonic::Code;

use crate::testing::meta_service_test_harness;
use crate::tests::meta_node::timeout;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::start_metasrv_with_context;

const SECRET: &str = "cluster-shared-secret";

/// How long a write may take to reach a follower before the test calls it lost.
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(10);

/// A node as it was before the shared secret existed: it sends none and checks
/// none.
fn silent_node(id: NodeId) -> MetaSrvTestContext<TokioRuntime> {
    MetaSrvTestContext::new(id)
}

/// A node that sends [`SECRET`] and accepts it, `strict` deciding whether it
/// refuses the peers that send nothing.
fn secret_node(id: NodeId, strict: bool) -> MetaSrvTestContext<TokioRuntime> {
    let mut tc = MetaSrvTestContext::new(id);

    tc.config.raft_config.raft_secret = Some(Secret::new(SECRET));
    tc.config.raft_config.raft_accepted_secrets = vec![Secret::new(SECRET)];
    tc.config.raft_config.raft_secret_strict = Some(strict);

    tc
}

/// Boot `tc` as the leader of a single node cluster.
///
/// The shared helpers in [`crate::tests::meta_node`] probe the raft port with a
/// client that sends no secret, which is the very thing a strict node is there
/// to refuse, so the startup path is spelled out here instead.
async fn boot(
    tc: &mut MetaSrvTestContext<TokioRuntime>,
) -> anyhow::Result<Arc<MetaNode<TokioRuntime>>> {
    let mn = MetaNode::<TokioRuntime>::boot(&tc.config).await?;
    tc.meta_node = Some(mn.clone());

    mn.raft
        .wait(timeout())
        .state(ServerState::Leader, "leader started")
        .await?;

    Ok(mn)
}

/// Add `tc` to `leader`'s cluster as a non-voter.
async fn join(
    leader: &Arc<MetaNode<TokioRuntime>>,
    tc: &mut MetaSrvTestContext<TokioRuntime>,
) -> anyhow::Result<Arc<MetaNode<TokioRuntime>>> {
    let id = tc.config.raft_config.id;
    let addr = tc
        .config
        .raft_config
        .raft_api_addr::<TokioRuntime>()
        .await?;

    let mn = MetaNode::<TokioRuntime>::open(&tc.config.raft_config).await?;
    tc.meta_node = Some(mn.clone());

    leader
        .add_node(
            id,
            Node::new(id, addr).with_grpc_advertise_address(tc.config.grpc.advertise_address()),
        )
        .await?;

    mn.raft
        .wait(timeout())
        .current_leader(0, "follower has leader")
        .await?;

    Ok(mn)
}

/// Write `key` through `mn`, which forwards to the leader if `mn` is not it.
async fn write(mn: &Arc<MetaNode<TokioRuntime>>, key: &str) -> anyhow::Result<()> {
    let upsert = UpsertKV::update(key, key.as_bytes());
    mn.write(LogEntry::new(Cmd::UpsertKV(upsert))).await?;
    Ok(())
}

/// Read `key` out of `mn`'s own state machine, without going through raft.
///
/// A write returns once the leader has applied it, so a follower reaches it
/// only afterwards: this waits for the key to show up rather than assume it
/// already has, and gives up after [`REPLICATION_TIMEOUT`] so a failure is an
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

/// Assert that both writes are readable on every node.
async fn assert_both_writes_replicated(
    nodes: &[&Arc<MetaNode<TokioRuntime>>],
) -> anyhow::Result<()> {
    for mn in nodes {
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

/// Requests this process served without an accepted secret, for lack of one.
///
/// The registry is process wide, so this is read through a node but is not a
/// property of that node. It is usable as an assertion because the mixed
/// cluster below is the only test in this binary that passes anything through.
fn passed_without_secret(mn: &Arc<MetaNode<TokioRuntime>>) -> u64 {
    mn.get_metrics()
        .raft_network
        .unauthenticated_passed
        .get("reason=missing")
        .copied()
        .unwrap_or(0)
}

/// Phase 1 of the rollout: a node that sends the secret and a node that
/// predates it serve each other, in both directions.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_mixed_cluster_replicates_in_both_directions() -> anyhow::Result<()> {
    let mut tc0 = secret_node(0, false);
    let mut tc1 = silent_node(1);

    let sending = boot(&mut tc0).await?;
    let passed_before = passed_without_secret(&sending);

    let silent = join(&sending, &mut tc1).await?;

    info!("--- leader to follower: the secret rides in a header the old node ignores");
    write(&sending, "written-on-leader").await?;

    info!("--- follower to leader: the write is forwarded with no secret, and passed");
    write(&silent, "written-on-follower").await?;

    assert_both_writes_replicated(&[&sending, &silent]).await?;

    assert!(
        passed_without_secret(&sending) > passed_before,
        "the forwarded write reached the leader without a secret, so it must have been counted"
    );

    Ok(())
}

/// Phase 2 of the rollout: with every node sending the secret, turning strict
/// on changes nothing that a client can see.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_strict_cluster_replicates_in_both_directions() -> anyhow::Result<()> {
    let mut tc0 = secret_node(0, true);
    let mut tc1 = secret_node(1, true);

    let leader = boot(&mut tc0).await?;
    let follower = join(&leader, &mut tc1).await?;

    write(&leader, "written-on-leader").await?;
    write(&follower, "written-on-follower").await?;

    assert_both_writes_replicated(&[&leader, &follower]).await?;

    Ok(())
}

/// Joining and leaving open their own connection to the cluster instead of
/// going through the network openraft replicates over, so each has to carry the
/// secret of its own accord. A strict cluster refuses them otherwise, and a
/// cluster nobody can join or leave is not one anybody can operate.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_strict_cluster_can_be_joined_and_left() -> anyhow::Result<()> {
    let mut tc0 = secret_node(0, true);
    start_metasrv_with_context(&mut tc0).await?;

    let leader_addr = tc0
        .config
        .raft_config
        .raft_api_addr::<TokioRuntime>()
        .await?
        .to_string();

    info!("--- join");
    let mut tc1 = secret_node(1, true);
    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![leader_addr.clone()];
    start_metasrv_with_context(&mut tc1).await?;

    info!("--- leave");
    let mut leaving = tc1.config.raft_config.clone();
    leaving.leave_via = vec![leader_addr];
    leaving.leave_id = Some(1);

    assert!(MetaNode::<TokioRuntime>::leave_cluster(&leaving).await?);

    Ok(())
}

/// Why phase 1 cannot be skipped: a strict node refuses outright.
///
/// `Unauthenticated` is not one of the codes the raft client falls back on, so
/// a peer that still sends nothing is not degraded quietly into a slower path.
/// It is reported unreachable, and a cluster of nodes that judge each other
/// unreachable churns elections.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_strict_node_refuses_a_peer_that_sends_no_secret() -> anyhow::Result<()> {
    let mut tc = secret_node(0, true);
    let _mn = boot(&mut tc).await?;

    // A plain client with no interceptor is exactly a peer that predates the
    // secret.
    let mut client = tc.raft_client().await?;

    info!("--- a unary RPC is refused, and named as such");
    {
        let req = ForwardRequest {
            forward_to_leader: 0,
            body: ForwardRequestBody::Ping,
        };

        let status = client.forward(req).await.unwrap_err();

        assert_eq!(status.code(), Code::Unauthenticated);
        assert!(
            status.message().starts_with("raft secret is missing"),
            "{}",
            status.message()
        );
    }

    info!("--- a streaming RPC is refused too: the check is on the service, not one method");
    {
        let chunks = stream::iter(Vec::<InstallEntryV004>::new());

        let status = client.install_snapshot_v004(chunks).await.unwrap_err();

        assert_eq!(status.code(), Code::Unauthenticated);
    }

    Ok(())
}

/// The counterpart of the refusal above, from outside this crate.
///
/// The `databend-meta` binary lives in another repository, and re-registers its
/// own address on every startup by forwarding a write of its own -- not through
/// `MetaNode`, and so not over the network openraft replicates on. A strict
/// leader refuses that registration unless the binary can build a client that
/// sends the secret, which is what [`connect_raft_service`] is public for. This
/// test stands in for that caller: it is in `tests/`, so it reaches the API the
/// same way the binary does.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_strict_node_accepts_a_registration_from_a_secret_client() -> anyhow::Result<()> {
    let mut tc = secret_node(0, true);
    let mn = boot(&mut tc).await?;

    let leader_addr = tc
        .config
        .raft_config
        .raft_api_addr::<TokioRuntime>()
        .await?;

    let leader = RaftPeerTarget::plaintext(&leader_addr);
    let mut client = connect_raft_service(&leader, &tc.config.raft_config).await?;

    let registered = Node::new(1, Endpoint::new("registering-node", 28104))
        .with_grpc_advertise_address(Some("registering-node:9191".to_string()));

    let req = ForwardRequest {
        forward_to_leader: 1,
        body: ForwardRequestBody::Write(LogEntry::new(Cmd::AddNode {
            node_id: 1,
            node: registered.clone(),
            overriding: true,
        })),
    };

    client.forward(req).await?;

    assert_eq!(mn.get_node(&1).await, Some(registered));

    Ok(())
}
