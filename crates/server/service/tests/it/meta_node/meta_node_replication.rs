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

#![allow(clippy::let_and_return)]

use std::fs;
use std::io::Read;

use databend_meta::message::ForwardRequest;
use databend_meta::meta_service::MetaNode;
use databend_meta_client::GetKVReq;
use databend_meta_client::MetaGrpcReadReq;
use databend_meta_raft_store::sm_v003::SnapshotStoreV004;
use databend_meta_raft_store::state_machine::MetaSnapshotId;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_types::Cmd;
use databend_meta_types::LogEntry;
use databend_meta_types::SeqV;
use databend_meta_types::UpsertKV;
use databend_meta_types::protobuf as pb;
use databend_meta_types::protobuf::SnapshotChunkRequestV003;
use databend_meta_types::raft_types::SnapshotMeta;
use databend_meta_types::raft_types::SnapshotResponse;
use databend_meta_types::raft_types::StoredMembership;
use databend_meta_types::raft_types::Vote;
use databend_meta_types::sys_data::SysData;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use itertools::Itertools;
use log::info;
use maplit::btreeset;
use openraft::LogIdOptionExt;
use openraft::ServerState;
use openraft::async_runtime::WatchReceiver;
use openraft::testing::log_id;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::meta_node::start_meta_node_cluster;
use crate::tests::meta_node::start_meta_node_non_voter;
use crate::tests::meta_node::timeout;
use crate::tests::service::MetaSrvTestContext;

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_node_snapshot_replication() -> anyhow::Result<()> {
    // - Bring up a cluster of 3.
    // - Write just enough logs to trigger a snapshot.
    // - Add a non-voter, test the snapshot is sync-ed
    // - Write logs to trigger another snapshot.
    // - Add

    // Create a snapshot every 10 logs
    let snap_logs = 10;

    let mut tc = MetaSrvTestContext::<TokioRuntime>::new(0);
    tc.config.raft_config.snapshot_logs_since_last = snap_logs;
    tc.config.raft_config.install_snapshot_timeout = 10_1000; // milli seconds. In a CI multi-threads test delays async task badly.
    tc.config.raft_config.max_applied_log_to_keep = 0;

    let mn = MetaNode::<TokioRuntime>::boot(&tc.config).await?;

    tc.assert_raft_server_connection().await?;

    mn.raft
        .wait(timeout())
        .state(ServerState::Leader, "leader started")
        .await?;

    mn.raft
        .wait(timeout())
        .current_leader(0, "node-0 has leader")
        .await?;

    // initial membership, leader blank log, add node.
    let mut log_index = 3;

    mn.raft
        .wait(timeout())
        .applied_index(Some(log_index), "leader init logs")
        .await?;

    let n_req = 12;

    for i in 0..n_req {
        let key = format!("test_meta_node_snapshot_replication-key-{}", i);
        mn.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(&key, b"v"))))
            .await?;
    }
    log_index += n_req;

    info!("--- check the log is locally applied");

    mn.raft
        .wait(timeout())
        .applied_index(Some(log_index), "applied on leader")
        .await?;

    info!("--- check the snapshot is created");

    mn.raft
        .wait(timeout())
        .metrics(
            |x| {
                x.snapshot.map(|x| x.leader_id.term) == Some(1)
                    && x.snapshot.next_index() >= snap_logs
            },
            "snapshot is created by leader",
        )
        .await?;

    info!("--- start a non_voter to receive snapshot replication");

    let (_, tc1) = start_meta_node_non_voter(mn.clone(), 1).await?;
    // add node, change membership
    log_index += 2;

    let mn1 = tc1.meta_node();

    mn1.raft
        .wait(timeout())
        .applied_index(Some(log_index), "non-voter replicated all logs")
        .await?;

    mn1.raft
        .wait(timeout())
        .metrics(
            |x| {
                x.snapshot.map(|x| x.leader_id.term) == Some(1)
                    && x.snapshot.next_index() >= snap_logs
            },
            "snapshot is received by non-voter",
        )
        .await?;

    for i in 0..n_req {
        let key = format!("test_meta_node_snapshot_replication-key-{}", i);
        let sm = mn1.raft_store.get_sm_v003();
        let got = sm.get_maybe_expired_kv(&key).await?;
        match got {
            None => {
                panic!("expect get some value for {}", key)
            }
            Some(SeqV { ref data, .. }) => {
                assert_eq!(data, b"v");
            }
        }
    }

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_raft_service_install_snapshot_v003() -> anyhow::Result<()> {
    // Transmit snapshot in one-piece in a stream via API install_snapshot_v003.

    let (_nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![]).await?;
    let tc0 = tcs.remove(0);

    let mut client0 = tc0.raft_client().await?;

    let last_log_id = log_id(10, 2, 4);

    let snapshot_id = MetaSnapshotId::new(Some(last_log_id), 1);

    let snapshot_meta = SnapshotMeta {
        last_log_id: Some(last_log_id),
        last_membership: StoredMembership::default(),
        snapshot_id: snapshot_id.to_string(),
    };

    // build a temp snapshot data
    let ss_store: SnapshotStoreV004<TokioRuntime> =
        SnapshotStoreV004::new(tc0.config.raft_config.clone());
    let writer = ss_store.new_writer()?;

    let snapshot_data = {
        // build an empty snapshot
        let strm = futures::stream::iter([]);
        let mut sys_data = SysData::default();
        *sys_data.last_applied_mut() = Some(last_log_id);
        let db = writer.write_kv_stream(strm, snapshot_id, sys_data).await?;

        // read the snapshot data
        let mut f = fs::File::open(db.path())?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        buf
    };

    let mut strm_data = snapshot_data
        .into_iter()
        .chunks(32)
        .into_iter()
        .map(|chunk| SnapshotChunkRequestV003::new_chunk(chunk.collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    strm_data.push(SnapshotChunkRequestV003::new_end_chunk(
        Vote::new_committed(10, 2),
        snapshot_meta,
    ));

    // Complete transmit

    let resp = client0
        .install_snapshot_v003(stream::iter(strm_data))
        .await?;
    let reply = resp.into_inner();

    let vote = reply.to_vote()?;
    let resp = SnapshotResponse { vote };

    assert_eq!(resp.vote, Vote::new_committed(10, 2));

    let meta_node = tc0.meta_node.as_ref().unwrap();
    let m = meta_node.raft.metrics().borrow_watched().clone();

    assert_eq!(Some(last_log_id), m.snapshot);

    // Incomplete

    let strm_data = [
        //
        SnapshotChunkRequestV003::new_chunk("\n".as_bytes().to_vec()),
    ];

    let err = client0
        .install_snapshot_v003(stream::iter(strm_data))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_raft_service_install_snapshot_v004() -> anyhow::Result<()> {
    // Transmit snapshot in one-piece in a stream via API install_snapshot_v004.

    let (_nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![]).await?;
    let tc0 = tcs.remove(0);

    let mut client0 = tc0.raft_client().await?;

    let last_log_id = log_id(10, 2, 4);

    let snapshot_id = MetaSnapshotId::new(Some(last_log_id), 1);

    // build a temp snapshot data
    let ss_store: SnapshotStoreV004<TokioRuntime> =
        SnapshotStoreV004::new(tc0.config.raft_config.clone());
    let writer = ss_store.new_writer()?;

    let db = {
        let strm = futures::stream::iter([]);
        let mut sys_data = SysData::default();
        *sys_data.last_applied_mut() = Some(last_log_id);
        writer
            .write_kv_stream(strm, snapshot_id.clone(), sys_data)
            .await?
    };

    let mut sys_data = db.sys_data().clone();
    // The stream contains an entry with SeqV.seq=1, so curr_seq must be >= 1
    // to satisfy the invariant that all DB entries have internal_seq <= curr_seq.
    sys_data.update_seq(1);
    let sys_data_str = serde_json::to_string(&sys_data)?;

    let strm_data = vec![
        //
        pb::InstallEntryV004 {
            version: 1,
            key_values: vec![
                //
                pb::StreamItem::new(
                    "kv--/a".to_string(),
                    Some(SeqV::new(1, b"foo".to_vec()).into()),
                ),
            ],
            commit: None,
        },
        pb::InstallEntryV004 {
            version: 1,
            key_values: vec![],
            commit: Some(pb::Commit {
                snapshot_id: snapshot_id.to_string(),
                sys_data: sys_data_str,
                vote: Some(pb::Vote::from(Vote::new_committed(10, 2))),
            }),
        },
    ];

    // Complete transmit

    let resp = client0
        .install_snapshot_v004(stream::iter(strm_data))
        .await?;
    let reply = resp.into_inner();

    let vote = reply.to_vote()?;

    assert_eq!(vote, Vote::new_committed(10, 2));

    let meta_node = tc0.meta_node.as_ref().unwrap();
    let m = meta_node.raft.metrics().borrow_watched().clone();

    assert_eq!(Some(last_log_id), m.snapshot);

    let (_endpoint, strm) = meta_node
        .handle_forwardable_request(ForwardRequest::<MetaGrpcReadReq> {
            forward_to_leader: 0,
            body: MetaGrpcReadReq::GetKV(GetKVReq {
                key: "a".to_string(),
            }),
        })
        .await?;

    let got = strm.try_collect::<Vec<_>>().await?;
    assert_eq!(got, vec![pb::StreamItem::new(
        "a".to_string(),
        Some(SeqV::new(1, b"foo".to_vec()).into())
    )]);

    // Incomplete

    let strm_data = [
        //
        pb::InstallEntryV004 {
            version: 1,
            key_values: vec![
                //
                pb::StreamItem::new(
                    "kv--/a".to_string(),
                    Some(SeqV::new(1, b"foo".to_vec()).into()),
                ),
            ],
            commit: None,
        },
    ];

    let err = client0
        .install_snapshot_v004(stream::iter(strm_data))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_raft_service_append_v001() -> anyhow::Result<()> {
    // Test the AppendV001 bidirectional streaming gRPC endpoint.
    //
    // Start a single-node leader (node 0, term 1, committed vote) with 3
    // initial log entries. To test `stream_append` we must act as a leader
    // sending to a follower: use a higher vote (term=2) to make the node
    // step down, then it processes appends as a follower.
    //
    // Test cases:
    // 1. Heartbeat (no entries) — expect success with last_log_id = prev_log_id
    // 2. Append with entries  — expect success with last_log_id = last appended
    // 3. Stale vote request   — expect rejected_by with the node's current vote

    let (_nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![]).await?;
    let tc0 = tcs.remove(0);
    let mut client0 = tc0.raft_client().await?;

    // Use a higher vote from a different node to make the leader step down.
    // node_id=1 ensures node 0 recognizes it's following another leader.
    let higher_vote = pb::Vote {
        term: 2,
        node_id: 1,
        committed: true,
    };

    // After startup, the leader has log entries at indexes 1..=3 (term=1, node=0).
    let prev_log_id_3 = pb::LogId {
        term: 1,
        node_id: 0,
        index: 3,
    };

    info!("--- heartbeat: no entries, should succeed");
    {
        let heartbeat = pb::AppendRequest {
            vote: Some(higher_vote),
            prev_log_id: Some(prev_log_id_3),
            entries: vec![],
            leader_commit: Some(prev_log_id_3),
        };

        let resp = client0.append_v001(stream::iter(vec![heartbeat])).await?;
        let mut output = resp.into_inner();

        let first = output.next().await;
        assert!(
            first.is_some(),
            "should receive at least one response for heartbeat"
        );
        let first = first.unwrap()?;

        // Heartbeat success: returns the matching prev_log_id as last_log_id
        assert_eq!(first, pb::AppendResponse {
            rejected_by: None,
            conflict_log_id: None,
            last_log_id: Some(prev_log_id_3),
        });

        // Stream should end after processing the single request
        let next = output.next().await;
        assert!(next.is_none(), "stream should end after single request");
    }

    info!("--- append with entries: should succeed and return last_log_id");
    {
        // Build a log entry at index 4 with a SetFeature command
        let entry = pb::LogEntry {
            log_id: Some(pb::LogId {
                term: 2,
                node_id: 1,
                index: 4,
            }),
            proposed_at_ms: None,
            cmd: Some(pb::log_entry::Cmd::SetFeature(pb::CmdSetFeature {
                feature: "test-feature".to_string(),
                enable: true,
            })),
        };

        let append_req = pb::AppendRequest {
            vote: Some(higher_vote),
            prev_log_id: Some(prev_log_id_3),
            entries: vec![entry],
            leader_commit: Some(prev_log_id_3),
        };

        let resp = client0.append_v001(stream::iter(vec![append_req])).await?;
        let mut output = resp.into_inner();

        let first = output.next().await.unwrap()?;

        // Success with entries: last_log_id points to the last appended entry
        assert_eq!(first, pb::AppendResponse {
            rejected_by: None,
            conflict_log_id: None,
            last_log_id: Some(pb::LogId {
                term: 2,
                node_id: 1,
                index: 4,
            }),
        });
    }

    info!("--- stale vote: should be rejected with higher vote");
    {
        let stale_vote = pb::Vote {
            term: 1,
            node_id: 0,
            committed: false,
        };

        let stale_req = pb::AppendRequest {
            vote: Some(stale_vote),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };

        let resp = client0.append_v001(stream::iter(vec![stale_req])).await?;
        let mut output = resp.into_inner();

        let first = output.next().await.unwrap()?;

        // Rejected: the node's current vote (term=2) is returned
        assert_eq!(first, pb::AppendResponse {
            rejected_by: Some(higher_vote),
            conflict_log_id: None,
            last_log_id: None,
        });
    }

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_raft_service_append_v001_multi_item_stream() -> anyhow::Result<()> {
    // Test AppendV001 with a single stream containing multiple requests,
    // each carrying multiple log entries. Verifies that:
    // - All requests in one stream produce corresponding responses
    // - Each response's `last_log_id` reflects the last entry of that batch
    // - `prev_log_id` chaining across requests works correctly

    let (_nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![]).await?;
    let tc0 = tcs.remove(0);
    let mut client0 = tc0.raft_client().await?;

    let vote = pb::Vote {
        term: 2,
        node_id: 1,
        committed: true,
    };

    let make_log_id = |index: u64| pb::LogId {
        term: 2,
        node_id: 1,
        index,
    };

    let make_entry = |index: u64, key: &str| pb::LogEntry {
        log_id: Some(make_log_id(index)),
        proposed_at_ms: None,
        cmd: Some(pb::log_entry::Cmd::SetFeature(pb::CmdSetFeature {
            feature: key.to_string(),
            enable: true,
        })),
    };

    // After startup: log indexes 1..=3 (term=1, node=0)
    let prev_3 = pb::LogId {
        term: 1,
        node_id: 0,
        index: 3,
    };

    // Request 1: prev=3, entries at 4,5
    let req1 = pb::AppendRequest {
        vote: Some(vote),
        prev_log_id: Some(prev_3),
        entries: vec![make_entry(4, "feat-a"), make_entry(5, "feat-b")],
        leader_commit: Some(prev_3),
    };

    // Request 2: prev=5, entries at 6,7,8
    let req2 = pb::AppendRequest {
        vote: Some(vote),
        prev_log_id: Some(make_log_id(5)),
        entries: vec![
            make_entry(6, "feat-c"),
            make_entry(7, "feat-d"),
            make_entry(8, "feat-e"),
        ],
        leader_commit: Some(make_log_id(5)),
    };

    let resp = client0.append_v001(stream::iter(vec![req1, req2])).await?;
    let results: Vec<pb::AppendResponse> = resp.into_inner().try_collect().await?;

    assert_eq!(results, vec![
        pb::AppendResponse {
            rejected_by: None,
            conflict_log_id: None,
            last_log_id: Some(make_log_id(5)),
        },
        pb::AppendResponse {
            rejected_by: None,
            conflict_log_id: None,
            last_log_id: Some(make_log_id(8)),
        },
    ]);

    Ok(())
}
