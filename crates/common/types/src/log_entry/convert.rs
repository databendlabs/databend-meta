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

//! Bidirectional conversions between native types and protobuf transport types
//! for LogEntry, AppendEntries request/response, and supporting types.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use openraft::EntryPayload;
use openraft::entry::RaftEntry;

use crate::Cmd;
use crate::LogEntry;
use crate::protobuf as pb;
use crate::raft_types;

// === Membership conversions (extension traits) ===

pub trait PbMembershipExt {
    fn from_raft(m: raft_types::Membership) -> pb::Membership;
    fn try_into_raft(self) -> Result<raft_types::Membership, String>;
}

impl PbMembershipExt for pb::Membership {
    fn from_raft(m: raft_types::Membership) -> pb::Membership {
        let configs = m
            .get_joint_config()
            .iter()
            .map(|voter_set| pb::VoterGroup {
                node_ids: voter_set.iter().copied().collect(),
            })
            .collect();

        let nodes = m.nodes().map(|(id, _)| *id).collect();

        pb::Membership { configs, nodes }
    }

    fn try_into_raft(self) -> Result<raft_types::Membership, String> {
        let configs: Vec<BTreeSet<u64>> = self
            .configs
            .into_iter()
            .map(|g| g.node_ids.into_iter().collect())
            .collect();

        let nodes: BTreeMap<u64, openraft::EmptyNode> = self
            .nodes
            .into_iter()
            .map(|id| (id, openraft::EmptyNode::default()))
            .collect();

        raft_types::Membership::new(configs, nodes).map_err(|e| e.to_string())
    }
}

// === Entry ↔ pb::LogEntry (extension traits) ===

pub trait PbLogEntryExt {
    fn from_raft(entry: raft_types::Entry) -> pb::LogEntry;
    fn try_into_raft(self) -> Result<raft_types::Entry, String>;
}

impl PbLogEntryExt for pb::LogEntry {
    fn from_raft(entry: raft_types::Entry) -> pb::LogEntry {
        let log_id = Some(pb::LogId::from(entry.log_id));

        match entry.payload {
            EntryPayload::Blank => pb::LogEntry {
                log_id,
                proposed_at_ms: None,
                cmd: None,
            },
            EntryPayload::Normal(log_entry) => {
                let cmd = match log_entry.cmd {
                    Cmd::AddNode {
                        node_id,
                        node,
                        overriding,
                    } => pb::log_entry::Cmd::AddNode(pb::CmdAddNode {
                        node_id,
                        node: Some(pb::Node::from_base_node(node)),
                        overriding,
                    }),
                    Cmd::RemoveNode { node_id } => {
                        pb::log_entry::Cmd::RemoveNode(pb::CmdRemoveNode { node_id })
                    }
                    Cmd::SetFeature { feature, enable } => {
                        pb::log_entry::Cmd::SetFeature(pb::CmdSetFeature { feature, enable })
                    }
                    Cmd::KvTransaction(txn) => pb::log_entry::Cmd::KvTransaction(txn.into()),
                    Cmd::UpsertKV(_) => {
                        panic!(
                            "UpsertKV is a legacy variant and cannot be converted to protobuf LogEntry"
                        )
                    }
                    Cmd::Transaction(_) => {
                        panic!(
                            "Transaction is a legacy variant and cannot be converted to protobuf LogEntry"
                        )
                    }
                };
                pb::LogEntry {
                    log_id,
                    proposed_at_ms: log_entry.time_ms,
                    cmd: Some(cmd),
                }
            }
            EntryPayload::Membership(m) => pb::LogEntry {
                log_id,
                proposed_at_ms: None,
                cmd: Some(pb::log_entry::Cmd::Membership(pb::Membership::from_raft(m))),
            },
        }
    }

    fn try_into_raft(self) -> Result<raft_types::Entry, String> {
        let log_id = self
            .log_id
            .map(Into::into)
            .ok_or_else(|| "LogEntry missing log_id".to_string())?;

        let payload = match self.cmd {
            None => EntryPayload::Blank,
            Some(pb::log_entry::Cmd::Membership(m)) => EntryPayload::Membership(m.try_into_raft()?),
            Some(cmd) => {
                let native_cmd = match cmd {
                    pb::log_entry::Cmd::AddNode(c) => Cmd::AddNode {
                        node_id: c.node_id,
                        node: c
                            .node
                            .map(pb::Node::into_base_node)
                            .ok_or_else(|| "CmdAddNode missing node".to_string())?,
                        overriding: c.overriding,
                    },
                    pb::log_entry::Cmd::RemoveNode(c) => Cmd::RemoveNode { node_id: c.node_id },
                    pb::log_entry::Cmd::SetFeature(c) => Cmd::SetFeature {
                        feature: c.feature,
                        enable: c.enable,
                    },
                    pb::log_entry::Cmd::KvTransaction(req) => Cmd::KvTransaction(req.into()),
                    pb::log_entry::Cmd::Membership(_) => unreachable!(),
                };
                EntryPayload::Normal(LogEntry {
                    time_ms: self.proposed_at_ms,
                    cmd: native_cmd,
                })
            }
        };

        Ok(raft_types::Entry::new(log_id, payload))
    }
}

// === AppendEntriesRequest ↔ pb::AppendRequest (extension traits) ===

pub trait PbAppendRequestExt {
    fn from_raft(req: raft_types::AppendEntriesRequest) -> pb::AppendRequest;
    fn try_into_raft(self) -> Result<raft_types::AppendEntriesRequest, String>;
}

impl PbAppendRequestExt for pb::AppendRequest {
    fn from_raft(req: raft_types::AppendEntriesRequest) -> pb::AppendRequest {
        pb::AppendRequest {
            vote: Some(pb::Vote::from(req.vote)),
            prev_log_id: req.prev_log_id.map(pb::LogId::from),
            entries: req
                .entries
                .into_iter()
                .map(pb::LogEntry::from_raft)
                .collect(),
            leader_commit: req.leader_commit.map(pb::LogId::from),
        }
    }

    fn try_into_raft(self) -> Result<raft_types::AppendEntriesRequest, String> {
        let vote = self
            .vote
            .map(Into::into)
            .ok_or_else(|| "AppendRequest missing vote".to_string())?;

        let entries: Result<Vec<raft_types::Entry>, _> = self
            .entries
            .into_iter()
            .map(pb::LogEntry::try_into_raft)
            .collect();

        Ok(raft_types::AppendEntriesRequest {
            vote,
            prev_log_id: self.prev_log_id.map(Into::into),
            entries: entries?,
            leader_commit: self.leader_commit.map(Into::into),
        })
    }
}

// === AppendEntriesResponse ↔ pb::AppendResponse (extension traits) ===

pub trait PbAppendResponseExt {
    fn from_raft_response(resp: raft_types::AppendEntriesResponse) -> pb::AppendResponse;
    fn into_raft_response(self) -> raft_types::AppendEntriesResponse;
    fn from_stream_result(r: raft_types::StreamAppendResult) -> pb::AppendResponse;
    fn into_stream_result(self) -> raft_types::StreamAppendResult;
}

impl PbAppendResponseExt for pb::AppendResponse {
    fn from_raft_response(resp: raft_types::AppendEntriesResponse) -> pb::AppendResponse {
        match resp {
            raft_types::AppendEntriesResponse::Success => pb::AppendResponse {
                rejected_by: None,
                conflict_log_id: None,
                last_log_id: None,
            },
            raft_types::AppendEntriesResponse::PartialSuccess(last) => pb::AppendResponse {
                rejected_by: None,
                conflict_log_id: None,
                last_log_id: last.map(pb::LogId::from),
            },
            raft_types::AppendEntriesResponse::Conflict => pb::AppendResponse {
                rejected_by: None,
                conflict_log_id: Some(pb::LogId {
                    term: 0,
                    node_id: 0,
                    index: 0,
                }),
                last_log_id: None,
            },
            raft_types::AppendEntriesResponse::HigherVote(vote) => pb::AppendResponse {
                rejected_by: Some(pb::Vote::from(vote)),
                conflict_log_id: None,
                last_log_id: None,
            },
        }
    }

    fn into_raft_response(self) -> raft_types::AppendEntriesResponse {
        if let Some(vote) = self.rejected_by {
            return raft_types::AppendEntriesResponse::HigherVote(vote.into());
        }

        if self.conflict_log_id.is_some() {
            return raft_types::AppendEntriesResponse::Conflict;
        }

        match self.last_log_id {
            Some(log_id) => raft_types::AppendEntriesResponse::PartialSuccess(Some(log_id.into())),
            None => raft_types::AppendEntriesResponse::Success,
        }
    }

    fn from_stream_result(r: raft_types::StreamAppendResult) -> pb::AppendResponse {
        match r {
            Ok(log_id) => pb::AppendResponse {
                rejected_by: None,
                conflict_log_id: None,
                last_log_id: log_id.map(pb::LogId::from),
            },
            Err(raft_types::StreamAppendError::Conflict(log_id)) => pb::AppendResponse {
                rejected_by: None,
                conflict_log_id: Some(pb::LogId::from(log_id)),
                last_log_id: None,
            },
            Err(raft_types::StreamAppendError::HigherVote(vote)) => pb::AppendResponse {
                rejected_by: Some(pb::Vote::from(vote)),
                conflict_log_id: None,
                last_log_id: None,
            },
        }
    }

    fn into_stream_result(self) -> raft_types::StreamAppendResult {
        if let Some(vote) = self.rejected_by {
            return Err(raft_types::StreamAppendError::HigherVote(vote.into()));
        }
        if let Some(log_id) = self.conflict_log_id {
            return Err(raft_types::StreamAppendError::Conflict(log_id.into()));
        }
        Ok(self.last_log_id.map(Into::into))
    }
}

#[cfg(test)]
mod tests {
    use openraft::entry::RaftEntry;

    use super::*;
    use crate::Cmd;
    use crate::Endpoint;
    use crate::LogEntry;
    use crate::kv_transaction::Branch;
    use crate::kv_transaction::Operation;
    use crate::kv_transaction::Predicate;
    use crate::kv_transaction::Transaction;
    use crate::node::Node;
    use crate::protobuf as pb;
    use crate::raft_types;

    fn round_trip_log_entry(entry: LogEntry) {
        let raft_entry = raft_types::Entry::new(
            raft_types::new_log_id(1, 0, 1),
            openraft::EntryPayload::Normal(entry.clone()),
        );
        let pb_entry = pb::LogEntry::from_raft(raft_entry);
        let back = pb_entry.try_into_raft().unwrap();
        match back.payload {
            openraft::EntryPayload::Normal(back_entry) => assert_eq!(entry, back_entry),
            _ => panic!("expected Normal payload"),
        }
    }

    #[test]
    fn test_log_payload_add_node() {
        round_trip_log_entry(LogEntry::new_with_time(
            Cmd::AddNode {
                node_id: 1,
                node: Node::new("n1", Endpoint::new("127.0.0.1", 9191))
                    .with_grpc_advertise_address(Some("10.0.0.1:9191")),
                overriding: true,
            },
            Some(1234567890),
        ));
    }

    #[test]
    fn test_log_payload_remove_node() {
        round_trip_log_entry(LogEntry::new(Cmd::RemoveNode { node_id: 42 }));
    }

    #[test]
    fn test_log_payload_set_feature() {
        round_trip_log_entry(LogEntry::new(Cmd::SetFeature {
            feature: "test_feature".to_string(),
            enable: true,
        }));
    }

    #[test]
    fn test_log_payload_kv_transaction() {
        let txn = Transaction {
            branches: vec![
                Branch::if_(Predicate::eq_seq("k", 0))
                    .then([Operation::put("k", b"v"), Operation::get("k2")]),
                Branch::else_().then([Operation::delete("k")]),
            ],
        };
        round_trip_log_entry(LogEntry::new_with_time(Cmd::KvTransaction(txn), Some(999)));
    }

    #[test]
    #[should_panic(expected = "UpsertKV is a legacy variant")]
    fn test_log_payload_upsert_kv_panics() {
        use crate::UpsertKV;

        let entry = LogEntry::new(Cmd::UpsertKV(UpsertKV::insert("k", b"v")));
        let raft_entry = raft_types::Entry::new(
            raft_types::new_log_id(1, 0, 1),
            openraft::EntryPayload::Normal(entry),
        );
        let _ = pb::LogEntry::from_raft(raft_entry);
    }

    #[test]
    #[should_panic(expected = "Transaction is a legacy variant")]
    fn test_log_payload_transaction_panics() {
        use crate::TxnRequest;

        let entry = LogEntry::new(Cmd::Transaction(TxnRequest::default()));
        let raft_entry = raft_types::Entry::new(
            raft_types::new_log_id(1, 0, 1),
            openraft::EntryPayload::Normal(entry),
        );
        let _ = pb::LogEntry::from_raft(raft_entry);
    }

    #[test]
    fn test_node_round_trip() {
        let n = Node::new("node1", Endpoint::new("10.0.0.1", 9191))
            .with_grpc_advertise_address(Some("grpc.example.com:443"));
        let pb_n = pb::Node::from_base_node(n.clone());
        let back = pb_n.into_base_node();
        assert_eq!(n, back);
    }

    #[test]
    fn test_membership_round_trip() {
        use std::collections::BTreeMap;
        use std::collections::BTreeSet;

        let configs = vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 4])];
        let nodes: BTreeMap<u64, openraft::EmptyNode> = [1, 2, 3, 4, 5]
            .into_iter()
            .map(|id| (id, openraft::EmptyNode::default()))
            .collect();
        let m = raft_types::Membership::new(configs.clone(), nodes.clone()).unwrap();
        let pb_m = pb::Membership::from_raft(m);
        let back = pb_m.try_into_raft().unwrap();

        assert_eq!(back.get_joint_config(), &configs);
        let back_node_ids: BTreeSet<u64> = back.nodes().map(|(id, _)| *id).collect();
        assert_eq!(back_node_ids, nodes.keys().copied().collect());
    }

    fn make_entry(
        term: u64,
        node_id: u64,
        index: u64,
        payload: raft_types::EntryPayload,
    ) -> raft_types::Entry {
        raft_types::Entry::new(raft_types::new_log_id(term, node_id, index), payload)
    }

    #[test]
    fn test_entry_blank_round_trip() {
        let entry = make_entry(1, 0, 10, raft_types::EntryPayload::Blank);
        let pb_entry = pb::LogEntry::from_raft(entry.clone());
        assert!(pb_entry.cmd.is_none());
        let back = pb_entry.try_into_raft().unwrap();
        assert_eq!(entry, back);
    }

    #[test]
    fn test_entry_normal_round_trip() {
        let log_entry = LogEntry::new(Cmd::RemoveNode { node_id: 5 });
        let entry = make_entry(2, 1, 20, raft_types::EntryPayload::Normal(log_entry));
        let pb_entry = pb::LogEntry::from_raft(entry.clone());
        assert!(matches!(
            pb_entry.cmd,
            Some(pb::log_entry::Cmd::RemoveNode(_))
        ));
        let back = pb_entry.try_into_raft().unwrap();
        assert_eq!(entry, back);
    }

    #[test]
    fn test_entry_membership_round_trip() {
        use std::collections::BTreeMap;
        use std::collections::BTreeSet;

        let configs = vec![BTreeSet::from([1, 2, 3])];
        let nodes: BTreeMap<u64, openraft::EmptyNode> = [1, 2, 3]
            .into_iter()
            .map(|id| (id, openraft::EmptyNode::default()))
            .collect();
        let m = raft_types::Membership::new(configs, nodes).unwrap();
        let entry = make_entry(3, 0, 30, raft_types::EntryPayload::Membership(m));
        let pb_entry = pb::LogEntry::from_raft(entry.clone());
        assert!(matches!(
            pb_entry.cmd,
            Some(pb::log_entry::Cmd::Membership(_))
        ));
        let back = pb_entry.try_into_raft().unwrap();
        assert_eq!(entry, back);
    }

    fn assert_append_request_eq(
        a: &raft_types::AppendEntriesRequest,
        b: &raft_types::AppendEntriesRequest,
    ) {
        assert_eq!(a.vote, b.vote);
        assert_eq!(a.prev_log_id, b.prev_log_id);
        assert_eq!(a.entries, b.entries);
        assert_eq!(a.leader_commit, b.leader_commit);
    }

    #[test]
    fn test_append_request_round_trip() {
        let req = raft_types::AppendEntriesRequest {
            vote: raft_types::Vote::new(5, 1),
            prev_log_id: Some(raft_types::new_log_id(4, 1, 99)),
            entries: vec![
                make_entry(5, 1, 100, raft_types::EntryPayload::Blank),
                make_entry(
                    5,
                    1,
                    101,
                    raft_types::EntryPayload::Normal(LogEntry::new(Cmd::RemoveNode { node_id: 3 })),
                ),
            ],
            leader_commit: Some(raft_types::new_log_id(5, 1, 100)),
        };
        let pb_req = pb::AppendRequest::from_raft(req.clone());
        let back = pb_req.try_into_raft().unwrap();
        assert_append_request_eq(&req, &back);
    }

    #[test]
    fn test_append_request_heartbeat_round_trip() {
        let req = raft_types::AppendEntriesRequest {
            vote: raft_types::Vote::new_committed(3, 0),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let pb_req = pb::AppendRequest::from_raft(req.clone());
        let back = pb_req.try_into_raft().unwrap();
        assert_append_request_eq(&req, &back);
    }

    #[test]
    fn test_append_response_success() {
        let resp = raft_types::AppendEntriesResponse::Success;
        let pb_resp = pb::AppendResponse::from_raft_response(resp.clone());
        let back = pb_resp.into_raft_response();
        assert_eq!(resp, back);
    }

    #[test]
    fn test_append_response_partial_success() {
        let resp = raft_types::AppendEntriesResponse::PartialSuccess(Some(raft_types::new_log_id(
            5, 1, 101,
        )));
        let pb_resp = pb::AppendResponse::from_raft_response(resp.clone());
        let back = pb_resp.into_raft_response();
        assert_eq!(resp, back);
    }

    #[test]
    fn test_append_response_conflict() {
        let resp = raft_types::AppendEntriesResponse::Conflict;
        let pb_resp = pb::AppendResponse::from_raft_response(resp.clone());
        let back = pb_resp.into_raft_response();
        assert_eq!(resp, back);
    }

    #[test]
    fn test_append_response_higher_vote() {
        let resp =
            raft_types::AppendEntriesResponse::HigherVote(raft_types::Vote::new_committed(10, 2));
        let pb_resp = pb::AppendResponse::from_raft_response(resp.clone());
        let back = pb_resp.into_raft_response();
        assert_eq!(resp, back);
    }

    #[test]
    fn test_stream_append_result_success_with_log_id() {
        let result: raft_types::StreamAppendResult = Ok(Some(raft_types::new_log_id(5, 1, 100)));
        let pb_resp = pb::AppendResponse::from_stream_result(result.clone());
        let back = pb_resp.into_stream_result();
        assert_eq!(result, back);
    }

    #[test]
    fn test_stream_append_result_success_none() {
        let result: raft_types::StreamAppendResult = Ok(None);
        let pb_resp = pb::AppendResponse::from_stream_result(result.clone());
        let back = pb_resp.into_stream_result();
        assert_eq!(result, back);
    }

    #[test]
    fn test_stream_append_result_conflict() {
        let result: raft_types::StreamAppendResult = Err(raft_types::StreamAppendError::Conflict(
            raft_types::new_log_id(3, 2, 50),
        ));
        let pb_resp = pb::AppendResponse::from_stream_result(result.clone());
        let back = pb_resp.into_stream_result();
        assert_eq!(result, back);
    }

    #[test]
    fn test_stream_append_result_higher_vote() {
        let result: raft_types::StreamAppendResult = Err(
            raft_types::StreamAppendError::HigherVote(raft_types::Vote::new_committed(10, 2)),
        );
        let pb_resp = pb::AppendResponse::from_stream_result(result.clone());
        let back = pb_resp.into_stream_result();
        assert_eq!(result, back);
    }
}
