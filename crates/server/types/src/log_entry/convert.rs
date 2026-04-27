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

use openraft::EntryPayload;
use openraft::entry::RaftEntry;

use crate::Cmd;
use crate::LogEntry;
use crate::MatchSeq;
use crate::MetaSpec;
use crate::Operation;
use crate::UpsertKV;
use crate::proto_ext::PbNodeExt;
use crate::protobuf as pb;
use crate::raft_types;
use crate::time::Interval;

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
                        node: Some(pb::Node::from_node(node)),
                        overriding,
                    }),
                    Cmd::RemoveNode { node_id } => {
                        pb::log_entry::Cmd::RemoveNode(pb::CmdRemoveNode { node_id })
                    }
                    Cmd::SetFeature { feature, enable } => {
                        pb::log_entry::Cmd::SetFeature(pb::CmdSetFeature { feature, enable })
                    }
                    Cmd::KvTransaction(txn) => pb::log_entry::Cmd::KvTransaction(txn.into()),
                    Cmd::UpsertKV(u) => pb::log_entry::Cmd::UpsertKv(upsert_kv_to_pb(u)),
                    Cmd::Transaction(txn) => pb::log_entry::Cmd::Transaction(txn),
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
                cmd: Some(pb::log_entry::Cmd::Membership(m.into())),
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
            Some(pb::log_entry::Cmd::Membership(m)) => {
                EntryPayload::Membership(m.try_into().map_err(|e: String| e)?)
            }
            Some(cmd) => {
                let native_cmd = match cmd {
                    pb::log_entry::Cmd::AddNode(c) => Cmd::AddNode {
                        node_id: c.node_id,
                        node: c
                            .node
                            .map(pb::Node::to_node)
                            .ok_or_else(|| "CmdAddNode missing node".to_string())?,
                        overriding: c.overriding,
                    },
                    pb::log_entry::Cmd::RemoveNode(c) => Cmd::RemoveNode { node_id: c.node_id },
                    pb::log_entry::Cmd::SetFeature(c) => Cmd::SetFeature {
                        feature: c.feature,
                        enable: c.enable,
                    },
                    pb::log_entry::Cmd::UpsertKv(c) => Cmd::UpsertKV(upsert_kv_from_pb(c)?),
                    pb::log_entry::Cmd::Transaction(txn) => Cmd::Transaction(txn),
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

// === UpsertKV ↔ pb::CmdUpsertKv ===

// Native `MatchSeq::Any` is equivalent to `ge=true, value=0` and is sent as such on the wire.
fn match_seq_to_pb(seq: MatchSeq) -> pb::MatchSeq {
    let (ge, value) = match seq {
        MatchSeq::Any => (true, 0),
        MatchSeq::Exact(n) => (false, n),
        MatchSeq::GE(n) => (true, n),
    };
    pb::MatchSeq { ge, value }
}

fn match_seq_from_pb(p: pb::MatchSeq) -> MatchSeq {
    if p.ge {
        MatchSeq::GE(p.value)
    } else {
        MatchSeq::Exact(p.value)
    }
}

// `Operation::AsIs` is deprecated and not transported.
fn value_to_pb(op: Operation<Vec<u8>>) -> Option<Vec<u8>> {
    match op {
        Operation::Update(v) => Some(v),
        Operation::Delete => None,
        #[allow(deprecated)]
        Operation::AsIs => panic!("Operation::AsIs is deprecated and must not appear in raft logs"),
    }
}

fn value_from_pb(value: Option<Vec<u8>>) -> Operation<Vec<u8>> {
    match value {
        Some(v) => Operation::Update(v),
        None => Operation::Delete,
    }
}

fn upsert_kv_to_pb(u: UpsertKV) -> pb::CmdUpsertKv {
    let (expire_at, ttl_ms) = match u.value_meta {
        Some(m) => (m.expire_at(), m.ttl().map(|i| i.millis())),
        None => (None, None),
    };
    pb::CmdUpsertKv {
        key: u.key,
        seq: Some(match_seq_to_pb(u.seq)),
        value: value_to_pb(u.value),
        expire_at,
        ttl_ms,
    }
}

fn upsert_kv_from_pb(p: pb::CmdUpsertKv) -> Result<UpsertKV, String> {
    let seq = p.seq.ok_or_else(|| "CmdUpsertKV missing seq".to_string())?;
    let value_meta = match (p.expire_at, p.ttl_ms) {
        (None, None) => None,
        (e, t) => Some(MetaSpec::new(e, t.map(Interval::from_millis))),
    };
    Ok(UpsertKV {
        key: p.key,
        seq: match_seq_from_pb(seq),
        value: value_from_pb(p.value),
        value_meta,
    })
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

    // The transport-level tests below feed each Cmd variant (with full content
    // coverage) through `pb::LogEntry::from_raft` -> `try_into_raft` and assert
    // semantic equality. This is the same path as receiving a `LogEntry` over
    // the wire and converting back to native types.

    #[test]
    fn test_log_payload_add_node() {
        // overriding=true + grpc_api_advertise_address present + time_ms set.
        round_trip_log_entry(LogEntry::new_with_time(
            Cmd::AddNode {
                node_id: 1,
                node: Node::new("n1", Endpoint::new("127.0.0.1", 9191))
                    .with_grpc_advertise_address(Some("10.0.0.1:9191")),
                overriding: true,
            },
            Some(1234567890),
        ));

        // overriding=false + no grpc_api_advertise_address + no time_ms.
        round_trip_log_entry(LogEntry::new(Cmd::AddNode {
            node_id: 2,
            node: Node::new("n2", Endpoint::new("10.0.0.2", 9191)),
            overriding: false,
        }));
    }

    #[test]
    fn test_log_payload_remove_node() {
        round_trip_log_entry(LogEntry::new(Cmd::RemoveNode { node_id: 42 }));
        round_trip_log_entry(LogEntry::new_with_time(
            Cmd::RemoveNode { node_id: 0 },
            Some(7),
        ));
    }

    #[test]
    fn test_log_payload_set_feature() {
        round_trip_log_entry(LogEntry::new(Cmd::SetFeature {
            feature: "feat_a".to_string(),
            enable: true,
        }));
        round_trip_log_entry(LogEntry::new(Cmd::SetFeature {
            feature: "feat_b".to_string(),
            enable: false,
        }));
    }

    #[test]
    fn test_log_payload_upsert_kv() {
        use std::time::Duration;

        use crate::MatchSeq;
        use crate::UpsertKV;

        // Update + Exact(0) (insert), no meta.
        round_trip_log_entry(LogEntry::new(Cmd::UpsertKV(UpsertKV::insert("k", b"v"))));

        // Delete + GE(1), no meta.
        round_trip_log_entry(LogEntry::new(Cmd::UpsertKV(UpsertKV::delete("k"))));

        // Update with empty bytes — distinct from Delete on the wire.
        round_trip_log_entry(LogEntry::new(Cmd::UpsertKV(UpsertKV::update("k", b""))));

        // Update + Exact(5) (CAS) + expire_at-only meta.
        round_trip_log_entry(LogEntry::new(Cmd::UpsertKV(UpsertKV::new(
            "k",
            MatchSeq::Exact(5),
            crate::Operation::Update(b"v".to_vec()),
            Some(crate::MetaSpec::new_expire(1700000000)),
        ))));

        // Update + GE(0) + ttl-only meta.
        round_trip_log_entry(LogEntry::new(Cmd::UpsertKV(
            UpsertKV::update("k", b"v").with_ttl(Duration::from_millis(5000)),
        )));

        // Update + GE(7) + both expire_at and ttl.
        round_trip_log_entry(LogEntry::new(Cmd::UpsertKV(UpsertKV::new(
            "k",
            MatchSeq::GE(7),
            crate::Operation::Update(b"v".to_vec()),
            Some(MetaSpec::new(
                Some(1700000000),
                Some(Interval::from_millis(5000)),
            )),
        ))));
    }

    #[test]
    fn test_log_payload_upsert_kv_any_normalizes_to_ge_zero() {
        // Native `MatchSeq::Any` is equivalent to `GE(0)` and is sent as such on
        // the wire. The reverse direction always rebuilds `GE(0)`, so this round
        // trip is intentionally asymmetric — assert the normalized result rather
        // than byte-equality.
        use crate::MatchSeq;
        use crate::UpsertKV;

        let any_kv = UpsertKV::new(
            "k",
            MatchSeq::Any,
            crate::Operation::Update(b"v".to_vec()),
            None,
        );
        let pb_entry = pb::LogEntry::from_raft(raft_types::Entry::new(
            raft_types::new_log_id(1, 0, 1),
            openraft::EntryPayload::Normal(LogEntry::new(Cmd::UpsertKV(any_kv))),
        ));
        let back = pb_entry.try_into_raft().unwrap();
        let openraft::EntryPayload::Normal(LogEntry {
            cmd: Cmd::UpsertKV(back_kv),
            ..
        }) = back.payload
        else {
            panic!("expected Cmd::UpsertKV");
        };
        assert_eq!(back_kv.seq, MatchSeq::GE(0));
    }

    #[test]
    fn test_log_payload_transaction() {
        use crate::TxnCondition;
        use crate::TxnOp;
        use crate::TxnRequest;

        // Empty TxnRequest (default): all four lists empty.
        round_trip_log_entry(LogEntry::new(Cmd::Transaction(TxnRequest::default())));

        // condition + if_then.
        let txn = TxnRequest::new(vec![TxnCondition::eq_value("k", b"v".to_vec())], vec![
            TxnOp::put("k", b"v".to_vec()),
        ]);
        round_trip_log_entry(LogEntry::new_with_time(Cmd::Transaction(txn), Some(42)));

        // condition + if_then + else_then with multiple ops.
        let txn = TxnRequest {
            condition: vec![
                TxnCondition::eq_value("k", b"v".to_vec()),
                TxnCondition::eq_seq("k", 3),
            ],
            if_then: vec![TxnOp::put("k", b"v2".to_vec()), TxnOp::get("other")],
            else_then: vec![TxnOp::delete("k")],
            operations: vec![],
        };
        round_trip_log_entry(LogEntry::new_with_time(Cmd::Transaction(txn), Some(99)));
    }

    #[test]
    fn test_log_payload_kv_transaction() {
        // Two branches: an `if` with a predicate and an `else`.
        let txn = Transaction {
            branches: vec![
                Branch::if_(Predicate::eq_seq("k", 0))
                    .then([Operation::put("k", b"v"), Operation::get("k2")]),
                Branch::else_().then([Operation::delete("k")]),
            ],
        };
        round_trip_log_entry(LogEntry::new_with_time(Cmd::KvTransaction(txn), Some(999)));

        // Single unconditional branch.
        let txn = Transaction {
            branches: vec![Branch::else_().then([Operation::put("k", b"v")])],
        };
        round_trip_log_entry(LogEntry::new(Cmd::KvTransaction(txn)));
    }

    #[test]
    fn test_node_round_trip() {
        use crate::proto_ext::PbNodeExt;

        let n = Node::new("node1", Endpoint::new("10.0.0.1", 9191))
            .with_grpc_advertise_address(Some("grpc.example.com:443"));
        let pb_n = pb::Node::from_node(n.clone());
        let back = pb_n.to_node();
        assert_eq!(n, back);
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
