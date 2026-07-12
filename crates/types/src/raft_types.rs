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

//! This mod wraps openraft types that have generics parameter with concrete types.

use openraft::RaftTypeConfig;
use openraft::TokioRuntime;
use openraft::error::Infallible;
use openraft::impls::OneshotResponder;
use openraft::vote::RaftLeaderId;

use crate::AppliedState;
use crate::LogEntry;
use crate::snapshot_db::DB;

pub type NodeId = u64;
pub type MembershipNode = openraft::EmptyNode;
pub type LogIndex = u64;
pub type Term = u64;

pub type LeaderId = openraft::impls::leader_id_adv::LeaderId<Term, NodeId>;
pub type CommittedLeaderId = openraft::vote::leader_id_adv::CommittedLeaderId<Term, NodeId>;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct TypeConfig {}
impl RaftTypeConfig for TypeConfig {
    type D = LogEntry;
    type R = AppliedState;
    type NodeId = NodeId;
    type Node = MembershipNode;
    type Term = Term;
    type LeaderId = LeaderId;
    type Vote = openraft::Vote<LeaderId>;
    type Entry = openraft::Entry<CommittedLeaderId, LogEntry, NodeId, MembershipNode>;
    type AsyncRuntime = TokioRuntime;
    type Responder<T>
        = OneshotResponder<Self, T>
    where T: openraft::OptionalSend + 'static;
    type Batch<T>
        = openraft::impls::InlineBatch<T>
    where T: openraft::OptionalSend + 'static;
    type ErrorSource = anyerror::AnyError;
}

pub type IOFlushed = openraft::storage::IOFlushed<TypeConfig>;

pub type LogId = openraft::LogId<CommittedLeaderId>;
pub type Vote = openraft::Vote<LeaderId>;

pub type Membership = openraft::Membership<NodeId, MembershipNode>;
pub type StoredMembership = openraft::StoredMembership<CommittedLeaderId, NodeId, MembershipNode>;

pub type EntryPayload = openraft::EntryPayload<LogEntry, NodeId, MembershipNode>;
pub type Entry = openraft::Entry<CommittedLeaderId, LogEntry, NodeId, MembershipNode>;

pub type SnapshotMeta = openraft::SnapshotMeta<CommittedLeaderId, NodeId, MembershipNode>;
pub type Snapshot = openraft::Snapshot<CommittedLeaderId, NodeId, MembershipNode, DB>;

pub type RaftMetrics = openraft::RaftMetrics<TypeConfig>;
pub type WatchReceiver<T> = openraft::type_config::alias::WatchReceiverOf<TypeConfig, T>;
pub type Wait = openraft::metrics::Wait<TypeConfig>;

pub type ErrorSubject = openraft::ErrorSubject<TypeConfig>;
pub type ErrorVerb = openraft::ErrorVerb;

pub type RPCError<E = Infallible> = openraft::error::RPCError<TypeConfig, E>;
pub type RemoteError<E> = openraft::error::RemoteError<TypeConfig, E>;
pub type RaftError<E = Infallible> = openraft::error::RaftError<TypeConfig, E>;
pub type NetworkError = openraft::error::NetworkError<TypeConfig>;

pub type StorageError = openraft::StorageError<TypeConfig>;
pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
pub type Fatal = openraft::error::Fatal<TypeConfig>;
pub type ChangeMembershipError = openraft::error::ChangeMembershipError<CommittedLeaderId, NodeId>;
pub type ClientWriteError = openraft::error::ClientWriteError<TypeConfig>;
pub type InitializeError = openraft::error::InitializeError<TypeConfig>;
pub type StreamingError = openraft::error::StreamingError<TypeConfig>;
pub type Unreachable = openraft::error::Unreachable<TypeConfig>;

pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type SnapshotResponse = openraft::raft::SnapshotResponse<TypeConfig>;
pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;
pub type TransferLeaderRequest = openraft::raft::TransferLeaderRequest<TypeConfig>;
pub type TransferLeaderResponse = openraft::raft::TransferLeaderResponse<TypeConfig>;
pub type EntryResponder = openraft::storage::EntryResponder<TypeConfig>;

pub fn new_log_id(term: u64, node_id: NodeId, index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(term, node_id), index)
}
