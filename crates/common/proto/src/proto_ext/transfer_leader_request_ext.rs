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

use openraft::RaftTypeConfig;
use openraft::vote::RaftLeaderId;
use openraft::vote::RaftVote;
use tonic::Status;

use crate::protobuf as pb;

impl<C> From<openraft::raft::TransferLeaderRequest<C>> for pb::TransferLeaderRequest
where
    C: RaftTypeConfig<NodeId = u64>,
    C::Vote: Into<pb::Vote>,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    fn from(req: openraft::raft::TransferLeaderRequest<C>) -> Self {
        pb::TransferLeaderRequest {
            from: Some(req.from_leader().clone().into()),
            to: *req.to_node_id(),
            last_log_id: req.last_log_id().cloned().map(pb::LogId::from),
        }
    }
}

impl<C> TryFrom<pb::TransferLeaderRequest> for openraft::raft::TransferLeaderRequest<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    type Error = Status;

    fn try_from(value: pb::TransferLeaderRequest) -> Result<Self, Self::Error> {
        let pb_vote = value
            .from
            .ok_or_else(|| Status::invalid_argument("missing from"))?;

        let leader_id = C::LeaderId::new(pb_vote.term, pb_vote.node_id);
        let from = C::Vote::from_leader_id(leader_id, pb_vote.committed);

        let last_log_id = value.last_log_id.map(Into::into);
        Ok(openraft::raft::TransferLeaderRequest::new(
            from,
            value.to,
            last_log_id,
        ))
    }
}
