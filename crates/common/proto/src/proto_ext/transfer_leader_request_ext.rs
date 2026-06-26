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

fn vote_from_pb<C>(pb_vote: pb::Vote) -> C::Vote
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
{
    let leader_id = C::LeaderId::new(pb_vote.term, pb_vote.node_id);
    C::Vote::from_leader_id(leader_id, pb_vote.committed)
}

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

        let from = vote_from_pb::<C>(pb_vote);

        let last_log_id = value.last_log_id.map(Into::into);
        Ok(openraft::raft::TransferLeaderRequest::new(
            from,
            value.to,
            last_log_id,
        ))
    }
}

impl<C> From<openraft::raft::TransferLeaderResponse<C>> for pb::TransferLeaderResponse
where
    C: RaftTypeConfig<NodeId = u64>,
    C::Vote: Into<pb::Vote>,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    fn from(resp: openraft::raft::TransferLeaderResponse<C>) -> Self {
        pb::TransferLeaderResponse {
            error: resp.err().map(pb::TransferLeaderError::from),
        }
    }
}

impl<C> TryFrom<pb::TransferLeaderResponse> for openraft::raft::TransferLeaderResponse<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    type Error = Status;

    fn try_from(value: pb::TransferLeaderResponse) -> Result<Self, Self::Error> {
        let Some(err) = value.error else {
            return Ok(Ok(()));
        };

        Ok(Err(err.try_into()?))
    }
}

impl<C> From<openraft::raft::TransferLeaderError<C>> for pb::TransferLeaderError
where
    C: RaftTypeConfig<NodeId = u64>,
    C::Vote: Into<pb::Vote>,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    fn from(err: openraft::raft::TransferLeaderError<C>) -> Self {
        use openraft::raft::TransferLeaderError;
        use pb::transfer_leader_error::Error;

        let error = match err {
            TransferLeaderError::VoteChanged { expected, actual } => {
                Error::VoteChanged(pb::TransferLeaderVoteChanged {
                    expected: Some(expected.into()),
                    actual: Some(actual.into()),
                })
            }
            TransferLeaderError::LogNotFlushed { expected, actual } => {
                Error::LogNotFlushed(pb::TransferLeaderLogNotFlushed {
                    expected: expected.map(pb::LogId::from),
                    actual: actual.map(pb::LogId::from),
                })
            }
        };

        pb::TransferLeaderError { error: Some(error) }
    }
}

impl<C> TryFrom<pb::TransferLeaderError> for openraft::raft::TransferLeaderError<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    type Error = Status;

    fn try_from(value: pb::TransferLeaderError) -> Result<Self, Self::Error> {
        use openraft::raft::TransferLeaderError;
        use pb::transfer_leader_error::Error;

        match value
            .error
            .ok_or_else(|| Status::invalid_argument("missing transfer leader error"))?
        {
            Error::VoteChanged(value) => {
                let expected = value
                    .expected
                    .ok_or_else(|| Status::invalid_argument("missing expected vote"))?;
                let actual = value
                    .actual
                    .ok_or_else(|| Status::invalid_argument("missing actual vote"))?;

                Ok(TransferLeaderError::VoteChanged {
                    expected: vote_from_pb::<C>(expected),
                    actual: vote_from_pb::<C>(actual),
                })
            }
            Error::LogNotFlushed(value) => Ok(TransferLeaderError::LogNotFlushed {
                expected: value.expected.map(Into::into),
                actual: value.actual.map(Into::into),
            }),
        }
    }
}
