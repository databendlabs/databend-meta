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

use crate::protobuf as pb;

impl<C> From<openraft::raft::VoteRequest<C>> for pb::VoteRequest
where
    C: RaftTypeConfig,
    C::Vote: Into<pb::Vote>,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    fn from(req: openraft::raft::VoteRequest<C>) -> Self {
        pb::VoteRequest {
            vote: Some(req.vote.into()),
            last_log_id: req.last_log_id.map(pb::LogId::from),
        }
    }
}

impl<C> From<pb::VoteRequest> for openraft::raft::VoteRequest<C>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    fn from(value: pb::VoteRequest) -> Self {
        let pb_vote = value.vote.unwrap_or_default();
        let leader_id = C::LeaderId::new(pb_vote.term, pb_vote.node_id);
        let vote = C::Vote::from_leader_id(leader_id, pb_vote.committed);
        let last_log_id = value.last_log_id.map(Into::into);
        openraft::raft::VoteRequest::new(vote, last_log_id)
    }
}

#[cfg(test)]
mod tests {
    use openraft::impls::leader_id_adv::LeaderId;

    use super::*;

    openraft::declare_raft_types!(TC:);

    fn new_log_id(term: u64, node_id: u64, index: u64) -> openraft::LogId<LeaderId<u64, u64>> {
        openraft::LogId::new(LeaderId::new(term, node_id), index)
    }

    #[test]
    fn test_vote_request_round_trip() {
        let vote = openraft::Vote::<LeaderId<u64, u64>>::new(5, 3);
        let log_id = new_log_id(4, 2, 99);
        let req = openraft::raft::VoteRequest::<TC>::new(vote, Some(log_id));

        let pb_req = pb::VoteRequest::from(req.clone());
        let back: openraft::raft::VoteRequest<TC> = pb_req.into();
        assert_eq!(back.vote, req.vote);
        assert_eq!(back.last_log_id, req.last_log_id);
    }

    #[test]
    fn test_vote_request_no_log_id() {
        let vote = openraft::Vote::<LeaderId<u64, u64>>::new(1, 0);
        let req = openraft::raft::VoteRequest::<TC>::new(vote, None);

        let pb_req = pb::VoteRequest::from(req.clone());
        assert!(pb_req.last_log_id.is_none());

        let back: openraft::raft::VoteRequest<TC> = pb_req.into();
        assert_eq!(back.vote, req.vote);
        assert!(back.last_log_id.is_none());
    }
}
