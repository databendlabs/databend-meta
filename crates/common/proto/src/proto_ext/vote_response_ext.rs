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

impl<C> From<openraft::raft::VoteResponse<C>> for pb::VoteResponse
where
    C: RaftTypeConfig,
    C::Vote: Into<pb::Vote>,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    fn from(resp: openraft::raft::VoteResponse<C>) -> Self {
        pb::VoteResponse {
            vote: Some(resp.vote.into()),
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.map(pb::LogId::from),
        }
    }
}

impl<C> From<pb::VoteResponse> for openraft::raft::VoteResponse<C>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
    <C::LeaderId as RaftLeaderId>::Committed: RaftLeaderId<Term = u64, NodeId = u64> + Ord,
{
    fn from(value: pb::VoteResponse) -> Self {
        let pb_vote = value.vote.unwrap_or_default();
        let leader_id = C::LeaderId::new(pb_vote.term, pb_vote.node_id);
        let vote = C::Vote::from_leader_id(leader_id, pb_vote.committed);
        let last_log_id = value.last_log_id.map(Into::into);
        openraft::raft::VoteResponse::new(vote, last_log_id, value.vote_granted)
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
    fn test_vote_response_round_trip_granted() {
        let vote = openraft::Vote::<LeaderId<u64, u64>>::new_committed(5, 3);
        let log_id = new_log_id(4, 2, 99);
        let resp = openraft::raft::VoteResponse::<TC>::new(vote, Some(log_id), true);

        let pb_resp = pb::VoteResponse::from(resp.clone());
        assert!(pb_resp.vote_granted);

        let back: openraft::raft::VoteResponse<TC> = pb_resp.into();
        assert_eq!(back.vote, resp.vote);
        assert_eq!(back.last_log_id, resp.last_log_id);
        assert_eq!(back.vote_granted, resp.vote_granted);
    }

    #[test]
    fn test_vote_response_round_trip_rejected() {
        let vote = openraft::Vote::<LeaderId<u64, u64>>::new(2, 1);
        let resp = openraft::raft::VoteResponse::<TC>::new(vote, None, false);

        let pb_resp = pb::VoteResponse::from(resp.clone());
        assert!(!pb_resp.vote_granted);
        assert!(pb_resp.last_log_id.is_none());

        let back: openraft::raft::VoteResponse<TC> = pb_resp.into();
        assert_eq!(back.vote, resp.vote);
        assert!(back.last_log_id.is_none());
        assert!(!back.vote_granted);
    }
}
