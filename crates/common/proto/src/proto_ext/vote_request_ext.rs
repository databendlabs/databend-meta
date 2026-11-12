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
            leadership_transfer_from: req.leadership_transfer_from.map(Into::into),
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
        let vote = vote_from_pb::<C>(value.vote.unwrap_or_default());
        let last_log_id = value.last_log_id.map(Into::into);
        let mut req = openraft::raft::VoteRequest::new(vote, last_log_id);
        req.leadership_transfer_from = value.leadership_transfer_from.map(vote_from_pb::<C>);
        req
    }
}

fn vote_from_pb<C>(pb_vote: pb::Vote) -> C::Vote
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId<Term = u64, NodeId = u64>,
{
    let leader_id = C::LeaderId::new(pb_vote.term, pb_vote.node_id);
    C::Vote::from_leader_id(leader_id, pb_vote.committed)
}

#[cfg(test)]
mod tests {
    use openraft::impls::leader_id_adv::LeaderId;
    use prost::Message;

    use super::*;

    openraft::declare_raft_types!(TC:);

    /// `VoteRequest` as defined before `leadership_transfer_from` was added.
    #[derive(Clone, PartialEq, prost::Message)]
    struct VoteRequestBefore010 {
        #[prost(message, optional, tag = "1")]
        vote: Option<pb::Vote>,
        #[prost(message, optional, tag = "2")]
        last_log_id: Option<pb::LogId>,
    }

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
        assert_eq!(back, req);
    }

    #[test]
    fn test_vote_request_no_log_id() {
        let vote = openraft::Vote::<LeaderId<u64, u64>>::new(1, 0);
        let req = openraft::raft::VoteRequest::<TC>::new(vote, None);

        let pb_req = pb::VoteRequest::from(req.clone());
        assert!(pb_req.last_log_id.is_none());

        let back: openraft::raft::VoteRequest<TC> = pb_req.into();
        assert_eq!(back, req);
    }

    #[test]
    fn test_vote_request_leadership_transfer_round_trip() {
        let vote = openraft::Vote::<LeaderId<u64, u64>>::new(6, 2);
        let mut req = openraft::raft::VoteRequest::<TC>::new(vote, Some(new_log_id(5, 1, 10)));
        req.leadership_transfer_from = Some(openraft::Vote::new_committed(5, 1));

        let pb_req = pb::VoteRequest::from(req.clone());
        assert_eq!(
            pb_req.leadership_transfer_from,
            Some(pb::Vote {
                term: 5,
                node_id: 1,
                committed: true,
            })
        );

        let back: openraft::raft::VoteRequest<TC> = pb_req.into();
        assert_eq!(back, req);
    }

    /// A request from a peer that predates the field is an ordinary election,
    /// and such a peer still decodes a transfer request from this version.
    #[test]
    fn test_vote_request_wire_compat_with_pre_010_peers() {
        let old = VoteRequestBefore010 {
            vote: Some(pb::Vote {
                term: 6,
                node_id: 2,
                committed: false,
            }),
            last_log_id: Some(pb::LogId {
                term: 5,
                node_id: 1,
                index: 10,
            }),
        };
        let decoded = pb::VoteRequest::decode(old.encode_to_vec().as_slice()).unwrap();
        let req: openraft::raft::VoteRequest<TC> = decoded.into();
        assert_eq!(req.leadership_transfer_from, None);
        assert_eq!(req.vote, openraft::Vote::new(6, 2));

        let new = pb::VoteRequest {
            vote: old.vote,
            last_log_id: old.last_log_id,
            leadership_transfer_from: Some(pb::Vote {
                term: 5,
                node_id: 1,
                committed: true,
            }),
        };
        let decoded_by_old = VoteRequestBefore010::decode(new.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded_by_old, old);
    }
}
