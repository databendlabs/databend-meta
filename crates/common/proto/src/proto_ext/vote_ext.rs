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

use openraft::vote::RaftLeaderId;
use openraft::vote::RaftVote;

use crate::protobuf as pb;

impl<LID> From<openraft::Vote<LID>> for pb::Vote
where LID: RaftLeaderId<Term = u64, NodeId = u64>
{
    fn from(vote: openraft::Vote<LID>) -> Self {
        pb::Vote {
            term: vote.leader_id().term(),
            node_id: *vote.leader_id().node_id(),
            committed: vote.is_committed(),
        }
    }
}

impl<Lid> From<pb::Vote> for openraft::Vote<Lid>
where Lid: RaftLeaderId<Term = u64, NodeId = u64>
{
    fn from(value: pb::Vote) -> Self {
        let leader_id = Lid::new(value.term, value.node_id);
        openraft::Vote::from_leader_id(leader_id, value.committed)
    }
}

#[cfg(test)]
mod tests {
    use openraft::impls::leader_id_adv::LeaderId;

    use super::*;

    type Lid = LeaderId<u64, u64>;
    type Vote = openraft::Vote<Lid>;

    #[test]
    fn test_vote_round_trip() {
        let vote = Vote::new(5, 3);
        let pb_vote = pb::Vote::from(vote);
        assert_eq!(pb_vote, pb::Vote {
            term: 5,
            node_id: 3,
            committed: false,
        });

        let back: Vote = pb_vote.into();
        assert_eq!(back, vote);
    }

    #[test]
    fn test_vote_committed_round_trip() {
        let vote = Vote::new_committed(7, 2);
        let pb_vote = pb::Vote::from(vote);
        assert_eq!(pb_vote, pb::Vote {
            term: 7,
            node_id: 2,
            committed: true,
        });

        let back: Vote = pb_vote.into();
        assert_eq!(back, vote);
    }
}
