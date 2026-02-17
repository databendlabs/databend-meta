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

use crate::protobuf as pb;
use crate::raft_types;

impl From<raft_types::VoteResponse> for pb::VoteResponse {
    fn from(resp: raft_types::VoteResponse) -> Self {
        pb::VoteResponse {
            vote: Some(resp.vote.into()),
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.map(|log_id| log_id.into()),
        }
    }
}

impl From<pb::VoteResponse> for raft_types::VoteResponse {
    fn from(resp: pb::VoteResponse) -> Self {
        let vote: raft_types::Vote = resp.vote.unwrap_or_default().into();
        let last_log_id = resp.last_log_id.map(|log_id| log_id.into());
        raft_types::VoteResponse::new(vote, last_log_id, resp.vote_granted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vote_response_round_trip_granted() {
        let vote = raft_types::Vote::new_committed(5, 3);
        let log_id = raft_types::new_log_id(4, 2, 99);
        let resp = raft_types::VoteResponse::new(vote, Some(log_id), true);

        let pb_resp: pb::VoteResponse = resp.clone().into();
        assert!(pb_resp.vote_granted);

        let back: raft_types::VoteResponse = pb_resp.into();
        assert_eq!(back.vote, resp.vote);
        assert_eq!(back.last_log_id, resp.last_log_id);
        assert_eq!(back.vote_granted, resp.vote_granted);
    }

    #[test]
    fn test_vote_response_round_trip_rejected() {
        let vote = raft_types::Vote::new(2, 1);
        let resp = raft_types::VoteResponse::new(vote, None, false);

        let pb_resp: pb::VoteResponse = resp.clone().into();
        assert!(!pb_resp.vote_granted);
        assert!(pb_resp.last_log_id.is_none());

        let back: raft_types::VoteResponse = pb_resp.into();
        assert_eq!(back.vote, resp.vote);
        assert!(back.last_log_id.is_none());
        assert!(!back.vote_granted);
    }
}
