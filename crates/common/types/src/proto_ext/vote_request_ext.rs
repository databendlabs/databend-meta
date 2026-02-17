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

impl From<raft_types::VoteRequest> for pb::VoteRequest {
    fn from(req: raft_types::VoteRequest) -> Self {
        pb::VoteRequest {
            vote: Some(req.vote.into()),
            last_log_id: req.last_log_id.map(|log_id| log_id.into()),
        }
    }
}

impl From<pb::VoteRequest> for raft_types::VoteRequest {
    fn from(req: pb::VoteRequest) -> Self {
        let vote: raft_types::Vote = req.vote.unwrap_or_default().into();
        let last_log_id = req.last_log_id.map(|log_id| log_id.into());
        raft_types::VoteRequest::new(vote, last_log_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vote_request_round_trip() {
        let vote = raft_types::Vote::new(5, 3);
        let log_id = raft_types::new_log_id(4, 2, 99);
        let req = raft_types::VoteRequest::new(vote, Some(log_id));

        let pb_req: pb::VoteRequest = req.clone().into();
        let back: raft_types::VoteRequest = pb_req.into();
        assert_eq!(back.vote, req.vote);
        assert_eq!(back.last_log_id, req.last_log_id);
    }

    #[test]
    fn test_vote_request_no_log_id() {
        let vote = raft_types::Vote::new(1, 0);
        let req = raft_types::VoteRequest::new(vote, None);

        let pb_req: pb::VoteRequest = req.clone().into();
        assert!(pb_req.last_log_id.is_none());

        let back: raft_types::VoteRequest = pb_req.into();
        assert_eq!(back.vote, req.vote);
        assert!(back.last_log_id.is_none());
    }
}
