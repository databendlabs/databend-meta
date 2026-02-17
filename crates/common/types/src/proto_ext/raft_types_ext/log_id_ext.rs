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

impl From<raft_types::LogId> for pb::LogId {
    fn from(log_id: raft_types::LogId) -> Self {
        pb::LogId {
            term: log_id.leader_id.term,
            node_id: log_id.leader_id.node_id,
            index: log_id.index,
        }
    }
}

impl From<pb::LogId> for raft_types::LogId {
    fn from(log_id: pb::LogId) -> Self {
        raft_types::new_log_id(log_id.term, log_id.node_id, log_id.index)
    }
}

#[cfg(test)]
mod tests {
    use crate::protobuf as pb;
    use crate::raft_types;

    #[test]
    fn test_log_id_round_trip() {
        let log_id = raft_types::new_log_id(3, 1, 100);
        let pb_log_id: pb::LogId = log_id.into();
        assert_eq!(pb_log_id.term, 3);
        assert_eq!(pb_log_id.node_id, 1);
        assert_eq!(pb_log_id.index, 100);

        let back: raft_types::LogId = pb_log_id.into();
        assert_eq!(back, log_id);
    }
}
