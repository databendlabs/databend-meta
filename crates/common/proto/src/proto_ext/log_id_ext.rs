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

use crate::protobuf as pb;
use crate::protobuf::LogId;

impl<Clid> From<openraft::LogId<Clid>> for pb::LogId
where Clid: RaftLeaderId<Term = u64, NodeId = u64> + Ord
{
    fn from(log_id: openraft::LogId<Clid>) -> Self {
        pb::LogId {
            term: log_id.leader_id.term(),
            node_id: *log_id.leader_id.node_id(),
            index: log_id.index,
        }
    }
}

impl<Clid> From<pb::LogId> for openraft::LogId<Clid>
where Clid: RaftLeaderId<Term = u64, NodeId = u64> + Ord
{
    fn from(value: LogId) -> Self {
        let leader_id = Clid::new(value.term, value.node_id);
        openraft::LogId::new(leader_id, value.index)
    }
}

#[cfg(test)]
mod tests {
    use openraft::impls::leader_id_adv::LeaderId;

    use super::*;

    type Clid = LeaderId<u64, u64>;

    fn new_log_id(term: u64, node_id: u64, index: u64) -> openraft::LogId<Clid> {
        openraft::LogId::new(Clid::new(term, node_id), index)
    }

    #[test]
    fn test_log_id_round_trip() {
        let log_id = new_log_id(3, 1, 100);
        let pb_log_id = pb::LogId::from(log_id);
        assert_eq!(pb_log_id, pb::LogId {
            term: 3,
            node_id: 1,
            index: 100,
        });

        let back: openraft::LogId<Clid> = pb_log_id.into();
        assert_eq!(back, log_id);
    }
}
