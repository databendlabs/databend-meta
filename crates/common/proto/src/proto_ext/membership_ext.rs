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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::protobuf as pb;

type Membership = openraft::Membership<u64, openraft::EmptyNode>;

impl From<Membership> for pb::Membership {
    fn from(m: Membership) -> Self {
        let configs = m
            .get_joint_config()
            .iter()
            .map(|voter_set| pb::VoterGroup {
                node_ids: voter_set.iter().copied().collect(),
            })
            .collect();

        let nodes = m.nodes().map(|(id, _)| *id).collect();

        pb::Membership { configs, nodes }
    }
}

impl TryFrom<pb::Membership> for Membership {
    type Error = String;

    fn try_from(m: pb::Membership) -> Result<Self, Self::Error> {
        let configs: Vec<BTreeSet<u64>> = m
            .configs
            .into_iter()
            .map(|g| g.node_ids.into_iter().collect())
            .collect();

        let nodes: BTreeMap<u64, openraft::EmptyNode> = m
            .nodes
            .into_iter()
            .map(|id| (id, openraft::EmptyNode::default()))
            .collect();

        Membership::new(configs, nodes).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership_round_trip() {
        let configs = vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 4])];
        let nodes: BTreeMap<u64, openraft::EmptyNode> = [1, 2, 3, 4, 5]
            .into_iter()
            .map(|id| (id, openraft::EmptyNode::default()))
            .collect();
        let m = Membership::new(configs.clone(), nodes.clone()).unwrap();
        let pb_m = pb::Membership::from(m);
        let back = Membership::try_from(pb_m).unwrap();

        assert_eq!(back.get_joint_config(), &configs);
        let back_node_ids: BTreeSet<u64> = back.nodes().map(|(id, _)| *id).collect();
        assert_eq!(back_node_ids, nodes.keys().copied().collect());
    }
}
