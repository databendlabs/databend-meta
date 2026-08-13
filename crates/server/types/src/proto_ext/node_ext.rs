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

use databend_meta_base::Endpoint;
use databend_meta_base::Node;

use crate::protobuf as pb;

pub trait PbNodeExt {
    fn from_node(n: Node) -> pb::Node;
    fn to_node(self) -> Node;
}

impl PbNodeExt for pb::Node {
    fn from_node(n: Node) -> pb::Node {
        pb::Node {
            name: n.name,
            addr: n.endpoint.addr().to_string(),
            port: n.endpoint.port() as u32,
            grpc_api_advertise_address: n.grpc_api_advertise_address,
            raft_tls_advertise_address: n.raft_tls_advertise_address,
        }
    }

    fn to_node(self) -> Node {
        Node::new(self.name, Endpoint::new(self.addr, self.port as u16))
            .with_grpc_advertise_address(self.grpc_api_advertise_address)
            .with_raft_tls_advertise_address(self.raft_tls_advertise_address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_round_trip() {
        for tls_addr in [None, Some("tls.example.com:29004")] {
            let n = Node::new("node1", Endpoint::new("10.0.0.1", 9191))
                .with_grpc_advertise_address(Some("grpc.example.com:443"))
                .with_raft_tls_advertise_address(tls_addr);
            let pb_n = pb::Node::from_node(n.clone());
            let back = pb_n.to_node();
            assert_eq!(n, back, "tls_addr: {:?}", tls_addr);
        }
    }

    /// A peer running a release that predates `raft_tls_advertise_address`
    /// never writes tag 6, and prost decodes an absent optional field to the
    /// field's default. So `pb::Node::default()` is exactly what such a peer's
    /// `AddNode` entry decodes to here, and it has to name a node that is
    /// dialed in plaintext.
    #[test]
    fn test_a_node_from_a_peer_that_never_sets_the_tls_address() {
        let from_old_peer = pb::Node::default();

        let got = from_old_peer.to_node();

        assert_eq!(got.raft_tls_advertise_address, None);
    }
}
