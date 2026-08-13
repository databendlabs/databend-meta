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

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::Endpoint;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Node {
    /// Node name for display.
    pub name: String,

    /// Raft service endpoint to connect to.
    pub endpoint: Endpoint,

    /// For backward compatibility, it can not be removed.
    /// 2023-02-09
    //#[deprecated(note = "it is listening addr, not advertise addr")]
    #[serde(skip)]
    pub grpc_api_addr: Option<String>,

    /// The address `ip:port` for a meta-client to connect to.
    pub grpc_api_advertise_address: Option<String>,

    /// The address `ip:port` where this node's raft TLS listener answers.
    ///
    /// A node publishes this itself instead of peers deriving it, so a peer
    /// learns it from the membership it already replicates. `None` means this
    /// node serves raft in plaintext only, and is what makes a peer dial
    /// `endpoint` rather than TLS.
    ///
    /// It is a whole address rather than a port beside `endpoint` so that a
    /// node can advertise TLS on an interface or a name it does not serve
    /// plaintext on, and so that its certificate can be issued for that name
    /// rather than for whatever `endpoint` happens to hold.
    ///
    /// Only set while the TLS listener is actually running: a published
    /// address that nothing answers on reads to peers as a node that is down.
    ///
    /// `serde(default)` is what keeps state machines, snapshots and exports
    /// written before this field readable, since none of them carry the key.
    /// Skipping the key when it is `None` is the other half of that: a cluster
    /// that never turns TLS on keeps writing byte-identical records, so its
    /// snapshots and exports stay readable by the release it upgraded from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raft_tls_advertise_address: Option<String>,
}

impl Node {
    pub fn new(name: impl ToString, endpoint: Endpoint) -> Self {
        Self {
            name: name.to_string(),
            endpoint,
            ..Default::default()
        }
    }

    pub fn with_grpc_advertise_address(mut self, g: Option<impl ToString>) -> Self {
        self.grpc_api_advertise_address = g.map(|x| x.to_string());
        self
    }

    pub fn with_raft_tls_advertise_address(mut self, addr: Option<impl ToString>) -> Self {
        self.raft_tls_advertise_address = addr.map(|x| x.to_string());
        self
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let grpc_addr_display = if let Some(grpc_addr) = &self.grpc_api_advertise_address {
            grpc_addr.to_string()
        } else {
            "".to_string()
        };
        let tls_addr_display = if let Some(tls_addr) = &self.raft_tls_advertise_address {
            tls_addr.to_string()
        } else {
            "".to_string()
        };
        write!(
            f,
            "id={} raft={} grpc={} raft_tls={}",
            self.name, self.endpoint, grpc_addr_display, tls_addr_display
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node record written before `raft_tls_advertise_address` existed
    /// carries no such key. JSON is how the snapshot's rotbl meta holds
    /// `SysData`, and how the sled store and the export format encode a node,
    /// so a build that cannot read those keyless records cannot open data
    /// written by any earlier release.
    ///
    /// See `test_messagepack_is_compatible_in_both_directions` for the other
    /// persisted form.
    #[test]
    fn test_deserialize_a_record_written_before_the_tls_address() -> anyhow::Result<()> {
        let old = r#"{"name":"1","endpoint":{"addr":"localhost","port":28103},"grpc_api_advertise_address":"0.0.0.0:9191"}"#;

        let got: Node = serde_json::from_str(old)?;

        assert_eq!(got, Node {
            name: "1".to_string(),
            endpoint: Endpoint::new("localhost", 28103),
            grpc_api_addr: None,
            grpc_api_advertise_address: Some("0.0.0.0:9191".to_string()),
            raft_tls_advertise_address: None,
        });

        Ok(())
    }

    /// The reverse direction: a record written by a node that knows the field
    /// stays readable by one that does not, because neither end refuses an
    /// unknown key. Standing in for the older `Node` here is a record carrying
    /// a key this version has never heard of.
    #[test]
    fn test_deserialize_a_record_carrying_an_unknown_key() -> anyhow::Result<()> {
        let newer = r#"{"name":"1","endpoint":{"addr":"localhost","port":28103},"grpc_api_advertise_address":null,"raft_tls_advertise_address":"tls.example.com:29004","some_later_field":7}"#;

        let got: Node = serde_json::from_str(newer)?;

        assert_eq!(got, Node {
            name: "1".to_string(),
            endpoint: Endpoint::new("localhost", 28103),
            grpc_api_addr: None,
            grpc_api_advertise_address: None,
            raft_tls_advertise_address: Some("tls.example.com:29004".to_string()),
        });

        Ok(())
    }

    /// The published address is what a peer dials, so it has to survive a
    /// write and read back unchanged, including the absent case.
    ///
    /// The written JSON is pinned too, because it is what lands in snapshots
    /// and exports. A node without a TLS listener must not write the key at
    /// all: that is what makes a cluster which never turns TLS on keep
    /// producing records the release it upgraded from can still read.
    #[test]
    fn test_json_round_trip() -> anyhow::Result<()> {
        let cases = [
            (
                None,
                r#"{"name":"1","endpoint":{"addr":"localhost","port":28103},"grpc_api_advertise_address":"0.0.0.0:9191"}"#,
            ),
            (
                Some("tls.example.com:29004"),
                r#"{"name":"1","endpoint":{"addr":"localhost","port":28103},"grpc_api_advertise_address":"0.0.0.0:9191","raft_tls_advertise_address":"tls.example.com:29004"}"#,
            ),
        ];

        for (tls_addr, want_json) in cases {
            let node = Node::new("1", Endpoint::new("localhost", 28103))
                .with_grpc_advertise_address(Some("0.0.0.0:9191"))
                .with_raft_tls_advertise_address(tls_addr);

            let json = serde_json::to_string(&node)?;
            assert_eq!(json, want_json, "tls_addr: {:?}", tls_addr);

            let back: Node = serde_json::from_str(&json)?;
            assert_eq!(node, back, "tls_addr: {:?}", tls_addr);
        }

        Ok(())
    }

    /// `Node` without the field, standing in for the release this one upgrades
    /// from. Compatibility across a version boundary is a claim about two
    /// struct definitions, so the older one has to be present to test against.
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct OldNode {
        name: String,
        endpoint: Endpoint,
        #[serde(skip)]
        grpc_api_addr: Option<String>,
        grpc_api_advertise_address: Option<String>,
    }

    /// The raft log is the persisted form that is not JSON: `Cw` in
    /// `databend-meta-raft-log` encodes each entry with
    /// `rmp_serde::encode::write_named`, and an entry carrying `Cmd::AddNode`
    /// carries a `Node`. A node's record therefore has to survive the same two
    /// version boundaries in MessagePack as it does in JSON.
    ///
    /// What makes it survive them is `write_named`, which emits a map keyed by
    /// field name rather than a bare sequence: a decoder skips keys it does not
    /// know, and `serde(default)` supplies keys that are absent. A positional
    /// encoding would break on both counts.
    #[test]
    fn test_messagepack_is_compatible_in_both_directions() -> anyhow::Result<()> {
        let old = OldNode {
            name: "1".to_string(),
            endpoint: Endpoint::new("localhost", 28103),
            grpc_api_addr: None,
            grpc_api_advertise_address: Some("0.0.0.0:9191".to_string()),
        };

        let mut old_bytes = Vec::new();
        rmp_serde::encode::write_named(&mut old_bytes, &old)?;

        // A log entry written by the earlier release, replayed here.
        let got: Node = rmp_serde::from_slice(&old_bytes)?;

        assert_eq!(got, Node {
            name: "1".to_string(),
            endpoint: Endpoint::new("localhost", 28103),
            grpc_api_addr: None,
            grpc_api_advertise_address: Some("0.0.0.0:9191".to_string()),
            raft_tls_advertise_address: None,
        });

        // A node with no TLS listener writes the bytes the earlier release
        // wrote, so a cluster that never turns TLS on leaves a raft log that
        // release could still replay unchanged.
        let node = Node::new("1", Endpoint::new("localhost", 28103))
            .with_grpc_advertise_address(Some("0.0.0.0:9191"));

        let mut plaintext_bytes = Vec::new();
        rmp_serde::encode::write_named(&mut plaintext_bytes, &node)?;

        assert_eq!(old_bytes, plaintext_bytes);

        // A node with a TLS listener writes one key more, and the earlier
        // release drops that key instead of failing to replay the entry.
        let with_tls = node.with_raft_tls_advertise_address(Some("tls.example.com:29004"));

        let mut tls_bytes = Vec::new();
        rmp_serde::encode::write_named(&mut tls_bytes, &with_tls)?;

        let back: OldNode = rmp_serde::from_slice(&tls_bytes)?;

        assert_eq!(back, old);

        Ok(())
    }

    #[test]
    fn test_display() {
        let node = Node::new("1", Endpoint::new("localhost", 28103))
            .with_grpc_advertise_address(Some("0.0.0.0:9191"));

        assert_eq!(
            node.to_string(),
            "id=1 raft=localhost:28103 grpc=0.0.0.0:9191 raft_tls="
        );

        let with_tls = node.with_raft_tls_advertise_address(Some("tls.example.com:29004"));

        assert_eq!(
            with_tls.to_string(),
            "id=1 raft=localhost:28103 grpc=0.0.0.0:9191 raft_tls=tls.example.com:29004"
        );
    }
}
