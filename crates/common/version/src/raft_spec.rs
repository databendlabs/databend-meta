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

//! Feature history and version compatibility for raft inter-node RPCs.
//!
//! Mirrors the client-server [`crate::GrpcSpec`] but tracks raft protocol
//! capabilities between cluster nodes.
//!
//! - **Raft server** = node receiving raft RPCs (handler side).
//! - **Raft client** = node sending raft RPCs (network side).

use std::collections::BTreeMap;

use crate::feature_span::FeatureSpec;
use crate::feature_span::add;
use crate::feature_span::add_optional;
use crate::feature_span::parse_pkg_version;
use crate::raft_feat::RaftFeature;
use crate::version::Version;

/// Type alias for raft inter-node feature spec.
pub type RaftSpec = FeatureSpec<RaftFeature>;

impl RaftSpec {
    /// Creates a new instance with all feature history for the current build version.
    pub fn load() -> Self {
        Self::new(parse_pkg_version())
    }

    fn new(version: Version) -> Self {
        let mut srv = BTreeMap::new();
        let mut cli = BTreeMap::new();

        type F = RaftFeature;

        const fn ver(major: u64, minor: u64, patch: u64) -> Version {
            Version::new(major, minor, patch)
        }

        {
            // 2021-05-08: since 0.0.120 (databend v0.0.120-nightly):
            // Initial raft implementation with binary-encoded RaftMes RPCs.
            add(&mut srv, F::AppendEntries, ver(0, 0, 120));
            add(&mut srv, F::Vote, ver(0, 0, 120));

            add(&mut cli, F::AppendEntries, ver(0, 0, 120));
            add(&mut cli, F::Vote, ver(0, 0, 120));

            // 2024-06-27: since 1.2.547 (databend v1.2.547-nightly):
            // On-disk state machine with rotbl snapshot format.
            add(&mut srv, F::SnapshotV003, ver(1, 2, 547));
            // 📡 raft client: optionally used since 1.2.547; required since 1.2.769.
            add_optional(&mut cli, F::SnapshotV003, ver(1, 2, 547));

            // 2025-07-03: since 1.2.769:
            // 📡 raft client: SnapshotV003 becomes required.
            add(&mut cli, F::SnapshotV003, ver(1, 2, 769));

            // 2024-08-07: since 1.2.599 (databend v1.2.599-nightly):
            // Leadership transfer RPC.
            add(&mut srv, F::TransferLeader, ver(1, 2, 599));
            // 📡 raft client: optionally used since 1.2.599; transfer_leader is a
            // cluster management action that doesn't affect normal raft operation.
            add_optional(&mut cli, F::TransferLeader, ver(1, 2, 599));

            // 2025-07-20: since 1.2.777 (databend v1.2.777-nightly):
            // Typed protobuf vote RPC (with fallback to legacy vote).
            add(&mut srv, F::VoteV001, ver(1, 2, 777));
            // 📡 raft client: optionally used since 1.2.777; falls back to legacy vote
            // if peer doesn't support it.
            add_optional(&mut cli, F::VoteV001, ver(1, 2, 777));

            // 2025-09-24: since 1.2.818 (databend v1.2.818-nightly):
            // KV entry streaming snapshot transfer.
            add(&mut srv, F::SnapshotV004, ver(1, 2, 818));
            // 📡 raft client: optionally used since 1.2.818; falls back to SnapshotV003
            // if peer doesn't support it.
            add_optional(&mut cli, F::SnapshotV004, ver(1, 2, 818));

            // 2026-05-12: since 260512.0.0 (this repo):
            // Streaming AppendV002 RPC carrying the full `Cmd` schema.
            //
            // Replaces the never-shipped `AppendV001`. AppendV001 exposed a
            // streaming endpoint without the legacy `UpsertKV`/`Transaction`
            // variants; servers in 260217.x.x..260427.x.x silently dropped
            // unknown oneof tags rather than rejecting them, so a client could
            // not safely detect schema completeness from the endpoint alone.
            // AppendV002 is a new RPC name with the complete schema baked in,
            // making capability detection unambiguous.
            //
            // 🖥 raft server: handles AppendV002.
            add(&mut srv, F::AppendV002, ver(260512, 0, 0));
            // 📡 raft client: optionally used since 260512.0.0; falls back to
            // legacy AppendEntries if the peer doesn't support it.
            add_optional(&mut cli, F::AppendV002, ver(260512, 0, 0));
        }

        FeatureSpec::build(version, srv, cli)
    }

    /// The half-open range `[min, max)` of peer node versions that a node
    /// running `node_version` can form a working raft connection with.
    ///
    /// A raft node is simultaneously a raft-server and a raft-client, so a peer
    /// must be both a server this node's client can talk to and a client this
    /// node's server can serve. The range is therefore the intersection of
    /// `compatible_server_range` and `compatible_client_range` at `node_version`.
    pub fn compatible_peer_range(&self, node_version: Version) -> (Version, Version) {
        let (server_lo, server_hi) = self.compatible_server_range(node_version);
        let (client_lo, client_hi) = self.compatible_client_range(node_version);

        (server_lo.max(client_lo), server_hi.min(client_hi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_changes_includes_all_features() {
        let spec = RaftSpec::load();

        RaftSpec::assert_all_features(spec.server_features());
        RaftSpec::assert_all_features(spec.client_features());
    }

    #[test]
    fn test_min_compatible_raft_server_version() {
        let spec = RaftSpec::load();
        let min_server = spec.min_compatible_server_version();

        assert_eq!(min_server, Version::new(1, 2, 547));
    }

    #[test]
    fn test_min_compatible_raft_client_version() {
        let spec = RaftSpec::load();
        let min_client = spec.min_compatible_client_version();

        assert_eq!(min_client, Version::new(0, 0, 0));
    }

    #[test]
    fn test_compatible_peer_range() {
        let spec = RaftSpec::load();
        let max = Version::max();

        let v = |a, b, c| Version::new(a, b, c);

        // Regime 1: server does not yet provide SnapshotV003. Compatible up to
        // (exclusive) 1.2.769, the first peer that *requires* SnapshotV003.
        assert_eq!(
            spec.compatible_peer_range(v(1, 0, 0)),
            (v(0, 0, 120), v(1, 2, 769))
        );
        assert_eq!(
            spec.compatible_peer_range(v(1, 2, 546)),
            (v(0, 0, 120), v(1, 2, 769))
        );

        // Regime 2: server provides SnapshotV003 (since 1.2.547) but the client
        // does not yet require it (before 1.2.769): compatible with any newer peer.
        assert_eq!(
            spec.compatible_peer_range(v(1, 2, 547)),
            (v(0, 0, 120), max)
        );
        assert_eq!(
            spec.compatible_peer_range(v(1, 2, 768)),
            (v(0, 0, 120), max)
        );

        // Regime 3: client requires SnapshotV003 (since 1.2.769): the floor rises
        // to the server's SnapshotV003 version (1.2.547).
        assert_eq!(
            spec.compatible_peer_range(v(1, 2, 769)),
            (v(1, 2, 547), max)
        );

        // Optional features (VoteV001, SnapshotV004, AppendV002) never cap the
        // upper bound, so every CalVer release stays in regime 3.
        assert_eq!(
            spec.compatible_peer_range(v(260512, 0, 0)),
            (v(1, 2, 547), max)
        );
    }

    #[test]
    fn test_compatible_peer_range_with_removed_feature() {
        use std::collections::BTreeMap;

        use crate::feature_span::remove;

        let v = |a, b, c| Version::new(a, b, c);

        // The live raft data never removes a feature, so build a synthetic spec
        // to exercise the `until` (removal) bounds. One feature is removed on the
        // server at 2.0.0 and dropped by the client at 1.5.0; all others stay
        // active from 1.0.0 onward.
        let removed = RaftFeature::TransferLeader;
        let mut srv = BTreeMap::new();
        let mut cli = BTreeMap::new();
        for &feature in RaftFeature::all() {
            add(&mut srv, feature, v(1, 0, 0));
            add(&mut cli, feature, v(1, 0, 0));
        }
        remove(&mut srv, removed, v(2, 0, 0));
        remove(&mut cli, removed, v(1, 5, 0));
        let spec = FeatureSpec::build(v(1, 0, 0), srv, cli);

        // At 1.2.0 the node's client still uses the feature, so a peer (as server)
        // must still provide it: peers are capped below its removal at 2.0.0.
        assert_eq!(
            spec.compatible_peer_range(v(1, 2, 0)),
            (v(1, 0, 0), v(2, 0, 0))
        );

        // At 2.5.0 the node's server has removed the feature, so a peer (as client)
        // must have dropped it: peers are floored at the client's drop at 1.5.0.
        assert_eq!(
            spec.compatible_peer_range(v(2, 5, 0)),
            (v(1, 5, 0), Version::max())
        );
    }

    #[test]
    fn test_compatible_peer_range_with_removed_optional_feature() {
        use std::collections::BTreeMap;

        use crate::feature_span::remove;

        let v = |a, b, c| Version::new(a, b, c);

        // A feature that was only ever optional for clients (add_optional, never
        // promoted to required) and is later removed on the server must not
        // exclude any peer: optional clients fall back. All other features stay
        // active from 1.0.0 onward.
        let optional = RaftFeature::AppendV002;
        let mut srv = BTreeMap::new();
        let mut cli = BTreeMap::new();
        for &feature in RaftFeature::all() {
            add(&mut srv, feature, v(1, 0, 0));
            if feature == optional {
                add_optional(&mut cli, feature, v(1, 0, 0));
            } else {
                add(&mut cli, feature, v(1, 0, 0));
            }
        }
        remove(&mut srv, optional, v(2, 0, 0));
        let spec = FeatureSpec::build(v(1, 0, 0), srv, cli);

        // A server past the optional feature's removal still serves peers: the
        // range stays [1.0.0, ∞) instead of collapsing to an empty range.
        assert_eq!(
            spec.compatible_peer_range(v(2, 5, 0)),
            (v(1, 0, 0), Version::max())
        );
    }
}
