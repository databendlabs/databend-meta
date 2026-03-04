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

            // 2026-02-26: since 260217.0.0 (this repo):
            // 🖥 raft server: accept streaming AppendV001 RPC
            add(&mut srv, F::AppendV001, ver(260217, 0, 0));
            // 📡 raft client: optionally used since 260217.0.0; falls back to
            // legacy AppendEntries if peer doesn't support it.
            add_optional(&mut cli, F::AppendV001, ver(260217, 0, 0));
        }

        FeatureSpec::build(version, srv, cli)
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
}
