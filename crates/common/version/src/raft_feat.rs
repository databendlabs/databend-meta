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

use crate::feature_span::FeatureSet;

/// A named capability in the raft inter-node protocol.
///
/// Each variant represents a feature whose lifetime is tracked in [`crate::RaftSpec`]
/// for version compatibility calculation between raft peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RaftFeature {
    /// Legacy binary-encoded append entries RPC.
    AppendEntries,

    /// Legacy binary-encoded vote RPC.
    Vote,

    /// Typed protobuf vote RPC.
    VoteV001,

    /// Typed streaming append RPC with `LogEntry` entries.
    AppendV001,

    /// Binary chunk snapshot transfer.
    SnapshotV003,

    /// KV entry streaming snapshot transfer.
    SnapshotV004,

    /// Leadership transfer RPC.
    TransferLeader,
}

impl RaftFeature {
    /// Returns all feature variants.
    pub const fn all() -> &'static [RaftFeature] {
        &[
            RaftFeature::AppendEntries,
            RaftFeature::Vote,
            RaftFeature::VoteV001,
            RaftFeature::AppendV001,
            RaftFeature::SnapshotV003,
            RaftFeature::SnapshotV004,
            RaftFeature::TransferLeader,
        ]
    }

    /// Returns the string identifier for this feature.
    pub const fn as_str(&self) -> &'static str {
        match self {
            RaftFeature::AppendEntries => "raft/append_entries",
            RaftFeature::Vote => "raft/vote",
            RaftFeature::VoteV001 => "raft/vote_v001",
            RaftFeature::AppendV001 => "raft/append_v001",
            RaftFeature::SnapshotV003 => "raft/snapshot_v003",
            RaftFeature::SnapshotV004 => "raft/snapshot_v004",
            RaftFeature::TransferLeader => "raft/transfer_leader",
        }
    }
}

impl FeatureSet for RaftFeature {
    fn all() -> &'static [Self] {
        RaftFeature::all()
    }
}

impl fmt::Display for RaftFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
