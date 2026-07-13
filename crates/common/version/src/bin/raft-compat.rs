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

//! Prints raft inter-node protocol compatibility per release.
//!
//! For every node version, prints the half-open range `[min, max)` of peer
//! versions it can form a working raft connection with:
//!
//! ```text
//! <node-version>    [<min-compatible-inclusive>, <max-compatible-exclusive>)
//! ```
//!
//! Run with: `cargo run -p databend-meta-version --bin raft-compat`

use databend_meta_version::RaftSpec;
use databend_meta_version::Version;

fn main() {
    let spec = RaftSpec::load();

    for version in candidate_versions() {
        let (min, max) = spec.compatible_peer_range(version);
        // Pad the stringified version: `Version`'s `Display` ignores format width.
        let version = version.to_string();
        let max = fmt_bound(max);
        println!("{version:<12} [{min}, {max})");
    }
}

/// Node versions to report, oldest first.
///
/// Two sources:
/// - Historical databend monorepo `1.x.x` releases (not tagged in this repo),
///   generated as contiguous patch ranges.
/// - CalVer releases tagged in this repo (one tag per minor, patch `0`).
///   Extend these when new releases are tagged.
fn candidate_versions() -> Vec<Version> {
    let mut versions = Vec::new();

    for patch in 0..=63 {
        versions.push(Version::new(1, 0, patch));
    }
    for patch in 0..=76 {
        versions.push(Version::new(1, 1, patch));
    }
    for patch in 0..=879 {
        versions.push(Version::new(1, 2, patch));
    }

    for minor in 0..=10 {
        versions.push(Version::new(260205, minor, 0));
    }
    for minor in 0..=10 {
        versions.push(Version::new(260312, minor, 0));
    }
    for minor in 0..=3 {
        versions.push(Version::new(260428, minor, 0));
    }
    for minor in 0..=5 {
        versions.push(Version::new(260512, minor, 0));
    }
    for minor in 0..=1 {
        versions.push(Version::new(260628, minor, 0));
    }
    versions.push(Version::new(260712, 0, 0));

    versions
}

/// Formats an exclusive upper bound, rendering [`Version::max`] as `∞`.
fn fmt_bound(version: Version) -> String {
    if version == Version::max() {
        "∞".to_string()
    } else {
        version.to_string()
    }
}
