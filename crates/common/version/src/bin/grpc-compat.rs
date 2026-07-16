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

//! Prints gRPC client/server protocol compatibility per CalVer release.
//!
//! For each CalVer major release, prints the minimum compatible client and
//! server versions, matching the format of databend's
//! `external-meta-min-compatibles.txt`:
//!
//! ```text
//! version         MIN_CLIENT             MIN_SERVER
//! ```
//!
//! `MIN_CLIENT` is the oldest client that can connect to a server of that
//! version; `MIN_SERVER` is the oldest server that client can connect to.
//!
//! Run with: `cargo run -p databend-meta-version --bin grpc-compat`

use databend_meta_version::GrpcSpec;
use databend_meta_version::Version;

fn main() {
    let spec = GrpcSpec::load();

    println!("version         MIN_CLIENT             MIN_SERVER");
    println!("{}", "-".repeat(73));

    for version in candidate_versions() {
        let min_client = spec.compatible_client_range(version).0.to_string();
        let min_server = spec.compatible_server_range(version).0.to_string();
        // `Version`'s `Display` ignores format width; stringify before padding.
        let version = version.to_string();
        println!("{version:<16}{min_client:<23}{min_server}");
    }
}

/// CalVer major releases to report, oldest first.
///
/// Extend this when a new CalVer major is tagged.
fn candidate_versions() -> Vec<Version> {
    vec![
        Version::new(260205, 0, 0),
        Version::new(260312, 0, 0),
        Version::new(260428, 0, 0),
        Version::new(260512, 0, 0),
        Version::new(260628, 0, 0),
        Version::new(260629, 0, 0),
    ]
}
