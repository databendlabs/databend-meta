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

//! Changelog of gRPC version compatibility.
//!
//! Records the minimum compatible client and server versions at each package
//! version where compatibility changed. Consumers use
//! `.range(..=v).last()` to find the applicable entry for a given version.

use std::collections::BTreeMap;

use crate::version::Version;

/// Min-compatible client and server versions at a given package version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcVersionCompat {
    pub min_client: Version,
    pub min_server: Version,
}

/// Returns a changelog mapping each package version (where compatibility
/// changed) to the min-compatible client and server versions at that point.
///
/// Only versions where `min_client` or `min_server` changed are included.
pub fn grpc_changelog() -> BTreeMap<Version, GrpcVersionCompat> {
    const fn ver(major: u64, minor: u64, patch: u64) -> Version {
        Version::new(major, minor, patch)
    }

    let mut m = BTreeMap::new();

    // 260205.0.0: client adds ExpireInMillis, PutSequential (server since 1.2.770)
    m.insert(ver(260205, 0, 0), GrpcVersionCompat {
        min_client: ver(1, 2, 676),
        min_server: ver(1, 2, 770),
    });

    // 260214.0.0: client adds KvGetMany(srv:1.2.869), TransactionPrevValue(srv:1.2.304)
    m.insert(ver(260214, 0, 0), GrpcVersionCompat {
        min_client: ver(1, 2, 676),
        min_server: ver(1, 2, 869),
    });

    // 260217.0.0: client adds KvTransactionPutMatchSeq (server since 260217.0.0)
    #[cfg(feature = "txn-put-match-seq")]
    m.insert(ver(260217, 0, 0), GrpcVersionCompat {
        min_client: ver(1, 2, 676),
        min_server: ver(260217, 0, 0),
    });

    // 260217.0.0: without txn-put-match-seq, no new client requirement
    #[cfg(not(feature = "txn-put-match-seq"))]
    m.insert(ver(260217, 0, 0), GrpcVersionCompat {
        min_client: ver(1, 2, 676),
        min_server: ver(1, 2, 869),
    });

    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MIN_CLIENT_VERSION;
    use crate::MIN_SERVER_VERSION;

    #[test]
    fn test_grpc_changelog_last_entry_matches_current() {
        let changelog = grpc_changelog();
        let (_, last) = changelog.iter().next_back().unwrap();

        assert_eq!(
            last.min_client, MIN_CLIENT_VERSION,
            "last changelog min_client must match MIN_CLIENT_VERSION"
        );

        assert_eq!(
            last.min_server, MIN_SERVER_VERSION,
            "last changelog min_server must match MIN_SERVER_VERSION"
        );
    }

    #[test]
    fn test_grpc_changelog_monotonic() {
        let changelog = grpc_changelog();
        let mut prev_client = Version::min();
        let mut prev_server = Version::min();

        for (ver, compat) in &changelog {
            assert!(
                compat.min_client >= prev_client,
                "min_client decreased at {}: {:?} < {:?}",
                ver,
                compat.min_client,
                prev_client
            );
            assert!(
                compat.min_server >= prev_server,
                "min_server decreased at {}: {:?} < {:?}",
                ver,
                compat.min_server,
                prev_server
            );
            prev_client = compat.min_client;
            prev_server = compat.min_server;
        }
    }
}
