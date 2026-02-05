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

//! Feature negotiation between meta-client and meta-server.
//!
//! This module defines the features that the meta-server supports and the
//! versions in which they were added. Clients use this information to
//! negotiate capabilities with the server during handshake.
//!
//! # Feature Lifecycle
//!
//! Each feature has a lifecycle defined by:
//! - `since`: The version when the feature was added (inclusive)
//! - `until`: The version when the feature was removed (exclusive), or
//!   `Version::max()` if still supported
//!
//! A server supports a feature if: `since <= server_version < until`
//!
//! # Example
//!
//! ```
//! use databend_meta_version::features::Changes;
//!
//! let changes = Changes::load();
//! let min_server = changes.min_compatible_server_version();
//! let min_client = changes.min_compatible_client_version();
//! ```

use std::collections::BTreeMap;
use std::fmt;

/// Identifies a specific capability that can be negotiated between client and server.
///
/// Each variant represents a distinct feature in the meta-service protocol.
/// Features are used during handshake to determine what operations are
/// supported by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Feature {
    /// Basic KV API for key-value operations.
    KvApi,

    /// KvApi variant GetKv
    KvApiGetKv,

    /// KvApi variant MGetKv
    KvApiMGetKv,

    /// KvApi variant ListKv
    KvApiListKv,

    /// Stream API for reading key-value pairs: `kv_read_v1()`.
    KvReadV1,

    /// Transaction support for multi-key atomic operations.
    Transaction,

    /// TxnReply use `error` field to return error
    TransactionReplyError,

    /// Allow to set the TTL for a key-value to put in a transaction
    TransactionPutWithTtl,

    /// Count by a prefix as a transaction condition
    TransactionConditionKeysPrefix,

    /// To specify a complex bool expression and corresponding operations
    /// `TxnRequest::operations`
    TransactionOperations,

    /// `Operation::AsIs`: keep value untouched, maybe update the value meta.
    OperationAsIs,

    /// Basic export API for dumping server data.
    Export,

    /// Enhanced export API allowing client to specify export chunk size: `export_v1()`.
    ExportV1,

    /// Watch API for subscribing to key change events.
    Watch,

    /// Watch stream flushes all keys in a range at the beginning.
    ///
    /// Adds `WatchRequest::initial_flush`.
    WatchInitialFlush,

    /// Watch response includes initialization flag.
    ///
    /// Adds `WatchResponse::is_initialization` to indicate whether the event
    /// is an initialization event or a change event.
    WatchResponseIsInit,

    /// Get cluster member list.
    MemberList,

    /// Get cluster status information.
    GetClusterStatus,

    /// Get client connection info including server time.
    GetClientInfo,

    /// Transaction put response includes current state.
    ///
    /// Adds `TxnPutResponse::current` - the state of the key after the put operation.
    PutResponseCurrent,

    /// Fetch and add u64 operation in transactions (deprecated, use `FetchIncreaseU64`).
    ///
    /// Adds `FetchAddU64` operation to `TxnOp`.
    FetchAddU64,

    /// Adaptive expiration time supporting both seconds and milliseconds.
    ///
    /// `expire_at` accepts both seconds and milliseconds timestamps. Before this, it is just in seconds.
    ExpireInMillis,

    /// Sequential put operation for generating monotonic sequence key.
    PutSequential,

    /// Store raft-log proposing time in KV metadata.
    ///
    /// Adds `proposed_at_ms` field to `KVMeta`.
    ProposedAtMs,

    /// Enhanced fetch-and-increase operation with max value support.
    ///
    /// Renamed from `FetchAddU64`, adds `max_value` parameter.
    FetchIncreaseU64,

    /// List keys with pagination support via streaming.
    ///
    /// `kv_list` gRPC API in protobuf with `limit` parameter, returns stream.
    KvList,

    /// Get multiple keys with streaming request and response.
    ///
    /// `kv_get_many` gRPC API in protobuf, receives stream, returns stream.
    KvGetMany,
}

impl Feature {
    /// Returns all feature variants.
    pub const fn all() -> &'static [Feature] {
        &[
            Feature::KvApi,
            Feature::KvApiGetKv,
            Feature::KvApiMGetKv,
            Feature::KvApiListKv,
            Feature::KvReadV1,
            Feature::Transaction,
            Feature::TransactionReplyError,
            Feature::TransactionPutWithTtl,
            Feature::TransactionConditionKeysPrefix,
            Feature::TransactionOperations,
            Feature::OperationAsIs,
            Feature::Export,
            Feature::ExportV1,
            Feature::Watch,
            Feature::WatchInitialFlush,
            Feature::WatchResponseIsInit,
            Feature::MemberList,
            Feature::GetClusterStatus,
            Feature::GetClientInfo,
            Feature::PutResponseCurrent,
            Feature::FetchAddU64,
            Feature::ExpireInMillis,
            Feature::PutSequential,
            Feature::ProposedAtMs,
            Feature::FetchIncreaseU64,
            Feature::KvList,
            Feature::KvGetMany,
        ]
    }

    /// Returns the wire-format string representation of this feature.
    ///
    /// This string is used in the protocol handshake to identify features.
    /// It matches the gRPC method or capability name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Feature::KvApi => "kv_api",
            Feature::KvApiGetKv => "kv_api/get_kv",
            Feature::KvApiMGetKv => "kv_api/mget_kv",
            Feature::KvApiListKv => "kv_api/list_kv",
            Feature::KvReadV1 => "kv_read_v1",
            Feature::Transaction => "transaction",
            Feature::TransactionReplyError => "transaction/reply_error",
            Feature::TransactionPutWithTtl => "transaction/put_with_ttl",
            Feature::TransactionConditionKeysPrefix => "transaction/condition_keys_prefix",
            Feature::TransactionOperations => "transaction/operations",
            Feature::OperationAsIs => "operation/as_is",
            Feature::Export => "export",
            Feature::ExportV1 => "export_v1",
            Feature::Watch => "watch",
            Feature::WatchInitialFlush => "watch/initial_flush",
            Feature::WatchResponseIsInit => "watch/init_flag",
            Feature::MemberList => "member_list",
            Feature::GetClusterStatus => "get_cluster_status",
            Feature::GetClientInfo => "get_client_info",
            Feature::PutResponseCurrent => "put_response/current",
            Feature::FetchAddU64 => "fetch_add_u64",
            Feature::ExpireInMillis => "expire_in_millis",
            Feature::PutSequential => "put_sequential",
            Feature::ProposedAtMs => "proposed_at_ms",
            Feature::FetchIncreaseU64 => "fetch_increase_u64",
            Feature::KvList => "kv_list",
            Feature::KvGetMany => "kv_get_many",
        }
    }
}

impl std::fmt::Display for Feature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<Feature> for &'static str {
    fn from(name: Feature) -> Self {
        name.as_str()
    }
}

/// A three-component version number (major, minor, patch).
///
/// Used to track when features were added or removed.
/// Unlike `semver::Version`, this is a simple const-compatible struct
/// for use in static feature definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Version {
    major: u64,
    minor: u64,
    patch: u64,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl Version {
    /// Creates a new version with the given components.
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Version {
            major,
            minor,
            patch,
        }
    }

    pub const fn to_digit(&self) -> u64 {
        self.major * 1_000_000 + self.minor * 1_000 + self.patch
    }

    pub const fn to_semver(&self) -> semver::Version {
        semver::Version::new(self.major, self.minor, self.patch)
    }

    /// Returns the major version component.
    pub const fn major(&self) -> u64 {
        self.major
    }

    /// Returns the minor version component.
    pub const fn minor(&self) -> u64 {
        self.minor
    }

    /// Returns the patch version component.
    pub const fn patch(&self) -> u64 {
        self.patch
    }

    /// Returns the version as a tuple `(major, minor, patch)`.
    pub const fn as_tuple(&self) -> (u64, u64, u64) {
        (self.major, self.minor, self.patch)
    }

    /// Returns the minimum possible version (0.0.0).
    ///
    /// Used as the initial value when calculating minimum compatible versions.
    pub const fn min() -> Self {
        Version {
            major: 0,
            minor: 0,
            patch: 0,
        }
    }

    /// Returns the maximum possible version.
    ///
    /// Used as the default `until` value for features that have not been
    /// removed (i.e., they are still supported in current versions).
    pub const fn max() -> Self {
        Version {
            major: u64::MAX,
            minor: u64::MAX,
            patch: u64::MAX,
        }
    }
}

impl From<Version> for semver::Version {
    fn from(v: Version) -> Self {
        semver::Version::new(v.major, v.minor, v.patch)
    }
}

impl From<&semver::Version> for Version {
    fn from(v: &semver::Version) -> Self {
        Version::new(v.major, v.minor, v.patch)
    }
}

impl From<semver::Version> for Version {
    fn from(v: semver::Version) -> Self {
        Version::new(v.major, v.minor, v.patch)
    }
}

/// Defines the lifecycle of a feature within the meta-service protocol.
///
/// Tracks when a feature was added and when (or if) it was removed.
/// This allows clients to determine server capabilities during handshake.
pub struct FeatureLifetime {
    /// The feature being described.
    pub feature: Feature,
    /// The version when this feature was added (inclusive).
    pub since: Version,
    /// The version when this feature was removed (exclusive).
    ///
    /// If the feature is still supported, this is `Version::max()`.
    pub until: Version,
}

impl FeatureLifetime {
    /// Creates a new feature lifetime with the given feature and version.
    ///
    /// The `until` field is initialized to `Version::max()`, indicating
    /// the feature is still supported. Use this constructor for features
    /// that have not been removed.
    ///
    /// # Arguments
    ///
    /// * `feature` - The feature being defined
    /// * `since` - The version when this feature became available
    pub const fn new(feature: Feature, since: Version) -> Self {
        FeatureLifetime {
            feature,
            since,
            until: Version::max(),
        }
    }

    pub const fn until(mut self, until: Version) -> Self {
        self.until = until;
        self
    }

    pub const fn until3(self, until_major: u64, until_minor: u64, until_patch: u64) -> Self {
        self.until(Version::new(until_major, until_minor, until_patch))
    }

    /// Returns true if feature is active at the given version.
    ///
    /// A feature is active when: `since <= version < until`
    pub fn is_active_at(&self, version: Version) -> bool {
        self.since <= version && version < self.until
    }
}

type F = Feature;

/// Helper function to create a version with less boilerplate.
const fn ver(major: u64, minor: u64, patch: u64) -> Version {
    Version::new(major, minor, patch)
}

/// Returns the current crate version as a `Version`.
///
/// This uses the version from `CARGO_PKG_VERSION` at compile time.
pub fn current_version() -> Version {
    *crate::version()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    Server,
    Client,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    Since,
    Until,
}

/// Tracks feature changes across versions for compatibility calculation.
///
/// This struct maintains the history of when features were added or removed
/// on both server and client sides, enabling calculation of minimum compatible
/// versions.
pub struct Changes {
    server_features: BTreeMap<F, FeatureLifetime>,
    client_features: BTreeMap<F, FeatureLifetime>,
}

impl Changes {
    /// Creates a new Changes instance with all feature history.
    pub fn load() -> Self {
        Self::new()
    }
}

impl Changes {
    fn apply(&mut self, role: Role, feature: F, op: Op, version: Version) {
        let feas = if role == Role::Server {
            &mut self.server_features
        } else {
            &mut self.client_features
        };

        if !feas.contains_key(&feature) {
            feas.insert(
                feature,
                FeatureLifetime::new(feature, Version::new(0, 0, 0)),
            );
        }

        let fea = feas.get_mut(&feature).unwrap();
        if op == Op::Since {
            debug_assert!(fea.since == Version::new(0, 0, 0));
            debug_assert!(fea.until == Version::max());

            fea.since = version;
        } else {
            debug_assert!(fea.since != Version::new(0, 0, 0));
            debug_assert!(fea.until == Version::max());

            fea.until = version;
        }
    }

    /// Server adds a provided feature, the feature can be used since this version, inclusive.
    fn server_adds(&mut self, feature: F, version: Version) {
        self.apply(Role::Server, feature, Op::Since, version);
    }

    /// Server removed a provided feature. the feature can not be used since this version, inclusive.
    fn server_removes(&mut self, feature: F, version: Version) {
        self.apply(Role::Server, feature, Op::Until, version);
    }

    /// Client adds a required feature, since this version, inclusive,
    fn client_adds(&mut self, feature: F, version: Version) {
        self.apply(Role::Client, feature, Op::Since, version);
    }

    /// Client removed a required feature, since this version, inclusive, the feature is no longer used.
    fn client_removes(&mut self, feature: F, version: Version) {
        self.apply(Role::Client, feature, Op::Until, version);
    }

    fn new() -> Self {
        let mut chs = Changes {
            server_features: BTreeMap::new(),
            client_features: BTreeMap::new(),
        };

        {
            // 2023-10-17: since 1.2.163:
            // 🖥 server: add stream api kv_read_v1().
            // (fake version for features already provided for a while)
            chs.server_adds(F::OperationAsIs, ver(1, 2, 163));
            chs.server_adds(F::KvApi, ver(1, 2, 163));
            chs.server_adds(F::KvApiGetKv, ver(1, 2, 163));
            chs.server_adds(F::KvApiMGetKv, ver(1, 2, 163));
            chs.server_adds(F::KvApiListKv, ver(1, 2, 163));
            chs.server_adds(F::KvReadV1, ver(1, 2, 163));

            chs.client_adds(F::OperationAsIs, ver(1, 2, 163));
            chs.client_adds(F::KvApi, ver(1, 2, 163));
            chs.client_adds(F::KvApiGetKv, ver(1, 2, 163));
            chs.client_adds(F::KvApiMGetKv, ver(1, 2, 163));
            chs.client_adds(F::KvApiListKv, ver(1, 2, 163));

            // 2023-10-20: since 1.2.176:
            // 👥 client: call stream api kv_read_v1(), revert to 1.1.32 if server < 1.2.163
            chs.client_adds(F::KvReadV1, ver(1, 2, 176));

            // 2023-12-16: since 1.2.258:
            // 🖥 server: add ttl to TxnPutRequest and Upsert
            chs.server_adds(F::Transaction, ver(1, 2, 258));
            chs.server_adds(F::TransactionReplyError, ver(1, 2, 258));
            chs.server_adds(F::TransactionPutWithTtl, ver(1, 2, 258));

            chs.client_adds(F::TransactionReplyError, ver(1, 2, 258));

            // 1.2.259: (fake version, 1.2.258 binary outputs 1.2.257)
            // 🖥 server: add Export, Watch, MemberList, GetClusterStatus, GetClientInfo
            chs.server_adds(F::Export, ver(1, 2, 259));
            chs.server_adds(F::Watch, ver(1, 2, 259));
            chs.server_adds(F::MemberList, ver(1, 2, 259));
            chs.server_adds(F::GetClusterStatus, ver(1, 2, 259));
            chs.server_adds(F::GetClientInfo, ver(1, 2, 259));

            chs.client_adds(F::Transaction, ver(1, 2, 259));
            chs.client_adds(F::Export, ver(1, 2, 259));
            chs.client_adds(F::Watch, ver(1, 2, 259));
            chs.client_adds(F::MemberList, ver(1, 2, 259));
            chs.client_adds(F::GetClusterStatus, ver(1, 2, 259));
            chs.client_adds(F::GetClientInfo, ver(1, 2, 259));

            // 2024-01-07: since 1.2.287:
            // 👥 client: remove calling RPC kv_api() with MetaGrpcReq::GetKV/MGetKV/ListKV
            chs.client_removes(F::KvApiGetKv, ver(1, 2, 287));
            chs.client_removes(F::KvApiMGetKv, ver(1, 2, 287));
            chs.client_removes(F::KvApiListKv, ver(1, 2, 287));

            // 2024-01-25: since 1.2.315:
            // 🖥 server: add export_v1() to let client specify export chunk size
            chs.server_adds(F::ExportV1, ver(1, 2, 315));

            // 2024-03-04: since 1.2.361:
            // 👥 client: `MetaSpec` use `ttl`, remove `expire_at`, require 1.2.258
            chs.client_adds(F::TransactionPutWithTtl, ver(1, 2, 361));

            // 2024-11-22: since 1.2.663:
            // 🖥 server: remove MetaGrpcReq::GetKV/MGetKV/ListKV
            chs.server_removes(F::KvApiGetKv, ver(1, 2, 663));
            chs.server_removes(F::KvApiMGetKv, ver(1, 2, 663));
            chs.server_removes(F::KvApiListKv, ver(1, 2, 663));

            // 2024-11-23: since 1.2.663:
            // 👥 client: remove use of Operation::AsIs
            chs.client_removes(F::OperationAsIs, ver(1, 2, 663));

            // 2024-12-16: since 1.2.674:
            // 🖥 server: add txn_condition::Target::KeysWithPrefix
            chs.server_adds(F::TransactionConditionKeysPrefix, ver(1, 2, 674));

            // 2024-12-20: since 1.2.676:
            // 🖥 server: add TxnRequest::operations
            // 🖥 server: no longer use TxnReply::error
            chs.server_adds(F::TransactionOperations, ver(1, 2, 676));

            // 👥 client: no longer use TxnReply::error
            chs.client_removes(F::TransactionReplyError, ver(1, 2, 676));

            // 2024-12-26: since 1.2.677:
            // 🖥 server: add WatchRequest::initial_flush
            chs.server_adds(F::WatchInitialFlush, ver(1, 2, 677));

            // 2025-04-15: since 1.2.726:
            // 👥 client: requires 1.2.677
            chs.client_adds(F::WatchInitialFlush, ver(1, 2, 726));
            chs.client_adds(F::WatchResponseIsInit, ver(1, 2, 726));
            chs.client_adds(F::TransactionConditionKeysPrefix, ver(1, 2, 726));
            chs.client_adds(F::TransactionOperations, ver(1, 2, 726));

            // 2025-05-08: since 1.2.736:
            // 🖥 server: add WatchResponse::is_initialization
            chs.server_adds(F::WatchResponseIsInit, ver(1, 2, 736));

            // 2025-06-09: since 1.2.755:
            // 🖥 server: remove TxnReply::error
            chs.server_removes(F::TransactionReplyError, ver(1, 2, 755));

            // 2025-06-11: since 1.2.756:
            // 🖥 server: add TxnPutResponse::current
            chs.server_adds(F::PutResponseCurrent, ver(1, 2, 756));

            chs.client_adds(F::PutResponseCurrent, ver(1, 2, 756));

            // 2025-06-24: since 1.2.764:
            // 🖥 server: add FetchAddU64 operation to the TxnOp
            chs.server_adds(F::FetchAddU64, ver(1, 2, 764));

            // 2025-07-03: since 1.2.770:
            // 🖥 server: adaptive expire_at support both seconds and milliseconds
            chs.server_adds(F::ExpireInMillis, ver(1, 2, 770));
            // 2025-07-04: since 1.2.770:
            // 🖥 server: add PutSequential
            chs.server_adds(F::PutSequential, ver(1, 2, 770));

            // 2025-09-27: since 1.2.821:
            // 👥 client: require 1.2.764(yanked), use 1.2.768, for FetchAddU64
            chs.client_adds(F::FetchAddU64, ver(1, 2, 821));

            // 2025-09-30: since 1.2.823:
            // 🖥 server: store raft-log proposing time proposed_at_ms in KVMeta
            chs.server_adds(F::ProposedAtMs, ver(1, 2, 823));

            // 2025-09-27: since 1.2.823:
            // 👥 client: require 1.2.770, remove calling RPC kv_api
            chs.client_removes(F::KvApi, ver(1, 2, 823));

            // 2025-10-16: since 1.2.828:
            // 🖥 server: rename FetchAddU64 to FetchIncreaseU64, add max_value
            chs.server_adds(F::FetchIncreaseU64, ver(1, 2, 828));

            // 2026-01-12: since 1.2.869:
            // 🖥 server: add kv_list gRPC API
            chs.server_adds(F::KvList, ver(1, 2, 869));
            // 2026-01-13: since 1.2.869:
            // 🖥 server: add kv_get_many gRPC API
            chs.server_adds(F::KvGetMany, ver(1, 2, 869));

            // 2026-02-05:
            // 👥 client: allows the application to use expire in millis
            chs.client_adds(F::ExpireInMillis, ver(260205, 0, 0));
            chs.client_adds(F::PutSequential, ver(260205, 0, 0));

            // client not yet using these features
            chs.client_adds(F::ExportV1, Version::max());
            chs.client_adds(F::ProposedAtMs, Version::max());
            chs.client_adds(F::FetchIncreaseU64, Version::max());
            chs.client_adds(F::KvList, Version::max());
            chs.client_adds(F::KvGetMany, Version::max());
        }

        Self::assert_all_features(&chs.server_features);
        Self::assert_all_features(&chs.client_features);

        chs
    }

    fn assert_all_features(features: &BTreeMap<F, FeatureLifetime>) {
        for feature in F::all() {
            assert!(
                features.contains_key(feature),
                "Missing feature: {:?}",
                feature
            );
        }
    }

    /// Calculate minimum compatible server version for current client.
    ///
    /// For each feature the client **requires** at the current version,
    /// the server must provide it. Returns the maximum `server.since`
    /// across all required features.
    ///
    /// See [Compatibility Algorithm](./compatibility_algorithm.md)
    pub fn min_compatible_server_version(&self) -> Version {
        let current = current_version();
        let mut min_server = Version::min();

        for feature in F::all() {
            let client_lt = self.client_features.get(feature).unwrap();
            let server_lt = self.server_features.get(feature).unwrap();

            // If client requires this feature at current version
            if client_lt.is_active_at(current) {
                min_server = min_server.max(server_lt.since);
            }
        }

        min_server
    }

    /// Calculate minimum compatible client version for current server.
    ///
    /// For each feature the server **removed** at the current version,
    /// the client must have stopped requiring it. Returns the maximum
    /// `client.until` across all removed features.
    ///
    /// See [Compatibility Algorithm](./compatibility_algorithm.md)
    pub fn min_compatible_client_version(&self) -> Version {
        let current = current_version();
        let mut min_client = Version::min();

        for feature in F::all() {
            let client_lt = self.client_features.get(feature).unwrap();
            let server_lt = self.server_features.get(feature).unwrap();

            // If server removed this feature at current version
            if !server_lt.is_active_at(current) {
                min_client = min_client.max(client_lt.until);
            }
        }

        min_client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_changes_includes_all_features() {
        // Changes::new() calls assert_all_features() internally,
        // which verifies all Feature::all() variants are present
        let changes = Changes::new();

        Changes::assert_all_features(&changes.server_features);
        Changes::assert_all_features(&changes.client_features);
    }

    #[test]
    fn test_version_min() {
        let min = Version::min();
        assert_eq!(min, Version::new(0, 0, 0));
        assert!(min < Version::new(0, 0, 1));
        assert!(min < Version::new(1, 0, 0));
    }

    #[test]
    fn test_version_ordering() {
        assert!(Version::min() < Version::new(1, 2, 163));
        assert!(Version::new(1, 2, 163) < Version::max());
        assert!(Version::min() < Version::max());
    }

    #[test]
    fn test_feature_lifetime_is_active_at() {
        let lt = FeatureLifetime::new(Feature::KvApi, Version::new(1, 2, 163));

        // Before since: not active
        assert!(!lt.is_active_at(Version::new(1, 2, 162)));

        // At since: active
        assert!(lt.is_active_at(Version::new(1, 2, 163)));

        // After since: active (until is max)
        assert!(lt.is_active_at(Version::new(1, 2, 164)));
        assert!(lt.is_active_at(Version::new(2, 0, 0)));
    }

    #[test]
    fn test_feature_lifetime_is_active_at_with_until() {
        let lt = FeatureLifetime::new(Feature::KvApi, Version::new(1, 2, 163))
            .until(Version::new(1, 2, 287));

        // Before since: not active
        assert!(!lt.is_active_at(Version::new(1, 2, 162)));

        // At since: active
        assert!(lt.is_active_at(Version::new(1, 2, 163)));

        // Between since and until: active
        assert!(lt.is_active_at(Version::new(1, 2, 200)));
        assert!(lt.is_active_at(Version::new(1, 2, 286)));

        // At until: not active (until is exclusive)
        assert!(!lt.is_active_at(Version::new(1, 2, 287)));

        // After until: not active
        assert!(!lt.is_active_at(Version::new(1, 2, 288)));
    }

    #[test]
    fn test_min_compatible_server_version() {
        let changes = Changes::new();
        let min_server = changes.min_compatible_server_version();

        assert_eq!(min_server, Version::new(1, 2, 770));
    }

    #[test]
    fn test_min_compatible_client_version() {
        let changes = Changes::new();
        let min_client = changes.min_compatible_client_version();

        // Server (at 1.2.873) has removed:
        // - KvApiGetKv/MGetKv/ListKv at 1.2.663 → client stopped at 1.2.287
        // - TransactionReplyError at 1.2.755 → client stopped at 1.2.676
        // max(1.2.287, 1.2.676) = 1.2.676
        assert_eq!(min_client, Version::new(1, 2, 676));
    }

    #[test]
    fn test_current_version() {
        let current = current_version();
        // Should match the crate version
        let sv = crate::version();
        assert_eq!(current, *sv);
    }
}
