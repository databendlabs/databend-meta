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

//! Feature history and version compatibility for meta-client and meta-server.
//!
//! This module tracks the lifecycle of every protocol feature — when it was
//! added and when (if ever) it was removed — on both the server and client
//! sides. The compatibility algorithm uses this history to compute the
//! minimum peer version required for a successful handshake.
//!
//! # Feature Lifecycle
//!
//! Each feature has a half-open lifetime `[since, until)`:
//! - `since`: the version when the feature was introduced (inclusive).
//! - `until`: the version when the feature was removed (exclusive),
//!   or `Version::max()` if the feature is still active.
//!
//! A feature is active at version V when `since <= V < until`.
//!
//! # Example
//!
//! ```
//! use databend_meta_version::Spec;
//!
//! let spec = Spec::load();
//! let min_server = spec.min_compatible_server_version();
//! let min_client = spec.min_compatible_client_version();
//! ```

use std::collections::BTreeMap;
use std::fmt;

/// A named capability in the meta-service protocol.
///
/// Each variant represents a feature whose lifetime is tracked in [`Spec`]
/// for version compatibility calculation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Feature {
    /// Unary `kv_api()` RPC for key-value operations.
    KvApi,

    /// `kv_api()` sub-operation: get a single key.
    KvApiGetKv,

    /// `kv_api()` sub-operation: get multiple keys.
    KvApiMGetKv,

    /// `kv_api()` sub-operation: list keys by prefix.
    KvApiListKv,

    /// Stream-based `kv_read_v1()` RPC for reading key-value pairs.
    KvReadV1,

    /// `transaction()` RPC for multi-key atomic operations.
    Transaction,

    /// `TxnReply::error` field for returning transaction errors.
    TransactionReplyError,

    /// TTL support in `TxnPutRequest`.
    TransactionPutWithTtl,

    /// Prefix-count condition in `TxnCondition`.
    TransactionConditionKeysPrefix,

    /// Bool-expression operations via `TxnRequest::operations`.
    TransactionOperations,

    /// `Operation::AsIs`: keep value untouched, update only the metadata.
    OperationAsIs,

    /// `export()` RPC for dumping server data.
    Export,

    /// `export_v1()` RPC with configurable chunk size.
    ExportV1,

    /// `watch()` RPC for subscribing to key change events.
    Watch,

    /// `WatchRequest::initial_flush`: flush existing keys at stream start.
    WatchInitialFlush,

    /// `WatchResponse::is_initialization` flag distinguishing init vs change events.
    WatchResponseIsInit,

    /// `member_list()` RPC for cluster membership.
    MemberList,

    /// `get_cluster_status()` RPC for cluster status.
    GetClusterStatus,

    /// `get_client_info()` RPC for connection info and server time.
    GetClientInfo,

    /// `TxnPutResponse::current`: key state after a put operation.
    PutResponseCurrent,

    /// `FetchAddU64` operation in `TxnOp` (deprecated by `FetchIncreaseU64`).
    FetchAddU64,

    /// `expire_at` accepts both seconds and milliseconds timestamps.
    ExpireInMillis,

    /// Sequential put for generating monotonic sequence keys.
    PutSequential,

    /// `KVMeta::proposed_at_ms`: raft-log proposing time in metadata.
    ProposedAtMs,

    /// `FetchIncreaseU64` operation in `TxnOp` with `max_value` support.
    FetchIncreaseU64,

    /// `kv_list()` RPC with pagination via streaming.
    KvList,

    /// `kv_get_many()` RPC with streaming request and response.
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

    /// Returns the string identifier for this feature.
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

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<Feature> for &'static str {
    fn from(name: Feature) -> Self {
        name.as_str()
    }
}

/// A `const`-compatible three-component version number (major, minor, patch).
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

/// The lifetime `[since, until)` of a feature.
pub struct FeatureSpan {
    /// The feature being described.
    pub feature: Feature,

    /// The version when this feature was added (inclusive).
    pub since: Version,

    /// The version when this feature was removed (exclusive).
    ///
    /// If the feature is still supported, this is `Version::max()`.
    pub until: Version,
}

impl FeatureSpan {
    /// Creates a lifetime starting at `since` with no end (`until = Version::max()`).
    pub const fn new(feature: Feature, since: Version) -> Self {
        FeatureSpan {
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

const fn ver(major: u64, minor: u64, patch: u64) -> Version {
    Version::new(major, minor, patch)
}

/// Parses `CARGO_PKG_VERSION` into a [`Version`].
fn parse_pkg_version() -> Version {
    let s = env!("CARGO_PKG_VERSION");
    let s = s.strip_prefix('v').unwrap_or(s);
    let sv = semver::Version::parse(s)
        .unwrap_or_else(|e| panic!("Invalid CARGO_PKG_VERSION: {:?}: {}", s, e));
    Version::from(sv)
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

/// Version and feature information for compatibility calculation.
///
/// Contains the current build version and the full history of when features
/// were added or removed on both server and client sides. Used to calculate
/// minimum compatible versions.
pub struct Spec {
    /// The build version this instance was created for.
    version: Version,

    /// When each feature was added/removed on the server side.
    server_features: BTreeMap<Feature, FeatureSpan>,

    /// When each feature was added/removed on the client side.
    client_features: BTreeMap<Feature, FeatureSpan>,
}

impl Spec {
    /// Creates a new instance with all feature history for the current build version.
    pub fn load() -> Self {
        Self::new(parse_pkg_version())
    }

    /// Returns the build version this instance was created for.
    pub fn version(&self) -> &Version {
        &self.version
    }

    /// Returns the server-side feature spans.
    pub fn server_features(&self) -> &BTreeMap<F, FeatureSpan> {
        &self.server_features
    }

    /// Returns the client-side feature spans.
    pub fn client_features(&self) -> &BTreeMap<F, FeatureSpan> {
        &self.client_features
    }
}

impl Spec {
    fn apply(&mut self, role: Role, feature: F, op: Op, version: Version) {
        let feas = if role == Role::Server {
            &mut self.server_features
        } else {
            &mut self.client_features
        };

        if !feas.contains_key(&feature) {
            feas.insert(feature, FeatureSpan::new(feature, Version::min()));
        }

        let fea = feas.get_mut(&feature).unwrap();
        if op == Op::Since {
            debug_assert!(fea.since == Version::min());
            debug_assert!(fea.until == Version::max());

            fea.since = version;
        } else {
            debug_assert!(fea.since != Version::min());
            debug_assert!(fea.until == Version::max());

            fea.until = version;
        }
    }

    /// Server provides a feature since `version`; usable at `version` and later.
    fn server_adds(&mut self, feature: F, version: Version) {
        self.apply(Role::Server, feature, Op::Since, version);
    }

    /// Server removes a feature at `version`; still usable at `version - 1`, not at `version`.
    fn server_removes(&mut self, feature: F, version: Version) {
        self.apply(Role::Server, feature, Op::Until, version);
    }

    /// Client requires a feature since `version`; required at `version` and later.
    fn client_adds(&mut self, feature: F, version: Version) {
        self.apply(Role::Client, feature, Op::Since, version);
    }

    /// Client drops a feature at `version`; still required at `version - 1`, not at `version`.
    fn client_removes(&mut self, feature: F, version: Version) {
        self.apply(Role::Client, feature, Op::Until, version);
    }

    fn new(version: Version) -> Self {
        let mut spec = Spec {
            version,
            server_features: BTreeMap::new(),
            client_features: BTreeMap::new(),
        };

        {
            // 2023-10-17: since 1.2.163:
            // 🖥 server: add stream api kv_read_v1().
            // (fake version for features already provided for a while)
            spec.server_adds(F::OperationAsIs, ver(1, 2, 163));
            spec.server_adds(F::KvApi, ver(1, 2, 163));
            spec.server_adds(F::KvApiGetKv, ver(1, 2, 163));
            spec.server_adds(F::KvApiMGetKv, ver(1, 2, 163));
            spec.server_adds(F::KvApiListKv, ver(1, 2, 163));
            spec.server_adds(F::KvReadV1, ver(1, 2, 163));

            spec.client_adds(F::OperationAsIs, ver(1, 2, 163));
            spec.client_adds(F::KvApi, ver(1, 2, 163));
            spec.client_adds(F::KvApiGetKv, ver(1, 2, 163));
            spec.client_adds(F::KvApiMGetKv, ver(1, 2, 163));
            spec.client_adds(F::KvApiListKv, ver(1, 2, 163));

            // 2023-10-20: since 1.2.176:
            // 👥 client: call stream api kv_read_v1(), revert to 1.1.32 if server < 1.2.163
            spec.client_adds(F::KvReadV1, ver(1, 2, 176));

            // 2023-12-16: since 1.2.258:
            // 🖥 server: add ttl to TxnPutRequest and Upsert
            spec.server_adds(F::Transaction, ver(1, 2, 258));
            spec.server_adds(F::TransactionReplyError, ver(1, 2, 258));
            spec.server_adds(F::TransactionPutWithTtl, ver(1, 2, 258));

            spec.client_adds(F::TransactionReplyError, ver(1, 2, 258));

            // 1.2.259: (fake version, 1.2.258 binary outputs 1.2.257)
            // 🖥 server: add Export, Watch, MemberList, GetClusterStatus, GetClientInfo
            spec.server_adds(F::Export, ver(1, 2, 259));
            spec.server_adds(F::Watch, ver(1, 2, 259));
            spec.server_adds(F::MemberList, ver(1, 2, 259));
            spec.server_adds(F::GetClusterStatus, ver(1, 2, 259));
            spec.server_adds(F::GetClientInfo, ver(1, 2, 259));

            spec.client_adds(F::Transaction, ver(1, 2, 259));
            spec.client_adds(F::Export, ver(1, 2, 259));
            spec.client_adds(F::Watch, ver(1, 2, 259));
            spec.client_adds(F::MemberList, ver(1, 2, 259));
            spec.client_adds(F::GetClusterStatus, ver(1, 2, 259));
            spec.client_adds(F::GetClientInfo, ver(1, 2, 259));

            // 2024-01-07: since 1.2.287:
            // 👥 client: remove calling RPC kv_api() with MetaGrpcReq::GetKV/MGetKV/ListKV
            spec.client_removes(F::KvApiGetKv, ver(1, 2, 287));
            spec.client_removes(F::KvApiMGetKv, ver(1, 2, 287));
            spec.client_removes(F::KvApiListKv, ver(1, 2, 287));

            // 2024-01-25: since 1.2.315:
            // 🖥 server: add export_v1() to let client specify export chunk size
            spec.server_adds(F::ExportV1, ver(1, 2, 315));

            // 2024-03-04: since 1.2.361:
            // 👥 client: `MetaSpec` use `ttl`, remove `expire_at`, require 1.2.258
            spec.client_adds(F::TransactionPutWithTtl, ver(1, 2, 361));

            // 2024-11-22: since 1.2.663:
            // 🖥 server: remove MetaGrpcReq::GetKV/MGetKV/ListKV
            spec.server_removes(F::KvApiGetKv, ver(1, 2, 663));
            spec.server_removes(F::KvApiMGetKv, ver(1, 2, 663));
            spec.server_removes(F::KvApiListKv, ver(1, 2, 663));

            // 2024-11-23: since 1.2.663:
            // 👥 client: remove use of Operation::AsIs
            spec.client_removes(F::OperationAsIs, ver(1, 2, 663));

            // 2024-12-16: since 1.2.674:
            // 🖥 server: add txn_condition::Target::KeysWithPrefix
            spec.server_adds(F::TransactionConditionKeysPrefix, ver(1, 2, 674));

            // 2024-12-20: since 1.2.676:
            // 🖥 server: add TxnRequest::operations
            // 🖥 server: no longer use TxnReply::error
            spec.server_adds(F::TransactionOperations, ver(1, 2, 676));

            // 👥 client: no longer use TxnReply::error
            spec.client_removes(F::TransactionReplyError, ver(1, 2, 676));

            // 2024-12-26: since 1.2.677:
            // 🖥 server: add WatchRequest::initial_flush
            spec.server_adds(F::WatchInitialFlush, ver(1, 2, 677));

            // 2025-04-15: since 1.2.726:
            // 👥 client: requires 1.2.677
            spec.client_adds(F::WatchInitialFlush, ver(1, 2, 726));
            spec.client_adds(F::WatchResponseIsInit, ver(1, 2, 726));
            spec.client_adds(F::TransactionConditionKeysPrefix, ver(1, 2, 726));
            spec.client_adds(F::TransactionOperations, ver(1, 2, 726));

            // 2025-05-08: since 1.2.736:
            // 🖥 server: add WatchResponse::is_initialization
            spec.server_adds(F::WatchResponseIsInit, ver(1, 2, 736));

            // 2025-06-09: since 1.2.755:
            // 🖥 server: remove TxnReply::error
            spec.server_removes(F::TransactionReplyError, ver(1, 2, 755));

            // 2025-06-11: since 1.2.756:
            // 🖥 server: add TxnPutResponse::current
            spec.server_adds(F::PutResponseCurrent, ver(1, 2, 756));

            spec.client_adds(F::PutResponseCurrent, ver(1, 2, 756));

            // 2025-06-24: since 1.2.764:
            // 🖥 server: add FetchAddU64 operation to the TxnOp
            spec.server_adds(F::FetchAddU64, ver(1, 2, 764));

            // 2025-07-03: since 1.2.770:
            // 🖥 server: adaptive expire_at support both seconds and milliseconds
            spec.server_adds(F::ExpireInMillis, ver(1, 2, 770));
            // 2025-07-04: since 1.2.770:
            // 🖥 server: add PutSequential
            spec.server_adds(F::PutSequential, ver(1, 2, 770));

            // 2025-09-27: since 1.2.821:
            // 👥 client: require 1.2.764(yanked), use 1.2.768, for FetchAddU64
            spec.client_adds(F::FetchAddU64, ver(1, 2, 821));

            // 2025-09-30: since 1.2.823:
            // 🖥 server: store raft-log proposing time proposed_at_ms in KVMeta
            spec.server_adds(F::ProposedAtMs, ver(1, 2, 823));

            // 2025-09-27: since 1.2.823:
            // 👥 client: require 1.2.770, remove calling RPC kv_api
            spec.client_removes(F::KvApi, ver(1, 2, 823));

            // 2025-10-16: since 1.2.828:
            // 🖥 server: rename FetchAddU64 to FetchIncreaseU64, add max_value
            spec.server_adds(F::FetchIncreaseU64, ver(1, 2, 828));

            // 2026-01-12: since 1.2.869:
            // 🖥 server: add kv_list gRPC API
            spec.server_adds(F::KvList, ver(1, 2, 869));
            // 2026-01-13: since 1.2.869:
            // 🖥 server: add kv_get_many gRPC API
            spec.server_adds(F::KvGetMany, ver(1, 2, 869));

            // 2026-02-05:
            // 👥 client: allows the application to use expire in millis
            spec.client_adds(F::ExpireInMillis, ver(260205, 0, 0));
            spec.client_adds(F::PutSequential, ver(260205, 0, 0));

            // client not yet using these features
            spec.client_adds(F::ExportV1, Version::max());
            spec.client_adds(F::ProposedAtMs, Version::max());
            spec.client_adds(F::FetchIncreaseU64, Version::max());
            spec.client_adds(F::KvList, Version::max());
            spec.client_adds(F::KvGetMany, Version::max());
        }

        Self::assert_all_features(&spec.server_features);
        Self::assert_all_features(&spec.client_features);

        spec
    }

    fn assert_all_features(features: &BTreeMap<F, FeatureSpan>) {
        for feature in F::all() {
            assert!(
                features.contains_key(feature),
                "Missing feature: {:?}",
                feature
            );
        }
    }

    /// Minimum server version that can serve this client.
    ///
    /// Returns `max(server.since)` across all features the client requires
    /// at `self.version`.
    pub fn min_compatible_server_version(&self) -> Version {
        let mut min_server = Version::min();

        for feature in F::all() {
            let client_lt = self.client_features.get(feature).unwrap();
            let server_lt = self.server_features.get(feature).unwrap();

            // If client requires this feature at current version
            if client_lt.is_active_at(self.version) {
                min_server = min_server.max(server_lt.since);
            }
        }

        min_server
    }

    /// Minimum client version that can connect to this server.
    ///
    /// Returns `max(client.until)` across all features the server has
    /// removed at `self.version`.
    pub fn min_compatible_client_version(&self) -> Version {
        let mut min_client = Version::min();

        for feature in F::all() {
            let client_lt = self.client_features.get(feature).unwrap();
            let server_lt = self.server_features.get(feature).unwrap();

            // If server removed this feature at current version
            if !server_lt.is_active_at(self.version) {
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
        let spec = Spec::load();

        Spec::assert_all_features(&spec.server_features);
        Spec::assert_all_features(&spec.client_features);
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
    fn test_feature_span_is_active_at() {
        let lt = FeatureSpan::new(Feature::KvApi, Version::new(1, 2, 163));

        // Before since: not active
        assert!(!lt.is_active_at(Version::new(1, 2, 162)));

        // At since: active
        assert!(lt.is_active_at(Version::new(1, 2, 163)));

        // After since: active (until is max)
        assert!(lt.is_active_at(Version::new(1, 2, 164)));
        assert!(lt.is_active_at(Version::new(2, 0, 0)));
    }

    #[test]
    fn test_feature_span_is_active_at_with_until() {
        let lt = FeatureSpan::new(Feature::KvApi, Version::new(1, 2, 163))
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
        let spec = Spec::load();
        let min_server = spec.min_compatible_server_version();

        assert_eq!(min_server, Version::new(1, 2, 770));
    }

    #[test]
    fn test_min_compatible_client_version() {
        let spec = Spec::load();
        let min_client = spec.min_compatible_client_version();

        assert_eq!(min_client, Version::new(1, 2, 676));
    }
}
