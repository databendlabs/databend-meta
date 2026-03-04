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

use std::collections::BTreeMap;
use std::fmt;

use crate::Version;

/// Trait for feature enum types used in [`FeatureSpec`].
pub trait FeatureSet: fmt::Debug + Clone + Copy + Ord + 'static {
    /// Returns all feature variants.
    fn all() -> &'static [Self];
}

/// The lifetime `[since, until)` of a feature.
pub struct FeatureSpan<F> {
    /// The feature being described.
    pub feature: F,

    /// The version when this feature was added (inclusive).
    pub since: Version,

    /// The version when this feature was removed (exclusive).
    ///
    /// If the feature is still supported, this is `Version::max()`.
    pub until: Version,

    /// The version when this feature started being used optionally on the
    /// client side (inclusive).
    ///
    /// Only meaningful for client-side spans. An optional feature is one the
    /// client may use when the peer supports it, but can fall back to an older
    /// API when the peer does not — failure does not cause an RPC error.
    ///
    /// This field does **not** affect the minimum-compatible-version
    /// calculation; it is purely for recording purposes.
    ///
    /// Invariant: if set, `optional_since <= since`.
    pub optional_since: Option<Version>,
}

impl<F> FeatureSpan<F> {
    /// Creates a lifetime starting at `since` with no end (`until = Version::max()`).
    pub const fn new(feature: F, since: Version) -> Self {
        FeatureSpan {
            feature,
            since,
            until: Version::max(),
            optional_since: None,
        }
    }

    /// Sets the exclusive end version, returning the modified span.
    pub const fn until(mut self, until: Version) -> Self {
        self.until = until;
        self
    }

    /// Sets the exclusive end version from individual components.
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

/// Record that a feature was added (required) at `version`.
///
/// If the feature was previously optional ([`add_optional`]), this promotes it
/// to required. The assertion allows `since` to be either `Version::min()`
/// (fresh entry) or `Version::max()` (set by `add_optional`).
pub(crate) fn add<F: FeatureSet>(
    features: &mut BTreeMap<F, FeatureSpan<F>>,
    feature: F,
    version: Version,
) {
    let span = features
        .entry(feature)
        .or_insert_with(|| FeatureSpan::new(feature, Version::min()));

    debug_assert!(
        span.since == Version::min() || span.since == Version::max(),
        "since ({}) must be min (fresh) or max (optional)",
        span.since
    );
    debug_assert!(span.until == Version::max());

    if let Some(opt) = span.optional_since {
        debug_assert!(
            opt <= version,
            "optional_since ({}) must be <= since ({})",
            opt,
            version
        );
    }

    span.since = version;
}

/// Record that a feature started being used optionally at `version`.
///
/// The feature is not required and the client can fall back when the peer
/// does not support it. Sets `since` to `Version::max()` (not required)
/// and records `optional_since`. This does not affect compatibility
/// calculation.
pub(crate) fn add_optional<F: FeatureSet>(
    features: &mut BTreeMap<F, FeatureSpan<F>>,
    feature: F,
    version: Version,
) {
    let span = features
        .entry(feature)
        .or_insert_with(|| FeatureSpan::new(feature, Version::min()));

    debug_assert!(span.since == Version::min());
    debug_assert!(span.optional_since.is_none());

    span.since = Version::max();
    span.optional_since = Some(version);
}

/// Record that a feature was removed at `version`.
pub(crate) fn remove<F: FeatureSet>(
    features: &mut BTreeMap<F, FeatureSpan<F>>,
    feature: F,
    version: Version,
) {
    let span = features
        .entry(feature)
        .or_insert_with(|| FeatureSpan::new(feature, Version::min()));

    debug_assert!(span.since != Version::min());
    debug_assert!(span.until == Version::max());

    span.until = version;
}

/// Parses `CARGO_PKG_VERSION` into a [`Version`].
pub(crate) fn parse_pkg_version() -> Version {
    let s = env!("CARGO_PKG_VERSION");
    let s = s.strip_prefix('v').unwrap_or(s);
    let sv = semver::Version::parse(s)
        .unwrap_or_else(|e| panic!("Invalid CARGO_PKG_VERSION: {:?}: {}", s, e));
    Version::from(sv)
}

/// Version and feature information for compatibility calculation.
///
/// Generic over the feature type `F`. Contains the current build version and
/// the full history of when features were added or removed on both server and
/// client sides. Used to calculate minimum compatible versions.
pub struct FeatureSpec<F: FeatureSet> {
    version: Version,
    server_features: BTreeMap<F, FeatureSpan<F>>,
    client_features: BTreeMap<F, FeatureSpan<F>>,
}

impl<F: FeatureSet> FeatureSpec<F> {
    /// Constructs a spec from pre-built feature maps.
    ///
    /// Asserts that every variant in `F::all()` is present in both maps.
    pub(crate) fn build(
        version: Version,
        server_features: BTreeMap<F, FeatureSpan<F>>,
        client_features: BTreeMap<F, FeatureSpan<F>>,
    ) -> Self {
        Self::assert_all_features(&server_features);
        Self::assert_all_features(&client_features);
        FeatureSpec {
            version,
            server_features,
            client_features,
        }
    }

    /// Returns the build version this instance was created for.
    pub fn version(&self) -> &Version {
        &self.version
    }

    /// Returns the server-side feature spans.
    pub fn server_features(&self) -> &BTreeMap<F, FeatureSpan<F>> {
        &self.server_features
    }

    /// Returns the client-side feature spans.
    pub fn client_features(&self) -> &BTreeMap<F, FeatureSpan<F>> {
        &self.client_features
    }

    /// Asserts that every variant in `F::all()` has an entry.
    pub(crate) fn assert_all_features(features: &BTreeMap<F, FeatureSpan<F>>) {
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
    use crate::grpc_feat::GrpcFeature;

    #[test]
    fn test_feature_span_is_active_at() {
        let lt = FeatureSpan::<GrpcFeature>::new(GrpcFeature::KvApi, Version::new(1, 2, 163));

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
        let lt = FeatureSpan::<GrpcFeature>::new(GrpcFeature::KvApi, Version::new(1, 2, 163))
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
}
