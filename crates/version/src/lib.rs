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

//! Version information for databend-meta.
//!
//! # Minimum Compatible Versions
//!
//! [`MIN_CLIENT_VERSION`] and [`MIN_SERVER_VERSION`] are the authoritative
//! source of truth for version compatibility checks during handshake.
//!
//! These values are hardcoded because Rust cannot compute them at const
//! initialization time. The corresponding functions in [`spec::Spec`]
//! (`min_compatible_client_version()` and `min_compatible_server_version()`)
//! exist to verify these values are correct - unit tests assert they match.
//!
//! When feature changes affect compatibility:
//! - Update `MIN_CLIENT_VERSION` when server removes features
//! - Update `MIN_SERVER_VERSION` when client requires new features
//! - Run tests to verify the values match the computed results
//!
//! See [Compatibility Algorithm](./compatibility_algorithm.md) for details.

mod feat;
mod feature_span;
mod spec;
mod version;

pub use self::feat::Feature;
pub use self::feature_span::FeatureSpan;
pub use self::spec::Spec;
pub use self::version::Version;

pub mod changelog {
    #![doc = include_str!("changes.md")]
}

/// Minimum compatible meta-client version.
///
/// See [module documentation](self) for details.
pub static MIN_CLIENT_VERSION: Version = Version::new(1, 2, 676);

/// Minimum compatible meta-server version.
///
/// See [module documentation](self) for details.
pub static MIN_SERVER_VERSION: Version = Version::new(1, 2, 770);

use std::sync::LazyLock;

/// The version string of this build.
const VERSION_STR: &str = env!("CARGO_PKG_VERSION");

/// Current version and feature compatibility spec.
static SPEC: LazyLock<Spec> = LazyLock::new(Spec::load);

/// Returns the version string (e.g., "260205.0.0").
pub fn version_str() -> &'static str {
    VERSION_STR
}

/// Returns the parsed version.
pub fn version() -> &'static Version {
    SPEC.version()
}

/// Returns the full version and feature compatibility spec.
pub fn spec() -> &'static Spec {
    &SPEC
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Convert semver::Version to tuple for comparison.
    fn semver_tuple(v: &Version) -> (u64, u64, u64) {
        (v.major(), v.minor(), v.patch())
    }

    /// Assert that a static version constant matches the computed value.
    fn assert_version_eq(name: &str, static_ver: &Version, computed: Version) {
        assert_eq!(
            semver_tuple(static_ver),
            computed.as_tuple(),
            "{} does not match computed value. Update {} in lib.rs.",
            name,
            name
        );
    }

    #[test]
    fn test_version_string() {
        assert_eq!(version_str(), "260205.0.0");
    }

    #[test]
    fn test_semver_components() {
        assert_eq!(semver_tuple(version()), (260205, 0, 0));
    }

    #[test]
    fn test_semver_display() {
        assert_eq!(version().to_semver().to_string(), "260205.0.0");
    }

    #[test]
    fn test_min_client_version_matches_computed() {
        let spec = spec();
        assert_version_eq(
            "MIN_CLIENT_VERSION",
            &MIN_CLIENT_VERSION,
            spec.min_compatible_client_version(),
        );
    }

    #[test]
    fn test_min_server_version_matches_computed() {
        let spec = spec();
        assert_version_eq(
            "MIN_SERVER_VERSION",
            &MIN_SERVER_VERSION,
            spec.min_compatible_server_version(),
        );
    }
}
