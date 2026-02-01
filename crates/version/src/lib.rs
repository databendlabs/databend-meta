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

mod digit_version;

pub mod changes {
    #![doc = include_str!("changes.md")]
}

pub use digit_version::from_digit_ver;
pub use digit_version::to_digit_ver;

/// Oldest compatible nightly meta-client version
///
/// It should be 1.2.287 but 1.2.287 does not contain complete binaries
pub static MIN_CLIENT_VERSION: Version = Version::new(1, 2, 288);

pub static MIN_SERVER_VERSION: Version = Version::new(1, 2, 770);

use std::sync::LazyLock;

use semver::Version;

/// The version string of this build.
const VERSION_STR: &str = env!("CARGO_PKG_VERSION");

/// Parsed semver version.
static SEMVER: LazyLock<Version> = LazyLock::new(|| {
    let s = VERSION_STR.strip_prefix('v').unwrap_or(VERSION_STR);
    Version::parse(s).unwrap_or_else(|e| panic!("Invalid semver: {:?}: {}", s, e))
});

/// Returns the version string (e.g., "1.2.873")
pub fn version() -> &'static str {
    VERSION_STR
}

/// Returns the parsed semantic version
pub fn semver() -> &'static Version {
    &SEMVER
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_string() {
        assert_eq!(version(), "1.2.873");
    }

    #[test]
    fn test_semver_components() {
        let v = semver();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 873);
    }

    #[test]
    fn test_semver_display() {
        assert_eq!(semver().to_string(), "1.2.873");
    }
}
