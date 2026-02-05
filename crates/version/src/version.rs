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

    pub const fn from_digit(u: u64) -> Self {
        Version {
            major: u / 1_000_000,
            minor: u / 1_000 % 1_000,
            patch: u % 1_000,
        }
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

impl From<Version> for semver::Version {
    fn from(v: Version) -> Self {
        semver::Version::new(v.major, v.minor, v.patch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_to_digit_basic() {
        assert_eq!(Version::new(1, 2, 873).to_digit(), 1_002_873);
        assert_eq!(Version::new(0, 0, 0).to_digit(), 0);
        assert_eq!(Version::new(999, 999, 999).to_digit(), 999_999_999);
    }

    #[test]
    fn test_from_digit_basic() {
        assert_eq!(Version::from_digit(1_002_873), Version::new(1, 2, 873));
        assert_eq!(Version::from_digit(0), Version::new(0, 0, 0));
        assert_eq!(
            Version::from_digit(999_999_999),
            Version::new(999, 999, 999)
        );
    }

    #[test]
    fn test_digit_roundtrip() {
        let versions = vec![
            Version::new(0, 0, 0),
            Version::new(1, 0, 0),
            Version::new(0, 1, 0),
            Version::new(0, 0, 1),
            Version::new(1, 2, 873),
            Version::new(10, 20, 30),
            Version::new(999, 999, 999),
            Version::new(260205, 0, 0),
            Version::new(260205, 1, 0),
            Version::new(260205, 0, 1),
            Version::new(261231, 999, 999),
        ];

        for ver in versions {
            assert_eq!(
                Version::from_digit(ver.to_digit()),
                ver,
                "Roundtrip failed for {:?}",
                ver
            );
        }
    }

    #[test]
    fn test_digit_single_component() {
        assert_eq!(Version::new(1, 0, 0).to_digit(), 1_000_000);
        assert_eq!(Version::new(0, 1, 0).to_digit(), 1_000);
        assert_eq!(Version::new(0, 0, 1).to_digit(), 1);
    }

    #[test]
    fn test_digit_calver_encoding() {
        assert_eq!(Version::new(260205, 1, 0).to_digit(), 260_205_001_000);
        assert_eq!(Version::new(260205, 0, 0).to_digit(), 260_205_000_000);
        assert_eq!(Version::new(260205, 999, 999).to_digit(), 260_205_999_999);
    }

    #[test]
    fn test_digit_calver_ordering() {
        let jan = Version::new(260101, 0, 0).to_digit();
        let feb = Version::new(260205, 0, 0).to_digit();
        let dec = Version::new(261231, 0, 0).to_digit();
        assert!(jan < feb);
        assert!(feb < dec);

        let v0 = Version::new(260205, 0, 0).to_digit();
        let v1 = Version::new(260205, 1, 0).to_digit();
        let v1_fix = Version::new(260205, 1, 1).to_digit();
        let v2 = Version::new(260205, 2, 0).to_digit();
        assert!(v0 < v1 && v1 < v1_fix && v1_fix < v2);
    }

    #[test]
    fn test_digit_old_versions_sort_before_calver() {
        let old = Version::new(1, 3, 0).to_digit();
        let calver = Version::new(260205, 0, 0).to_digit();
        assert!(old < calver);
    }

    #[test]
    fn test_digit_minor_overflow_corrupts_roundtrip() {
        let v = Version::new(260205, 1000, 0);
        let recovered = Version::from_digit(v.to_digit());
        assert_ne!(v, recovered);
        assert_eq!(recovered, Version::new(260206, 0, 0));
    }

    #[test]
    fn test_digit_patch_overflow_corrupts_roundtrip() {
        let v = Version::new(260205, 0, 1000);
        let recovered = Version::from_digit(v.to_digit());
        assert_ne!(v, recovered);
        assert_eq!(recovered, Version::new(260205, 1, 0));
    }
}
