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

//! Convert between semver::Version and a compact u64 representation.
//!
//! The digit version format encodes major.minor.patch into a u64 as:
//! `major * 1_000_000 + minor * 1_000 + patch`
//!
//! This allows each component to be up to 3 digits (0-999).
//! For example, version 1.2.873 becomes 1_002_873.

use semver::Version;

/// Converts a semantic version to a compact u64 representation.
///
/// The encoding formula is: `major * 1_000_000 + minor * 1_000 + patch`
///
/// # Examples
///
/// ```
/// use databend_meta_version::to_digit_ver;
/// use semver::Version;
///
/// let ver = Version::new(1, 2, 873);
/// assert_eq!(to_digit_ver(&ver), 1_002_873);
/// ```
///
/// # Panics
///
/// This function does not panic, but will overflow if version components
/// exceed 999. In debug mode, this will trigger a panic.
pub fn to_digit_ver(v: &Version) -> u64 {
    v.major * 1_000_000 + v.minor * 1_000 + v.patch
}

/// Converts a u64 digit version back to a semantic version.
///
/// The decoding formula is:
/// - major = value / 1_000_000
/// - minor = (value / 1_000) % 1_000
/// - patch = value % 1_000
///
/// # Examples
///
/// ```
/// use databend_meta_version::from_digit_ver;
/// use semver::Version;
///
/// let ver = from_digit_ver(1_002_873);
/// assert_eq!(ver, Version::new(1, 2, 873));
/// ```
pub fn from_digit_ver(u: u64) -> Version {
    Version::new(u / 1_000_000, u / 1_000 % 1_000, u % 1_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_digit_ver_basic() {
        assert_eq!(to_digit_ver(&Version::new(1, 2, 873)), 1_002_873);
        assert_eq!(to_digit_ver(&Version::new(0, 0, 0)), 0);
        assert_eq!(to_digit_ver(&Version::new(999, 999, 999)), 999_999_999);
    }

    #[test]
    fn test_from_digit_ver_basic() {
        assert_eq!(from_digit_ver(1_002_873), Version::new(1, 2, 873));
        assert_eq!(from_digit_ver(0), Version::new(0, 0, 0));
        assert_eq!(from_digit_ver(999_999_999), Version::new(999, 999, 999));
    }

    #[test]
    fn test_roundtrip_conversion() {
        let versions = vec![
            Version::new(0, 0, 0),
            Version::new(1, 0, 0),
            Version::new(0, 1, 0),
            Version::new(0, 0, 1),
            Version::new(1, 2, 873),
            Version::new(10, 20, 30),
            Version::new(999, 999, 999),
        ];

        for ver in versions {
            let digit = to_digit_ver(&ver);
            let recovered = from_digit_ver(digit);
            assert_eq!(ver, recovered, "Roundtrip failed for {:?}", ver);
        }
    }

    #[test]
    fn test_single_component_versions() {
        assert_eq!(to_digit_ver(&Version::new(1, 0, 0)), 1_000_000);
        assert_eq!(to_digit_ver(&Version::new(0, 1, 0)), 1_000);
        assert_eq!(to_digit_ver(&Version::new(0, 0, 1)), 1);
    }

    #[test]
    fn test_leading_zeros() {
        // Versions like 1.02.003 should be equivalent to 1.2.3
        // after roundtrip since semver normalizes them
        let ver = Version::new(1, 2, 3);
        let digit = to_digit_ver(&ver);
        assert_eq!(digit, 1_002_003);
        assert_eq!(from_digit_ver(digit), ver);
    }

    #[test]
    fn test_max_values() {
        // Maximum representable version
        let max_ver = Version::new(999, 999, 999);
        assert_eq!(to_digit_ver(&max_ver), 999_999_999);
        assert_eq!(from_digit_ver(999_999_999), max_ver);
    }
}
