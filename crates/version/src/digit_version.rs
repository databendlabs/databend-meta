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
//! # Version Semantics
//!
//! The version uses a [CalVer](https://calver.org/) scheme: `YYMMDD.MINOR.MICRO`.
//!
//! - `YYMMDD` is the release date as a single integer (e.g., `260205` = 2026-02-05)
//! - `MINOR` is a non-breaking feature edition
//! - `MICRO` (also called "patch" in semver) is a bug fix
//!
//! # Digit Encoding
//!
//! The digit version format encodes `major.minor.patch` into a single `u64`:
//!
//! ```text
//! major * 1_000_000 + minor * 1_000 + patch
//! ```
//!
//! Minor and patch are each limited to 3 digits (0-999).
//! For example, version `260205.1.0` becomes `260_205_001_000`.

use semver::Version;

/// Converts a semantic version to a compact u64 representation.
///
/// The encoding formula is: `major * 1_000_000 + minor * 1_000 + patch`
///
/// Minor and patch must each be in the range 0-999.
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

    #[test]
    fn test_calver_encoding() {
        // YYMMDD major: 260205.1.0 → 260_205 * 1_000_000 + 1 * 1_000
        assert_eq!(to_digit_ver(&Version::new(260205, 1, 0)), 260_205_001_000);
        assert_eq!(to_digit_ver(&Version::new(260205, 0, 0)), 260_205_000_000);
        assert_eq!(
            to_digit_ver(&Version::new(260205, 999, 999)),
            260_205_999_999
        );
    }

    #[test]
    fn test_calver_roundtrip() {
        let versions = vec![
            Version::new(260101, 0, 0),     // first day of year
            Version::new(260205, 1, 0),     // typical release
            Version::new(260205, 0, 1),     // bugfix
            Version::new(261231, 999, 999), // last day of year, max minor/patch
            Version::new(300101, 0, 0),     // far future
            Version::new(990101, 0, 0),     // 2099
        ];

        for ver in versions {
            let digit = to_digit_ver(&ver);
            let recovered = from_digit_ver(digit);
            assert_eq!(ver, recovered, "CalVer roundtrip failed for {:?}", ver);
        }
    }

    #[test]
    fn test_calver_ordering() {
        // Chronological date ordering is preserved
        let jan = to_digit_ver(&Version::new(260101, 0, 0));
        let feb = to_digit_ver(&Version::new(260205, 0, 0));
        let dec = to_digit_ver(&Version::new(261231, 0, 0));
        assert!(jan < feb);
        assert!(feb < dec);

        // Same date: feature then bugfix ordering
        let v0 = to_digit_ver(&Version::new(260205, 0, 0));
        let v1 = to_digit_ver(&Version::new(260205, 1, 0));
        let v1_fix = to_digit_ver(&Version::new(260205, 1, 1));
        let v2 = to_digit_ver(&Version::new(260205, 2, 0));
        assert!(v0 < v1);
        assert!(v1 < v1_fix);
        assert!(v1_fix < v2);
    }

    #[test]
    fn test_old_versions_sort_before_calver() {
        let old = to_digit_ver(&Version::new(1, 3, 0));
        let calver = to_digit_ver(&Version::new(260205, 0, 0));
        assert!(old < calver);
    }

    #[test]
    fn test_calver_minor_patch_boundary() {
        // minor and patch at 0
        let v = Version::new(260205, 0, 0);
        assert_eq!(from_digit_ver(to_digit_ver(&v)), v);

        // minor and patch at max valid (999)
        let v = Version::new(260205, 999, 999);
        assert_eq!(from_digit_ver(to_digit_ver(&v)), v);

        // minor at max, patch at 0
        let v = Version::new(260205, 999, 0);
        assert_eq!(from_digit_ver(to_digit_ver(&v)), v);

        // minor at 0, patch at max
        let v = Version::new(260205, 0, 999);
        assert_eq!(from_digit_ver(to_digit_ver(&v)), v);
    }

    #[test]
    fn test_minor_overflow_corrupts_roundtrip() {
        // minor >= 1000 bleeds into the major field
        let v = Version::new(260205, 1000, 0);
        let recovered = from_digit_ver(to_digit_ver(&v));
        assert_ne!(v, recovered);
        assert_eq!(recovered, Version::new(260206, 0, 0));
    }

    #[test]
    fn test_patch_overflow_corrupts_roundtrip() {
        // patch >= 1000 bleeds into the minor field
        let v = Version::new(260205, 0, 1000);
        let recovered = from_digit_ver(to_digit_ver(&v));
        assert_ne!(v, recovered);
        assert_eq!(recovered, Version::new(260205, 1, 0));
    }
}
