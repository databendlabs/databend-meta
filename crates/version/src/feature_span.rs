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

use crate::Version;
use crate::feat::Feature;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
