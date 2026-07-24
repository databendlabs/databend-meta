use crate::leveled_store::immutable::Immutable;

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

impl Immutable {
    /// Merge two immutable levels after their historical versions are no longer visible.
    pub fn compact(&self, other: &Self) -> Self {
        // TODO: only allows a self to be newer and other to be older.
        let new_level = self.level.compact(&other.level);

        // Keep the greater LevelIndex of the two instances
        let greater_index = if self.level_index() > other.level_index() {
            *self.level_index()
        } else {
            *other.level_index()
        };

        Self::new_with_index(new_level, greater_index)
    }
}

#[cfg(test)]
mod tests {
    use state_machine_api::UserKey;

    use super::*;
    use crate::leveled_store::level::Level;

    // Helper function to create Vec<u8> values
    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().as_bytes().to_vec()
    }

    // Helper function to compare Immutable levels
    fn assert_immutable_eq(actual: &Immutable, expected: &Immutable, test_name: &str) {
        // Compare LevelIndex
        assert_eq!(
            actual.level_index(),
            expected.level_index(),
            "{}: LevelIndex mismatch",
            test_name
        );

        assert_eq!(
            actual.sys_data().curr_seq(),
            expected.sys_data().curr_seq(),
            "{}: sys_data curr_seq mismatch",
            test_name
        );

        // Compare key-value data by checking the entire inner BTreeMap
        let got = actual.kv.inner.clone().into_iter().collect::<Vec<_>>();
        let want = expected.kv.inner.clone().into_iter().collect::<Vec<_>>();

        assert_eq!(got, want, "{}: kv data mismatch", test_name);

        let got = actual.expire.inner.clone().into_iter().collect::<Vec<_>>();
        let want = expected
            .expire
            .inner
            .clone()
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(got, want, "{}: expire data mismatch", test_name);
    }

    #[test]
    fn test_compact_immutable_levels_with_tombstones() {
        // Create two levels for compaction
        let mut level1 = Level::default();
        let mut level2 = Level::default();

        // Set up different sequence numbers for sys_data
        level1.with_sys_data(|s| s.update_seq(100));
        level2.with_sys_data(|s| s.update_seq(200));

        // Create test keys and values
        let k1 = || UserKey::new("k1");
        let k2 = || UserKey::new("k2");
        let v1 = || (None, b("v1"));
        let v2 = || (None, b("v2"));
        let v3 = || (None, b("v3"));

        // Populate level1 with initial data (older level)
        level1.kv.insert(k1(), 10, v1()).unwrap();
        level1.kv.insert(k2(), 15, v3()).unwrap();

        // Populate level2 with updates and tombstone (newer level)
        level2.kv.insert(k1(), 20, v2()).unwrap();
        level2.kv.insert_tombstone(k1(), 30).unwrap(); // Tombstone at seq 30

        // Create immutable levels
        let immutable1 = Immutable::new_from_level(level1);
        let immutable2 = Immutable::new_from_level(level2);

        // Get the greater index once before all test cases
        let greater_index = if immutable2.level_index() > immutable1.level_index() {
            *immutable2.level_index()
        } else {
            *immutable1.level_index()
        };

        let mut want_level = Level::default();
        want_level.with_sys_data(|s| s.update_seq(200));
        want_level.kv.insert(k2(), 15, v3()).unwrap();
        want_level.kv.insert_tombstone(k1(), 30).unwrap();
        let want = Immutable::new_with_index(want_level, greater_index);

        assert_immutable_eq(&immutable2.compact(&immutable1), &want, "compact");
        assert_immutable_eq(&immutable1.compact(&immutable2), &want, "reverse_compact");
    }
}
