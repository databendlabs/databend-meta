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

use display_more::DisplaySliceExt;

use crate::leveled_store::immutable_levels::ImmutableLevels;

impl ImmutableLevels {
    /// Determine if the layout needs compaction based on the total size of all levels.
    pub(crate) fn need_compact(&self) -> Result<(), String> {
        let min_size = self.newest_to_oldest().map(|l| l.size()).min().unwrap_or(0);
        let max_size = self.newest_to_oldest().map(|l| l.size()).max().unwrap_or(0);

        let n_levels = self.immutables.len() as u64;

        if n_levels < 6 {
            return Err("not enough levels(<6) to compact".to_string());
        }

        if n_levels > 20 {
            return Ok(());
        }

        if max_size <= min_size * 4 {
            return Err(format!(
                "size difference between levels(min: {}, max: {}) is not large enough, stat: {}",
                min_size,
                max_size,
                self.stat().display()
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use display_more::DisplaySliceExt;
    use state_machine_api::UserKey;

    use crate::leveled_store::immutable::Immutable;
    use crate::leveled_store::immutable_levels::ImmutableLevels;
    use crate::leveled_store::level::Level;

    /// Build immutable levels holding the given number of kv entries each.
    fn levels_of_sizes(sizes: &[u64]) -> ImmutableLevels {
        let immutables = sizes.iter().enumerate().map(|(i, size)| {
            let mut level = Level::default();
            for j in 0..*size {
                level
                    .kv
                    .insert(UserKey::new(format!("k{}-{}", i, j)), 1, (None, vec![]))
                    .unwrap();
            }
            Immutable::new_from_level(level)
        });

        ImmutableLevels::new_form_iter(immutables)
    }

    fn size_error(levels: &ImmutableLevels, min: u64, max: u64) -> Result<(), String> {
        Err(format!(
            "size difference between levels(min: {}, max: {}) is not large enough, stat: {}",
            min,
            max,
            levels.stat().display()
        ))
    }

    #[test]
    fn test_need_compact_rejects_fewer_than_6_levels() {
        let too_few = Err("not enough levels(<6) to compact".to_string());

        assert_eq!(levels_of_sizes(&[]).need_compact(), too_few);
        assert_eq!(levels_of_sizes(&[9, 1, 1, 1, 1]).need_compact(), too_few);
    }

    #[test]
    fn test_need_compact_rejects_evenly_sized_levels() {
        let levels = levels_of_sizes(&[1, 1, 1, 1, 1, 1]);
        assert_eq!(levels.need_compact(), size_error(&levels, 1, 1));

        // A level exactly 4 times the smallest is still not skewed enough.
        let levels = levels_of_sizes(&[4, 1, 1, 1, 1, 1]);
        assert_eq!(levels.need_compact(), size_error(&levels, 1, 4));

        let levels = levels_of_sizes(&[0, 0, 0, 0, 0, 0]);
        assert_eq!(levels.need_compact(), size_error(&levels, 0, 0));
    }

    #[test]
    fn test_need_compact_accepts_skewed_levels() {
        assert_eq!(levels_of_sizes(&[5, 1, 1, 1, 1, 1]).need_compact(), Ok(()));
        assert_eq!(levels_of_sizes(&[1, 1, 1, 1, 1, 0]).need_compact(), Ok(()));
    }

    #[test]
    fn test_need_compact_accepts_more_than_20_evenly_sized_levels() {
        let levels = levels_of_sizes(&[1; 20]);
        assert_eq!(levels.need_compact(), size_error(&levels, 1, 1));

        // Beyond 20 levels the size check is bypassed.
        assert_eq!(levels_of_sizes(&[1; 21]).need_compact(), Ok(()));
    }
}
