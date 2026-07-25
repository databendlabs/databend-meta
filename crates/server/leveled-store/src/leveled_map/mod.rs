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

//! Multi-level state-machine storage and its read tracking.

mod active_read_guard;
mod active_read_registration;
mod active_read_tracker;
mod compactor_acquirer;
mod map;
mod writer_acquirer;

pub mod compactor;
mod impl_commit;
mod impl_scoped_seq_bounded_range;
mod impl_seq_bounded_get;
pub mod leveled_map_data;

pub(crate) use active_read_guard::ActiveReadGuard;
pub use compactor_acquirer::CompactorPermit;
pub use map::LeveledMap;
pub use writer_acquirer::WriterPermit;

#[cfg(test)]
mod leveled_map_test;
