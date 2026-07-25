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

//! The replicated state machine: applies raft log entries and serves reads.

mod kv_api;
#[allow(clippy::module_inception)]
mod state_machine;

#[cfg(test)]
mod acquire_compactor_test;
#[cfg(test)]
mod compact_with_db_test;
#[cfg(test)]
pub(crate) mod state_machine_test;

pub use state_machine::StateMachine;
