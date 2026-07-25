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

//! The pre-V004 sled on-disk layout.
//!
//! V004 stores raft logs in a WAL and the state machine in a leveled store, so
//! nothing here is written any more. It is retained to read data left by an
//! older version: the upgrader migrates it, and the exporter still emits its
//! entry format. This module is the unit to delete once V003 compatibility is
//! dropped.

pub mod key_spaces;
pub mod log_meta;
pub mod raft_state_kv;
pub mod state_machine_meta;

pub use log_meta::LogMetaKey;
pub use log_meta::LogMetaValue;
pub use raft_state_kv::RaftStateKey;
pub use raft_state_kv::RaftStateValue;
pub use state_machine_meta::StateMachineMetaKey;
pub use state_machine_meta::StateMachineMetaValue;

/// The sled tree name storing the raft state (node id, vote, committed).
pub const TREE_RAFT_STATE: &str = "raft_state";
