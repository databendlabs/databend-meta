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

//! Configuration and on-disk format versioning for a databend-meta node.
//!
//! This is the bottom layer of the raft store: it names where data lives, which
//! format version it is in, and how the node is tuned. It knows nothing about
//! how that data is read or written.

pub mod config;
pub mod data_version;
pub mod header;
pub mod meta_startup_error;
pub mod snapshot_config;
mod state_machine_features;

pub use meta_startup_error::MetaStartupError;
pub use state_machine_features::StateMachineFeature;
