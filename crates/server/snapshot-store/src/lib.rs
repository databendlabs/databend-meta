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

//! On-disk snapshot files: building them, shipping them, and loading them back.
//!
//! A sibling of the state machine, not a layer under it: the two exchange
//! [`DB`](databend_meta_snapshot_db::DB) handles and neither imports the other.

#![allow(clippy::uninlined_format_args)]
#![feature(try_blocks)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::manual_is_multiple_of)]

mod error;
mod snapshot_id;
mod store;
mod writer;

pub mod open_snapshot;
pub mod received;
pub mod receiver;
pub mod snapshot_loader;
pub mod write_entry;
pub mod writer_stat;

#[cfg(test)]
mod store_test;

pub use error::SnapshotStoreError;
pub use snapshot_id::MetaSnapshotId;
pub use store::SnapshotStore;
pub use store::SnapshotStoreV003;
pub use write_entry::WriteEntry;
pub use writer::SnapshotWriter;
