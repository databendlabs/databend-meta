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

mod snapshot_id;
mod snapshot_store_error;
mod snapshot_store_v003;
mod writer_v003;

mod db_open_snapshot_impl;
pub mod open_snapshot;
pub mod received;
pub mod receiver_v003;
pub mod snapshot_loader;
pub mod write_entry;
pub mod writer_stat;

#[cfg(test)]
mod snapshot_store_test;

pub use snapshot_id::MetaSnapshotId;
pub use snapshot_store_error::SnapshotStoreError;
pub use snapshot_store_v003::SnapshotStoreV003;
pub use snapshot_store_v003::SnapshotStoreV004;
pub use write_entry::WriteEntry;
pub use writer_v003::WriterV003;
