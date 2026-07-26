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

//! A multi-level MVCC key-value engine.
//!
//! Writes land in a writable level, frozen levels stack behind it, and a
//! compactor folds them into a persisted rotbl table. Readers see a consistent
//! view at a sequence number across all levels.

#![allow(clippy::uninlined_format_args)]
#![feature(coroutines)]
#![feature(impl_trait_in_assoc_type)]
#![allow(clippy::diverging_sub_expression)]
#![allow(clippy::let_and_return)]
#![allow(clippy::collapsible_if)]

pub mod db_builder;
pub mod immutable;
pub mod immutable_levels;
pub mod level;
pub mod level_index;
pub mod leveled_map;
pub mod map_api;
pub mod mvcc;
pub mod persisted_codec;
pub mod rotbl_codec;
pub mod sys_data;
pub mod sys_data_api;
pub mod util;

#[cfg(any(test, feature = "testing"))]
pub mod testing_data;

#[cfg(test)]
mod db_scoped_seq_bounded_read_test;
pub mod immutable_data;
