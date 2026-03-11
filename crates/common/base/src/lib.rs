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

#![allow(clippy::uninlined_format_args)]
#![allow(clippy::collapsible_if)]

//! Foundational types with minimal dependencies for the databend-meta service.

mod change;
mod endpoint;
mod invalid_reply;
mod meta_spec;
mod operation;
mod seq_num;
mod upsert_kv;
mod with;

pub mod node;
pub mod normalize_meta;
pub mod time;

pub use change::Change;
pub use endpoint::Endpoint;
pub use invalid_reply::InvalidReply;
pub use meta_spec::MetaSpec;
pub use node::Node;
pub use operation::MetaId;
pub use operation::Operation;
pub use seq_num::SeqNum;
pub use time::Interval;
pub use time::Time;
pub use upsert_kv::UpsertKV;
pub use with::With;
