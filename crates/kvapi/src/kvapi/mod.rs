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

mod api;
mod item;
mod key;
mod kv_api_ext;
mod list_options;
mod message;
mod pair;
mod value;
mod value_with_name;

pub mod testing;

pub use api::ApiBuilder;
pub use api::KVApi;
pub use api::KVStream;
pub use api::fail_fast;
pub use api::limit_stream;
pub use item::Item;
pub use item::NonEmptyItem;
pub use key::Key;
pub use kv_api_ext::KvApiExt;
pub use list_options::ListOptions;
pub use message::GetKVReply;
pub use message::GetKVReq;
pub use message::ListKVReply;
pub use message::ListKVReq;
pub use message::MGetKVReply;
pub use message::MGetKVReq;
pub use message::UpsertKVReply;
pub use pair::BasicPair;
pub use pair::Pair;
pub use pair::SeqPair;
// Re-export structkey types under `Key*` aliases. Within `kvapi` -- a
// crate that mixes Key, Value, and API concepts -- the prefix tells
// the reader which concept each type belongs to. (`structkey` itself
// has no such ambiguity, so the underlying types are unprefixed.)
pub use structkey::Builder as KeyBuilder;
pub use structkey::Codec as KeyCodec;
pub use structkey::DirName;
pub use structkey::Error as KeyError;
pub use structkey::Parser as KeyParser;
pub use structkey::Raw;
pub use structkey::StructKey;
pub use value::Value;
pub use value_with_name::ValueWithName;
