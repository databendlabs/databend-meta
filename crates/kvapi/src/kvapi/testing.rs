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

// Re-export structkey's test helpers under `kvapi::testing` so application
// crates that depend on `databend_meta_client::kvapi` do not need an extra
// dev-dependency on `structkey`.
pub use structkey::testing::*;

use crate::kvapi::Key;
use crate::kvapi::KeyCodec;
use crate::kvapi::StructKey;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(KeyCodec)]
pub(crate) struct FooKey {
    pub(crate) a: u64,
    pub(crate) b: String,
    pub(crate) c: u64,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct FooValue;

impl StructKey for FooKey {
    const PREFIX: &'static str = "pref";
}

impl Key for FooKey {
    type ValueType = FooValue;
}
