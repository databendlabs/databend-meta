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

use crate::Key;
use crate::KeyCodec;
use crate::StructKey;
use crate::Value;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, KeyCodec)]
pub(crate) struct FooKey {
    pub(crate) a: u64,
    pub(crate) b: String,
    pub(crate) c: u64,
}

impl StructKey for FooKey {
    const PREFIX: &'static str = "pref";
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct FooValue;

impl Key for FooKey {
    type ValueType = FooValue;

    fn parent(&self) -> Option<String> {
        None
    }
}

impl Value for FooValue {
    type KeyType = FooKey;

    fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
        []
    }
}

#[cfg(test)]
mod tests {
    use structkey::testing::assert_round_trip;

    use super::FooKey;

    /// Anchor the wire format kvapi promises through its `KeyCodec` re-export.
    /// A `structkey` upgrade that silently changed the format would break this.
    #[test]
    fn foo_key_wire_format_round_trip() {
        assert_round_trip(
            FooKey {
                a: 1,
                b: "hello world".to_string(),
                c: 42,
            },
            "pref/1/hello%20world/42",
        );
    }
}
