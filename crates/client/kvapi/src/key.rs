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

//! Defines kvapi::KVApi key behaviors.

use std::fmt::Debug;

use crate as kvapi;
use crate::DirName;
use crate::StructKey;

/// A `kvapi::KVApi` key: a `StructKey` paired with the value type stored at it.
///
/// `StructKey` provides the encoding (`PREFIX`, `to_string_key`, `from_str_key`);
/// this trait layers on the kvapi-specific `ValueType` association.
pub trait Key: StructKey + Debug
where Self: Sized
{
    type ValueType: kvapi::Value;
}

/// `DirName<K>` participates in `Key` whenever the inner `K` does.
///
/// It carries the inner key's `ValueType`.
impl<K: Key> Key for DirName<K> {
    type ValueType = K::ValueType;
}

#[cfg(test)]
mod tests {

    use crate::DirName;
    use crate::StructKey;
    use crate::testing::FooKey;

    #[test]
    fn test_with_key_space() {
        let k = FooKey {
            a: 1,
            b: "b".to_string(),
            c: 2,
        };

        let dir = DirName::new(k);
        assert_eq!("pref/1/b", dir.to_string_key());

        let k = FooKey {
            a: 1,
            b: "b".to_string(),
            c: 2,
        };

        assert_eq!("pref/1/b/2", k.to_string_key());
    }
}
