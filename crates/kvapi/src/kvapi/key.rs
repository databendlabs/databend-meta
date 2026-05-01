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

use structkey::DirName;
use structkey::StructKey;

use crate::kvapi;

/// Convert structured key to a string key used by kvapi::KVApi and backwards.
///
/// Extends `structkey::StructKey` with the kvapi-specific contract:
/// an associated `ValueType` and a hierarchical `parent()`. The codec
/// layer (PREFIX, encode/decode, segment counting) is inherited from
/// `StructKey`.
pub trait Key: StructKey + Debug
where Self: Sized
{
    type ValueType: kvapi::Value;

    /// Return the parent key of this key.
    ///
    /// For example, a table name's(`(database_id, table_name)`) parent is the database id.
    fn parent(&self) -> Option<String>;
}

/// `DirName<K>` participates in `Key` whenever the inner `K` does.
///
/// It carries the inner key's `ValueType` and forwards `parent()` as
/// `unimplemented!`, matching the original kvapi behavior: a directory
/// view is not a record and has no parent of its own.
impl<K: Key> Key for DirName<K> {
    type ValueType = K::ValueType;

    fn parent(&self) -> Option<String> {
        unimplemented!("DirName is not a record thus it has no parent")
    }
}

#[cfg(test)]
mod tests {

    use structkey::DirName;
    use structkey::StructKey;

    use crate::kvapi::testing::FooKey;

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
