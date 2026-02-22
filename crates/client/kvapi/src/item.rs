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

use databend_meta_types::SeqV;

use crate::Key;

/// Key-Value item contains key and optional value with seq number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item<K: Key> {
    pub key: K,
    pub seqv: Option<SeqV<K::ValueType>>,
}

impl<K: Key> Item<K> {
    pub fn new(key: K, seqv: Option<SeqV<K::ValueType>>) -> Self {
        Item { key, seqv }
    }
}

/// Key-Value item contains key and non-optional value with seq number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonEmptyItem<K: Key> {
    pub key: K,
    pub seqv: SeqV<K::ValueType>,
}

impl<K: Key> NonEmptyItem<K> {
    pub fn new(key: K, seqv: SeqV<K::ValueType>) -> Self {
        NonEmptyItem { key, seqv }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_types::SeqV;

    use super::*;
    use crate::testing::FooKey;
    use crate::testing::FooValue;

    fn foo_key(a: u64, b: &str, c: u64) -> FooKey {
        FooKey {
            a,
            b: b.to_string(),
            c,
        }
    }

    #[test]
    fn test_item_with_value() {
        let k = foo_key(1, "x", 2);
        let sv = SeqV::new(5, FooValue);
        let item = Item::new(k.clone(), Some(sv));

        assert_eq!(item.key, k);
        assert!(item.seqv.is_some());
        assert_eq!(item.seqv.as_ref().unwrap().seq, 5);
    }

    #[test]
    fn test_item_without_value() {
        let k = foo_key(1, "x", 2);
        let item = Item::<FooKey>::new(k.clone(), None);

        assert_eq!(item.key, k);
        assert!(item.seqv.is_none());
    }

    #[test]
    fn test_non_empty_item() {
        let k = foo_key(1, "x", 2);
        let sv = SeqV::new(10, FooValue);
        let item = NonEmptyItem::new(k.clone(), sv);

        assert_eq!(item.key, k);
        assert_eq!(item.seqv.seq, 10);
    }
}
