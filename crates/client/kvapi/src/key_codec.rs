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

use crate as kvapi;
use crate::KeyError;

/// Encode or decode part of a meta-service key.
pub trait KeyCodec {
    /// Encode fields of the structured key into a key builder.
    fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder;

    /// Decode fields of the structured key from a key parser.
    fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, KeyError>
    where Self: Sized;
}

mod impls {
    use crate::KeyBuilder;
    use crate::KeyCodec;
    use crate::KeyError;
    use crate::KeyParser;

    impl KeyCodec for String {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_str(self)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, KeyError>
        where Self: Sized {
            let s = p.next_str()?;
            Ok(s)
        }
    }

    impl KeyCodec for u64 {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(*self)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, KeyError>
        where Self: Sized {
            let s = p.next_u64()?;
            Ok(s)
        }
    }

    impl KeyCodec for () {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b
        }

        fn decode_key(_p: &mut KeyParser) -> Result<Self, KeyError>
        where Self: Sized {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::KeyBuilder;
    use crate::KeyCodec;
    use crate::KeyParser;
    use crate::testing::FooKey;

    #[test]
    fn test_string_round_trip() {
        let s = "hello world".to_string();
        let encoded = s.encode_key(KeyBuilder::new()).done();
        let mut p = KeyParser::new(&encoded);
        let decoded = String::decode_key(&mut p).unwrap();
        assert_eq!(s, decoded);
    }

    #[test]
    fn test_string_with_special_chars() {
        let s = "a/b%c".to_string();
        let encoded = s.encode_key(KeyBuilder::new()).done();
        let mut p = KeyParser::new(&encoded);
        let decoded = String::decode_key(&mut p).unwrap();
        assert_eq!(s, decoded);
    }

    #[test]
    fn test_u64_round_trip() {
        for v in [0u64, 1, 42, u64::MAX] {
            let encoded = v.encode_key(KeyBuilder::new()).done();
            let mut p = KeyParser::new(&encoded);
            let decoded = u64::decode_key(&mut p).unwrap();
            assert_eq!(v, decoded);
        }
    }

    #[test]
    fn test_unit_round_trip() {
        let encoded = ().encode_key(KeyBuilder::new()).done();
        assert_eq!(encoded, "");
        let mut p = KeyParser::new(&encoded);
        <()>::decode_key(&mut p).unwrap();
    }

    #[test]
    fn test_foo_key_round_trip() {
        let k = FooKey {
            a: 1,
            b: "hello world".to_string(),
            c: 42,
        };
        let encoded = k.encode_key(KeyBuilder::new_prefixed("pref")).done();
        assert_eq!(encoded, "pref/1/hello%20world/42");

        let mut p = KeyParser::new_prefixed(&encoded, "pref").unwrap();
        let decoded = FooKey::decode_key(&mut p).unwrap();
        assert_eq!(k, decoded);
    }
}
