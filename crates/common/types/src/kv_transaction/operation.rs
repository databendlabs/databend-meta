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

//! Individual operation types used within a transaction branch.
//!
//! Reference via the module: `operation::Get`, `operation::Put`, etc.

use serde::Deserialize;
use serde::Serialize;

use crate::Interval;
use crate::MetaSpec;

/// Value payload with optional expiration metadata.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Payload {
    pub value: Vec<u8>,
    /// Absolute expiration timestamp in milliseconds since epoch.
    pub expire_at_ms: Option<u64>,
    /// Time-to-live in milliseconds, relative to the log entry's propose time.
    pub ttl_ms: Option<u64>,
}

impl Payload {
    pub fn new(value: impl Into<Vec<u8>>, expire_at_ms: Option<u64>, ttl_ms: Option<u64>) -> Self {
        Self {
            value: value.into(),
            expire_at_ms,
            ttl_ms,
        }
    }

    pub fn just(value: impl Into<Vec<u8>>) -> Self {
        Self::new(value, None, None)
    }

    pub fn to_meta_spec(&self) -> MetaSpec {
        MetaSpec::new(self.expire_at_ms, self.ttl_ms.map(Interval::from_millis))
    }
}

/// A key with an optional sequence guard for conditional mutation.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct KeyLookup {
    pub key: String,
    /// Only proceed if the key's current sequence matches this value.
    pub match_seq: Option<u64>,
}

impl KeyLookup {
    pub fn new(key: impl Into<String>, match_seq: Option<u64>) -> Self {
        Self {
            key: key.into(),
            match_seq,
        }
    }

    pub fn just(key: impl Into<String>) -> Self {
        Self::new(key, None)
    }
}

/// Read a key's current value and sequence number.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Get {
    pub key: String,
}

/// Write a value to a key, optionally conditional on sequence and with expiration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Put {
    pub target: KeyLookup,
    pub payload: Payload,
}

impl Put {
    pub fn key(&self) -> &str {
        &self.target.key
    }

    pub fn match_seq(&self) -> Option<u64> {
        self.target.match_seq
    }
}

/// Delete a key, optionally conditional on its current sequence number.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Delete {
    pub target: KeyLookup,
}

impl Delete {
    pub fn key(&self) -> &str {
        &self.target.key
    }

    pub fn match_seq(&self) -> Option<u64> {
        self.target.match_seq
    }
}

/// Delete all keys sharing a given prefix.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct DeleteByPrefix {
    pub prefix: String,
}

/// Atomically read-modify-write a u64 counter stored as JSON.
///
/// The update formula is: `after = max(current, floor) + delta`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct FetchIncreaseU64 {
    pub target: KeyLookup,
    /// Amount to add to the counter (can be negative).
    pub delta: i64,
    /// Floor value applied before adding delta.
    pub floor: u64,
}

impl FetchIncreaseU64 {
    pub fn key(&self) -> &str {
        &self.target.key
    }

    pub fn match_seq(&self) -> Option<u64> {
        self.target.match_seq
    }
}

/// Insert a value under an auto-incrementing sequential key.
///
/// Reads the counter at `sequence_key`, uses it to build `{prefix}{counter}`,
/// inserts the value there, then increments the counter.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct PutSequential {
    /// Prefix for the generated key: the final key is `{prefix}{sequence}`.
    pub prefix: String,
    /// The key holding the auto-increment counter.
    pub sequence_key: String,
    pub payload: Payload,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_new() {
        let p = Payload::new(b"hello", Some(1000), Some(500));
        assert_eq!(p.value, b"hello");
        assert_eq!(p.expire_at_ms, Some(1000));
        assert_eq!(p.ttl_ms, Some(500));
    }

    #[test]
    fn test_payload_just() {
        let p = Payload::just(b"hello");
        assert_eq!(p.value, b"hello");
        assert_eq!(p.expire_at_ms, None);
        assert_eq!(p.ttl_ms, None);
    }

    #[test]
    fn test_payload_to_meta_spec() {
        let p = Payload::new(b"v", Some(5000), Some(3000));
        let spec = p.to_meta_spec();
        assert_eq!(spec.expire_at, Some(5000));
        assert_eq!(spec.ttl, Some(Interval::from_millis(3000)));
    }

    #[test]
    fn test_payload_to_meta_spec_none() {
        let p = Payload::just(b"v");
        let spec = p.to_meta_spec();
        assert_eq!(spec.expire_at, None);
        assert_eq!(spec.ttl, None);
    }

    #[test]
    fn test_key_lookup_new() {
        let kl = KeyLookup::new("mykey", Some(42));
        assert_eq!(kl.key, "mykey");
        assert_eq!(kl.match_seq, Some(42));
    }

    #[test]
    fn test_key_lookup_just() {
        let kl = KeyLookup::just("mykey");
        assert_eq!(kl.key, "mykey");
        assert_eq!(kl.match_seq, None);
    }

    #[test]
    fn test_put_delegates() {
        let p = Put {
            target: KeyLookup::new("k", Some(10)),
            payload: Payload::just(b"v"),
        };
        assert_eq!(p.key(), "k");
        assert_eq!(p.match_seq(), Some(10));
    }

    #[test]
    fn test_delete_delegates() {
        let d = Delete {
            target: KeyLookup::new("k", Some(5)),
        };
        assert_eq!(d.key(), "k");
        assert_eq!(d.match_seq(), Some(5));
    }

    #[test]
    fn test_fetch_increase_u64_delegates() {
        let f = FetchIncreaseU64 {
            target: KeyLookup::new("c", Some(3)),
            delta: 10,
            floor: 0,
        };
        assert_eq!(f.key(), "c");
        assert_eq!(f.match_seq(), Some(3));
    }
}
