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

/// Read a key's current value and sequence number.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Get {
    pub key: String,
}

/// Write a value to a key, optionally with expiration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Put {
    pub key: String,
    pub value: Vec<u8>,
    /// Absolute expiration timestamp in milliseconds since epoch.
    pub expire_at_ms: Option<u64>,
    /// Time-to-live in milliseconds, relative to the log entry's propose time.
    pub ttl_ms: Option<u64>,
}

/// Delete a key, optionally conditional on its current sequence number.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Delete {
    pub key: String,
    /// Only delete if the key's current sequence matches this value.
    pub match_seq: Option<u64>,
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
    pub key: String,
    /// Only update if the key's current sequence matches. Unchanged on mismatch.
    pub match_seq: Option<u64>,
    /// Amount to add to the counter (can be negative).
    pub delta: i64,
    /// Floor value applied before adding delta.
    pub floor: u64,
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
    pub value: Vec<u8>,
    /// Absolute expiration timestamp in milliseconds since epoch.
    pub expire_at_ms: Option<u64>,
    /// Time-to-live in milliseconds, relative to the log entry's propose time.
    pub ttl_ms: Option<u64>,
}
