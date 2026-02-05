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

use std::fmt;

/// A named capability in the meta-service protocol.
///
/// Each variant represents a feature whose lifetime is tracked in [`crate::Spec`]
/// for version compatibility calculation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Feature {
    /// Unary `kv_api()` RPC for key-value operations.
    KvApi,

    /// `kv_api()` sub-operation: get a single key.
    KvApiGetKv,

    /// `kv_api()` sub-operation: get multiple keys.
    KvApiMGetKv,

    /// `kv_api()` sub-operation: list keys by prefix.
    KvApiListKv,

    /// Stream-based `kv_read_v1()` RPC for reading key-value pairs.
    KvReadV1,

    /// `transaction()` RPC for multi-key atomic operations.
    Transaction,

    /// `TxnReply::error` field for returning transaction errors.
    TransactionReplyError,

    /// TTL support in `TxnPutRequest`.
    TransactionPutWithTtl,

    /// Prefix-count condition in `TxnCondition`.
    TransactionConditionKeysPrefix,

    /// Bool-expression operations via `TxnRequest::operations`.
    TransactionOperations,

    /// `Operation::AsIs`: keep value untouched, update only the metadata.
    OperationAsIs,

    /// `export()` RPC for dumping server data.
    Export,

    /// `export_v1()` RPC with configurable chunk size.
    ExportV1,

    /// `watch()` RPC for subscribing to key change events.
    Watch,

    /// `WatchRequest::initial_flush`: flush existing keys at stream start.
    WatchInitialFlush,

    /// `WatchResponse::is_initialization` flag distinguishing init vs change events.
    WatchResponseIsInit,

    /// `member_list()` RPC for cluster membership.
    MemberList,

    /// `get_cluster_status()` RPC for cluster status.
    GetClusterStatus,

    /// `get_client_info()` RPC for connection info and server time.
    GetClientInfo,

    /// `TxnPutResponse::current`: key state after a put operation.
    PutResponseCurrent,

    /// `FetchAddU64` operation in `TxnOp` (deprecated by `FetchIncreaseU64`).
    FetchAddU64,

    /// `expire_at` accepts both seconds and milliseconds timestamps.
    ExpireInMillis,

    /// Sequential put for generating monotonic sequence keys.
    PutSequential,

    /// `KVMeta::proposed_at_ms`: raft-log proposing time in metadata.
    ProposedAtMs,

    /// `FetchIncreaseU64` operation in `TxnOp` with `max_value` support.
    FetchIncreaseU64,

    /// `kv_list()` RPC with pagination via streaming.
    KvList,

    /// `kv_get_many()` RPC with streaming request and response.
    KvGetMany,
}

impl Feature {
    /// Returns all feature variants.
    pub const fn all() -> &'static [Feature] {
        &[
            Feature::KvApi,
            Feature::KvApiGetKv,
            Feature::KvApiMGetKv,
            Feature::KvApiListKv,
            Feature::KvReadV1,
            Feature::Transaction,
            Feature::TransactionReplyError,
            Feature::TransactionPutWithTtl,
            Feature::TransactionConditionKeysPrefix,
            Feature::TransactionOperations,
            Feature::OperationAsIs,
            Feature::Export,
            Feature::ExportV1,
            Feature::Watch,
            Feature::WatchInitialFlush,
            Feature::WatchResponseIsInit,
            Feature::MemberList,
            Feature::GetClusterStatus,
            Feature::GetClientInfo,
            Feature::PutResponseCurrent,
            Feature::FetchAddU64,
            Feature::ExpireInMillis,
            Feature::PutSequential,
            Feature::ProposedAtMs,
            Feature::FetchIncreaseU64,
            Feature::KvList,
            Feature::KvGetMany,
        ]
    }

    /// Returns the string identifier for this feature.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Feature::KvApi => "kv_api",
            Feature::KvApiGetKv => "kv_api/get_kv",
            Feature::KvApiMGetKv => "kv_api/mget_kv",
            Feature::KvApiListKv => "kv_api/list_kv",
            Feature::KvReadV1 => "kv_read_v1",
            Feature::Transaction => "transaction",
            Feature::TransactionReplyError => "transaction/reply_error",
            Feature::TransactionPutWithTtl => "transaction/put_with_ttl",
            Feature::TransactionConditionKeysPrefix => "transaction/condition_keys_prefix",
            Feature::TransactionOperations => "transaction/operations",
            Feature::OperationAsIs => "operation/as_is",
            Feature::Export => "export",
            Feature::ExportV1 => "export_v1",
            Feature::Watch => "watch",
            Feature::WatchInitialFlush => "watch/initial_flush",
            Feature::WatchResponseIsInit => "watch/init_flag",
            Feature::MemberList => "member_list",
            Feature::GetClusterStatus => "get_cluster_status",
            Feature::GetClientInfo => "get_client_info",
            Feature::PutResponseCurrent => "put_response/current",
            Feature::FetchAddU64 => "fetch_add_u64",
            Feature::ExpireInMillis => "expire_in_millis",
            Feature::PutSequential => "put_sequential",
            Feature::ProposedAtMs => "proposed_at_ms",
            Feature::FetchIncreaseU64 => "fetch_increase_u64",
            Feature::KvList => "kv_list",
            Feature::KvGetMany => "kv_get_many",
        }
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
