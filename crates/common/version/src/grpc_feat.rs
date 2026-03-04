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

use crate::feature_span::FeatureSet;

/// A named capability in the meta-service protocol.
///
/// Each variant represents a feature whose lifetime is tracked in [`crate::GrpcSpec`]
/// for version compatibility calculation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum GrpcFeature {
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

    /// `TxnPutRequest.prev_value` and `TxnDeleteRequest.prev_value`: always return previous value in put response, without considering the flag in TxnPutRequest or TxnDeleteRequest
    TransactionPrevValue,

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

    /// `kv_transaction()` RPC for multi-key atomic operations with Rust-native storage types.
    KvTransaction,

    /// `TxnPutRequest.match_seq`: conditional put via sequence number.
    KvTransactionPutMatchSeq,

    /// Server returns errors in `RaftReply.error` (JSON-serialized `MetaAPIError`).
    ///
    /// Used by the `kv_api` and `forward` RPCs since the beginning.
    /// Client reads `RaftReply.error` via `reply_to_api_result()` / `parse_raft_reply()`.
    RaftReplyError,
}

impl GrpcFeature {
    /// Returns all feature variants.
    pub const fn all() -> &'static [GrpcFeature] {
        &[
            GrpcFeature::KvApi,
            GrpcFeature::KvApiGetKv,
            GrpcFeature::KvApiMGetKv,
            GrpcFeature::KvApiListKv,
            GrpcFeature::KvReadV1,
            GrpcFeature::Transaction,
            GrpcFeature::TransactionReplyError,
            GrpcFeature::TransactionPutWithTtl,
            GrpcFeature::TransactionPrevValue,
            GrpcFeature::TransactionConditionKeysPrefix,
            GrpcFeature::TransactionOperations,
            GrpcFeature::OperationAsIs,
            GrpcFeature::Export,
            GrpcFeature::ExportV1,
            GrpcFeature::Watch,
            GrpcFeature::WatchInitialFlush,
            GrpcFeature::WatchResponseIsInit,
            GrpcFeature::MemberList,
            GrpcFeature::GetClusterStatus,
            GrpcFeature::GetClientInfo,
            GrpcFeature::PutResponseCurrent,
            GrpcFeature::FetchAddU64,
            GrpcFeature::ExpireInMillis,
            GrpcFeature::PutSequential,
            GrpcFeature::ProposedAtMs,
            GrpcFeature::FetchIncreaseU64,
            GrpcFeature::KvList,
            GrpcFeature::KvGetMany,
            GrpcFeature::KvTransaction,
            GrpcFeature::KvTransactionPutMatchSeq,
            GrpcFeature::RaftReplyError,
        ]
    }

    /// Returns the string identifier for this feature.
    pub const fn as_str(&self) -> &'static str {
        match self {
            GrpcFeature::KvApi => "kv_api",
            GrpcFeature::KvApiGetKv => "kv_api/get_kv",
            GrpcFeature::KvApiMGetKv => "kv_api/mget_kv",
            GrpcFeature::KvApiListKv => "kv_api/list_kv",
            GrpcFeature::KvReadV1 => "kv_read_v1",
            GrpcFeature::Transaction => "transaction",
            GrpcFeature::TransactionReplyError => "transaction/reply_error",
            GrpcFeature::TransactionPutWithTtl => "transaction/put_with_ttl",
            GrpcFeature::TransactionPrevValue => "transaction/prev_value",
            GrpcFeature::TransactionConditionKeysPrefix => "transaction/condition_keys_prefix",
            GrpcFeature::TransactionOperations => "transaction/operations",
            GrpcFeature::OperationAsIs => "operation/as_is",
            GrpcFeature::Export => "export",
            GrpcFeature::ExportV1 => "export_v1",
            GrpcFeature::Watch => "watch",
            GrpcFeature::WatchInitialFlush => "watch/initial_flush",
            GrpcFeature::WatchResponseIsInit => "watch/init_flag",
            GrpcFeature::MemberList => "member_list",
            GrpcFeature::GetClusterStatus => "get_cluster_status",
            GrpcFeature::GetClientInfo => "get_client_info",
            GrpcFeature::PutResponseCurrent => "put_response/current",
            GrpcFeature::FetchAddU64 => "fetch_add_u64",
            GrpcFeature::ExpireInMillis => "expire_in_millis",
            GrpcFeature::PutSequential => "put_sequential",
            GrpcFeature::ProposedAtMs => "proposed_at_ms",
            GrpcFeature::FetchIncreaseU64 => "fetch_increase_u64",
            GrpcFeature::KvList => "kv_list",
            GrpcFeature::KvGetMany => "kv_get_many",
            GrpcFeature::KvTransaction => "kv_transaction",
            GrpcFeature::KvTransactionPutMatchSeq => "kv_transaction/put_match_seq",
            GrpcFeature::RaftReplyError => "raft_reply/error",
        }
    }
}

impl FeatureSet for GrpcFeature {
    fn all() -> &'static [Self] {
        GrpcFeature::all()
    }
}

impl fmt::Display for GrpcFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
