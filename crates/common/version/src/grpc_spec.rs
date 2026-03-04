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

//! Feature history and version compatibility for meta-client and meta-server.
//!
//! This module tracks the lifecycle of every protocol feature — when it was
//! added and when (if ever) it was removed — on both the server and client
//! sides. The compatibility algorithm uses this history to compute the
//! minimum peer version required for a successful handshake.
//!
//! # Feature Lifecycle
//!
//! Each feature has a half-open lifetime `[since, until)`:
//! - `since`: the version when the feature was introduced (inclusive).
//! - `until`: the version when the feature was removed (exclusive),
//!   or `Version::max()` if the feature is still active.
//!
//! A feature is active at version V when `since <= V < until`.
//!
//! # Example
//!
//! ```
//! use databend_meta_version::GrpcSpec;
//!
//! let spec = GrpcSpec::load();
//! let min_server = spec.min_compatible_server_version();
//! let min_client = spec.min_compatible_client_version();
//! ```

use std::collections::BTreeMap;

use crate::feature_span::FeatureSpec;
use crate::feature_span::add;
use crate::feature_span::parse_pkg_version;
use crate::feature_span::remove;
use crate::grpc_feat::GrpcFeature;
use crate::version::Version;

/// Type alias for client-server feature spec.
pub type GrpcSpec = FeatureSpec<GrpcFeature>;

impl GrpcSpec {
    /// Creates a new instance with all feature history for the current build version.
    pub fn load() -> Self {
        Self::new(parse_pkg_version())
    }

    fn new(version: Version) -> Self {
        let mut srv = BTreeMap::new();
        let mut cli = BTreeMap::new();

        type F = GrpcFeature;

        const fn ver(major: u64, minor: u64, patch: u64) -> Version {
            Version::new(major, minor, patch)
        }

        {
            // 2023-10-17: since 1.2.163:
            // 🖥 server: add stream api kv_read_v1().
            // (fake version for features already provided for a while)
            add(&mut srv, F::OperationAsIs, ver(1, 2, 163));
            add(&mut srv, F::KvApi, ver(1, 2, 163));
            add(&mut srv, F::RaftReplyError, ver(1, 2, 163));
            add(&mut srv, F::KvApiGetKv, ver(1, 2, 163));
            add(&mut srv, F::KvApiMGetKv, ver(1, 2, 163));
            add(&mut srv, F::KvApiListKv, ver(1, 2, 163));
            add(&mut srv, F::KvReadV1, ver(1, 2, 163));

            add(&mut cli, F::OperationAsIs, ver(1, 2, 163));
            add(&mut cli, F::KvApi, ver(1, 2, 163));
            add(&mut cli, F::RaftReplyError, ver(1, 2, 163));
            add(&mut cli, F::KvApiGetKv, ver(1, 2, 163));
            add(&mut cli, F::KvApiMGetKv, ver(1, 2, 163));
            add(&mut cli, F::KvApiListKv, ver(1, 2, 163));

            // 2023-10-20: since 1.2.176:
            // 👥 client: call stream api kv_read_v1(), revert to 1.1.32 if server < 1.2.163
            add(&mut cli, F::KvReadV1, ver(1, 2, 176));

            // 2023-12-16: since 1.2.258:
            // 🖥 server: add ttl to TxnPutRequest and Upsert
            add(&mut srv, F::Transaction, ver(1, 2, 258));
            add(&mut srv, F::TransactionReplyError, ver(1, 2, 258));
            add(&mut srv, F::TransactionPutWithTtl, ver(1, 2, 258));

            add(&mut cli, F::TransactionReplyError, ver(1, 2, 258));

            // 1.2.259: (fake version, 1.2.258 binary outputs 1.2.257)
            // 🖥 server: add Export, Watch, MemberList, GetClusterStatus, GetClientInfo
            add(&mut srv, F::Export, ver(1, 2, 259));
            add(&mut srv, F::Watch, ver(1, 2, 259));
            add(&mut srv, F::MemberList, ver(1, 2, 259));
            add(&mut srv, F::GetClusterStatus, ver(1, 2, 259));
            add(&mut srv, F::GetClientInfo, ver(1, 2, 259));

            add(&mut cli, F::Transaction, ver(1, 2, 259));
            add(&mut cli, F::Export, ver(1, 2, 259));
            add(&mut cli, F::Watch, ver(1, 2, 259));
            add(&mut cli, F::MemberList, ver(1, 2, 259));
            add(&mut cli, F::GetClusterStatus, ver(1, 2, 259));
            add(&mut cli, F::GetClientInfo, ver(1, 2, 259));

            // 2024-01-07: since 1.2.287:
            // 👥 client: remove calling RPC kv_api() with MetaGrpcReq::GetKV/MGetKV/ListKV
            remove(&mut cli, F::KvApiGetKv, ver(1, 2, 287));
            remove(&mut cli, F::KvApiMGetKv, ver(1, 2, 287));
            remove(&mut cli, F::KvApiListKv, ver(1, 2, 287));

            // 2024-01-17: since 1.2.304:
            // 🖥 server: do not use TxnPutRequest.prev_value;
            // 🖥 server: do not use TxnDeleteRequest.prev_value;
            // Always return the previous value;
            // field index is reserved, no compatibility changes.
            add(&mut srv, F::TransactionPrevValue, ver(1, 2, 304));

            // 2024-01-25: since 1.2.315:
            // 🖥 server: add export_v1() to let client specify export chunk size
            add(&mut srv, F::ExportV1, ver(1, 2, 315));

            // 2024-03-04: since 1.2.361:
            // 👥 client: `MetaSpec` use `ttl`, remove `expire_at`, require 1.2.258
            add(&mut cli, F::TransactionPutWithTtl, ver(1, 2, 361));

            // 2024-11-22: since 1.2.663:
            // 🖥 server: remove MetaGrpcReq::GetKV/MGetKV/ListKV
            remove(&mut srv, F::KvApiGetKv, ver(1, 2, 663));
            remove(&mut srv, F::KvApiMGetKv, ver(1, 2, 663));
            remove(&mut srv, F::KvApiListKv, ver(1, 2, 663));

            // 2024-11-23: since 1.2.663:
            // 👥 client: remove use of Operation::AsIs
            remove(&mut cli, F::OperationAsIs, ver(1, 2, 663));

            // 2024-12-16: since 1.2.674:
            // 🖥 server: add txn_condition::Target::KeysWithPrefix
            add(&mut srv, F::TransactionConditionKeysPrefix, ver(1, 2, 674));

            // 2024-12-20: since 1.2.676:
            // 🖥 server: add TxnRequest::operations
            // 🖥 server: no longer use TxnReply::error
            add(&mut srv, F::TransactionOperations, ver(1, 2, 676));

            // 👥 client: no longer use TxnReply::error
            remove(&mut cli, F::TransactionReplyError, ver(1, 2, 676));

            // 2024-12-26: since 1.2.677:
            // 🖥 server: add WatchRequest::initial_flush
            add(&mut srv, F::WatchInitialFlush, ver(1, 2, 677));

            // 2025-04-15: since 1.2.726:
            // 👥 client: requires 1.2.677
            add(&mut cli, F::WatchInitialFlush, ver(1, 2, 726));
            add(&mut cli, F::WatchResponseIsInit, ver(1, 2, 726));
            add(&mut cli, F::TransactionConditionKeysPrefix, ver(1, 2, 726));
            add(&mut cli, F::TransactionOperations, ver(1, 2, 726));

            // 2025-05-08: since 1.2.736:
            // 🖥 server: add WatchResponse::is_initialization
            add(&mut srv, F::WatchResponseIsInit, ver(1, 2, 736));

            // 2025-06-09: since 1.2.755:
            // 🖥 server: remove TxnReply::error
            remove(&mut srv, F::TransactionReplyError, ver(1, 2, 755));

            // 2025-06-11: since 1.2.756:
            // 🖥 server: add TxnPutResponse::current
            add(&mut srv, F::PutResponseCurrent, ver(1, 2, 756));

            add(&mut cli, F::PutResponseCurrent, ver(1, 2, 756));

            // 2025-06-24: since 1.2.764:
            // 🖥 server: add FetchAddU64 operation to the TxnOp
            add(&mut srv, F::FetchAddU64, ver(1, 2, 764));

            // 2025-07-03: since 1.2.770:
            // 🖥 server: adaptive expire_at support both seconds and milliseconds
            add(&mut srv, F::ExpireInMillis, ver(1, 2, 770));
            // 2025-07-04: since 1.2.770:
            // 🖥 server: add PutSequential
            add(&mut srv, F::PutSequential, ver(1, 2, 770));

            // 2025-09-27: since 1.2.821:
            // 👥 client: require 1.2.764(yanked), use 1.2.768, for FetchAddU64
            add(&mut cli, F::FetchAddU64, ver(1, 2, 821));

            // 2025-09-30: since 1.2.823:
            // 🖥 server: store raft-log proposing time proposed_at_ms in KVMeta
            add(&mut srv, F::ProposedAtMs, ver(1, 2, 823));

            // 2025-09-27: since 1.2.823:
            // 👥 client: require 1.2.770, remove calling RPC kv_api
            remove(&mut cli, F::KvApi, ver(1, 2, 823));

            // 2025-10-16: since 1.2.828:
            // 🖥 server: rename FetchAddU64 to FetchIncreaseU64, add max_value
            add(&mut srv, F::FetchIncreaseU64, ver(1, 2, 828));

            // 2026-01-12: since 1.2.869:
            // 🖥 server: add kv_list gRPC API
            add(&mut srv, F::KvList, ver(1, 2, 869));
            // 2026-01-13: since 1.2.869:
            // 🖥 server: add kv_get_many gRPC API
            add(&mut srv, F::KvGetMany, ver(1, 2, 869));

            // 2026-02-05:
            // 👥 client: allows the application to use expire in millis
            add(&mut cli, F::ExpireInMillis, ver(260205, 0, 0));
            add(&mut cli, F::PutSequential, ver(260205, 0, 0));

            // 2026-02-14: since 260214.0.0
            // 👥 client: use `kv_get_many` gRPC API.
            add(&mut cli, F::KvGetMany, ver(260214, 0, 0));

            // 2026-02-14: since 260214.0.0
            // 👥 client: remove `prev_value` from TxnPutRequest and TxnDeleteRequest
            // Client starts relying on this feature for removing `prev_value`.
            // In this version we removed KVApi::upsert, and provide a default impl upon transaction(), which require it to always return prev_value
            add(&mut cli, F::TransactionPrevValue, ver(260214, 0, 0));

            // 2026-02-17: since 260217.0.0
            // 🖥 server: add kv_transaction gRPC API with Rust-native storage types
            add(&mut srv, F::KvTransaction, ver(260217, 0, 0));

            // 2026-02-18: since 260217.0.0
            // 🖥 server: add TxnPutRequest.match_seq for conditional put
            add(&mut srv, F::KvTransactionPutMatchSeq, ver(260217, 0, 0));

            // Sever does not set this field but the field still present.
            // And the error is set instead in Status.
            //
            // // 2026-02-25: since 260225.0.0
            // // 🖥 server: no longer return errors in `RaftReply.error`;
            // //            use gRPC `Status` instead.
            // remove(&mut srv, F::RaftReplyError, ver(260225, 0, 0));

            // client not yet using these features
            add(&mut cli, F::ExportV1, Version::max());
            add(&mut cli, F::ProposedAtMs, Version::max());
            add(&mut cli, F::FetchIncreaseU64, Version::max());
            add(&mut cli, F::KvList, Version::max());
            add(&mut cli, F::KvTransaction, Version::max());

            #[cfg(feature = "txn-put-match-seq")]
            add(&mut cli, F::KvTransactionPutMatchSeq, ver(260217, 0, 0));

            #[cfg(not(feature = "txn-put-match-seq"))]
            add(&mut cli, F::KvTransactionPutMatchSeq, Version::max());
        }

        FeatureSpec::build(version, srv, cli)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_changes_includes_all_features() {
        let spec = GrpcSpec::load();

        GrpcSpec::assert_all_features(spec.server_features());
        GrpcSpec::assert_all_features(spec.client_features());
    }

    #[test]
    fn test_min_compatible_server_version() {
        let spec = GrpcSpec::load();
        let min_server = spec.min_compatible_server_version();

        #[cfg(feature = "txn-put-match-seq")]
        assert_eq!(min_server, Version::new(260217, 0, 0));

        #[cfg(not(feature = "txn-put-match-seq"))]
        assert_eq!(min_server, Version::new(1, 2, 869));
    }

    #[test]
    fn test_min_compatible_client_version() {
        let spec = GrpcSpec::load();
        let min_client = spec.min_compatible_client_version();

        assert_eq!(min_client, Version::new(1, 2, 676));
    }
}
