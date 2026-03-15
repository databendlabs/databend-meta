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

#![allow(clippy::uninlined_format_args)]
#![allow(clippy::collapsible_if)]
#![allow(non_local_definitions)]

//! This crate defines data types used in meta data storage service.

mod cluster;
mod grpc_helper;

pub mod errors;
pub mod proto_ext;
pub mod raft_types;

// Re-export base types via proto to ensure type compatibility with proto's public API.
// Proto uses `databend_meta_base` types in its methods; re-exporting through proto
// guarantees the same concrete types are used by both client-types and proto.
pub use anyerror;
pub use cluster::NodeInfo;
pub use cluster::NodeType;
pub use databend_meta_base::Change;
pub use databend_meta_base::Endpoint;
pub use databend_meta_base::InvalidReply;
pub use databend_meta_base::MetaId;
pub use databend_meta_base::MetaSpec;
pub use databend_meta_base::Node;
pub use databend_meta_base::Operation;
pub use databend_meta_base::SeqNum;
pub use databend_meta_base::UpsertKV;
pub use databend_meta_base::With;
pub use databend_meta_base::node;
pub use databend_meta_base::normalize_meta;
pub use databend_meta_base::time;
pub use databend_meta_base::time::Interval;
pub use databend_meta_base::time::Time;
use databend_meta_proto::databend_meta_base;
pub use databend_meta_proto::protobuf;
pub use errors::meta_api_errors::MetaAPIError;
pub use errors::meta_api_errors::MetaDataError;
pub use errors::meta_api_errors::MetaDataReadError;
pub use errors::meta_client_errors::MetaClientError;
pub use errors::meta_errors::MetaError;
pub use errors::meta_handshake_errors::MetaHandshakeError;
pub use errors::meta_network_errors::ConnectionError;
pub use errors::meta_network_errors::InvalidArgument;
pub use errors::meta_network_errors::MetaNetworkError;
pub use errors::meta_network_errors::MetaNetworkResult;
pub use map_api::Expirable;
pub mod match_seq {
    pub use map_api::match_seq::MatchSeq;
    pub use map_api::match_seq::MatchSeqExt;
    pub use map_api::match_seq::errors::ConflictSeq;
}
pub use match_seq::ConflictSeq;
pub use match_seq::MatchSeq;
pub use match_seq::MatchSeqExt;
pub use proto_ext::TxnReplyUpsertExt;
pub use proto_ext::TxnRequestUpsertExt;
pub use protobuf::TxnCondition;
pub use protobuf::TxnDeleteByPrefixRequest;
pub use protobuf::TxnDeleteByPrefixResponse;
pub use protobuf::TxnDeleteRequest;
pub use protobuf::TxnDeleteResponse;
pub use protobuf::TxnGetRequest;
pub use protobuf::TxnGetResponse;
pub use protobuf::TxnOp;
pub use protobuf::TxnOpResponse;
pub use protobuf::TxnPutRequest;
pub use protobuf::TxnPutResponse;
pub use protobuf::TxnReply;
pub use protobuf::TxnRequest;
pub use protobuf::txn_condition;
pub use protobuf::txn_condition::ConditionResult;
pub use protobuf::txn_op;
pub use protobuf::txn_op_response;
pub use state_machine_api::SeqV;

pub use crate::grpc_helper::GrpcHelper;
