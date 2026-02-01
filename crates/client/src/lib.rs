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

extern crate core;

mod channel_manager;
mod client_conf;
mod client_handle;
pub mod endpoints;
pub mod errors;
pub(crate) mod established_client;
mod grpc_action;
mod grpc_client;
mod message;
mod pool;
pub mod required;
pub(crate) mod rpc_handler;

pub use channel_manager::MetaChannelManager;
pub use client_conf::RpcClientConf;
pub use client_conf::RpcClientTlsConfig;
pub use client_handle::ClientHandle;
pub use databend_meta_version::MIN_SERVER_VERSION;
pub use databend_meta_version::MIN_SERVER_VERSION as MIN_METASRV_SEMVER;
pub use databend_meta_version::from_digit_ver;
pub use databend_meta_version::to_digit_ver;
pub use grpc_action::GetKVReply;
pub use grpc_action::GetKVReq;
pub use grpc_action::ListKVReply;
pub use grpc_action::ListKVReq;
pub use grpc_action::MGetKVReply;
pub use grpc_action::MGetKVReq;
pub use grpc_action::MetaGrpcReadReq;
pub use grpc_action::MetaGrpcReq;
pub use grpc_action::RequestFor;
pub use grpc_action::UpsertKVReply;
pub use grpc_client::MetaGrpcClient;
pub use grpc_client::handshake;
pub use message::ClientWorkerRequest;
pub use message::InitFlag;
pub use message::Streamed;
pub use required::FeatureSpec;
pub use required::VersionTuple;
