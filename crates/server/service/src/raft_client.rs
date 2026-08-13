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

//! The clients this node's outbound raft RPCs are sent through.
//!
//! Both kinds of client are built here: the one openraft drives, and the one
//! this node's own forwarded RPCs use. Neither is built from the generated stub
//! directly, because a client built that way sends no shared secret and is
//! refused by any peer that checks one.

use databend_base::counter;
use databend_meta_raft_config::config::RaftConfig;
use databend_meta_types::ConnectionError;
use databend_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_meta_types::raft_types::NodeId;
use log::debug;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::channel::Channel;

use crate::metrics::raft_metrics;
use crate::raft_secret::RaftSecretInterceptor;
use crate::raft_transport::RaftPeerTarget;

/// A raft client that carries this node's secret on every RPC it sends.
pub type SecretRaftServiceClient =
    RaftServiceClient<InterceptedService<Channel, RaftSecretInterceptor>>;

/// Connect to `peer`'s raft service with a client that sends the secret.
///
/// Every outgoing raft RPC has to come from a client built this way, or from
/// `RaftClient` for the ones openraft sends. A client built straight from the
/// generated stub compiles, connects and works, and sends no secret: the peers
/// that check one refuse it.
///
/// This is public because the `databend-meta` binary lives in another
/// repository and forwards its own RPCs -- node registration on startup among
/// them. Those calls need this, not the generated stub.
pub async fn connect_raft_service(
    peer: &RaftPeerTarget,
    config: &RaftConfig,
) -> Result<SecretRaftServiceClient, ConnectionError> {
    let channel = peer.connect().await?;

    Ok(RaftServiceClient::with_interceptor(
        channel,
        RaftSecretInterceptor::new(config),
    ))
}

/// A metrics reporter of active raft peers.
#[derive(Debug)]
pub struct PeerCounter {
    target: NodeId,
    endpoint_str: String,
}

impl counter::Counter for PeerCounter {
    fn incr(&mut self, n: i64) {
        raft_metrics::network::incr_active_peers(&self.target, &self.endpoint_str, n)
    }
}

/// RaftClient is a grpc client bound with a metrics reporter..
pub type RaftClient = counter::Counted<PeerCounter, SecretRaftServiceClient>;

/// Defines the API of the client to a raft node.
pub trait RaftClientApi {
    /// `address` is the address `channel` was dialed at, which is what the
    /// `active_peers` metric is labelled by.
    fn new(target: NodeId, address: &str, channel: Channel, config: &RaftConfig) -> Self;
}

impl RaftClientApi for RaftClient {
    fn new(target: NodeId, address: &str, channel: Channel, config: &RaftConfig) -> Self {
        let endpoint_str = address.to_string();

        debug!(
            "RaftClient::new: target: {} endpoint: {}",
            target, endpoint_str
        );

        let max_msg_size = config.raft_grpc_max_message_size();
        let cli = RaftServiceClient::with_interceptor(channel, RaftSecretInterceptor::new(config))
            .max_decoding_message_size(max_msg_size)
            .max_encoding_message_size(max_msg_size);
        counter::Counted::new(cli, PeerCounter {
            target,
            endpoint_str,
        })
    }
}
