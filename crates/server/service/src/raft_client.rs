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

use databend_base::counter;
use databend_meta_raft_config::config::RaftConfig;
use databend_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_meta_types::raft_types::NodeId;
use log::debug;
use tonic::transport::channel::Channel;

use crate::metrics::raft_metrics;
use crate::raft_secret::RaftSecretInterceptor;
use crate::raft_secret::SecretRaftServiceClient;

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
