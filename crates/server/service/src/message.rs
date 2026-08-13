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

use anyerror::AnyError;
use databend_meta_client::MetaGrpcReadReq;
use databend_meta_kvapi::GetKVReply;
use databend_meta_kvapi::GetKVReq;
use databend_meta_kvapi::ListKVReply;
use databend_meta_kvapi::ListKVReq;
use databend_meta_kvapi::MGetKVReply;
use databend_meta_kvapi::MGetKVReq;
use databend_meta_types::AppliedState;
use databend_meta_types::Endpoint;
use databend_meta_types::GrpcHelper;
use databend_meta_types::LogEntry;
use databend_meta_types::node::Node;
use databend_meta_types::protobuf::RaftRequest;
use databend_meta_types::raft_types::NodeId;

use crate::meta_node::request_handler_error::CanNotForwardError;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub endpoint: Endpoint,

    #[serde(skip)]
    #[deprecated(note = "it is listening addr, not advertise addr")]
    pub grpc_api_addr: String,

    pub grpc_api_advertise_address: Option<String>,

    /// See [`Node::raft_tls_advertise_address`].
    ///
    /// `serde(default)` is what lets a node built before this field existed
    /// join a cluster that already knows it: a rolling upgrade sends exactly
    /// such a request, and it carries no key for this field.
    #[serde(default)]
    pub raft_tls_advertise_address: Option<String>,

    /// Valid role: "voter", "learner".
    /// A learner node does not participate in voting.
    pub role: Option<String>,
}

impl JoinRequest {
    /// Build a request that asks the leader to store `node` under `node_id`.
    ///
    /// The advertised addresses come from one `Node` rather than one argument
    /// each, so that a node joins with the record it publishes elsewhere. The
    /// destructuring is exhaustive on purpose: an address added to `Node` later
    /// stops this from compiling until the join path carries it too.
    pub fn new(node_id: NodeId, node: Node) -> Self {
        let Node {
            name: _,
            endpoint,
            grpc_api_addr: _,
            grpc_api_advertise_address,
            raft_tls_advertise_address,
        } = node;

        Self {
            node_id,
            endpoint,
            grpc_api_advertise_address,
            raft_tls_advertise_address,
            ..Default::default()
        }
    }

    /// The node record this request asks the leader to store.
    pub fn node(&self) -> Node {
        Node::new(self.node_id, self.endpoint.clone())
            .with_grpc_advertise_address(self.grpc_api_advertise_address.clone())
            .with_raft_tls_advertise_address(self.raft_tls_advertise_address.clone())
    }

    pub fn with_role_voter(self) -> Self {
        self.with_role("voter")
    }

    pub fn with_role_learner(self) -> Self {
        self.with_role("learner")
    }

    fn with_role(mut self, role: impl ToString) -> Self {
        self.role = Some(role.to_string());
        self
    }

    pub fn role(&self) -> String {
        self.role.clone().unwrap_or_else(|| "voter".to_string())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LeaveRequest {
    pub node_id: NodeId,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    derive_more::From,
    derive_more::TryInto,
)]
pub enum ForwardRequestBody {
    Ping,

    Join(JoinRequest),
    Leave(LeaveRequest),

    Write(LogEntry),

    GetKV(GetKVReq),
    MGetKV(MGetKVReq),
    ListKV(ListKVReq),
}

/// A request that is forwarded from one raft node to another
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForwardRequest<T> {
    /// Forward the request to leader if the node received this request is not leader.
    pub forward_to_leader: u64,

    pub body: T,
}

impl<T> ForwardRequest<T> {
    pub fn new(forward_to_leader: u64, body: T) -> Self {
        Self {
            forward_to_leader,
            body,
        }
    }

    pub fn decr_forward(&mut self) {
        self.forward_to_leader -= 1;
    }

    /// Return a new request that will be forwarded to the leader.
    ///
    /// If the max forward number is reached, it returns an error.
    pub fn next(&self) -> Result<Self, CanNotForwardError>
    where T: Clone {
        self.ensure_forwardable()?;

        let mut next = self.clone();
        next.decr_forward();

        Ok(next)
    }

    pub fn ensure_forwardable(&self) -> Result<(), CanNotForwardError> {
        if self.can_forward() {
            Ok(())
        } else {
            Err(CanNotForwardError(AnyError::error(
                "max number of forward reached",
            )))
        }
    }

    pub fn can_forward(&self) -> bool {
        self.forward_to_leader > 0
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, derive_more::TryInto,
)]
#[allow(clippy::large_enum_variant)]
pub enum ForwardResponse {
    #[try_into(ignore)]
    Pong,

    Join(()),
    Leave(()),
    AppliedState(AppliedState),

    GetKV(GetKVReply),
    MGetKV(MGetKVReply),
    ListKV(ListKVReply),
}

impl tonic::IntoRequest<RaftRequest> for ForwardRequest<ForwardRequestBody> {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = GrpcHelper::encode_raft_request(&self).expect("fail to serialize");
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for ForwardRequest<MetaGrpcReadReq> {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = GrpcHelper::encode_raft_request(&self).expect("fail to serialize");
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftRequest> for ForwardRequest<ForwardRequestBody> {
    type Error = tonic::Status;

    fn try_from(mes: RaftRequest) -> Result<Self, Self::Error> {
        let req = serde_json::from_str(&mes.data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        Ok(req)
    }
}
