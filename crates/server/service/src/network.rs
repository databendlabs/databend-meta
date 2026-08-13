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

use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::time::Duration;

use anyerror::AnyError;
use backon::BackoffBuilder;
use backon::ExponentialBuilder;
use databend_base::counter::Counter;
use databend_base::futures::ElapsedFutureExt;
use databend_meta_leveled_store::persisted_codec::PersistedCodec;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_snapshot_db::DB;
use databend_meta_snapshot_db::Snapshot;
use databend_meta_types::ConnectionError;
use databend_meta_types::GrpcHelper;
use databend_meta_types::MetaNetworkError;
use databend_meta_types::PbAppendRequestExt;
use databend_meta_types::PbAppendResponseExt;
use databend_meta_types::protobuf as pb;
use databend_meta_types::protobuf::InstallEntryV004;
use databend_meta_types::protobuf::RaftReply;
use databend_meta_types::raft_types::AppendEntriesRequest;
use databend_meta_types::raft_types::AppendEntriesResponse;
use databend_meta_types::raft_types::MembershipNode;
use databend_meta_types::raft_types::NetworkError;
use databend_meta_types::raft_types::NodeId;
use databend_meta_types::raft_types::RPCError;
use databend_meta_types::raft_types::RaftError;
use databend_meta_types::raft_types::SnapshotResponse;
use databend_meta_types::raft_types::StorageError;
use databend_meta_types::raft_types::StreamAppendResult;
use databend_meta_types::raft_types::StreamingError;
use databend_meta_types::raft_types::TransferLeaderRequest;
use databend_meta_types::raft_types::TransferLeaderResponse;
use databend_meta_types::raft_types::TypeConfig;
use databend_meta_types::raft_types::Unreachable;
use databend_meta_types::raft_types::Vote;
use databend_meta_types::raft_types::VoteRequest;
use databend_meta_types::raft_types::VoteResponse;
use fastrace::func_name;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use openraft::MessageSummary;
use openraft::RaftNetworkFactory;
use openraft::base::BoxFuture;
use openraft::base::BoxStream;
use openraft::error::ReplicationClosed;
use openraft::network::NetAppend;
use openraft::network::NetBackoff;
use openraft::network::NetSnapshot;
use openraft::network::NetStreamAppend;
use openraft::network::NetTransferLeader;
use openraft::network::NetVote;
use openraft::network::RPCOption;
use openraft::network::stream_append_sequential;
use prost::Message;
use seq_marked::SeqData;
use seq_marked::SeqV;
use state_machine_api::MetaValue;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::metrics::raft_metrics;
use crate::raft_client::RaftClient;
use crate::raft_client::RaftClientApi;
use crate::raft_transport::RaftPeerTarget;
use crate::store::RaftStore;

const APPEND_V002_CHANNEL_SIZE: usize = 64;

fn split_append_request_v002(
    req: AppendEntriesRequest,
    advisory_message_size: usize,
) -> Vec<pb::AppendRequest> {
    let full = pb::AppendRequest::from_raft(req.clone());

    let should_split = !req.entries.is_empty() && full.encoded_len() > advisory_message_size;
    if !should_split {
        return vec![full];
    }

    (0..req.entries.len())
        .map(|i| {
            let prev_log_id = if i == 0 {
                req.prev_log_id
            } else {
                Some(req.entries[i - 1].log_id)
            };

            pb::AppendRequest::from_raft(AppendEntriesRequest {
                vote: req.vote,
                prev_log_id,
                entries: vec![req.entries[i].clone()],
                leader_commit: req.leader_commit,
            })
        })
        .collect()
}

#[derive(Debug, Clone)]
pub(crate) struct Backoff {
    /// delay increase ratio of meta
    ///
    /// should be not little than 1.0
    back_off_ratio: f32,
    /// min delay duration of back off
    back_off_min_delay: Duration,
    /// max delay duration of back off
    back_off_max_delay: Duration,
    /// chances of back off
    back_off_chances: u64,
}

impl Backoff {
    /// Set exponential back off policy for meta service
    ///
    /// - `ratio`: delay increase ratio of meta
    ///
    ///   should be not smaller than 1.0
    /// - `min_delay`: minimum back off duration, where the backoff duration vary starts from
    /// - `max_delay`: maximum back off duration, if the backoff duration is larger than this, no backoff will be raised
    /// - `chances`: maximum back off times, chances off backoff
    #[allow(dead_code)]
    pub fn with_back_off_policy(
        mut self,
        ratio: f32,
        min_delay: Duration,
        max_delay: Duration,
        chances: u64,
    ) -> Self {
        self.back_off_ratio = ratio;
        self.back_off_min_delay = min_delay;
        self.back_off_max_delay = max_delay;
        self.back_off_chances = chances;
        self
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            back_off_ratio: 1.5,
            back_off_min_delay: Duration::from_millis(50),
            back_off_max_delay: Duration::from_millis(1_000),
            back_off_chances: 10,
        }
    }
}

#[derive(Clone)]
pub struct NetworkFactory<SP> {
    sto: RaftStore<SP>,

    backoff: Backoff,

    _phantom: PhantomData<SP>,
}

impl<SP: SpawnApi> NetworkFactory<SP> {
    pub fn new(sto: RaftStore<SP>) -> Self {
        Self {
            sto,
            backoff: Backoff::default(),
            _phantom: PhantomData,
        }
    }
}

pub struct Network<SP> {
    /// This node id
    id: NodeId,

    /// The node id to send message to.
    target: NodeId,

    /// Where the target node is reached, and over which transport.
    ///
    /// Rebuilt from the target's own record on every reconnect, so a peer that
    /// starts or stops serving TLS is dialed the new way as soon as it says so.
    /// Whatever names this connection afterwards -- a log line, an error
    /// context, the `active_peers` metric -- names the address held here, which
    /// is the address the connection was dialed at.
    peer: RaftPeerTarget,

    client: Mutex<Option<RaftClient>>,

    sto: RaftStore<SP>,

    backoff: Backoff,

    _phantom: PhantomData<SP>,
}

impl<SP: SpawnApi> Network<SP> {
    /// Create a new RaftClient to the specified target node.
    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    pub async fn new_client(&self) -> Result<RaftClient, ConnectionError> {
        info!(id = self.id; "Raft NetworkConnection connect: target={}: {}", self.target, self.peer);

        let channel = self
            .peer
            .connect()
            .log_elapsed_debug(format!(
                "Raft NetworkConnection new_client: connect target: {}",
                self.target
            ))
            .await?;

        let client =
            RaftClientApi::new(self.target, self.peer.address(), channel, &self.sto.config);

        info!(
            "Raft NetworkConnection connected to: target={}: {}",
            self.target, self.peer
        );

        Ok(client)
    }

    /// Take the last used client or create a new one.
    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    async fn take_client(&mut self) -> Result<RaftClient, Unreachable> {
        let mut client = self.client.lock().await;

        if let Some(c) = client.take() {
            return Ok(c);
        }

        let n = 3;
        for _i in 0..n {
            self.peer = self
                .resolve_target()
                .log_elapsed_debug(format!(
                    "Raft NetworkConnection take_client lookup_target_address: target: {}",
                    self.target
                ))
                .await
                .map_err(|e| {
                    let any_err = AnyError::new(&e).add_context(|| {
                        format!(
                            "Raft NetworkConnection fail to lookup target address: target={}",
                            self.target
                        )
                    });
                    warn!("{}", any_err);
                    Unreachable::new(&any_err)
                })?;

            let res = self.new_client().await;
            match res {
                Ok(c) => {
                    return Ok(c);
                }
                Err(e) => {
                    warn!(
                        "Raft NetworkConnection fail to connect: target={}: addr={}: {:?}",
                        self.target, self.peer, e
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

        let any_err = AnyError::error(format!(
            "Raft NetworkConnection fail to connect: target={}, retry={}",
            self.target, n
        ));
        error!("{}", any_err);

        Err(Unreachable::new(&any_err))
    }

    /// Read the target's own record, which is where both the address to dial
    /// and the transport to dial it over come from.
    async fn resolve_target(&self) -> Result<RaftPeerTarget, MetaNetworkError> {
        debug!(
            "Raft NetworkConnection lookup target address: start: target={}",
            self.target
        );

        let node = self.sto.get_node(&self.target).await.ok_or_else(|| {
            MetaNetworkError::GetNodeAddrError(format!(
                "Node {} not found in state machine",
                self.target
            ))
        })?;

        let peer = RaftPeerTarget::of_node(&node, &self.sto.config).await?;

        Ok(peer)
    }

    pub(crate) fn report_metrics_snapshot(&self, success: bool) {
        raft_metrics::network::incr_sendto_result(&self.target, success);
        raft_metrics::network::incr_snapshot_sendto_result(&self.target, success);
    }

    /// Wrap a RaftError with RPCError
    pub(crate) fn to_rpc_err<E: Error + 'static>(&self, e: RaftError<E>) -> RPCError {
        RPCError::Unreachable(Unreachable::new(&e))
    }

    /// Build a partial AppendEntriesRequest with only the first `n` entries.
    fn build_partial_append_request(
        original: &AppendEntriesRequest,
        n: usize,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            vote: original.vote,
            prev_log_id: original.prev_log_id,
            leader_commit: original.leader_commit,
            entries: original.entries[..n].to_vec(),
        }
    }

    /// Reduce entry count by half. Returns `None` if already at minimum.
    fn try_reduce_entries(&self, current: usize, reason: &str) -> Option<usize> {
        if current <= 1 {
            return None;
        }

        let new_count = current / 2;
        warn!(
            "append_entries: target={}, {}, reducing entries {} -> {}",
            self.target, reason, current, new_count
        );
        Some(new_count)
    }

    pub(crate) fn back_off(&self) -> impl Iterator<Item = Duration> + use<SP> {
        let policy = ExponentialBuilder::default()
            .with_factor(self.backoff.back_off_ratio)
            .with_min_delay(self.backoff.back_off_min_delay)
            .with_max_delay(self.backoff.back_off_max_delay)
            .with_max_times(self.backoff.back_off_chances as usize)
            .build();
        // the last period of back off should be zero
        // so the longest back off will not be wasted
        let zero = vec![Duration::default()].into_iter();
        policy.chain(zero)
    }

    fn parse_grpc_resp<R, E>(
        &self,
        grpc_res: Result<tonic::Response<RaftReply>, tonic::Status>,
    ) -> Result<R, RPCError>
    where
        R: serde::de::DeserializeOwned + 'static,
        E: serde::de::DeserializeOwned + 'static,
        E: std::error::Error,
    {
        // Return status error
        let resp = grpc_res.map_err(|e| RPCError::Unreachable(self.status_to_unreachable(e)))?;

        // Parse serialized response into `Result<RaftReply.data, RaftReply.error>`
        let raft_res = GrpcHelper::parse_raft_reply::<R, E>(resp).map_err(|serde_err| {
            new_net_err(&serde_err, || {
                let t = std::any::type_name::<R>();
                format!("parse reply for {}", t)
            })
        })?;

        // Wrap RaftError with RPCError
        raft_res.map_err(|e| self.to_rpc_err(e))
    }

    /// Convert gRPC status to `Unreachable`
    fn status_to_unreachable(&self, status: tonic::Status) -> Unreachable {
        Self::status_to_unreachable_at(self.target, self.peer.address(), status)
    }

    /// Convert gRPC status to `Unreachable` without borrowing `self`.
    fn status_to_unreachable_at(
        target: NodeId,
        endpoint: &str,
        status: tonic::Status,
    ) -> Unreachable {
        warn!(
            "target={}, endpoint={} gRPC error: {:?}",
            target, endpoint, status
        );

        let any_err = AnyError::new(&status)
            .add_context(|| format!("gRPC target={}, endpoint={}", target, endpoint));

        Unreachable::new(&any_err)
    }

    /// Forward OpenRaft append requests into the already established AppendV002
    /// request stream.
    async fn send_append_requests_v002<S>(
        target: NodeId,
        mut input: S,
        tx: mpsc::Sender<pb::AppendRequest>,
        advisory_message_size: usize,
    ) where
        S: Stream<Item = AppendEntriesRequest> + Send + Unpin + 'static,
    {
        while let Some(req) = input.next().await {
            let entry_count = req.entries.len();
            let requests = split_append_request_v002(req, advisory_message_size);

            if requests.len() > 1 {
                warn!(
                    "append_v002: target={} split oversized request: entries={}, chunks={}",
                    target,
                    entry_count,
                    requests.len()
                );
            }

            for pb_req in requests {
                let bytes = pb_req.encoded_len() as u64;

                if tx.send(pb_req).await.is_err() {
                    debug!(
                        "append_v002: target={} request stream closed before input was exhausted",
                        target
                    );
                    return;
                }

                raft_metrics::network::incr_sendto_bytes(&target, bytes);
            }
        }
    }

    fn should_reuse_client_after_append_v002_failure(status: &tonic::Status) -> bool {
        !matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::Unknown
        )
    }

    /// Stream all KV entries from snapshot DB for V004 replication.
    /// Converts SeqMarked entries to protobuf format and sends via channel.
    /// Skips tombstones.
    async fn send_snapshot_in_stream_v004(
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
        target: NodeId,
        tx: mpsc::Sender<InstallEntryV004>,
    ) -> Result<(), StreamingError> {
        let snapshot_meta = snapshot.meta;
        let db = snapshot.snapshot;

        info!(
            "start to transmit snapshot via v004: {}; db.file_size: {}; db.stat: {}",
            snapshot_meta,
            db.file_size(),
            db.stat()
        );

        let mut c = std::pin::pin!(cancel);

        // Stream KV data from the snapshot DB
        let strm = db.inner_range();

        // Discard tombstones and convert SeqMarked to SeqData and then to protobuf SeqV
        let strm = strm.try_filter_map(|(k, v)| async move {
            let seq_data: Option<SeqData<_>> = v.into();
            let Some(seq_data) = seq_data else {
                // Tombstone, skip
                return Ok(None);
            };

            let seq_data = SeqData::<MetaValue>::decode_from(seq_data)?;
            let seq_v = SeqV::from(seq_data);
            let pb_seq_v = pb::SeqV::from(seq_v);
            let item = pb::StreamItem::new(k, Some(pb_seq_v));
            Ok(Some(item))
        });

        // Chunk the stream into batches of 64 items for efficiency
        let mut strm = strm.try_chunks(64).boxed();

        let mut kv_count = 0u64;

        while let Some(chunk) = strm.try_next().await.map_err(|err| {
            StorageError::read_snapshot(Some(snapshot_meta.signature()), (&err.1).into())
        })? {
            // Check for cancellation
            if let Some(err) = c.as_mut().now_or_never() {
                return Err(err.into());
            }

            // Total length of keys and values in this chunk
            let total_kv_len = chunk
                .iter()
                .map(|item| item.key.len() + item.value.as_ref().map(|v| v.data.len()).unwrap_or(0))
                .sum::<usize>();

            kv_count += chunk.len() as u64;

            // Send KV entry
            let kv_entry = InstallEntryV004 {
                version: 4,
                key_values: chunk,
                commit: None,
            };

            let send_res = tx.send(kv_entry).await;
            if let Err(e) = send_res {
                warn!("error sending to snapshot stream: {}, maybe closed", e);
                return Ok(());
            }

            if kv_count % 10000 == 0 {
                info!("V004 snapshot streaming: sent {} KV entries", kv_count);
            }

            raft_metrics::network::incr_sendto_bytes(&target, total_kv_len as u64);
        }

        info!("V004 snapshot streaming: completed {} KV entries", kv_count);

        // Send commit entry
        let sys_data_json = serde_json::to_string(db.sys_data()).map_err(|e| {
            StorageError::read_snapshot(Some(snapshot_meta.signature()), (&e).into())
        })?;

        // Convert Vote to protobuf Vote using existing conversion
        let pb_vote = pb::Vote::from(vote);

        let commit = pb::Commit {
            snapshot_id: snapshot_meta.snapshot_id.to_string(),
            sys_data: sys_data_json,
            vote: Some(pb_vote),
        };

        let final_entry = InstallEntryV004 {
            version: 4,
            key_values: vec![],
            commit: Some(commit),
        };

        let send_res = tx.send(final_entry).await;
        if let Err(e) = send_res {
            error!("error sending commit entry to snapshot stream: {}", e);
        }

        Ok(())
    }

    /// Send snapshot using V004 streaming protocol.
    ///
    /// Creates streaming connection and sends KV entries followed by commit message.
    /// More memory efficient than V003 as it doesn't buffer entire snapshot.
    async fn send_snapshot_via_v004(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError> {
        let ctx = format!(
            "send_snapshot_via_v004 id={} target={}, snapshot={}",
            self.id, self.target, snapshot.meta
        );

        info!("{}", ctx);

        let target = self.target;
        let (tx, rx) = mpsc::channel(64);
        let strm = ReceiverStream::new(rx);

        let strm_handle = SP::spawn(
            Self::send_snapshot_in_stream_v004(vote, snapshot, cancel, option, target, tx),
            Some("send_snapshot_via_v004".into()),
        );

        let mut client = self
            .take_client()
            .log_elapsed_debug("Raft NetworkConnection install_snapshot_v004 take_client()")
            .await?;

        let grpc_res = client
            .install_snapshot_v004(strm)
            .inspect_elapsed(observe_snapshot_send_spent(target))
            .await;

        info!("{}: grpc_result: {:?}", ctx, grpc_res,);

        match &grpc_res {
            Ok(_) => {
                self.client.lock().await.replace(client);
            }
            Err(e) => {
                warn!("{} failed: {}", ctx, e);
            }
        }

        let res: Result<SnapshotResponse, StreamingError> = try {
            let join_res = strm_handle.await;
            match join_res {
                Err(e) => {
                    warn!("{} Snapshot sending thread error: {}", ctx, e);
                }
                Ok(strm_res) => {
                    if let Err(e) = strm_res {
                        warn!("{} Snapshot sending thread error: {}", ctx, e);
                        Err(e)?;
                    }
                }
            }
            let grpc_response =
                grpc_res.map_err(|e| StreamingError::Unreachable(self.status_to_unreachable(e)))?;
            let snapshot_response = grpc_response.into_inner();

            // Convert protobuf Vote back to internal Vote
            let proto_vote = snapshot_response.vote.ok_or_else(|| {
                StreamingError::Network(NetworkError::new(&AnyError::error(
                    "Missing vote in response",
                )))
            })?;
            let vote = proto_vote.into();
            SnapshotResponse { vote }
        };

        self.report_metrics_snapshot(res.is_ok());
        res
    }
}

// === Sub-trait impls ===
//
// We implement openraft's split network sub-traits directly instead of the
// umbrella `RaftNetworkV2`. This is required because `NetStreamAppend` is
// blanket-implemented for any `RaftNetworkV2` impl, so to provide a custom
// `stream_append` we cannot also impl `RaftNetworkV2`.

/// Unary AppendEntries via the legacy single-RPC endpoint.
///
/// Used by [`stream_append_sequential`] in the [`NetStreamAppend`] impl below
/// as the per-request adapter. A subsequent commit will add an `AppendV002`
/// fast path on top of this.
impl<SP: SpawnApi> NetAppend<TypeConfig> for Network<SP> {
    /// Send AppendEntries RPC with automatic payload size management.
    ///
    /// If the payload exceeds gRPC size limit, reduces entry count and retries.
    /// Returns error if a single entry exceeds the limit.
    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError> {
        debug!(
            id = self.id,
            target = self.target,
            rpc = rpc.summary();
            "send_append_entries",
        );

        let total = rpc.entries.len();
        let mut entries_to_send = rpc.entries.len();

        loop {
            let partial_rpc = Self::build_partial_append_request(&rpc, entries_to_send);
            let raft_req =
                GrpcHelper::encode_raft_request(&partial_rpc).map_err(|e| Unreachable::new(&e))?;
            let payload_size = raft_req.data.len();

            // Check size before sending
            if payload_size > self.sto.config.raft_grpc_advisory_message_size() {
                let reason = format!("payload too large: {} bytes", payload_size);
                match self.try_reduce_entries(entries_to_send, &reason) {
                    Some(n) => {
                        entries_to_send = n;
                        continue;
                    }
                    None => {
                        let err = AnyError::error(reason);
                        return Err(RPCError::Unreachable(Unreachable::new(&err)));
                    }
                }
            }

            // Send the request
            let req = SP::prepare_request(tonic::Request::new(raft_req));
            raft_metrics::network::incr_sendto_bytes(&self.target, req.get_ref().data.len() as u64);

            let mut client = self
                .take_client()
                .log_elapsed_debug("Raft NetworkConnection append_entries take_client()")
                .await?;

            let grpc_res = client
                .append_entries(req)
                .inspect_elapsed(observe_append_send_spent(self.target))
                .await;

            debug!(
                "append_entries resp from: target={}: {:?}",
                self.target, grpc_res
            );

            match &grpc_res {
                Ok(_) => {
                    self.client.lock().await.replace(client);

                    // If we sent partial entries, return PartialSuccess
                    if entries_to_send < total {
                        let last_log_id = partial_rpc.entries.last().map(|e| e.log_id);
                        return Ok(AppendEntriesResponse::PartialSuccess(last_log_id));
                    }

                    return self.parse_grpc_resp::<_, openraft::error::Infallible>(grpc_res);
                }
                Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                    match self.try_reduce_entries(entries_to_send, "ResourceExhausted") {
                        Some(n) => {
                            entries_to_send = n;
                            continue;
                        }
                        None => {
                            let err = AnyError::error("ResourceExhausted: single entry too large");
                            return Err(RPCError::Unreachable(Unreachable::new(&err)));
                        }
                    }
                }
                Err(e) => {
                    warn!(target = self.target, rpc = partial_rpc.summary(); "append_entries failed: {}", e);
                    return self.parse_grpc_resp::<_, openraft::error::Infallible>(grpc_res);
                }
            }
        }
    }
}

/// Streaming AppendEntries.
///
/// Uses the typed AppendV002 streaming RPC when it can be established. If the
/// peer rejects the stream at establishment time, no input item has been
/// consumed yet, so it can fall back to the legacy unary append adapter.
impl<SP: SpawnApi> NetStreamAppend<TypeConfig> for Network<SP> {
    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        option: RPCOption,
    ) -> BoxFuture<'s, Result<BoxStream<'s, Result<StreamAppendResult, RPCError>>, RPCError>>
    where
        S: Stream<Item = AppendEntriesRequest> + Send + Unpin + 'static,
    {
        Box::pin(async move {
            let target = self.target;

            let (tx, rx) = mpsc::channel(APPEND_V002_CHANNEL_SIZE);
            let strm = ReceiverStream::new(rx);
            let req = SP::prepare_request(tonic::Request::new(strm));

            let mut client = self
                .take_client()
                .log_elapsed_debug("Raft NetworkConnection append_v002 take_client()")
                .await?;

            let grpc_res = client
                .append_v002(req)
                .inspect_elapsed(observe_append_send_spent(target))
                .await;

            let response = match grpc_res {
                Ok(response) => {
                    self.client.lock().await.replace(client);
                    response
                }
                Err(status) => {
                    drop(tx);

                    if Self::should_reuse_client_after_append_v002_failure(&status) {
                        self.client.lock().await.replace(client);
                    }

                    warn!(
                        target = self.target;
                        "append_v002 failed while establishing stream, falling back to append_entries: {}",
                        status
                    );

                    return stream_append_sequential(self, input, option).await;
                }
            };

            SP::spawn(
                Self::send_append_requests_v002(
                    target,
                    input,
                    tx,
                    self.sto.config.raft_grpc_advisory_message_size(),
                ),
                Some("append_v002_request_stream".into()),
            );

            let endpoint = self.peer.address().to_string();
            let response_stream = response.into_inner().map(move |resp| match resp {
                Ok(pb_resp) => Ok(pb_resp.into_stream_result()),
                Err(status) => Err(RPCError::Unreachable(Self::status_to_unreachable_at(
                    target, &endpoint, status,
                ))),
            });

            Ok(Box::pin(response_stream) as BoxStream<'s, Result<StreamAppendResult, RPCError>>)
        })
    }
}

impl<SP: SpawnApi> NetSnapshot<TypeConfig> for Network<SP> {
    type SnapshotData = DB;

    /// Send snapshot to the target node via the V004 KV-entry streaming protocol.
    ///
    /// The V003 raw-rotbl fallback was removed: rotbl 0.3.0 writes V002-format
    /// blocks that pre-V004 peers cannot decode, so SnapshotV004 is required
    /// (see `RaftSpec`).
    #[logcall::logcall(err = "error", input = "")]
    #[fastrace::trace]
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError> {
        debug!(id = self.id, target = self.target; "{}", func_name!());

        let _g = snapshot_send_inflight(self.target).counted_guard();

        self.send_snapshot_via_v004(vote, snapshot, cancel, option)
            .await
    }
}

impl<SP: SpawnApi> NetVote<TypeConfig> for Network<SP> {
    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    async fn vote(
        &mut self,
        rpc: VoteRequest,
        _option: RPCOption,
    ) -> Result<VoteResponse, RPCError> {
        info!(id = self.id, target = self.target, rpc = rpc.summary(); "send_vote");

        let mut client = self
            .take_client()
            .log_elapsed_debug("Raft NetworkConnection vote take_client()")
            .await?;

        // First, try VoteV001 with native protobuf types
        let vote_req_pb = pb::VoteRequest::from(rpc.clone());
        let req_v001 = SP::prepare_request(tonic::Request::new(vote_req_pb));

        let grpc_res_v001 = client.vote_v001(req_v001).await;
        info!(
            "vote_v001: resp from target={} {:?}",
            self.target, grpc_res_v001
        );

        match grpc_res_v001 {
            Ok(response) => {
                // VoteV001 succeeded, parse the VoteResponse directly
                self.client.lock().await.replace(client);
                let vote_response = response.into_inner();
                let vote_resp: VoteResponse = vote_response.into();
                return Ok(vote_resp);
            }
            Err(e) => {
                // Only fall back for specific status codes indicating method not implemented
                if e.code() == tonic::Code::Unimplemented || e.code() == tonic::Code::NotFound {
                    warn!(target = self.target, rpc = rpc.summary(); "vote_v001 not implemented, falling back to vote: {}", e);
                } else {
                    // For other errors, don't fall back - return the error
                    return Err(RPCError::Unreachable(self.status_to_unreachable(e.clone())));
                }
            }
        }

        // Fallback to old Vote RPC using RaftRequest
        let raft_req = GrpcHelper::encode_raft_request(&rpc).map_err(|e| Unreachable::new(&e))?;
        let req = SP::prepare_request(tonic::Request::new(raft_req));

        let bytes = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

        let grpc_res = client.vote(req).await;
        info!("vote: resp from target={} {:?}", self.target, grpc_res);

        match &grpc_res {
            Ok(_) => {
                self.client.lock().await.replace(client);
            }
            Err(e) => {
                warn!(target = self.target, rpc = rpc.summary(); "vote failed: {}", e);
            }
        }

        self.parse_grpc_resp::<_, openraft::error::Infallible>(grpc_res)
    }
}

impl<SP: SpawnApi> NetTransferLeader<TypeConfig> for Network<SP> {
    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse, RPCError> {
        info!(id = self.id, target = self.target, req :? = req; "{}", func_name!());

        let pb_req = pb::TransferLeaderRequest::from(req);

        let mut client = self
            .take_client()
            .log_elapsed_debug("Raft NetworkConnection transfer_leader take_client()")
            .await?;

        let req = SP::prepare_request(tonic::Request::new(pb_req));

        let grpc_res = client.transfer_leader_v001(req).await;
        info!(
            "{}_v001: resp from target={} {:?}",
            func_name!(),
            self.target,
            grpc_res
        );

        match grpc_res {
            Ok(resp) => {
                self.client.lock().await.replace(client);
                let resp = resp.into_inner();

                resp.try_into().map_err(|e| {
                    RPCError::Network(new_net_err(&e, || {
                        format!("parse transfer_leader_v001 response from {}", self.target)
                    }))
                })
            }
            Err(e) => {
                if e.code() != tonic::Code::Unimplemented && e.code() != tonic::Code::NotFound {
                    warn!(target = self.target; "{}_v001 failed: {}", func_name!(), e);
                    return Err(RPCError::Unreachable(self.status_to_unreachable(e)));
                }

                warn!(target = self.target; "{}_v001 not implemented, falling back to {}", func_name!(), func_name!());

                let req = SP::prepare_request(tonic::Request::new(pb_req));
                let grpc_res = client.transfer_leader(req).await;
                info!(
                    "{}: resp from target={} {:?}",
                    func_name!(),
                    self.target,
                    grpc_res
                );

                match grpc_res {
                    Ok(_) => {
                        self.client.lock().await.replace(client);
                        Ok(Ok(()))
                    }
                    Err(e) => {
                        warn!(target = self.target; "{} failed: {}", func_name!(), e);
                        Err(RPCError::Unreachable(self.status_to_unreachable(e)))
                    }
                }
            }
        }
    }
}

impl<SP: SpawnApi> NetBackoff<TypeConfig> for Network<SP> {
    /// When a `Unreachable` error is returned from the `Network`,
    /// Openraft will call this method to build a backoff instance.
    fn backoff(&self) -> Option<openraft::network::Backoff> {
        warn!("backoff is required: target={}", self.target);
        Some(openraft::network::Backoff::new(self.back_off()))
    }
}

impl<SP: SpawnApi> RaftNetworkFactory<TypeConfig> for NetworkFactory<SP> {
    type Network = Network<SP>;

    async fn new_client(
        self: &mut NetworkFactory<SP>,
        target: NodeId,
        node: &MembershipNode,
    ) -> Self::Network {
        info!(
            "new raft communication client: id:{}, target:{}, node:{}",
            self.sto.id, target, node
        );

        Network {
            id: self.sto.id,
            target,
            sto: self.sto.clone(),
            backoff: self.backoff.clone(),
            peer: Default::default(),
            client: Default::default(),
            _phantom: PhantomData,
        }
    }
}

fn new_net_err<D: Display>(
    e: &(impl std::error::Error + 'static),
    msg: impl FnOnce() -> D,
) -> NetworkError {
    NetworkError::new(&AnyError::new(e).add_context(msg))
}

/// Create a function record the time cost of append sending.
fn observe_append_send_spent<T>(target: NodeId) -> impl Fn(&T, Duration, Duration) {
    move |_output, t, _b| {
        raft_metrics::network::observe_append_sendto_spent(&target, t.as_secs() as f64);
    }
}

/// Create a function record the time cost of snapshot sending.
fn observe_snapshot_send_spent<T>(target: NodeId) -> impl Fn(&T, Duration, Duration) {
    move |_output, t, _b| {
        raft_metrics::network::observe_snapshot_sendto_spent(&target, t.as_secs() as f64);
    }
}

/// Create a function that increases metric value of inflight snapshot sending.
fn snapshot_send_inflight(target: NodeId) -> impl FnMut(i64) {
    move |i: i64| raft_metrics::network::incr_snapshot_sendto_inflight(&target, i)
}

#[allow(dead_code)]
fn ensure_not_unimplemented<T>(res: &Result<T, tonic::Status>) -> Result<(), tonic::Status> {
    match res {
        Err(e) if e.code() == tonic::Code::Unimplemented || e.code() == tonic::Code::NotFound => {
            Err(e.clone())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_types::raft_types::Entry;
    use databend_meta_types::raft_types::EntryPayload;
    use databend_meta_types::raft_types::Vote;
    use databend_meta_types::raft_types::new_log_id;

    use super::*;

    fn blank_entry(index: u64) -> Entry {
        Entry {
            log_id: new_log_id(1, 1, index),
            payload: EntryPayload::Blank,
        }
    }

    #[test]
    fn test_split_append_request_v002_uses_single_entry_chunks() {
        let req = AppendEntriesRequest {
            vote: Vote::new_committed(1, 1),
            prev_log_id: Some(new_log_id(1, 1, 9)),
            entries: (10..=13).map(blank_entry).collect(),
            leader_commit: Some(new_log_id(1, 1, 13)),
        };
        let advisory_message_size = pb::AppendRequest::from_raft(AppendEntriesRequest {
            vote: req.vote,
            prev_log_id: req.prev_log_id,
            entries: vec![req.entries[0].clone()],
            leader_commit: req.leader_commit,
        })
        .encoded_len();

        let chunks = split_append_request_v002(req, advisory_message_size);

        assert_eq!(chunks.len(), 4);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encoded_len() <= advisory_message_size)
        );

        let mut expected_prev_log_id = Some(new_log_id(1, 1, 9));
        let mut expected_index = 10;

        for chunk in chunks {
            let chunk = chunk.try_into_raft().unwrap();

            assert_eq!(chunk.prev_log_id, expected_prev_log_id);
            assert_eq!(chunk.leader_commit, Some(new_log_id(1, 1, 13)));
            assert!(!chunk.entries.is_empty());

            for entry in chunk.entries {
                assert_eq!(entry.log_id, new_log_id(1, 1, expected_index));
                expected_prev_log_id = Some(entry.log_id);
                expected_index += 1;
            }
        }

        assert_eq!(expected_index, 14);
    }

    #[test]
    fn test_split_append_request_v002_keeps_heartbeat() {
        let req = AppendEntriesRequest {
            vote: Vote::new_committed(1, 1),
            prev_log_id: Some(new_log_id(1, 1, 9)),
            entries: vec![],
            leader_commit: Some(new_log_id(1, 1, 9)),
        };

        let chunks = split_append_request_v002(req, 1);

        assert_eq!(chunks.len(), 1);

        let chunk = chunks.into_iter().next().unwrap().try_into_raft().unwrap();
        assert_eq!(chunk.prev_log_id, Some(new_log_id(1, 1, 9)));
        assert!(chunk.entries.is_empty());
        assert_eq!(chunk.leader_commit, Some(new_log_id(1, 1, 9)));
    }
}
