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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future;
use std::io;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicI32;
use std::time::Duration;

use anyerror::AnyError;
use databend_meta_client::RequestFor;
use databend_meta_raft_config::MetaStartupError;
use databend_meta_raft_config::StateMachineFeature;
use databend_meta_raft_config::config::RaftConfig;
use databend_meta_raft_config::data_version::DATA_VERSION;
use databend_meta_raft_log::RaftLogStat;
use databend_meta_raft_log::RaftLogStore;
use databend_meta_runtime_api::JoinHandle;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_snapshot_db::DBStat;
use databend_meta_state_machine::RaftStateMachineStore;
use databend_meta_state_machine::utils::seq_marked_to_seqv;
use databend_meta_types::AppliedState;
use databend_meta_types::Cmd;
use databend_meta_types::Endpoint;
use databend_meta_types::GrpcHelper;
use databend_meta_types::LogEntry;
use databend_meta_types::MetaAPIError;
use databend_meta_types::MetaError;
use databend_meta_types::MetaNetworkError;
use databend_meta_types::kv_transaction;
use databend_meta_types::node::Node;
use databend_meta_types::protobuf::KvGetManyRequest;
use databend_meta_types::protobuf::KvTransactionReply;
use databend_meta_types::protobuf::StreamItem;
use databend_meta_types::protobuf::WatchRequest;
use databend_meta_types::protobuf::WatchResponse;
use databend_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_meta_types::protobuf::watch_request::FilterType;
use databend_meta_types::raft_types::ClientWriteError;
use databend_meta_types::raft_types::ForwardToLeader;
use databend_meta_types::raft_types::InitializeError;
use databend_meta_types::raft_types::MembershipNode;
use databend_meta_types::raft_types::NodeId;
use databend_meta_types::raft_types::RaftMetrics;
use databend_meta_types::raft_types::TypeConfig;
use databend_meta_types::raft_types::WatchReceiver;
use databend_meta_types::raft_types::new_log_id;
use fastrace::func_name;
use fastrace::prelude::*;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use itertools::Itertools;
use log::debug;
use log::error;
use log::info;
use log::warn;
use map_api::mvcc::ViewRange;
use maplit::btreemap;
use openraft;
use openraft::ChangeMembers;
use openraft::Config;
use openraft::Raft;
use openraft::ServerState;
use openraft::SnapshotPolicy;
use openraft::async_runtime::RecvError;
use openraft::async_runtime::WatchReceiver as WatchReceiverTrait;
use openraft::error::RaftError;
use peel_off::Peel;
use state_machine_api::UserKey;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio::time::sleep;
use tonic::Status;
use watcher::EventFilter;
use watcher::dispatch::Command;
use watcher::dispatch::Dispatcher;
use watcher::key_range::build_key_range;
use watcher::util::new_initialization_sink;
use watcher::util::try_forward;
use watcher::watch_stream::WatchStream;
use watcher::watch_stream::WatchStreamSender;

use crate::analysis::request_histogram;
use crate::api::grpc::grpc_service::try_remove_sender;
use crate::configs::MetaServiceConfig;
use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::message::ForwardResponse;
use crate::message::JoinRequest;
use crate::message::LeaveRequest;
use crate::meta_node::meta_management_error::MetaManagementError;
use crate::meta_node::meta_node_builder::MetaNodeBuilder;
use crate::meta_node::meta_node_metrics::MetaMetrics;
use crate::meta_node::meta_node_status::MetaNodeStatus;
use crate::meta_node::request_handler_error::CanNotForwardError;
use crate::meta_node::request_handler_error::ForwardRequestError;
use crate::meta_service::MetaForwarder;
use crate::meta_service::errors::channel_error_to_network_err;
use crate::meta_service::forward_rpc_error::ForwardRPCError;
use crate::meta_service::meta_leader::MetaLeader;
use crate::meta_service::meta_operation_error::MetaOperationError;
use crate::meta_service::runtime_config::RuntimeConfig;
use crate::meta_service::watcher::DispatcherHandle;
use crate::meta_service::watcher::WatchTypes;
use crate::metrics::network_metrics;
use crate::metrics::server_metrics;
use crate::raft_client::connect_raft_service;
use crate::raft_secret::RaftSecretInterceptor;
use crate::raft_transport::RaftPeerTarget;
use crate::request_handling::Forwarder;
use crate::request_handling::Handler;
use crate::store::RaftStore;
use crate::util::reply_to_api_result;

pub type LogStore = RaftLogStore;
pub type SMStore<SP> = RaftStateMachineStore<SP>;

/// MetaRaft is an implementation of the generic Raft handling metadata R/W.
pub type MetaRaft<SP> = Raft<TypeConfig, RaftStateMachineStore<SP>>;

/// MetaNode is the container of metadata related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct MetaNode<SP: SpawnApi> {
    pub raft_store: RaftStore<SP>,
    /// MetaNode hold a strong reference to the dispatcher handle.
    ///
    /// Other components should keep a weak one.
    pub dispatcher_handle: Arc<DispatcherHandle>,
    pub raft: MetaRaft<SP>,
    pub runtime_config: RuntimeConfig,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<Result<(), AnyError>>>>,
    pub joined_tasks: AtomicI32,
}

impl<SP: SpawnApi> Drop for MetaNode<SP> {
    fn drop(&mut self) {
        info!(
            "MetaNode(id={}, raft={}) is dropping",
            self.raft_store.id,
            self.raft_store.config.raft_api_advertise_host_string()
        );
    }
}

impl<SP: SpawnApi> MetaNode<SP> {
    pub fn builder(config: &RaftConfig) -> MetaNodeBuilder<SP> {
        let raft_config = Self::new_raft_config(config);

        MetaNodeBuilder {
            node_id: None,
            raft_config: Some(raft_config),
            sto: None,
            raft_service_endpoint: None,
        }
    }

    pub fn new_raft_config(config: &RaftConfig) -> Config {
        let hb = config.heartbeat_interval;

        let election_timeouts = config.election_timeout();

        Config {
            cluster_name: config.cluster_name.clone(),
            heartbeat_interval: hb,
            election_timeout_min: election_timeouts.0,
            election_timeout_max: election_timeouts.1,
            install_snapshot_timeout: config.install_snapshot_timeout,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_logs_since_last),
            max_in_snapshot_log_to_keep: config.max_applied_log_to_keep,
            snapshot_max_chunk_size: config.snapshot_chunk_size,
            // Allow Leader to reset replication if a follower clears its log.
            // Useful in a testing environment.
            allow_log_reversion: Some(true),
            ..Default::default()
        }
        .validate()
        .expect("building raft Config from databend-metasrv config")
    }

    /// Open or create a meta node.
    #[fastrace::trace]
    pub async fn open(config: &RaftConfig) -> Result<Arc<MetaNode<SP>>, MetaStartupError> {
        info!("MetaNode::open, config: {:?}", config);

        let config = config.clone();

        let log_store = RaftStore::open(&config).await?;

        // config.id only used for the first time
        let self_node_id = log_store.id;

        let builder = Self::builder(&config)
            .sto(log_store.clone())
            .node_id(self_node_id)
            .raft_service_endpoint(config.raft_api_listen_host_endpoint());
        let mn = builder.build().await?;

        info!("MetaNode started: {:?}", config);

        Ok(mn)
    }

    /// Open or create a metasrv node.
    ///
    /// Optionally boot a single node cluster.
    /// If `initialize_cluster` is `Some`, initialize the cluster as a single-node cluster.
    #[fastrace::trace]
    pub async fn open_boot(
        config: &RaftConfig,
        initialize_cluster: Option<Node>,
    ) -> Result<Arc<MetaNode<SP>>, MetaStartupError> {
        let mn = Self::open(config).await?;

        if let Some(node) = initialize_cluster {
            mn.init_cluster(node).await?;
        }
        Ok(mn)
    }

    #[fastrace::trace]
    pub async fn stop(&self) -> Result<i32, MetaError> {
        let mut rx = self.raft.metrics();

        let res = self.raft.shutdown().await;
        info!("raft shutdown res: {:?}", res);

        // The returned error does not mean this function call failed.
        // Do not need to return this error. Keep shutting down other tasks.
        if let Err(ref e) = res {
            error!("raft shutdown error: {:?}", e);
        }

        // safe unwrap: receiver wait for change.
        self.running_tx.send(()).unwrap();

        // wait for raft to close the metrics tx
        loop {
            let r = rx.changed().await;
            if r.is_err() {
                break;
            }
            info!(
                "waiting for raft to shutdown, metrics: {:?}",
                rx.borrow_watched()
            );
        }
        info!("shutdown raft");

        for j in self.join_handles.lock().await.iter_mut() {
            let res = j.await;
            info!("task quit res: {:?}", res);

            // The returned error does not mean this function call failed.
            // Do not need to return this error. Keep shutting down other tasks.
            if let Err(ref e) = res {
                error!("task quit with error: {:?}", e);
            }

            self.joined_tasks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        info!("shutdown: id={}", self.raft_store.id);
        let joined = self.joined_tasks.load(std::sync::atomic::Ordering::Relaxed);
        Ok(joined)
    }

    /// Spawn a monitor to watch raft state changes and report metrics changes.
    pub async fn subscribe_metrics(mn: Arc<Self>, metrics_rx: WatchReceiver<RaftMetrics>) {
        info!("Start a task subscribing raft metrics and forward to metrics API");

        let fut = Self::report_metrics_loop(mn.clone(), metrics_rx);

        let h = SP::spawn(
            fut.in_span(Span::enter_with_local_parent("watch-metrics")),
            Some("watch-metrics".into()),
        );

        {
            let mut jh = mn.join_handles.lock().await;
            jh.push(h);
        }
    }

    /// Report metrics changes periodically.
    async fn report_metrics_loop(
        meta_node: Arc<Self>,
        mut metrics_rx: WatchReceiver<RaftMetrics>,
    ) -> Result<(), AnyError> {
        const RATE_LIMIT_INTERVAL: Duration = Duration::from_millis(200);
        const HISTOGRAM_REPORT_INTERVAL: Duration = Duration::from_secs(1);
        const HISTOGRAM_RESET_INTERVAL: Duration = Duration::from_secs(10);

        let mut last_leader: Option<u64> = None;
        let mut last_histogram_report = Instant::now();
        let mut last_histogram_reset = Instant::now();

        loop {
            let loop_start = Instant::now();

            let changed = metrics_rx.changed().await;

            if let Err(changed_err) = changed {
                // Shutting down.
                info!(
                    "{}; when:(watching metrics_rx); quit subscribe_metrics() loop",
                    changed_err
                );
                break;
            }

            let mm = metrics_rx.borrow_watched().clone();

            // Report metrics about server state and role.
            server_metrics::set_node_is_health(
                mm.state == ServerState::Follower || mm.state == ServerState::Leader,
            );
            if mm.current_leader.is_some() && mm.current_leader != last_leader {
                server_metrics::incr_leader_change();
            }
            server_metrics::set_current_leader(mm.current_leader.unwrap_or_default());
            server_metrics::set_is_leader(mm.current_leader == Some(meta_node.raft_store.id));

            // metrics about raft log and state machine.
            server_metrics::set_current_term(mm.current_term);
            server_metrics::set_last_log_index(mm.last_log_index.unwrap_or_default());
            server_metrics::set_proposals_applied(mm.last_applied.map(|id| id.index).unwrap_or(0));
            server_metrics::set_last_seq(meta_node.get_last_seq().await);

            let raft_log_stat = meta_node.get_raft_log_stat().await;
            server_metrics::set_raft_log_stat(&raft_log_stat);

            // metrics about server storage
            server_metrics::set_raft_log_size(meta_node.get_raft_log_size().await);
            server_metrics::set_snapshot_key_count(meta_node.get_snapshot_key_count().await);
            {
                let stat = meta_node.get_snapshot_key_space_stat().await;

                server_metrics::set_snapshot_primary_index_count(
                    stat.get("kv--").copied().unwrap_or_default(),
                );

                server_metrics::set_snapshot_expire_index_count(
                    stat.get("exp-").copied().unwrap_or_default(),
                )
            }

            {
                let db_stat = meta_node.get_snapshot_db_stat().await;
                let snapshot = server_metrics::snapshot();
                snapshot.block_count.set(db_stat.block_num as i64);
                snapshot.data_size.set(db_stat.data_size as i64);
                snapshot.index_size.set(db_stat.index_size as i64);
                snapshot.avg_block_size.set(db_stat.avg_block_size as i64);
                snapshot
                    .avg_keys_per_block
                    .set(db_stat.avg_keys_per_block as i64);
                snapshot.read_block.set(db_stat.read_block as i64);
                snapshot
                    .read_block_from_cache
                    .set(db_stat.read_block_from_cache as i64);
                snapshot
                    .read_block_from_disk
                    .set(db_stat.read_block_from_disk as i64);
            }

            last_leader = mm.current_leader;

            let parsed_metrics = serde_json::to_value(meta_node.get_metrics()).unwrap();
            info!("metrics: {}", parsed_metrics);

            if last_histogram_report.elapsed() >= HISTOGRAM_REPORT_INTERVAL {
                let histogram_report = request_histogram::report();
                info!("request latency: {}", histogram_report);
                info!(
                    "raft log stats: {}",
                    Self::raft_log_stat_to_json(&raft_log_stat)
                );

                // Log openraft per-entry stage latency (proposed -> applied)
                // histograms for debugging where time is spent in the log
                // lifecycle. Formatted as a single line so line-wise grep
                // captures the whole report.
                match meta_node.raft.runtime_stats().await {
                    Ok(mut stats) => {
                        stats.build_log_stage_histograms();
                        info!("openraft runtime stats: {}", stats.display().compact());

                        let h = &stats.log_stage_histograms;
                        let stages = [
                            ("proposed->received", &h.proposed_to_received),
                            ("received->submitted", &h.received_to_submitted),
                            ("submitted->persisted", &h.submitted_to_persisted),
                            ("persisted->committed", &h.persisted_to_committed),
                            ("committed->applied", &h.committed_to_applied),
                            ("proposed->applied", &h.proposed_to_applied),
                        ];
                        let line = stages
                            .iter()
                            .map(|(name, hist)| {
                                let s = hist.percentile_stats();
                                format!("{}(n={} p90={}us p99={}us)", name, s.samples, s.p90, s.p99)
                            })
                            .join(", ");
                        info!("raft log stage latency: {}", line);
                    }
                    Err(e) => {
                        warn!("failed to fetch openraft runtime stats: {}", e);
                    }
                }

                last_histogram_report = Instant::now();
            }

            if last_histogram_reset.elapsed() >= HISTOGRAM_RESET_INTERVAL {
                request_histogram::reset();
                last_histogram_reset = Instant::now();
            }

            let elapsed = loop_start.elapsed();
            if elapsed < RATE_LIMIT_INTERVAL {
                let sleep_duration = RATE_LIMIT_INTERVAL - elapsed;
                sleep(sleep_duration).await;
            }
        }

        Ok(())
    }

    fn raft_log_stat_to_json(st: &RaftLogStat) -> serde_json::Value {
        let closed_chunk_total_size = st.closed_chunks.iter().map(|c| c.size).sum::<u64>();
        let fm = &st.flush_metrics;

        serde_json::json!({
            "payload_cache": {
                "items": st.payload_cache_item_count,
                "max_items": st.payload_cache_max_item,
                "size": st.payload_cache_size,
                "capacity": st.payload_cache_capacity,
                "miss": st.payload_cache_miss,
                "hit": st.payload_cache_hit,
            },
            "wal": {
                "open_chunk_size": st.open_chunk.size,
                "offset": st.open_chunk.global_end,
                "closed_chunk_count": st.closed_chunks.len(),
                "closed_chunk_total_size": closed_chunk_total_size,
                "total_size": closed_chunk_total_size + st.open_chunk.size,
            },
            "flush": {
                "batch_count": fm.batch_count,
                "sync_batch_count": fm.sync_batch_count,
                "write_request_count": fm.write_request_count,
                "write_bytes": fm.write_bytes,
                "callback_count": fm.callback_count,
                "group_wait_count": fm.group_wait_count,
                "group_wait_us": fm.group_wait_us,
                "group_wait_max_us": fm.group_wait_max_us,
                "queued_wait_us": fm.queued_wait_us,
                "queued_wait_max_us": fm.queued_wait_max_us,
                "write_us": fm.write_us,
                "write_max_us": fm.write_max_us,
                "sync_us": fm.sync_us,
                "sync_max_us": fm.sync_max_us,
                "batch_us": fm.batch_us,
                "batch_max_us": fm.batch_max_us,
                "batch_size_max": fm.batch_size_max,
                "batch_bytes_max": fm.batch_bytes_max,
                "last_batch_size": fm.last_batch_size,
                "last_batch_bytes": fm.last_batch_bytes,
                "last_callback_count": fm.last_callback_count,
                "last_sync_us": fm.last_sync_us,
                "last_queued_wait_max_us": fm.last_queued_wait_max_us,
                "avg_batch_size": Self::avg_u64(fm.write_request_count, fm.batch_count),
                "avg_batch_bytes": Self::avg_u64(fm.write_bytes, fm.batch_count),
                "avg_callbacks_per_batch": Self::avg_u64(fm.callback_count, fm.batch_count),
                "avg_group_wait_us": Self::avg_u64(fm.group_wait_us, fm.group_wait_count),
                "avg_queued_wait_us_per_write": Self::avg_u64(fm.queued_wait_us, fm.write_request_count),
                "avg_write_us_per_batch": Self::avg_u64(fm.write_us, fm.batch_count),
                "avg_sync_us_per_sync_batch": Self::avg_u64(fm.sync_us, fm.sync_batch_count),
                "avg_batch_us": Self::avg_u64(fm.batch_us, fm.batch_count),
                "latency_percentiles_us": {
                    "group_wait": Self::latency_percentiles_to_json(&fm.group_wait_percentiles),
                    "queued_wait": Self::latency_percentiles_to_json(&fm.queued_wait_percentiles),
                    "write": Self::latency_percentiles_to_json(&fm.write_percentiles),
                    "sync": Self::latency_percentiles_to_json(&fm.sync_percentiles),
                    "batch": Self::latency_percentiles_to_json(&fm.batch_percentiles),
                },
            },
        })
    }

    fn latency_percentiles_to_json(
        percentiles: &raft_log::FlushLatencyPercentiles,
    ) -> serde_json::Value {
        serde_json::json!({
            "p50": percentiles.p50_us,
            "p90": percentiles.p90_us,
            "p99": percentiles.p99_us,
        })
    }

    fn avg_u64(total: u64, count: u64) -> u64 {
        if count == 0 {
            return 0;
        }

        total / count
    }

    /// Start MetaNode in either `boot`, `single`, `join` or `open` mode,
    /// according to config.
    #[fastrace::trace]
    pub async fn start(config: &MetaServiceConfig) -> Result<Arc<MetaNode<SP>>, MetaStartupError> {
        info!(config :? =(config); "start()");
        let mn = Self::do_start(config).await?;
        info!("Done starting MetaNode: {:?}", config);
        Ok(mn)
    }

    /// Leave the cluster if `--leave` is specified.
    ///
    /// Return whether it has left the cluster.
    #[fastrace::trace]
    pub async fn leave_cluster(conf: &RaftConfig) -> Result<bool, MetaManagementError> {
        if conf.leave_via.is_empty() {
            info!("'--leave-via' is empty, do not need to leave cluster");
            return Ok(false);
        }

        let leave_id = if let Some(id) = conf.leave_id {
            id
        } else {
            info!("'--leave-id' is None, do not need to leave cluster");
            return Ok(false);
        };

        let mut errors = vec![];
        let addrs = &conf.leave_via;
        info!("node-{} about to leave cluster via {:?}", leave_id, addrs);

        #[allow(clippy::never_loop)]
        for addr in addrs {
            info!("leave cluster via {}...", addr);

            let conn_res = connect_raft_service(&RaftPeerTarget::plaintext(addr), conf).await;
            let mut raft_client = match conn_res {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "fail connecting to {} while leaving cluster, err: {:?}",
                        addr, e
                    );
                    errors.push(
                        AnyError::new(&e)
                            .add_context(|| format!("leave {} via: {}", leave_id, addr.clone())),
                    );
                    continue;
                }
            };

            let req = ForwardRequest {
                forward_to_leader: 1,
                body: ForwardRequestBody::Leave(LeaveRequest { node_id: leave_id }),
            };

            let leave_res = raft_client.forward(req).await;
            match leave_res {
                Ok(resp) => {
                    let reply = resp.into_inner();

                    if !reply.data.is_empty() {
                        info!("Done leaving cluster via {} reply: {:?}", addr, reply.data);
                        return Ok(true);
                    } else {
                        error!("leaving cluster via {} fail: {:?}", addr, reply.error);
                        errors.push(
                            AnyError::error(reply.error).add_context(|| {
                                format!("leave {} via: {}", leave_id, addr.clone())
                            }),
                        );
                    }
                }
                Err(s) => {
                    error!("leaving cluster via {} fail: {:?}", addr, s);
                    errors.push(
                        AnyError::new(&s)
                            .add_context(|| format!("leave {} via: {}", leave_id, addr.clone())),
                    );
                }
            };
        }
        Err(MetaManagementError::Leave(AnyError::error(format!(
            "fail to leave {} cluster via {:?}, caused by errors: {}",
            leave_id,
            addrs,
            errors.into_iter().map(|e| e.to_string()).join(", ")
        ))))
    }

    /// Join to an existent cluster if:
    /// - `--join` is specified
    /// - and this node is not in a cluster.
    #[fastrace::trace]
    pub async fn join_cluster(
        &self,
        config: &MetaServiceConfig,
    ) -> Result<Result<(), String>, MetaManagementError> {
        if config.raft_config.join.is_empty() {
            info!("'--join' is empty, do not need joining cluster");
            return Ok(Err("Did not join: --join is empty".to_string()));
        }

        // Try to join a cluster only when this node has no log.
        // Joining a node with log has risk messing up the data in this node and in the target cluster.
        let in_cluster = self
            .is_in_cluster()
            .await
            .map_err(|e| MetaManagementError::Join(AnyError::new(&e)))?;

        if let Ok(reason) = in_cluster {
            info!("skip joining, because: {}", reason);
            return Ok(Err(format!("Did not join: {}", reason)));
        }

        self.do_join_cluster(config).await?;
        Ok(Ok(()))
    }

    #[fastrace::trace]
    async fn do_join_cluster(&self, config: &MetaServiceConfig) -> Result<(), MetaManagementError> {
        let mut errors = vec![];
        let addrs = &config.raft_config.join;

        #[allow(clippy::never_loop)]
        for addr in addrs {
            if addr == &config.raft_config.raft_api_advertise_host_string() {
                info!("avoid join via self: {}", addr);
                continue;
            }

            for _i in 0..3 {
                let res = self.join_via(config, addr).await;
                match res {
                    Ok(x) => return Ok(x),
                    Err(api_err) => {
                        warn!("{} while joining cluster via {}", api_err, addr);
                        let can_retry = api_err.is_retryable();
                        errors.push(api_err);

                        if can_retry {
                            sleep(Duration::from_millis(1_000)).await;
                            continue;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        Err(MetaManagementError::Join(AnyError::error(format!(
            "fail to join node-{} to cluster via {:?}, errors: {}",
            self.raft_store.id,
            addrs,
            errors.into_iter().map(|e| e.to_string()).join(", ")
        ))))
    }

    #[fastrace::trace]
    async fn join_via(
        &self,
        config: &MetaServiceConfig,
        addr: &String,
    ) -> Result<(), MetaAPIError> {
        // Joining a cluster publishes the same record this node would have
        // published had it booted a cluster of its own, advertise addresses
        // included. Building it anywhere but here would be a second answer to
        // what this node advertises, and a chance for the two to disagree.
        let advertised_node = config.get_node();

        let timeout = Some(Duration::from_millis(10_000));
        info!(
            "try to join cluster via {}, timeout: {:?}...",
            addr, timeout
        );

        let chan_res = SP::connect(addr.clone(), timeout, None).await;
        let chan = match chan_res {
            Ok(c) => c,
            Err(e) => {
                error!("connect to {} join cluster fail: {:?}", addr, e);
                let net_err = channel_error_to_network_err(e);
                return Err(MetaAPIError::NetworkError(net_err));
            }
        };
        let mut raft_client = RaftServiceClient::with_interceptor(
            chan,
            RaftSecretInterceptor::new(&config.raft_config),
        );

        let join_req = JoinRequest::new(config.raft_config.id, advertised_node);

        let join_req = if config.raft_config.learner {
            join_req.with_role_learner()
        } else {
            join_req
        };

        let req = ForwardRequest {
            forward_to_leader: 1,
            body: ForwardRequestBody::Join(join_req),
        };

        let join_res = raft_client.forward(req.clone()).await;
        info!("join cluster result: {:?}", join_res);

        match join_res {
            Ok(r) => {
                let reply = r.into_inner();

                let res: Result<ForwardResponse, MetaAPIError> = reply_to_api_result(reply);
                match res {
                    Ok(v) => {
                        info!("join cluster via {} success: {:?}", addr, v);
                        Ok(())
                    }
                    Err(e) => {
                        error!("join cluster via {} fail: {}", addr, e);
                        Err(e)
                    }
                }
            }
            Err(s) => {
                error!("join cluster via {} fail: {:?}", addr, s);
                let net_err = MetaNetworkError::from(s);
                Err(MetaAPIError::NetworkError(net_err))
            }
        }
    }

    /// Check meta-node state to see if it's appropriate to join to a cluster.
    ///
    /// If there is no StorageError, it returns a `Result`: `Ok` indicates this node is already in a cluster.
    /// `Err` explains the reason why it is not in cluster.
    ///
    /// Meta node should decide whether to execute `join_cluster()`:
    ///
    /// - It can not rely on if there are logs.
    ///   It's possible the leader has setup a replication to this new
    ///   node but not yet added it as a **voter**. In such a case, this node will
    ///   never be added into the cluster automatically.
    ///
    /// - It must detect if there is a committed **membership** config
    ///   that includes this node. Thus only when a node has already joined to a
    ///   cluster(leader committed the membership and has replicated it to this node),
    ///   it skips the join process.
    ///
    ///   Why skip checking membership in raft logs:
    ///
    ///   A leader may have replicated **non-committed** membership to this node and the crashed.
    ///   Then the next leader does not know about this new node.
    ///
    ///   Only when the membership is committed, this node can be sure it is in a cluster.
    async fn is_in_cluster(&self) -> Result<Result<String, String>, io::Error> {
        let membership = {
            let sm = &self.raft_store.get_state_machine();
            sm.sys_data().last_membership_ref().membership().clone()
        };
        info!("is_in_cluster: membership: {:?}", membership);

        let voter_ids = membership.voter_ids().collect::<BTreeSet<_>>();

        if voter_ids.contains(&self.raft_store.id) {
            return Ok(Ok(format!(
                "node {} already in cluster",
                self.raft_store.id
            )));
        }

        Ok(Err(format!(
            "node {} has membership but not in it",
            self.raft_store.id
        )))
    }

    async fn do_start(conf: &MetaServiceConfig) -> Result<Arc<MetaNode<SP>>, MetaStartupError> {
        let raft_conf = &conf.raft_config;

        if raft_conf.single {
            let mn = Self::open(raft_conf).await?;
            mn.init_cluster(conf.get_node()).await?;
            return Ok(mn);
        }

        let mn = Self::open(raft_conf).await?;
        Ok(mn)
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    #[fastrace::trace]
    pub async fn boot(config: &MetaServiceConfig) -> Result<Arc<MetaNode<SP>>, MetaStartupError> {
        let mn = Self::open(&config.raft_config).await?;
        mn.init_cluster(config.get_node()).await?;
        Ok(mn)
    }

    /// Initialized a single node cluster if this node is just created:
    /// - Initializing raft membership.
    /// - Adding current node into the meta data.
    #[fastrace::trace]
    pub async fn init_cluster(&self, node: Node) -> Result<(), MetaStartupError> {
        info!("Initialize node as single node cluster: {:?}", node);

        let node_id = self.raft_store.id;

        let mut cluster_node_ids = BTreeSet::new();
        cluster_node_ids.insert(node_id);

        // initialize() and add_node() are not done atomically.
        // There is an issue that just after initializing the cluster,
        // the node will be used but no node info is found.
        // Thus, meta-server can only be initialized with a single node.
        //
        // We do not store node info in membership config,
        // because every start a meta-server node updates its latest configured address.
        let res = self.raft.initialize(cluster_node_ids.clone()).await;
        match res {
            Ok(_) => {
                info!("Initialized with: {:?}", cluster_node_ids);
            }
            Err(e) => match e {
                RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(e) => {
                        info!("Already initialized: {}", e);
                    }
                    InitializeError::NotInMembers(e) => {
                        return Err(MetaStartupError::InvalidConfig(e.to_string()));
                    }
                },
                RaftError::Fatal(fatal) => {
                    return Err(MetaStartupError::MetaServiceError(fatal.to_string()));
                }
            },
        }

        if self.get_node(&node_id).await.is_none() {
            info!(
                "This node not found in state-machine; add node: {}:{:?}",
                node_id, node
            );
            self.add_node(node_id, node.clone()).await.map_err(|e| {
                MetaStartupError::AddNodeError {
                    source: AnyError::new(&e),
                }
            })?;
        } else {
            info!("This node already in state-machine; No need to add");
        }

        info!("Done initializing node as single node cluster: {:?}", node);

        Ok(())
    }

    #[fastrace::trace]
    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        // inconsistent get: from local state machine

        let sm = self.raft_store.get_state_machine();
        let sys_data = sm.sys_data();
        info!("get_node: node_id: {}, sys_data: {:?}", node_id, sys_data);

        let n = sys_data.nodes_ref().get(node_id).cloned();
        n
    }

    #[fastrace::trace]
    pub async fn get_nodes(&self) -> Vec<Node> {
        // inconsistent get: from local state machine

        let sm = self.raft_store.get_state_machine();
        let nodes = sm
            .sys_data()
            .nodes_ref()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        nodes
    }

    /// Get the size in bytes of the on disk files of the raft log storage.
    async fn get_raft_log_size(&self) -> u64 {
        self.raft_store.log().read().await.on_disk_size()
    }

    async fn get_raft_log_stat(&self) -> RaftLogStat {
        self.raft_store.log().read().await.stat()
    }

    async fn get_snapshot_key_count(&self) -> u64 {
        self.raft_store
            .try_get_snapshot_key_count()
            .await
            .unwrap_or_default()
    }

    async fn get_snapshot_key_space_stat(&self) -> BTreeMap<String, u64> {
        self.raft_store.get_snapshot_key_space_stat().await
    }

    async fn get_snapshot_db_stat(&self) -> DBStat {
        self.raft_store.get_snapshot_db_stat().await
    }

    /// Return a structured, typed snapshot of the entire metric registry.
    ///
    /// This is the programmatic counterpart of the Prometheus text exposition
    /// ([`meta_metrics_to_prometheus_string`]): instead of a string to grep, the
    /// caller reads typed fields, e.g. `metrics.server.current_term`.
    ///
    /// [`meta_metrics_to_prometheus_string`]: crate::metrics::meta_metrics_to_prometheus_string
    pub fn get_metrics(&self) -> MetaMetrics {
        MetaMetrics::from_metric_set(&crate::metrics::meta_metrics_to_metric_set())
    }

    pub async fn get_status(&self, binary_version: &str) -> MetaNodeStatus {
        let voters = self
            .raft_store
            .get_nodes(|ms| ms.voter_ids().collect::<Vec<_>>())
            .await;

        let learners = self
            .raft_store
            .get_nodes(|ms| ms.learner_ids().collect::<Vec<_>>())
            .await;

        let endpoint = self
            .raft_store
            .get_node_raft_endpoint(&self.raft_store.id)
            .await;

        let raft_log_status = self.get_raft_log_stat().await.into();
        let snapshot_key_count = self.get_snapshot_key_count().await;
        let snapshot_key_space_stat = self.get_snapshot_key_space_stat().await;

        let metrics = self.raft.metrics().borrow_watched().clone();

        let leader = if let Some(leader_id) = metrics.current_leader {
            self.get_node(&leader_id).await
        } else {
            None
        };

        let last_seq = self.get_last_seq().await;

        MetaNodeStatus {
            id: self.raft_store.id,
            binary_version: binary_version.to_string(),
            data_version: DATA_VERSION,
            endpoint: endpoint.map(|x| x.to_string()),
            raft_log: raft_log_status,
            snapshot_key_count,
            snapshot_key_space_stat,
            state: format!("{:?}", metrics.state),
            is_leader: metrics.state == openraft::ServerState::Leader,
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index.unwrap_or(0),
            last_applied: metrics.last_applied.unwrap_or(new_log_id(0, 0, 0)),
            snapshot_last_log_id: metrics.snapshot,
            purged: metrics.purged,
            leader,
            replication: metrics.replication,
            voters,
            non_voters: learners,
            last_seq,
        }
    }

    pub(crate) async fn get_last_seq(&self) -> u64 {
        let sm = self.raft_store.get_state_machine();
        sm.sys_data().curr_seq()
    }

    #[fastrace::trace]
    pub async fn get_grpc_advertise_addrs(&self) -> Vec<String> {
        // Maybe stale get: from local state machine

        let nodes = {
            let sm = self.raft_store.get_state_machine();
            sm.sys_data()
                .nodes_ref()
                .values()
                .cloned()
                .collect::<Vec<_>>()
        };

        let endpoints: Vec<String> = nodes
            .iter()
            .map(|n: &Node| {
                if let Some(addr) = n.grpc_api_advertise_address.clone() {
                    addr
                } else {
                    // for compatibility with old version that not have grpc_api_addr in NodeInfo.
                    "".to_string()
                }
            })
            .collect();
        endpoints
    }

    #[fastrace::trace]
    pub async fn handle_forwardable_request<Req>(
        &self,
        req: ForwardRequest<Req>,
    ) -> Result<(Option<Endpoint>, Req::Reply), ForwardRequestError>
    where
        Req: RequestFor + Clone,
        for<'a> MetaLeader<'a, SP>: Handler<Req>,
        for<'a> MetaForwarder<'a, SP>: Forwarder<Req>,
    {
        let id = self.raft_store.id;
        debug!(
            "id={} forward_quota={} handle_forwardable_request req={:?}",
            id, req.forward_to_leader, req
        );

        let mut n_retry = 20;
        let mut slp = Duration::from_millis(1_000);

        loop {
            let assume_leader_res = self.assume_leader().await;
            debug!(
                "id={} assume_leader: is_err: {}",
                id,
                assume_leader_res.is_err()
            );

            // Handle the request locally, or get a ForwardToLeader hint.
            let to_leader = match assume_leader_res {
                Ok(leader) => match leader.handle(req.clone()).await.peel() {
                    Ok(Ok(reply)) => return Ok((None, reply)),
                    Ok(Err(data_err)) => return Err(data_err.into()),
                    Err(ftl) => ftl,
                },
                Err(ftl) => ftl,
            };

            let leader_id = to_leader.leader_id.ok_or_else(|| {
                CanNotForwardError(AnyError::error("need to forward but no known leader"))
            })?;

            let req_cloned = req.next()?;

            let f = MetaForwarder::new(self);
            let res = f.forward(leader_id, req_cloned).await;

            let forward_err = match res {
                Ok(reply) => {
                    let leader_grpc_endpoint = self
                        .get_node(&leader_id)
                        .await
                        .and_then(|node| node.grpc_api_advertise_address)
                        .and_then(|leader_grpc_address| {
                            let endpoint_res = Endpoint::parse(&leader_grpc_address);

                            match endpoint_res {
                                Ok(o) => Some(o),
                                Err(e) => {
                                    error!(
                                        "fail to parse leader_grpc_address: {}; error: {}",
                                        &leader_grpc_address, e
                                    );
                                    None
                                }
                            }
                        });

                    return Ok((leader_grpc_endpoint, reply));
                }
                Err(forward_err) => forward_err,
            };

            match forward_err {
                ForwardRPCError::NetworkError(ref net_err) => {
                    warn!(
                        "{} retries left, sleep time: {:?}; forward_to {} failed: {}",
                        n_retry, slp, leader_id, net_err
                    );

                    n_retry -= 1;
                    if n_retry == 0 {
                        error!("no more retry for forward_to {}", leader_id);
                        return Err(forward_err.into());
                    } else {
                        tokio::time::sleep(slp).await;
                        slp = std::cmp::min(slp * 2, Duration::from_secs(1));
                        continue;
                    }
                }
                ForwardRPCError::RemoteError(_) => {
                    return Err(forward_err.into());
                }
            }
        }
    }

    /// Return a MetaLeader if `self` believes it is the leader.
    ///
    /// Otherwise it returns the leader in a ForwardToLeader error.
    #[fastrace::trace]
    pub async fn assume_leader(&self) -> Result<MetaLeader<'_, SP>, ForwardToLeader> {
        let leader_id = self.get_leader().await.map_err(|e| {
            error!("raft metrics rx closed: {}", e);
            ForwardToLeader {
                leader_id: None,
                leader_node: None,
            }
        })?;

        debug!("curr_leader_id: {:?}", leader_id);

        if leader_id == Some(self.raft_store.id) {
            return Ok(MetaLeader::new(self));
        }

        Err(ForwardToLeader {
            leader_id,
            leader_node: None,
        })
    }

    /// Add a new node into this cluster.
    /// The node info is committed with raft, thus it must be called on an initialized node.
    pub async fn add_node(
        &self,
        node_id: NodeId,
        node: Node,
    ) -> Result<AppliedState, MetaAPIError> {
        let cmd = Cmd::AddNode {
            node_id,
            node,
            overriding: false,
        };
        let resp = self.write(LogEntry::new(cmd)).await?;

        self.raft
            .change_membership(
                ChangeMembers::AddNodes(btreemap! {node_id => MembershipNode{}}),
                true,
            )
            .await
            .map_err(MetaOperationError::from)?;

        Ok(resp)
    }

    /// Propose a log entry to set a feature.
    pub async fn set_feature(
        &self,
        feature: StateMachineFeature,
        enable: bool,
    ) -> Result<(), MetaAPIError> {
        let cmd = Cmd::SetFeature {
            feature: feature.to_string(),
            enable,
        };

        self.write(LogEntry::new(cmd)).await?;
        Ok(())
    }

    /// Submit a write request to the known leader. Returns the response after applying the request.
    #[fastrace::trace]
    pub async fn write(&self, req: LogEntry) -> Result<AppliedState, ForwardRequestError> {
        debug!("{} req: {:?}", func_name!(), req);

        // TODO: enable returning endpoint
        let (_endpoint, res) = self
            .handle_forwardable_request(ForwardRequest::new(
                1,
                ForwardRequestBody::Write(req.clone()),
            ))
            .await?;

        let res: AppliedState = res.try_into().expect("expect AppliedState");

        Ok(res)
    }

    /// Try to get the leader from the latest metrics of the local raft node.
    /// If leader is absent, wait for an metrics update in which a leader is set.
    ///
    /// databend-meta requires every node (including the leader) to be in the
    /// effective membership. openraft 0.10-alpha.18 changed
    /// `RaftMetrics::current_leader` to expose the raw vote leader id without
    /// validating it against the membership, which is valid in openraft's
    /// model but violates our invariant. Treat a leader that is not a voter
    /// as "no known leader" so the node can self-elect (single-voter
    /// clusters) or wait for a real leader to emerge.
    #[fastrace::trace]
    pub async fn get_leader(&self) -> Result<Option<NodeId>, RecvError> {
        let mut rx = self.raft.metrics();

        let mut expire_at: Option<Instant> = None;

        loop {
            let leader_id = {
                let m = rx.borrow_watched();
                m.current_leader
                    .filter(|id| m.membership_config.voter_ids().any(|v| &v == id))
            };

            if let Some(l) = leader_id {
                return Ok(Some(l));
            }

            if expire_at.is_none() {
                let timeout = Duration::from_millis(2_000);
                expire_at = Some(Instant::now() + timeout);
            }
            if Some(Instant::now()) > expire_at {
                warn!("timeout waiting for a leader");
                return Ok(None);
            }

            // Wait for metrics to change and re-fetch the leader id.
            //
            // Note that when it returns, `changed()` will mark the most recent value as **seen**.
            rx.changed().await?;
        }
    }

    pub(crate) async fn handle_watch(
        &self,
        watch: WatchRequest,
    ) -> Result<BoxStream<'static, Result<WatchResponse, Status>>, Status> {
        info!("{}: Received WatchRequest: {}", func_name!(), watch);

        let key_range = build_key_range(
            &UserKey::new(&watch.key),
            &watch.key_end.as_ref().map(UserKey::new),
        )
        .map_err(Status::invalid_argument)?;
        let flush = watch.initial_flush;

        let (tx, rx) = mpsc::channel(4);

        let mn = self;

        // Atomically:
        // - add watcher tx to dispatcher;
        // - reads and forwards a range of key-value pairs to the provided `tx`.
        //
        // This ensures consistency by:
        // 1. Queuing all data publishing through the singleton sender to maintain event ordering
        // 2. Reading the key-value range atomically within the state machine
        // 3. Forwarding the data to the event sender in a single transaction
        //
        // This approach prevents race conditions and guarantees that no events will be
        // delivered out of order to the watcher.
        let stream = {
            // Acquire exclusive writer permit to ensure atomicity of the entire operation:
            //
            // 1. Register the watcher to dispatcher
            // 2. Read the snapshot of the key range
            // 3. Queue the initialization data for forwarding
            //
            // This permit blocks all state machine writes (Raft log application) during this block,
            // preventing the race condition where:
            // - A write is applied to state machine after snapshot read but before watcher registration
            // - The watcher would miss events for that write
            //
            // The permit is held until the end of this scope (line 1575), which includes:
            // - Creating and registering the watch sender
            // - Reading the snapshot (if flush=true)
            // - Queuing the initialization future to dispatcher
            //
            // Since watch ranges are typically small and snapshot reads are fast,
            // the blocking duration is acceptable for maintaining correctness.
            //
            // The permit is acquired BEFORE fetching the state machine handle:
            // snapshot installation swaps the state machine while holding this permit,
            // so a handle fetched afterwards stays current for the whole block.
            // With the reverse order, an installation finishing in between would leave
            // this block registering the watcher on the discarded state machine and
            // reading its stale data.
            let _permit = mn
                .raft_store
                .get_state_machine()
                .acquire_writer_permit()
                .await;

            let sm = mn.raft_store.get_state_machine();

            let sender = mn.new_watch_sender(watch, tx.clone())?;
            let sender_str = sender.to_string();
            let weak_sender = mn.insert_watch_sender(sender);

            // Build a closure to remove the stream tx from Dispatcher when the stream is dropped.
            let on_drop = {
                let weak_handle = Arc::downgrade(&mn.dispatcher_handle);
                move || {
                    try_remove_sender(weak_sender, weak_handle, "on-drop-WatchStream");
                }
            };

            let stream = WatchStream::new(rx, Box::new(on_drop));

            let stream = stream.map(move |item| {
                if let Ok(ref resp) = item {
                    network_metrics::incr_watch_sent(resp);
                }
                item
            });

            if flush {
                let ctx = "watch-Dispatcher";
                let snk = new_initialization_sink::<WatchTypes>(tx.clone(), ctx);
                let strm = sm.to_read_view().range(key_range).await?;
                let strm = strm
                    .try_filter_map(|(k, marked)| future::ready(Ok(seq_marked_to_seqv(k, marked))));

                info!("created initialization stream for {}", sender_str);

                let sndr = sender_str.clone();

                let fu = async move {
                    try_forward(strm, snk, ctx).await;

                    info!("initialization flush complete for watcher {}", sndr);

                    // Send an empty message with `is_initialization=false` to indicate
                    // the end of the initialization flush.
                    tx.send(Ok(WatchResponse::new_initialization_complete()))
                        .await
                        .map_err(|e| {
                            error!("failed to send flush complete message: {}", e);
                        })
                        .ok();

                    info!(
                        "finished sending initialization complete flag for watcher {}",
                        sndr
                    );
                };
                let fu = Box::pin(fu);

                info!(
                    "sending initial flush Future to watcher {} via Dispatcher",
                    sender_str
                );

                mn.dispatcher_handle.send_command(Command::Future(fu));
            }

            stream
        };
        Ok(Box::pin(stream))
    }

    pub(crate) fn insert_watch_sender(
        &self,
        sender: Arc<WatchStreamSender<WatchTypes>>,
    ) -> Weak<WatchStreamSender<WatchTypes>> {
        let weak = Arc::downgrade(&sender);

        self.dispatcher_handle
            .request(move |dispatcher: &mut Dispatcher<WatchTypes>| {
                dispatcher.insert_watch_stream_sender(sender);
            });

        weak
    }

    pub(crate) fn new_watch_sender(
        &self,
        request: WatchRequest,
        tx: mpsc::Sender<Result<WatchResponse, Status>>,
    ) -> Result<Arc<WatchStreamSender<WatchTypes>>, Status> {
        let key_range = match build_key_range(&request.key, &request.key_end) {
            Ok(kr) => kr,
            Err(e) => return Err(Status::invalid_argument(e.to_string())),
        };

        let interested = event_filter_from_filter_type(request.filter_type());

        let sender = Dispatcher::new_watch_stream_sender(key_range.clone(), interested, tx);
        Ok(sender)
    }

    pub fn runtime_config(&self) -> &RuntimeConfig {
        &self.runtime_config
    }

    /// Get the gRPC endpoint for the leader node.
    async fn get_leader_endpoint(&self, leader_id: Option<NodeId>) -> Option<Endpoint> {
        let leader_id = leader_id?;
        let node = self.get_node(&leader_id).await?;
        let addr = node.grpc_api_advertise_address.as_ref()?;
        Endpoint::parse(addr).ok()
    }

    /// Return a `MetaLeader` if this node is the leader.
    ///
    /// Otherwise, resolve the leader endpoint and return a forward-to-leader gRPC `Status`.
    async fn leader_or_forward(&self) -> Result<MetaLeader<'_, SP>, Status> {
        match self.assume_leader().await {
            Ok(leader) => Ok(leader),
            Err(forward) => {
                let endpoint = self.get_leader_endpoint(forward.leader_id).await;
                Err(GrpcHelper::status_forward_to_leader(endpoint.as_ref()))
            }
        }
    }

    /// Handle KvTransaction request. Must be leader to process.
    ///
    /// If this node is not the leader, returns a `Status` error with leader endpoint in metadata.
    pub async fn handle_kv_transaction(
        &self,
        txn: kv_transaction::Transaction,
    ) -> Result<KvTransactionReply, Status> {
        let leader = self.leader_or_forward().await?;

        let entry = LogEntry::new(Cmd::KvTransaction(txn));
        let applied = match leader.write(entry).await {
            Ok(applied) => applied,
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                let endpoint = self.get_leader_endpoint(forward.leader_id).await;
                return Err(GrpcHelper::status_forward_to_leader(endpoint.as_ref()));
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        let reply: KvTransactionReply = applied.try_into().expect("expect KvTransactionReply");

        Ok(reply)
    }

    /// Handle KvList request. Must be leader to process.
    ///
    /// Returns a stream of key-value pairs matching the prefix.
    /// If this node is not the leader, returns a `Status` error with leader endpoint in metadata.
    pub async fn handle_kv_list(
        &self,
        prefix: String,
        limit: Option<u64>,
    ) -> Result<BoxStream<'static, Result<StreamItem, Status>>, Status> {
        let leader = self.leader_or_forward().await?;

        let strm = leader
            .kv_list(&prefix, limit)
            .await
            .map_err(|e| Status::internal(format!("kv_list error: {}", e)))?;

        Ok(strm)
    }

    /// Handle KvGetMany request. Must be leader to process.
    ///
    /// Takes a stream of keys and returns a stream of key-value pairs.
    /// If this node is not the leader, returns a `Status` error with leader endpoint in metadata.
    pub async fn handle_kv_get_many(
        &self,
        input: impl Stream<Item = Result<KvGetManyRequest, Status>> + Send + 'static,
    ) -> Result<BoxStream<'static, Result<StreamItem, Status>>, Status> {
        let leader = self.leader_or_forward().await?;

        let strm = leader
            .kv_get_many(input)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(strm)
    }
}

pub(crate) fn event_filter_from_filter_type(filter_type: FilterType) -> EventFilter {
    match filter_type {
        FilterType::All => EventFilter::all(),
        FilterType::Update => EventFilter::update(),
        FilterType::Delete => EventFilter::delete(),
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_runtime_api::TokioRuntime;

    use super::*;

    fn empty_chunk_stat<T: Default>() -> raft_log::ChunkStat<T> {
        raft_log::ChunkStat {
            chunk_id: raft_log::ChunkId(0),
            records_count: 0,
            global_start: 0,
            global_end: 0,
            size: 0,
            log_state: Default::default(),
        }
    }

    fn empty_raft_log_stat() -> RaftLogStat {
        RaftLogStat {
            closed_chunks: vec![],
            open_chunk: empty_chunk_stat(),
            payload_cache_last_evictable: None,
            payload_cache_item_count: 0,
            payload_cache_max_item: 0,
            payload_cache_size: 0,
            payload_cache_capacity: 0,
            payload_cache_miss: 0,
            payload_cache_hit: 0,
            flush_metrics: Default::default(),
        }
    }

    #[test]
    fn test_raft_log_stat_to_json_zero_flush_counts() {
        let result = MetaNode::<TokioRuntime>::raft_log_stat_to_json(&empty_raft_log_stat());
        let flush = &result["flush"];

        assert_eq!(
            serde_json::json!({
                "avg_batch_size": flush["avg_batch_size"],
                "avg_batch_bytes": flush["avg_batch_bytes"],
                "avg_callbacks_per_batch": flush["avg_callbacks_per_batch"],
                "avg_group_wait_us": flush["avg_group_wait_us"],
                "avg_queued_wait_us_per_write": flush["avg_queued_wait_us_per_write"],
                "avg_write_us_per_batch": flush["avg_write_us_per_batch"],
                "avg_sync_us_per_sync_batch": flush["avg_sync_us_per_sync_batch"],
                "avg_batch_us": flush["avg_batch_us"],
            }),
            serde_json::json!({
                "avg_batch_size": 0,
                "avg_batch_bytes": 0,
                "avg_callbacks_per_batch": 0,
                "avg_group_wait_us": 0,
                "avg_queued_wait_us_per_write": 0,
                "avg_write_us_per_batch": 0,
                "avg_sync_us_per_sync_batch": 0,
                "avg_batch_us": 0,
            })
        );
    }
}
