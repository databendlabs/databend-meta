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

//! Raft cluster configuration including networking, storage, and performance tuning.

use std::io;
use std::net::Ipv4Addr;
use std::path::Path;
use std::time::Duration;

use databend_meta_runtime_api::SpawnApi;
use databend_meta_types::Endpoint;
use databend_meta_types::raft_types::NodeId;
use raft_log::chunked_wal::Config as WalConfig;

use crate::MetaStartupError;
use crate::data_version::DATA_VERSION;
use crate::secret::Secret;

// Size unit constants
const KB: u64 = 1024;
const MB: u64 = 1024 * KB;
const GB: u64 = 1024 * MB;

/// Configuration for a Raft node including networking, storage paths, and performance tuning.
///
/// Controls cluster behavior, network endpoints, storage persistence, and operational parameters
/// like heartbeat intervals and snapshot thresholds.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftConfig {
    /// Identify a config.
    /// This is only meant to make debugging easier with more than one Config involved.
    pub config_id: String,

    /// The local listening host for metadata communication.
    /// This config does not need to be stored in raft-store,
    /// only used when metasrv startup and listen to.
    pub raft_listen_host: String,

    /// The hostname that other nodes will use to connect this node.
    /// This host should be stored in raft store and be replicated to the raft cluster,
    /// i.e., when calling add_node().
    /// Use `localhost` by default.
    pub raft_advertise_host: String,

    /// The listening port for metadata communication.
    pub raft_api_port: u16,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    pub raft_dir: String,

    /// Maximum log entries to cache in memory.
    ///
    /// Higher values improve read performance but use more memory.
    /// Default: 1,000,000 entries.
    pub log_cache_max_items: u64,

    /// Maximum memory for log cache in bytes.
    ///
    /// Total memory limit for cached log entries.
    /// Default: 1GB (1,073,741,824 bytes).
    pub log_cache_capacity: u64,

    /// Maximum log records per WAL chunk
    pub log_wal_chunk_max_records: u64,

    /// Maximum WAL chunk size in bytes
    pub log_wal_chunk_max_size: u64,

    /// Trigger snapshot after this many logs since last snapshot.
    ///
    /// Lower values create more frequent snapshots but increase I/O.
    /// Default: 1024 log entries.
    pub snapshot_logs_since_last: u64,

    /// Leader heartbeat interval in milliseconds.
    ///
    /// Must be > 0. Typical values: 500-2000ms.
    /// Affects leader election timeout (2x-3x this value).
    /// Default: 1000ms.
    pub heartbeat_interval: u64,

    /// Install snapshot timeout in milliseconds.
    ///
    /// Time to wait for snapshot installation to complete.
    /// Default: 4000ms.
    pub install_snapshot_timeout: u64,

    /// Maximum applied logs to keep before purging.
    ///
    /// Controls disk usage vs recovery time tradeoff.
    /// Default: 1000 log entries.
    pub max_applied_log_to_keep: u64,

    /// Snapshot transmission chunk size in bytes.
    ///
    /// Larger chunks reduce network overhead but increase memory usage.
    /// Default: 4MB (4,194,304 bytes).
    pub snapshot_chunk_size: u64,

    /// Whether to check keys fed to snapshot are sorted.
    ///
    /// Enable for debugging snapshot corruption issues.
    /// Adds performance overhead in debug builds.
    /// Default: true.
    pub snapshot_db_debug_check: bool,

    /// Maximum keys per snapshot database block.
    ///
    /// Higher values improve compression but increase memory usage.
    /// Default: 8000 keys per block.
    pub snapshot_db_block_keys: u64,

    /// Total cache size for snapshot blocks in bytes.
    ///
    /// Controls memory usage for snapshot block caching. Since rotbl 0.2.10
    /// the block cache is weight-bounded by bytes (`moka::sync::Cache` with
    /// a weigher), so this is the only cache sizing knob.
    /// Default: 1GB (1,073,741,824 bytes).
    pub snapshot_db_block_cache_size: u64,

    /// Interval in milliseconds to compact the in memory immutable levels.
    pub compact_immutables_ms: Option<u64>,

    /// Single node metasrv. It creates a single node cluster if meta data is not initialized.
    /// Otherwise it opens the previous one.
    /// This is mainly for testing purpose.
    pub single: bool,

    /// Bring up a metasrv node and join a cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    pub join: Vec<String>,

    /// Whether this node is a learner.
    ///
    /// A learner does not vote in elections, and does not count towards quorum.
    pub learner: bool,

    /// Do not run databend-meta, but just remove a node from its cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `leave` request.
    pub leave_via: Vec<String>,

    /// The node id to leave from the cluster.
    ///
    /// It will be ignored if `--leave-via` is absent.
    pub leave_id: Option<NodeId>,

    /// The node id. Only used when this server is not initialized,
    ///  e.g. --boot or --single for the first time.
    ///  Otherwise this argument is ignored.
    pub id: NodeId,

    ///  The node name. If the user specifies a name,
    /// the user-supplied name is used, if not, the default name is used.
    pub cluster_name: String,

    /// Max timeout(in milli seconds) when waiting a cluster leader.
    pub wait_leader_timeout: u64,

    /// Maximum gRPC message size for raft communication in bytes.
    ///
    /// Used for both encoding and decoding limits on raft gRPC channels.
    /// Default: 32MB (33,554,432 bytes).
    pub raft_grpc_max_message_size: Option<usize>,

    /// The secret this node attaches to the raft RPCs it sends.
    ///
    /// `None` sends nothing, which is how a not yet configured node behaves.
    /// Peers that are not strict still accept such a node, so a cluster can
    /// adopt the secret without downtime.
    ///
    /// Visible ASCII only, with no space: it is sent verbatim as an HTTP header
    /// value. [`RaftConfig::check`] refuses anything else.
    pub raft_secret: Option<Secret>,

    /// The secrets this node accepts on the raft RPCs it receives.
    ///
    /// Accepting more than one is what makes rotation possible without
    /// downtime: add the new secret here on every node first, then move
    /// `raft_secret` over node by node, then drop the old one from here.
    ///
    /// Restricted to the same characters as `raft_secret`, since these are the
    /// values other nodes send.
    pub raft_accepted_secrets: Vec<Secret>,

    /// Whether to reject a received raft RPC carrying no or an unaccepted secret.
    ///
    /// `None` means unspecified, which is distinct from an explicit `false`:
    /// the layer that merges config sources has to tell the two apart.
    /// Must stay off until every node of the cluster sends a secret: turning
    /// it on earlier makes the nodes declare each other unreachable.
    /// Default: false.
    pub raft_secret_strict: Option<bool>,

    /// Path to the certificate chain the raft TLS listener presents, in PEM.
    ///
    /// Read by the listening half of the node. [`RaftConfig::check`] refuses it
    /// without `raft_tls_server_key`, since half an identity cannot serve TLS.
    pub raft_tls_server_cert: Option<String>,

    /// Path to the private key of `raft_tls_server_cert`, in PEM.
    pub raft_tls_server_key: Option<String>,

    /// Path to the CA that a peer's certificate is verified against.
    ///
    /// Read by the dialing half of the node, and setting it is what turns
    /// dialing over TLS on: verifying a peer takes a CA to verify it against,
    /// and there is no usable default, since the OS trust store holds public
    /// CAs rather than the internal CA that signs raft certificates.
    ///
    /// This decides whether *this node* is willing to speak TLS at all.
    /// Whether a given peer is actually dialed over TLS is a separate question,
    /// answered per peer by whether that peer publishes a TLS port.
    pub raft_tls_client_root_ca_cert: Option<String>,

    /// The name this node expects in a peer's certificate.
    ///
    /// One certificate issued for this single name serves the whole cluster,
    /// and the name does not have to resolve in DNS. `None` verifies the peer
    /// against the address that was dialed instead. Only consulted when
    /// `raft_tls_client_root_ca_cert` is set, so [`RaftConfig::check`] refuses
    /// this one without it rather than letting it sit unused.
    pub raft_tls_client_domain_name: Option<String>,

    /// The port this node's raft TLS listener binds.
    ///
    /// A node publishes this port in its own `Node` record, so a peer reads it
    /// from membership instead of deriving it, and every node is free to pick
    /// its own. A peer that publishes no TLS port is dialed in plaintext, which
    /// is what keeps the two upgrades from having to happen in a fixed order.
    /// `None` disables the TLS listener.
    pub raft_tls_port: Option<u16>,
}

pub fn get_default_raft_advertise_host() -> String {
    match hostname::get() {
        Ok(h) => match h.into_string() {
            Ok(h) => h,
            _ => "UnknownHost".to_string(),
        },
        _ => "UnknownHost".to_string(),
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            config_id: "".to_string(),
            raft_listen_host: "127.0.0.1".to_string(),
            raft_advertise_host: get_default_raft_advertise_host(),
            raft_api_port: 28004,
            raft_dir: "./.databend/meta".to_string(),

            log_cache_max_items: 1_000_000,
            log_cache_capacity: GB,
            log_wal_chunk_max_records: 100_000,
            log_wal_chunk_max_size: 256 * MB,

            snapshot_logs_since_last: 1024,
            heartbeat_interval: 1000,
            install_snapshot_timeout: 4000,
            max_applied_log_to_keep: 1000,
            snapshot_chunk_size: 4 * MB,

            snapshot_db_debug_check: true,
            snapshot_db_block_keys: 8000,
            snapshot_db_block_cache_size: GB,

            compact_immutables_ms: None,
            single: false,
            join: vec![],
            learner: false,
            leave_via: vec![],
            leave_id: None,
            id: 0,
            cluster_name: "foo_cluster".to_string(),
            wait_leader_timeout: 70000,
            raft_grpc_max_message_size: None,
            raft_secret: None,
            raft_accepted_secrets: vec![],
            raft_secret_strict: None,
            raft_tls_server_cert: None,
            raft_tls_server_key: None,
            raft_tls_client_root_ca_cert: None,
            raft_tls_client_domain_name: None,
            raft_tls_port: None,
        }
    }
}

const DEFAULT_RAFT_GRPC_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

impl RaftConfig {
    /// Returns the maximum gRPC message size for raft communication.
    pub fn raft_grpc_max_message_size(&self) -> usize {
        self.raft_grpc_max_message_size
            .unwrap_or(DEFAULT_RAFT_GRPC_MESSAGE_SIZE)
    }

    /// Returns the advisory maximum message size (90% of max) for raft communication.
    ///
    /// Used to check payload size before sending to avoid hitting the hard limit.
    pub fn raft_grpc_advisory_message_size(&self) -> usize {
        self.raft_grpc_max_message_size() * 9 / 10
    }

    /// Returns whether to reject a raft RPC carrying no or an unaccepted secret.
    pub fn raft_secret_strict(&self) -> bool {
        self.raft_secret_strict.unwrap_or(false)
    }

    /// Returns whether this node is willing to dial its peers over TLS.
    ///
    /// Configuring a CA is the whole switch: without one there is nothing to
    /// verify a peer's certificate against, and with one there is no reason to
    /// refuse. Which peers are then actually dialed over TLS is decided per
    /// peer, by whether that peer publishes a TLS port in its `Node` record.
    pub fn raft_tls_client_enabled(&self) -> bool {
        self.raft_tls_client_root_ca_cert.is_some()
    }

    /// Returns whether this node listens on a raft TLS port.
    ///
    /// Serving TLS takes an identity to present and a port to bind, so a node
    /// missing the certificate, its key, or the port does not listen. That is
    /// what makes a node with no TLS configuration at all behave as before.
    pub fn raft_tls_listener_enabled(&self) -> bool {
        let has_cert = self.raft_tls_server_cert.is_some();
        let has_key = self.raft_tls_server_key.is_some();
        has_cert && has_key && self.raft_tls_port.is_some()
    }

    pub fn to_rotbl_config(&self) -> rotbl::v001::Config {
        rotbl::v001::Config::default()
            .with_debug_check(self.snapshot_db_debug_check)
            .with_block_config(
                rotbl::v001::BlockConfig::default()
                    .with_max_items(self.snapshot_db_block_keys as usize),
            )
            .with_block_cache_config(
                rotbl::v001::BlockCacheConfig::default()
                    .with_capacity(self.snapshot_db_block_cache_size as usize),
            )
    }

    /// Build [`raft_log::RaftLog`] config from [`RaftConfig`].
    pub fn to_raft_log_config(&self) -> raft_log::Config {
        let p = Path::new(&self.raft_dir)
            .join("df_meta")
            .join(format!("{}", DATA_VERSION))
            .join("log");

        let dir = p.to_str().unwrap().to_string();

        raft_log::Config {
            wal: WalConfig {
                dir,
                chunk_max_records: Some(self.log_wal_chunk_max_records as usize),
                chunk_max_size: Some(self.log_wal_chunk_max_size as usize),
                flush_batch_wait: Some(Duration::from_millis(1)),
                flush_batch_max_items: Some(1024),
                ..Default::default()
            },
            log_cache_max_items: Some(self.log_cache_max_items as usize),
            log_cache_capacity: Some(self.log_cache_capacity as usize),
        }
    }

    pub fn raft_api_listen_host_string(&self) -> String {
        format!("{}:{}", self.raft_listen_host, self.raft_api_port)
    }

    pub fn raft_api_advertise_host_string(&self) -> String {
        format!("{}:{}", self.raft_advertise_host, self.raft_api_port)
    }

    /// The address a peer dials to reach this node's raft TLS listener.
    ///
    /// `None` unless that listener is configured to run, because this is what
    /// the node publishes to its peers: an address that nothing answers on
    /// reads to them as a node that is down.
    ///
    /// It is built from `raft_advertise_host` today, so TLS is advertised on
    /// the same host as plaintext. Peers read the whole address rather than
    /// just the port, so giving TLS a host of its own later is a config
    /// addition and not a change to what is published.
    pub fn raft_tls_advertise_host_string(&self) -> Option<String> {
        if !self.raft_tls_listener_enabled() {
            return None;
        }

        let port = self.raft_tls_port?;
        let endpoint = Endpoint::new(&self.raft_advertise_host, port);

        Some(endpoint.to_address())
    }

    pub fn raft_api_listen_host_endpoint(&self) -> Endpoint {
        Endpoint::new(&self.raft_listen_host, self.raft_api_port)
    }

    pub fn raft_api_advertise_host_endpoint(&self) -> Endpoint {
        Endpoint::new(&self.raft_advertise_host, self.raft_api_port)
    }

    /// The address this node's raft TLS listener binds.
    ///
    /// `None` unless that listener is configured to run, which is what decides
    /// whether the node opens a second raft port at all.
    ///
    /// It binds `raft_listen_host`, the same interface the plaintext raft port
    /// binds; only the port differs.
    pub fn raft_tls_listen_host_endpoint(&self) -> Option<Endpoint> {
        if !self.raft_tls_listener_enabled() {
            return None;
        }

        let port = self.raft_tls_port?;

        Some(Endpoint::new(&self.raft_listen_host, port))
    }

    /// Resolves the advertise host to an endpoint, supporting both IP addresses and hostnames.
    pub async fn raft_api_addr<R: SpawnApi>(&self) -> Result<Endpoint, io::Error> {
        if let Ok(addr) = self.raft_advertise_host.parse::<Ipv4Addr>() {
            return Ok(Endpoint::new(addr, self.raft_api_port));
        }

        let ips = R::resolve(&self.raft_advertise_host).await?;
        let ip = ips.into_iter().next().ok_or_else(|| {
            io::Error::other(format!(
                "No IP address found for hostname: {}",
                self.raft_advertise_host
            ))
        })?;

        Ok(Endpoint::new(ip, self.raft_api_port))
    }

    /// Returns the min and max election timeout, in milli seconds.
    ///
    /// Raft will choose a random timeout in this range for next election.
    pub fn election_timeout(&self) -> (u64, u64) {
        (self.heartbeat_interval * 2, self.heartbeat_interval * 3)
    }

    /// Validates configuration parameters for consistency and correctness.
    ///
    /// # Errors
    /// Returns `MetaStartupError::InvalidConfig` if:
    /// - The sending raft secret is empty
    /// - An accepted raft secret is empty
    /// - A raft secret holds anything but visible ASCII
    /// - `raft_secret_strict` is enabled with no accepted secret
    /// - A raft TLS path or name is set to an empty string
    /// - Only one of the raft TLS certificate and its key is set
    /// - The raft TLS domain name is set with no root CA to go with it
    /// - `leave_via` is specified without `leave_id`
    /// - Neither `single` nor `join` is specified
    /// - Both `single` and `join` are specified
    /// - Node tries to join itself (self-reference in join addresses)
    pub fn check(&self) -> Result<(), MetaStartupError> {
        if let Some(secret) = &self.raft_secret {
            if secret.expose().is_empty() {
                return Err(MetaStartupError::InvalidConfig(String::from(
                    "`raft_secret` must not be empty",
                )));
            }

            check_secret_encoding("raft_secret", secret)?;
        }

        // If just leaving, does not need to check server or cluster config.
        //
        // Without `leave_id` there is nothing to leave with, and leaving is
        // skipped: accepting the config here would start a normal server that
        // never had its topology or its secret checked.
        if !self.leave_via.is_empty() {
            if self.leave_id.is_none() {
                return Err(MetaStartupError::InvalidConfig(String::from(
                    "`leave_via` is set but `leave_id` is not: \
                     the node to remove from the cluster is unknown",
                )));
            }
            return Ok(());
        }

        if self
            .raft_accepted_secrets
            .iter()
            .any(|secret| secret.expose().is_empty())
        {
            return Err(MetaStartupError::InvalidConfig(String::from(
                "`raft_accepted_secrets` must not contain an empty secret",
            )));
        }

        for secret in &self.raft_accepted_secrets {
            check_secret_encoding("raft_accepted_secrets", secret)?;
        }

        if self.raft_secret_strict() && self.raft_accepted_secrets.is_empty() {
            return Err(MetaStartupError::InvalidConfig(String::from(
                "`raft_secret_strict` is enabled but `raft_accepted_secrets` is empty: \
                 the node would reject every incoming raft RPC",
            )));
        }

        // A half-configured node is refused here rather than started into a
        // state that fails later. A TLS failure is especially worth refusing
        // early: `status_to_unreachable_at()` turns it into `Unreachable`, so a
        // misconfigured node is indistinguishable from a node that is down.
        // An empty string is a mistake rather than an absent value, in the same
        // way an empty `raft_secret` is: `None` is how these keys go
        // unconfigured, so `Some("")` only arises from a config source that set
        // one of them to nothing.
        let tls_string_keys = [
            ("raft_tls_server_cert", &self.raft_tls_server_cert),
            ("raft_tls_server_key", &self.raft_tls_server_key),
            (
                "raft_tls_client_root_ca_cert",
                &self.raft_tls_client_root_ca_cert,
            ),
            (
                "raft_tls_client_domain_name",
                &self.raft_tls_client_domain_name,
            ),
        ];

        for (key, value) in tls_string_keys {
            if value.as_deref() == Some("") {
                return Err(MetaStartupError::InvalidConfig(format!(
                    "`{}` is set to an empty string: leave it unset instead",
                    key
                )));
            }
        }

        let has_cert = self.raft_tls_server_cert.is_some();
        let has_key = self.raft_tls_server_key.is_some();
        if has_cert != has_key {
            return Err(MetaStartupError::InvalidConfig(String::from(
                "`raft_tls_server_cert` and `raft_tls_server_key` must be set together: \
                 a TLS listener needs both halves of its identity",
            )));
        }

        let has_domain_name = self.raft_tls_client_domain_name.is_some();
        if has_domain_name && self.raft_tls_client_root_ca_cert.is_none() {
            return Err(MetaStartupError::InvalidConfig(String::from(
                "`raft_tls_client_domain_name` is set but `raft_tls_client_root_ca_cert` is not: \
                 the name would never be consulted, since a node that cannot verify \
                 a peer certificate does not dial TLS at all",
            )));
        }

        // There two cases:
        // - both join and single is set
        // - neither join nor single is set
        if self.join.is_empty() != self.single {
            return Err(MetaStartupError::InvalidConfig(String::from(
                "at least one of `single` and `join` needs to be enabled",
            )));
        }

        let self_addr = self.raft_api_listen_host_string();
        if self.join.contains(&self_addr) {
            return Err(MetaStartupError::InvalidConfig(String::from(
                "--join must not be set to itself",
            )));
        }
        Ok(())
    }
}

/// Reject a secret that cannot travel unchanged as an HTTP header value.
///
/// A raft secret is sent verbatim in a header. A header value cannot hold a
/// control character at all, and holds a byte above 127 only as something HTTP
/// deprecated and each end is free to render differently. Requiring visible
/// ASCII keeps the configured secret and the wire secret the same string, and
/// it excludes the space so that a secret cannot pick up or lose one unnoticed
/// at the edges.
///
/// Checking here is what makes a bad secret a node that refuses to start
/// instead of a node that starts and then fails every raft RPC it sends --
/// which its peers read as unreachable, not as misconfigured.
fn check_secret_encoding(key: &str, secret: &Secret) -> Result<(), MetaStartupError> {
    if secret.expose().bytes().all(|b| b.is_ascii_graphic()) {
        return Ok(());
    }

    Err(MetaStartupError::InvalidConfig(format!(
        "`{}` must hold visible ASCII only, with no space: \
         it is sent verbatim as an HTTP header value",
        key
    )))
}
