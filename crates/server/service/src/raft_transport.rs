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

//! How a raft connection is carried: which address each end is reached at, and
//! whether the transport is TLS.
//!
//! This answers only how bytes get between two nodes. What travels over the
//! connection once it is open -- the shared secret every raft RPC carries -- is
//! [`crate::raft_secret`], and it is checked the same way on both transports.

use std::fmt;

use anyerror::AnyError;
use databend_meta_raft_config::config::RaftConfig;
use databend_meta_types::ConnectionError;
use databend_meta_types::MetaNetworkError;
use databend_meta_types::node::Node;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Identity;
use tonic::transport::ServerTlsConfig;

/// Where one peer's raft service is reached, and over which transport.
///
/// The address and the decision to encrypt are one value because they are one
/// decision: a peer reached over TLS is reached at the address it published for
/// its TLS listener, not at its plaintext one. Splitting them lets a connection
/// be dialed at one address and named by the other, which is how a log line or
/// the `active_peers` metric comes to report a peer this node is not talking
/// to.
#[derive(Clone, Default)]
pub struct RaftPeerTarget {
    /// The `host:port` the connection is dialed at.
    address: String,

    /// The settings the peer's certificate is verified with. `Some` is what
    /// makes this connection TLS, and `address` the peer's TLS address.
    client_config: Option<ClientTlsConfig>,
}

impl fmt::Display for RaftPeerTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}://{}", self.scheme(), self.address)
    }
}

impl RaftPeerTarget {
    /// Where to reach a peer whose record the cluster replicates.
    ///
    /// The transport is TLS only when both ends are ready for it: `node`
    /// published the address its TLS listener answers on, and this node has a
    /// CA to verify that peer against. Because the question is answered per
    /// peer, out of data the cluster already replicates, the nodes may be
    /// upgraded in any order.
    ///
    /// The CA is read here, on the way to a connection that will use it, rather
    /// than once at startup. That keeps a CA that cannot be read from costing a
    /// node its plaintext peers as well, which during a migration is most of
    /// the cluster.
    pub async fn of_node(node: &Node, config: &RaftConfig) -> Result<Self, ConnectionError> {
        let Some(tls_address) = &node.raft_tls_advertise_address else {
            return Ok(Self::plaintext(&node.endpoint));
        };

        // A node with no CA has nothing to verify a peer against, so what the
        // peer published is of no use to it.
        let Some(client_config) = Self::client_tls_config(config).await? else {
            return Ok(Self::plaintext(&node.endpoint));
        };

        Ok(Self {
            address: tls_address.clone(),
            client_config: Some(client_config),
        })
    }

    /// Where to reach an address that belongs to no known peer: joining a
    /// cluster, registering a node, leaving one.
    ///
    /// Such a caller has no peer record to read a published TLS address out of,
    /// so it dials plaintext.
    pub fn plaintext(addr: impl fmt::Display) -> Self {
        Self {
            address: addr.to_string(),
            client_config: None,
        }
    }

    /// The `host:port` this target is dialed at, which is what a log line, an
    /// error context or a metric label has to name.
    pub fn address(&self) -> &str {
        &self.address
    }

    fn scheme(&self) -> &'static str {
        if self.client_config.is_some() {
            "https"
        } else {
            "http"
        }
    }

    /// The settings this node verifies its peers' certificates with, or `None`
    /// when it has no CA configured, which is what makes it a node that dials
    /// every peer in plaintext.
    ///
    /// With no `raft_tls_client_domain_name` configured the peer is verified
    /// against the address it was dialed on, which is the ordinary behaviour
    /// and a legitimate configuration: it asks each node's certificate to cover
    /// the address its peers reach it at, instead of one name covering the
    /// cluster.
    async fn client_tls_config(
        config: &RaftConfig,
    ) -> Result<Option<ClientTlsConfig>, ConnectionError> {
        let Some(ca_path) = &config.raft_tls_client_root_ca_cert else {
            return Ok(None);
        };

        let pem = tokio::fs::read(ca_path)
            .await
            .map_err(|e| ConnectionError::new(e, format!("read raft TLS CA {}", ca_path)))?;

        install_crypto_provider();

        let mut tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem));

        if let Some(domain_name) = &config.raft_tls_client_domain_name {
            tls = tls.domain_name(domain_name);
        }

        Ok(Some(tls))
    }

    /// Open the transport to this peer's raft service, without wrapping it in a
    /// client.
    ///
    /// Every outbound raft connection is opened here, so that the address and
    /// the scheme are decided once instead of at each call site.
    ///
    /// A TLS connection is never retried in plaintext. Falling back would hand
    /// anyone who can drop a packet the power to turn the encryption off.
    pub(crate) async fn connect(&self) -> Result<Channel, ConnectionError> {
        match &self.client_config {
            Some(client_config) => self.connect_tls(client_config).await,
            None => self.connect_plaintext().await,
        }
    }

    /// Connect to the address the peer published for its TLS listener.
    ///
    /// The scheme and the TLS settings are set together on purpose. tonic reads
    /// the scheme alone to decide whether to hand the connection to the TLS
    /// connector, so `http://` carrying a `ClientTlsConfig` connects in
    /// plaintext and reports nothing.
    async fn connect_tls(
        &self,
        client_config: &ClientTlsConfig,
    ) -> Result<Channel, ConnectionError> {
        let uri = self.to_string();

        let endpoint =
            Channel::from_shared(uri.clone()).map_err(|e| ConnectionError::new(e, uri.clone()))?;

        let endpoint = endpoint
            .tls_config(client_config.clone())
            .map_err(|e| ConnectionError::new(e, uri.clone()))?;

        endpoint
            .connect()
            .await
            .map_err(|e| ConnectionError::new(e, uri))
    }

    /// Connect to the address the peer is reached at when TLS is not in play.
    async fn connect_plaintext(&self) -> Result<Channel, ConnectionError> {
        let uri = self.to_string();

        let endpoint =
            Channel::from_shared(uri.clone()).map_err(|e| ConnectionError::new(e, uri.clone()))?;

        endpoint
            .connect()
            .await
            .map_err(|e| ConnectionError::new(e, uri))
    }
}
/// Read this node's TLS identity off disk.
///
/// Failing here fails startup, which is the point: a node that cannot load
/// its certificate but starts anyway serves plaintext only, and peers see
/// that as a node that is down rather than as a misconfigured one.
pub(crate) async fn server_tls_config(
    config: &RaftConfig,
) -> Result<ServerTlsConfig, MetaNetworkError> {
    let read = async |path: &Option<String>| -> Result<Vec<u8>, MetaNetworkError> {
        // `raft_tls_listener_enabled()` is what guarantees both are set.
        let Some(path) = path else {
            let e = AnyError::error("raft TLS listener enabled without a certificate or key");
            return Err(MetaNetworkError::TLSConfigError(e));
        };

        let content = tokio::fs::read(path).await.map_err(|e| {
            let e = AnyError::new(&e).add_context(|| format!("read raft TLS file {}", path));
            MetaNetworkError::TLSConfigError(e)
        })?;

        Ok(content)
    };

    let cert = read(&config.raft_tls_server_cert).await?;
    let key = read(&config.raft_tls_server_key).await?;

    let identity = Identity::from_pem(cert, key);

    Ok(ServerTlsConfig::new().identity(identity))
}

/// Install the rustls crypto provider this node's TLS runs on.
///
/// Idempotent, and called from both the dialing and the listening path: a node
/// that only dials TLS never reaches the listener path, and one that only
/// serves it never reaches the dialing path.
pub(crate) fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
