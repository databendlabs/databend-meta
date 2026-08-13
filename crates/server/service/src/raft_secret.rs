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

//! The shared secret authenticating raft RPCs between the nodes of a cluster.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use databend_meta_raft_config::config::RaftConfig;
use databend_meta_types::ConnectionError;
use databend_meta_types::node::Node;
use databend_meta_types::protobuf::raft_service_client::RaftServiceClient;
use log::warn;
use subtle::Choice;
use subtle::ConstantTimeEq;
use tonic::Request;
use tonic::Status;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;

use crate::metrics::raft_metrics;

/// The request metadata key carrying the cluster shared secret.
///
/// A node that does not know this key ignores it, which is what lets a cluster
/// start sending the secret before any node starts requiring it.
pub(crate) const RAFT_SECRET_HEADER: &str = "x-databend-meta-raft-secret";

/// Attaches the cluster shared secret to every raft RPC this node sends.
///
/// A node with no `raft_secret` configured sends nothing, leaving its requests
/// indistinguishable from those of a node that predates the secret. That is
/// what makes the first stage of the rollout free of downtime.
#[derive(Clone)]
pub struct RaftSecretInterceptor {
    /// The header value to send, built once instead of per RPC.
    ///
    /// `Err` holds the message every RPC then fails with. It is unreachable
    /// through [`RaftConfig::check`], which refuses a secret that cannot be a
    /// header value, and is kept for the callers that skip that check: a
    /// request that could not be signed must not go out unsigned.
    secret: Option<Result<AsciiMetadataValue, String>>,
}

impl RaftSecretInterceptor {
    pub(crate) fn new(config: &RaftConfig) -> Self {
        Self {
            secret: config.raft_secret.as_ref().map(|secret| {
                let mut value = AsciiMetadataValue::try_from(secret.expose())
                    .map_err(|e| format!("`raft_secret` is not a valid header value: {}", e))?;

                // Debug formatting renders a sensitive value as `Sensitive`
                // instead of verbatim, and h2 keeps it out of the HPACK dynamic
                // table. Without this, h2 logging a frame at DEBUG prints the
                // credential.
                value.set_sensitive(true);

                Ok(value)
            }),
        }
    }
}

impl Interceptor for RaftSecretInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let Some(secret) = &self.secret else {
            return Ok(request);
        };

        // Rejected rather than dropped: a node that silently stopped sending
        // the secret would be evicted the moment its peers turn strict.
        let value = secret
            .as_ref()
            .map_err(|message| Status::internal(message.clone()))?;

        request
            .metadata_mut()
            .insert(RAFT_SECRET_HEADER, value.clone());

        Ok(request)
    }
}

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

        // Idempotent, and needed here for its own sake: a node that dials TLS
        // without serving it has installed no provider on the listener path.
        let _ = rustls::crypto::ring::default_provider().install_default();

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

/// What a [`RaftSecretChecker`] made of a request, separate from acting on it.
///
/// Keeping the two apart is what makes "passed, nothing to say" distinguishable
/// from "passed, but the peer should be reported" without reading the log.
#[derive(Debug, PartialEq, Eq)]
enum Decision {
    /// This node checks nothing, or the presented secret is accepted.
    Pass,
    /// The secret is missing or unaccepted. Carries the word naming which.
    Refused(&'static str),
}

/// How long a peer stays out of the log after being reported once.
const REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// Remembers when each peer was last reported, so that a mixed cluster does not
/// bury its log under one line per raft heartbeat per peer.
///
/// Entries that have aged out are dropped as they are passed over, which bounds
/// this by the peers seen in the last [`REPORT_INTERVAL`] rather than by the
/// peers ever seen. That distinction matters here: the check runs after the
/// connection is accepted, so who appears in this map is not up to us.
#[derive(Default)]
struct ReportLimiter {
    /// Keyed by address rather than by connection, so that a peer reconnecting
    /// cannot report itself again, and by IP rather than by socket, so that it
    /// cannot do so from a new port either.
    ///
    /// The `None` key is the one bucket for peers of unknown address.
    reported_at: HashMap<Option<IpAddr>, Instant>,
}

impl ReportLimiter {
    /// Whether `peer` should be reported at `now`, recording it if so.
    ///
    /// A peer that keeps failing is reported once per [`REPORT_INTERVAL`]: being
    /// suppressed does not refresh its entry, so its silence has an end.
    fn should_report(&mut self, peer: Option<IpAddr>, now: Instant) -> bool {
        self.reported_at
            .retain(|_, at| now.duration_since(*at) < REPORT_INTERVAL);

        match self.reported_at.entry(peer) {
            Entry::Occupied(_) => false,
            Entry::Vacant(slot) => {
                slot.insert(now);
                true
            }
        }
    }
}

/// Checks the cluster shared secret on every raft RPC this node receives.
///
/// A node with no accepted secret configured checks nothing, so an unconfigured
/// cluster behaves exactly as before -- unless `strict` is on, in which case it
/// refuses everything. [`RaftConfig::check`] refuses that combination at
/// startup, but a caller that skips the check must not end up with a strict node
/// that authenticates nothing.
#[derive(Clone)]
pub(crate) struct RaftSecretChecker {
    accepted: Vec<String>,
    strict: bool,
    /// Shared, because the router hands each request its own clone of the
    /// service stack: a limiter kept as a plain field would start empty every
    /// time and rate limit nothing.
    limiter: Arc<Mutex<ReportLimiter>>,
}

impl RaftSecretChecker {
    pub(crate) fn new(config: &RaftConfig) -> Self {
        Self {
            accepted: config
                .raft_accepted_secrets
                .iter()
                .map(|secret| secret.expose().to_string())
                .collect(),
            strict: config.raft_secret_strict(),
            limiter: Default::default(),
        }
    }

    /// Whether `presented` is one of the accepted secrets.
    ///
    /// Every candidate is compared in constant time and none of them short
    /// circuits, so neither the matching secret nor its position leaks through
    /// the time this takes. Lengths are still observable, as they are for any
    /// comparison of variable length secrets.
    fn accepts(&self, presented: &[u8]) -> bool {
        let mut hit = Choice::from(0u8);
        for secret in &self.accepted {
            hit |= presented.ct_eq(secret.as_bytes());
        }
        bool::from(hit)
    }

    fn decide(&self, presented: Option<&[u8]>) -> Decision {
        // The short circuit is what keeps an unconfigured cluster quiet, and it
        // must not apply to a strict node: with nothing to accept, a strict node
        // refuses everything rather than accepts everything. `RaftConfig::check`
        // rejects that combination, but only the startup paths that call it.
        if self.accepted.is_empty() && !self.strict {
            return Decision::Pass;
        }

        match presented {
            Some(value) if self.accepts(value) => Decision::Pass,
            Some(_) => Decision::Refused("unaccepted"),
            None => Decision::Refused("missing"),
        }
    }

    /// Whether this refusal is the one to log, or a repeat to suppress.
    ///
    /// The guard is dropped before the caller writes its line: every raft RPC
    /// of every peer that fails the check arrives here, so the lock must not be
    /// held for as long as writing a line takes.
    fn should_report(&self, addr: Option<SocketAddr>) -> bool {
        self.limiter
            .lock()
            .unwrap()
            .should_report(addr.map(|addr| addr.ip()), Instant::now())
    }
}

impl Interceptor for RaftSecretChecker {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let presented = request.metadata().get(RAFT_SECRET_HEADER);

        let Decision::Refused(reason) = self.decide(presented.map(|v| v.as_encoded_bytes())) else {
            return Ok(request);
        };

        // Never log the value that was presented: on a misconfigured peer it is
        // a valid secret of some other cluster.
        let addr = request.remote_addr();
        let peer = addr
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown address".to_string());

        if self.strict {
            // Counted and logged before returning: raft turns any status into
            // `Unreachable`, so this node's own account of why it refused a
            // peer is the only one that says `Unauthenticated` anywhere.
            raft_metrics::network::incr_unauthenticated_refused(reason);

            if self.should_report(addr) {
                warn!(
                    "raft secret is {}: from:{}: refused because `raft_secret_strict` is on; \
                     further reports about this peer are suppressed for {:?}",
                    reason, peer, REPORT_INTERVAL
                );
            }

            return Err(Status::unauthenticated(format!(
                "raft secret is {}: from:{}",
                reason, peer
            )));
        }

        raft_metrics::network::incr_unauthenticated_passed(reason);

        if self.should_report(addr) {
            warn!(
                "raft secret is {}: from:{}: accepted because `raft_secret_strict` is off; \
                 further reports about this peer are suppressed for {:?}",
                reason, peer, REPORT_INTERVAL
            );
        }

        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::time::Duration;
    use std::time::Instant;

    use databend_meta_raft_config::Secret;
    use databend_meta_raft_config::config::RaftConfig;
    use tonic::Request;
    use tonic::service::Interceptor;

    use crate::raft_secret::Decision;
    use crate::raft_secret::RAFT_SECRET_HEADER;
    use crate::raft_secret::REPORT_INTERVAL;
    use crate::raft_secret::RaftSecretChecker;
    use crate::raft_secret::RaftSecretInterceptor;
    use crate::raft_secret::ReportLimiter;

    fn intercept(config: &RaftConfig) -> Result<Request<()>, tonic::Status> {
        RaftSecretInterceptor::new(config).call(Request::new(()))
    }

    fn receiver(accepted: &[&str], strict: bool) -> RaftConfig {
        RaftConfig {
            raft_accepted_secrets: accepted.iter().map(|s| Secret::new(*s)).collect(),
            raft_secret_strict: Some(strict),
            ..Default::default()
        }
    }

    fn check(config: &RaftConfig, presented: Option<&str>) -> Result<Request<()>, tonic::Status> {
        let mut request = Request::new(());
        if let Some(secret) = presented {
            request
                .metadata_mut()
                .insert(RAFT_SECRET_HEADER, secret.parse().unwrap());
        }

        RaftSecretChecker::new(config).call(request)
    }

    #[test]
    fn test_a_configured_secret_is_the_only_metadata_added() -> anyhow::Result<()> {
        let config = RaftConfig {
            raft_secret: Some(Secret::new("s3cr3t")),
            ..Default::default()
        };

        let request = intercept(&config)?;

        assert_eq!(request.metadata().len(), 1);
        assert_eq!(
            request.metadata().get(RAFT_SECRET_HEADER).unwrap(),
            "s3cr3t"
        );

        Ok(())
    }

    /// The value carries a credential, so it is marked sensitive: h2 logs whole
    /// frames at DEBUG and would otherwise print it, and would be free to put it
    /// in the HPACK dynamic table shared by the connection.
    #[test]
    fn test_the_secret_is_sent_as_a_sensitive_value() -> anyhow::Result<()> {
        let config = RaftConfig {
            raft_secret: Some(Secret::new("s3cr3t")),
            ..Default::default()
        };

        let request = intercept(&config)?;
        let value = request.metadata().get(RAFT_SECRET_HEADER).unwrap();

        assert!(value.is_sensitive());

        // What a log line would show. The whole metadata map is rendered, since
        // that is what a frame dump reaches the value through.
        let rendered = format!("{:?}", request.metadata());
        assert!(!rendered.contains("s3cr3t"), "{}", rendered);
        assert!(rendered.contains("Sensitive"), "{}", rendered);

        Ok(())
    }

    #[test]
    fn test_no_secret_leaves_the_request_untouched() -> anyhow::Result<()> {
        let request = intercept(&RaftConfig::default())?;

        assert_eq!(request.metadata().len(), 0);

        Ok(())
    }

    /// A control character in the secret would be a header injection, so it is
    /// reported rather than sent. `RaftConfig::check` rejects such a secret at
    /// startup, so reaching this means the check was skipped -- and a request
    /// that cannot be signed still must not go out unsigned.
    #[test]
    fn test_a_secret_that_cannot_be_a_header_is_reported() {
        let config = RaftConfig {
            raft_secret: Some(Secret::new("line\nbreak")),
            ..Default::default()
        };

        let status = intercept(&config).unwrap_err();

        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(
            status.message().starts_with("`raft_secret` is not a valid"),
            "{}",
            status.message()
        );
    }

    /// Every accepted secret is honored, which is what carries a cluster
    /// through a rotation: the new one is accepted before anyone sends it.
    #[test]
    fn test_any_accepted_secret_passes_whether_strict_or_not() -> anyhow::Result<()> {
        for strict in [true, false] {
            let config = receiver(&["old", "new"], strict);

            for known in ["old", "new"] {
                check(&config, Some(known))?;
            }
        }

        Ok(())
    }

    #[test]
    fn test_strict_rejects_a_wrong_or_missing_secret() {
        let config = receiver(&["s3cr3t"], true);

        // The presented value is not echoed: on a misconfigured peer it is a
        // valid secret of some other cluster.
        for (presented, expected) in [
            (Some("from-another-cluster"), "unaccepted"),
            (None, "missing"),
        ] {
            let status = check(&config, presented).unwrap_err();

            assert_eq!(status.code(), tonic::Code::Unauthenticated);
            assert_eq!(
                status.message(),
                format!("raft secret is {}: from:unknown address", expected)
            );
        }
    }

    /// The value of `<metric>_total{reason="<reason>"}` in a fresh scrape, or 0
    /// when the series is absent because that counter has never fired.
    ///
    /// The registry is process wide and the tests of this binary run in
    /// parallel, so a total is a number any other test may have moved. What a
    /// test can assert on its own is that its own call moved it.
    fn scraped_counter(metric: &str, reason: &str) -> u64 {
        let scraped = crate::metrics::meta_metrics_to_prometheus_string();
        let prefix = format!("{}_total{{reason=\"{}\"}} ", metric, reason);

        scraped
            .lines()
            .find_map(|line| line.strip_prefix(prefix.as_str()))
            .map_or(0, |value| value.parse().unwrap())
    }

    /// A refusal is the only account of itself that leaves this node. Raft maps
    /// every gRPC status to `Unreachable`, so a peer left behind by a rotation
    /// looks down rather than rejected, and nothing but this counter says which
    /// it was.
    ///
    /// The rate-limited warning next to it is not asserted here: it is the same
    /// limiter the permissive branch uses, pinned by
    /// [`test_report_limiter_reports_each_peer_once_per_interval`].
    #[test]
    fn test_strict_counts_what_it_refuses() {
        const REFUSED: &str = "metasrv_raft_network_unauthenticated_refused";

        let config = receiver(&["s3cr3t"], true);

        for (presented, reason) in [
            (Some("from-another-cluster"), "unaccepted"),
            (None, "missing"),
        ] {
            let before = scraped_counter(REFUSED, reason);

            check(&config, presented).unwrap_err();

            assert!(
                scraped_counter(REFUSED, reason) > before,
                "{} was not counted",
                reason
            );
        }
    }

    /// Counting is what tells an operator when `raft_secret_strict` can be
    /// turned on: it is safe once this has stopped growing, and turning it on
    /// while it still grows evicts whoever is being counted.
    #[test]
    fn test_permissive_accepts_and_counts_a_wrong_or_missing_secret() -> anyhow::Result<()> {
        const PASSED: &str = "metasrv_raft_network_unauthenticated_passed";

        let config = receiver(&["s3cr3t"], false);

        for (presented, reason) in [
            (Some("from-another-cluster"), "unaccepted"),
            (None, "missing"),
        ] {
            let before = scraped_counter(PASSED, reason);

            check(&config, presented)?;

            assert!(
                scraped_counter(PASSED, reason) > before,
                "{} was not counted",
                reason
            );
        }

        Ok(())
    }

    /// A peer that keeps failing is reported once per interval, and one that
    /// has gone quiet for an interval is forgotten instead of remembered.
    #[test]
    fn test_report_limiter_reports_each_peer_once_per_interval() {
        let mut limiter = ReportLimiter::default();
        let start = Instant::now();
        let peer = Some(IpAddr::from([127, 0, 0, 1]));

        assert!(limiter.should_report(peer, start));
        assert!(!limiter.should_report(peer, start));

        // Each peer is silenced on its own account, not on another's.
        assert!(limiter.should_report(Some(IpAddr::from([127, 0, 0, 2])), start));
        assert!(limiter.should_report(None, start));

        // Being suppressed the whole way through does not extend the silence.
        assert!(!limiter.should_report(peer, start + REPORT_INTERVAL - Duration::from_millis(1)));
        assert!(limiter.should_report(peer, start + REPORT_INTERVAL));

        // The peers that aged out were dropped rather than accumulated.
        assert_eq!(limiter.reported_at.len(), 1);
    }

    /// A cluster that never configured a secret keeps working untouched, and
    /// stays quiet: without the short circuit every raft RPC it serves would
    /// be reported as missing a secret it never asked for.
    #[test]
    fn test_a_node_with_no_accepted_secret_checks_nothing() -> anyhow::Result<()> {
        let config = RaftConfig::default();
        let checker = RaftSecretChecker::new(&config);

        for presented in [Some(b"anything".as_slice()), None] {
            assert_eq!(checker.decide(presented), Decision::Pass);
            check(&config, presented.map(|_| "anything"))?;
        }

        Ok(())
    }

    /// The same empty list under `strict` refuses everything instead. The check
    /// has to fail closed on its own: whether `RaftConfig::check` ran is up to
    /// the caller, and a strict node that authenticates nothing is the one state
    /// this whole mechanism exists to prevent.
    #[test]
    fn test_a_strict_node_with_no_accepted_secret_refuses_everything() {
        let config = receiver(&[], true);
        let checker = RaftSecretChecker::new(&config);

        assert_eq!(
            checker.decide(Some(b"anything")),
            Decision::Refused("unaccepted")
        );
        assert_eq!(checker.decide(None), Decision::Refused("missing"));

        for presented in [Some("anything"), None] {
            let status = check(&config, presented).unwrap_err();
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
        }
    }

    #[test]
    fn test_decide_names_why_a_secret_was_refused() {
        let checker = RaftSecretChecker::new(&receiver(&["s3cr3t"], true));

        assert_eq!(checker.decide(Some(b"s3cr3t")), Decision::Pass);
        assert_eq!(
            checker.decide(Some(b"wrong")),
            Decision::Refused("unaccepted")
        );
        assert_eq!(checker.decide(None), Decision::Refused("missing"));
    }

    /// The two sides have to agree on the header name and encoding; sending
    /// into the checker is what proves they do.
    #[test]
    fn test_what_is_sent_is_what_is_accepted() -> anyhow::Result<()> {
        let sender = RaftConfig {
            raft_secret: Some(Secret::new("s3cr3t")),
            ..Default::default()
        };

        RaftSecretChecker::new(&receiver(&["s3cr3t"], true)).call(intercept(&sender)?)?;

        Ok(())
    }
}
