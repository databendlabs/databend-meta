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

use databend_meta_raft_config::config::RaftConfig;
use log::warn;
use subtle::Choice;
use subtle::ConstantTimeEq;
use tonic::Request;
use tonic::Status;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;

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
pub(crate) struct RaftSecretInterceptor {
    secret: Option<String>,
}

impl RaftSecretInterceptor {
    pub(crate) fn new(config: &RaftConfig) -> Self {
        Self {
            secret: config
                .raft_secret
                .as_ref()
                .map(|secret| secret.expose().to_string()),
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
        let value = AsciiMetadataValue::try_from(secret.as_str()).map_err(|e| {
            Status::internal(format!("`raft_secret` is not a valid header value: {}", e))
        })?;

        request.metadata_mut().insert(RAFT_SECRET_HEADER, value);

        Ok(request)
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

/// Checks the cluster shared secret on every raft RPC this node receives.
///
/// A node with no accepted secret configured checks nothing, so an unconfigured
/// cluster behaves exactly as before. Enabling `strict` without an accepted
/// secret is refused at startup by [`RaftConfig::check`], so that empty case can
/// never silently disable a strict node.
#[derive(Clone)]
pub(crate) struct RaftSecretChecker {
    accepted: Vec<String>,
    strict: bool,
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
        if self.accepted.is_empty() {
            return Decision::Pass;
        }

        match presented {
            Some(value) if self.accepts(value) => Decision::Pass,
            Some(_) => Decision::Refused("unaccepted"),
            None => Decision::Refused("missing"),
        }
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
        let peer = request
            .remote_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown address".to_string());

        if self.strict {
            return Err(Status::unauthenticated(format!(
                "raft secret is {}: from:{}",
                reason, peer
            )));
        }

        warn!(
            "raft secret is {}: from:{}: accepted because `raft_secret_strict` is off",
            reason, peer
        );

        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_raft_config::Secret;
    use databend_meta_raft_config::config::RaftConfig;
    use tonic::Request;
    use tonic::service::Interceptor;

    use crate::raft_secret::Decision;
    use crate::raft_secret::RAFT_SECRET_HEADER;
    use crate::raft_secret::RaftSecretChecker;
    use crate::raft_secret::RaftSecretInterceptor;

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

    #[test]
    fn test_no_secret_leaves_the_request_untouched() -> anyhow::Result<()> {
        let request = intercept(&RaftConfig::default())?;

        assert_eq!(request.metadata().len(), 0);

        Ok(())
    }

    /// A control character in the secret would be a header injection, so it is
    /// reported rather than sent. Note that non-ASCII bytes are accepted: a
    /// header value may carry them even though the metadata type is named
    /// after ASCII.
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

    #[test]
    fn test_permissive_accepts_a_wrong_or_missing_secret() -> anyhow::Result<()> {
        let config = receiver(&["s3cr3t"], false);

        for presented in [Some("from-another-cluster"), None] {
            check(&config, presented)?;
        }

        Ok(())
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
