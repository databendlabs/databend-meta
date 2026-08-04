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

#[cfg(test)]
mod tests {
    use databend_meta_raft_config::Secret;
    use databend_meta_raft_config::config::RaftConfig;
    use tonic::Request;
    use tonic::service::Interceptor;

    use crate::raft_secret::RAFT_SECRET_HEADER;
    use crate::raft_secret::RaftSecretInterceptor;

    fn intercept(config: &RaftConfig) -> Result<Request<()>, tonic::Status> {
        RaftSecretInterceptor::new(config).call(Request::new(()))
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
}
