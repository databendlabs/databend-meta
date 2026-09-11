use std::collections::HashSet;

use databend_meta_raft_config::MetaStartupError;
use databend_meta_raft_config::Secret;

/// Authentication configuration for the client-facing gRPC service.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct GrpcAuthConfig {
    /// Accepted username and password pairs.
    pub credentials: Vec<GrpcCredential>,

    /// Whether to reject a missing or incorrect password.
    pub strict: bool,
}

impl GrpcAuthConfig {
    /// Validates the configured credentials.
    pub(super) fn validate(&self) -> Result<(), MetaStartupError> {
        if self.credentials.is_empty() {
            return Err(invalid_config(
                "`grpc_auth.credentials` must contain at least one credential",
            ));
        }

        let mut usernames = HashSet::with_capacity(self.credentials.len());
        for credential in &self.credentials {
            credential.validate()?;
            let inserted = usernames.insert(credential.username.as_str());
            if !inserted {
                return Err(invalid_config(
                    "gRPC authentication usernames must be unique",
                ));
            }
        }

        Ok(())
    }
}

/// One accepted username and password pair.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct GrpcCredential {
    /// Username sent by the gRPC client.
    pub username: String,

    /// Password sent by the gRPC client.
    pub password: Secret,
}

impl GrpcCredential {
    fn validate(&self) -> Result<(), MetaStartupError> {
        if self.username.is_empty() {
            return Err(invalid_config(
                "gRPC authentication username must not be empty",
            ));
        }
        if self.password.expose().is_empty() {
            return Err(invalid_config(
                "gRPC authentication password must not be empty",
            ));
        }

        Ok(())
    }
}

fn invalid_config(message: &str) -> MetaStartupError {
    MetaStartupError::InvalidConfig(message.to_string())
}
