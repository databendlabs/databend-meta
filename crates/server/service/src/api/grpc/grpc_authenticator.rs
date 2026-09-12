use std::collections::HashMap;

use arrow_flight::BasicAuth;
use databend_meta_raft_config::Secret;
use sha2::Digest;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tonic::Status;

use crate::configs::GrpcAuthConfig;

pub(super) struct GrpcAuthenticator {
    credentials: HashMap<String, Secret>,
    strict: bool,
}

impl GrpcAuthenticator {
    pub(super) fn from_config(config: &GrpcAuthConfig) -> Self {
        let mut credentials = HashMap::with_capacity(config.credentials.len());
        for credential in &config.credentials {
            credentials.insert(credential.username.clone(), credential.password.clone());
        }

        Self {
            credentials,
            strict: config.strict,
        }
    }

    pub(super) fn authenticate(&self, auth: &BasicAuth) -> Result<Option<&'static str>, Status> {
        let Some(expected_password) = self.credentials.get(&auth.username) else {
            return Err(unknown_user(&auth.username));
        };
        if password_matches(&auth.password, expected_password) {
            return Ok(None);
        }

        let reason = if auth.password.is_empty() {
            "missing"
        } else {
            "incorrect"
        };
        self.invalid_password(reason)
    }

    fn invalid_password(&self, reason: &'static str) -> Result<Option<&'static str>, Status> {
        if self.strict {
            return Err(Status::unauthenticated("Invalid password"));
        }

        Ok(Some(reason))
    }
}

pub(super) fn unknown_user(username: &str) -> Status {
    Status::unauthenticated(format!("Unknown user: {username}"))
}

fn password_matches(password: &str, expected_password: &Secret) -> bool {
    let actual_digest = Sha256::digest(password.as_bytes());
    let expected_digest = Sha256::digest(expected_password.expose().as_bytes());
    bool::from(actual_digest.ct_eq(&expected_digest))
}
