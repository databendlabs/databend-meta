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

use std::net::SocketAddr;

use databend_meta_raft_config::MetaStartupError;
use databend_meta_raft_config::Secret;
use databend_meta_raft_config::config::RaftConfig;
use databend_meta_types::node::Node;

const SHA256_HEX_LENGTH: usize = 64;
const EMPTY_PASSWORD_HASH: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// TLS configuration for server endpoints.
///
/// This struct holds the paths to TLS certificate and private key files
/// used to secure server connections.
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize)]
pub struct TlsConfig {
    /// Path to the TLS certificate file.
    /// Leave empty to disable TLS.
    pub cert: String,

    /// Path to the TLS private key file.
    /// Leave empty to disable TLS.
    pub key: String,
}

impl TlsConfig {
    /// Returns `true` if TLS is enabled (both cert and key are provided).
    pub fn enabled(&self) -> bool {
        !self.key.is_empty() && !self.cert.is_empty()
    }
}

/// Configuration for the gRPC API server.
///
/// This struct holds settings for the gRPC endpoint that serves client requests,
/// including the listening address, optional advertise host for cluster communication,
/// and TLS certificates for secure connections.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct GrpcConfig {
    /// The host the gRPC server listens on, e.g., "0.0.0.0" or "127.0.0.1".
    pub listen_host: String,

    /// The port the gRPC server listens on.
    /// When `None`, `do_start()` is disallowed—only `do_start_with_incoming()` can be used.
    /// This is typically `None` in tests where the OS assigns an ephemeral port.
    pub listen_port: Option<u16>,

    /// Optional hostname to advertise to other nodes in the cluster.
    /// If set, this host combined with the listen_port forms the
    /// address other nodes use to connect to this server.
    pub advertise_host: Option<String>,

    /// Username accepted by the gRPC handshake.
    /// Must be configured together with `auth_password_hash`.
    pub auth_username: Option<String>,

    /// Lowercase hexadecimal SHA-256 hash of the gRPC handshake password.
    /// Must be configured together with `auth_username`.
    pub auth_password_hash: Option<Secret>,

    /// Whether to reject a missing or incorrect gRPC handshake password.
    /// Default: false.
    pub auth_strict: Option<bool>,

    /// TLS configuration for the gRPC server.
    pub tls: TlsConfig,

    /// Maximum gRPC message size in bytes.
    ///
    /// Used for both encoding and decoding limits on the gRPC API server.
    /// Default: 32MB (33,554,432 bytes).
    pub max_message_size: Option<usize>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            listen_host: "127.0.0.1".to_string(),
            listen_port: Some(9191),
            advertise_host: None,
            auth_username: None,
            auth_password_hash: None,
            auth_strict: None,
            tls: TlsConfig::default(),
            max_message_size: None,
        }
    }
}

pub const DEFAULT_GRPC_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

impl GrpcConfig {
    /// Returns the maximum gRPC message size.
    pub fn max_message_size(&self) -> usize {
        self.max_message_size.unwrap_or(DEFAULT_GRPC_MESSAGE_SIZE)
    }

    /// Creates a config for local/embedded usage with OS-assigned port.
    ///
    /// The `listen_port` is set to `None`, meaning the OS will assign an ephemeral port
    /// when binding. Use `do_start_with_incoming()` to start the server.
    ///
    /// This is used for embedded meta stores that don't serve publicly.
    pub fn new_local(host: impl Into<String>) -> Self {
        let host = host.into();
        Self {
            listen_host: host.clone(),
            listen_port: None,
            advertise_host: Some(host),
            auth_username: None,
            auth_password_hash: None,
            auth_strict: None,
            tls: TlsConfig::default(),
            max_message_size: None,
        }
    }

    /// Returns "host:port" if port is known, None otherwise.
    pub fn api_address(&self) -> Option<String> {
        self.listen_port
            .map(|p| format!("{}:{}", self.listen_host, p))
    }

    /// Returns the advertise address if `advertise_host` is set.
    /// The address is formed by combining `advertise_host` with the listen_port.
    pub fn advertise_address(&self) -> Option<String> {
        let port = self.listen_port?;
        self.advertise_host
            .as_ref()
            .map(|h| format!("{}:{}", h, port))
    }

    /// Returns whether invalid gRPC passwords are rejected.
    pub fn auth_strict(&self) -> bool {
        self.auth_strict.unwrap_or(false)
    }

    fn validate_auth(&self) -> Result<(), MetaStartupError> {
        let has_username = self.auth_username.is_some();
        let has_password_hash = self.auth_password_hash.is_some();
        if has_username != has_password_hash {
            return Err(invalid_config(
                "`grpc_auth_username` and `grpc_auth_password_hash` must be set together",
            ));
        }

        if self.auth_username.as_deref() == Some("") {
            return Err(invalid_config("`grpc_auth_username` must not be empty"));
        }

        if let Some(hash) = &self.auth_password_hash {
            validate_password_hash(hash)?;
        }

        if self.auth_strict() && !has_username {
            return Err(invalid_config(
                "`grpc_auth_strict` is enabled but gRPC credentials are not configured",
            ));
        }

        Ok(())
    }
}

fn invalid_config(message: &str) -> MetaStartupError {
    MetaStartupError::InvalidConfig(message.to_string())
}

fn validate_password_hash(hash: &Secret) -> Result<(), MetaStartupError> {
    let hash = hash.expose();
    let has_valid_length = hash.len() == SHA256_HEX_LENGTH;
    let is_lowercase_hex = hash.bytes().all(|byte| {
        let is_lowercase_letter = (b'a'..=b'f').contains(&byte);
        byte.is_ascii_digit() || is_lowercase_letter
    });
    if !has_valid_length || !is_lowercase_hex {
        return Err(invalid_config(
            "`grpc_auth_password_hash` must be a lowercase hexadecimal SHA-256 hash",
        ));
    }

    if hash == EMPTY_PASSWORD_HASH {
        return Err(invalid_config(
            "`grpc_auth_password_hash` must not represent an empty password",
        ));
    }

    Ok(())
}

/// Configuration for the Admin HTTP API server.
///
/// This struct holds settings for the HTTP endpoint that serves administrative
/// requests such as health checks, metrics, and cluster management operations.
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize)]
pub struct AdminConfig {
    /// The address the admin HTTP server listens on, e.g., "0.0.0.0:28002".
    pub api_address: String,

    /// TLS configuration for the admin server.
    pub tls: TlsConfig,
}

/// Configuration for the meta service.
///
/// This struct contains only the configuration needed by the service library
/// to run a meta node. CLI-specific fields (cmd, config_file, log, admin)
/// are kept in the cli-config crate's `MetaConfig`.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct MetaServiceConfig {
    pub grpc: GrpcConfig,
    pub raft_config: RaftConfig,
}

impl MetaServiceConfig {
    pub fn validate(&self) -> Result<(), MetaStartupError> {
        self.grpc.validate_auth()?;

        // For production, port should be set.
        // For tests, port can be None (will use do_start_with_incoming).
        if let Some(addr) = self.grpc.api_address() {
            let _a: SocketAddr = addr.parse().map_err(|e| {
                MetaStartupError::InvalidConfig(format!("{} while parsing {}", e, addr))
            })?;
        }
        Ok(())
    }

    /// Create `Node` from config
    pub fn get_node(&self) -> Node {
        Node::new(
            self.raft_config.id,
            self.raft_config.raft_api_advertise_host_endpoint(),
        )
        .with_grpc_advertise_address(self.grpc.advertise_address())
        .with_raft_tls_advertise_address(self.raft_config.raft_tls_advertise_host_string())
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_raft_config::Secret;

    use super::GrpcConfig;
    use super::MetaServiceConfig;

    const PASSWORD_HASH: &str = "9246aa9be8de7b40d64eb664986430793b6cc13a19d2a456981e44f28303f9cf";

    #[test]
    fn test_grpc_auth_credentials_must_be_configured_together() {
        let mut config = MetaServiceConfig::default();
        config.grpc.auth_username = Some("meta".to_string());

        let error = config.validate().unwrap_err();

        assert!(error.to_string().contains("must be set together"));
    }

    #[test]
    fn test_grpc_auth_strict_requires_credentials() {
        let mut config = MetaServiceConfig::default();
        config.grpc.auth_strict = Some(true);

        let error = config.validate().unwrap_err();

        assert!(error.to_string().contains("credentials are not configured"));
    }

    #[test]
    fn test_grpc_auth_rejects_invalid_password_hash() {
        let mut config = MetaServiceConfig::default();
        config.grpc.auth_username = Some("meta".to_string());
        config.grpc.auth_password_hash = Some(Secret::new("not-a-sha256-hash"));

        let error = config.validate().unwrap_err();

        assert!(error.to_string().contains("lowercase hexadecimal SHA-256"));
    }

    #[test]
    fn test_grpc_auth_rejects_empty_password_hash() {
        let mut config = MetaServiceConfig::default();
        config.grpc.auth_username = Some("meta".to_string());
        config.grpc.auth_password_hash = Some(Secret::new(super::EMPTY_PASSWORD_HASH));

        let error = config.validate().unwrap_err();

        assert!(error.to_string().contains("empty password"));
    }

    #[test]
    fn test_grpc_auth_config_redacts_password_hash() -> anyhow::Result<()> {
        let config = GrpcConfig {
            auth_username: Some("meta".to_string()),
            auth_password_hash: Some(Secret::new(PASSWORD_HASH)),
            ..Default::default()
        };

        let debug = format!("{:?}", config);
        let serialized = serde_json::to_string(&config)?;

        assert!(!debug.contains(PASSWORD_HASH));
        assert!(!serialized.contains(PASSWORD_HASH));
        assert!(serialized.contains("***"));
        Ok(())
    }
}
