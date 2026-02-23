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

use std::io;

use anyerror::AnyError;
use databend_meta_types::MetaNetworkError;
use databend_meta_types::raft_types::InitializeError;
use databend_meta_types::raft_types::RaftError;

/// Error raised when meta-server startup.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum MetaStartupError {
    #[error(transparent)]
    InitializeError(#[from] InitializeError),

    #[error("fail to add node to cluster: {source}")]
    AddNodeError { source: AnyError },

    #[error("{0}")]
    InvalidConfig(String),

    #[error("fail to open store: {0}")]
    StoreOpenError(AnyError),

    #[error(transparent)]
    ServiceStartupError(#[from] MetaNetworkError),

    #[error("raft state present id={0}, can not create")]
    MetaStoreAlreadyExists(u64),

    #[error("raft state absent, can not open")]
    MetaStoreNotFound,

    #[error("{0}")]
    MetaServiceError(String),
}

impl From<RaftError<InitializeError>> for MetaStartupError {
    fn from(value: RaftError<InitializeError>) -> Self {
        match value {
            RaftError::APIError(e) => e.into(),
            RaftError::Fatal(f) => Self::MetaServiceError(f.to_string()),
        }
    }
}

impl From<io::Error> for MetaStartupError {
    fn from(e: io::Error) -> Self {
        MetaStartupError::StoreOpenError(AnyError::new(&e))
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use anyerror::AnyError;
    use databend_meta_types::MetaNetworkError;

    use super::MetaStartupError;

    #[test]
    fn test_display_variants() {
        let e = MetaStartupError::InvalidConfig("bad config".into());
        assert_eq!(e.to_string(), "bad config");

        let e = MetaStartupError::StoreOpenError(AnyError::error("disk full"));
        assert!(e.to_string().contains("disk full"), "{}", e);

        let e = MetaStartupError::MetaStoreAlreadyExists(5);
        assert!(e.to_string().contains("5"), "{}", e);

        let e = MetaStartupError::MetaStoreNotFound;
        assert!(e.to_string().contains("absent"), "{}", e);

        let e = MetaStartupError::MetaServiceError("service failed".into());
        assert_eq!(e.to_string(), "service failed");

        let e = MetaStartupError::AddNodeError {
            source: AnyError::error("join failed"),
        };
        assert!(e.to_string().contains("join failed"), "{}", e);
    }

    #[test]
    fn test_from_io_error() {
        let io_err = io::Error::other("disk error");
        let e: MetaStartupError = io_err.into();
        assert!(e.to_string().contains("disk error"), "{}", e);
    }

    #[test]
    fn test_from_meta_network_error() {
        let net = MetaNetworkError::GetNodeAddrError("no addr".into());
        let e: MetaStartupError = net.into();
        assert!(e.to_string().contains("no addr"), "{}", e);
    }
}
