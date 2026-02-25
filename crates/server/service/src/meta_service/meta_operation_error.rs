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

use databend_meta_types::MetaAPIError;
use databend_meta_types::MetaDataError;
use databend_meta_types::MetaDataReadError;
use databend_meta_types::raft_types::ChangeMembershipError;
use databend_meta_types::raft_types::ClientWriteError;
use databend_meta_types::raft_types::Fatal;
use databend_meta_types::raft_types::ForwardToLeader;
use databend_meta_types::raft_types::RaftError;
use peel_off::Peel;

/// Errors raised when handling a request by raft node.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum MetaOperationError {
    /// If a request can only be dealt by a leader, it informs the caller to forward the request to a leader it knows of.
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader),

    #[error(transparent)]
    WriteError(#[from] Fatal),

    #[error(transparent)]
    ChangeMembershipError(#[from] ChangeMembershipError),

    #[error(transparent)]
    ReadError(#[from] MetaDataReadError),
}

impl Peel for MetaOperationError {
    type Peeled = ForwardToLeader;
    type Residual = MetaDataError;

    fn peel(self) -> Result<MetaDataError, ForwardToLeader> {
        match self {
            Self::ForwardToLeader(ftl) => Err(ftl),
            Self::WriteError(e) => Ok(MetaDataError::WriteError(e)),
            Self::ChangeMembershipError(e) => Ok(MetaDataError::ChangeMembershipError(e)),
            Self::ReadError(e) => Ok(MetaDataError::ReadError(e)),
        }
    }
}

impl From<MetaOperationError> for MetaAPIError {
    fn from(e: MetaOperationError) -> Self {
        match e {
            MetaOperationError::ForwardToLeader(e) => e.into(),
            MetaOperationError::WriteError(e) => MetaDataError::WriteError(e).into(),
            MetaOperationError::ChangeMembershipError(e) => {
                MetaDataError::ChangeMembershipError(e).into()
            }
            MetaOperationError::ReadError(e) => MetaDataError::ReadError(e).into(),
        }
    }
}

// Collection of errors that occur when change membership on local raft node.
pub type RaftChangeMembershipError = ClientWriteError;

impl From<RaftChangeMembershipError> for MetaOperationError {
    fn from(e: RaftChangeMembershipError) -> Self {
        match e {
            RaftChangeMembershipError::ForwardToLeader(to_leader) => to_leader.into(),
            RaftChangeMembershipError::ChangeMembershipError(c) => Self::ChangeMembershipError(c),
        }
    }
}

impl From<RaftError<ClientWriteError>> for MetaOperationError {
    fn from(e: RaftError<ClientWriteError>) -> Self {
        match e {
            RaftError::APIError(cli_write_err) => cli_write_err.into(),
            RaftError::Fatal(f) => Self::WriteError(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use databend_meta_sled_store::openraft::error::EmptyMembership;
    use databend_meta_types::MetaAPIError;
    use databend_meta_types::MetaDataError;
    use databend_meta_types::MetaDataReadError;
    use databend_meta_types::raft_types::ChangeMembershipError;
    use databend_meta_types::raft_types::ClientWriteError;
    use databend_meta_types::raft_types::Fatal;
    use databend_meta_types::raft_types::ForwardToLeader;
    use databend_meta_types::raft_types::RaftError;
    use peel_off::Peel;

    use super::MetaOperationError;

    #[test]
    fn test_from_meta_operation_error() {
        let ftl = ForwardToLeader {
            leader_id: None,
            leader_node: None,
        };
        let op_err = MetaOperationError::ForwardToLeader(ftl);
        let api_err: MetaAPIError = op_err.into();
        assert_eq!(api_err.name(), "ForwardToLeader");

        let read_err = MetaDataReadError::new("r", "m", &io::Error::other("e"));
        let op_err = MetaOperationError::ReadError(read_err);
        let api_err: MetaAPIError = op_err.into();
        assert_eq!(api_err.name(), "DataError");
    }

    #[test]
    fn test_meta_data_read_error_into_operation_error() {
        let read_err = MetaDataReadError::new("read", "msg", &io::Error::other("io"));
        let op_err: MetaOperationError = read_err.into();
        let s = op_err.to_string();
        assert!(s.contains("read"), "{}", s);
    }

    #[test]
    fn test_raft_change_membership_forward_to_leader() {
        let ftl = ForwardToLeader {
            leader_id: Some(3),
            leader_node: None,
        };
        let cwe = ClientWriteError::ForwardToLeader(ftl);
        let op_err: MetaOperationError = cwe.into();
        assert!(matches!(op_err, MetaOperationError::ForwardToLeader(_)));
    }

    #[test]
    fn test_raft_change_membership_error() {
        let cm = ChangeMembershipError::EmptyMembership(EmptyMembership {});
        let cwe = ClientWriteError::ChangeMembershipError(cm);
        let op_err: MetaOperationError = cwe.into();
        assert!(matches!(
            op_err,
            MetaOperationError::ChangeMembershipError(_)
        ));
    }

    #[test]
    fn test_raft_error_fatal_into_meta_operation_error() {
        let fatal = Fatal::Panicked;
        let raft_err: RaftError<ClientWriteError> = RaftError::Fatal(fatal);
        let op_err: MetaOperationError = raft_err.into();
        assert!(matches!(op_err, MetaOperationError::WriteError(_)));
    }

    #[test]
    fn test_raft_error_api_forward_into_meta_operation_error() {
        let ftl = ForwardToLeader {
            leader_id: None,
            leader_node: None,
        };
        let cwe = ClientWriteError::ForwardToLeader(ftl);
        let raft_err: RaftError<ClientWriteError> = RaftError::APIError(cwe);
        let op_err: MetaOperationError = raft_err.into();
        assert!(matches!(op_err, MetaOperationError::ForwardToLeader(_)));
    }

    #[test]
    fn test_split_forward_to_leader() {
        let ftl = ForwardToLeader {
            leader_id: Some(5),
            leader_node: None,
        };
        let op_err = MetaOperationError::ForwardToLeader(ftl.clone());
        assert_eq!(op_err.peel(), Err(ftl));
    }

    #[test]
    fn test_split_write_error() {
        let op_err = MetaOperationError::WriteError(Fatal::Panicked);
        assert_eq!(
            op_err.peel(),
            Ok(MetaDataError::WriteError(Fatal::Panicked))
        );
    }

    #[test]
    fn test_split_read_error() {
        let read_err = MetaDataReadError::new("r", "m", &io::Error::other("e"));
        let op_err = MetaOperationError::ReadError(read_err.clone());
        assert_eq!(op_err.peel(), Ok(MetaDataError::ReadError(read_err)));
    }
}
