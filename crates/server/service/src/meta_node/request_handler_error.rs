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

use anyerror::AnyError;
use databend_meta_types::InvalidReply;
use databend_meta_types::MetaAPIError;
use databend_meta_types::MetaDataError;
use databend_meta_types::MetaNetworkError;

use crate::meta_service::forward_rpc_error::ForwardRPCError;

/// The forwarding quota is exhausted or no leader is known.
#[derive(thiserror::Error, serde::Serialize, Debug, Clone, PartialEq, Eq)]
#[error("can not forward any more: {0}")]
pub struct CanNotForwardError(pub AnyError);

/// Errors from `handle_forwardable_request()`.
///
/// Contains exactly the errors this method can produce:
/// forwarding infrastructure failures and business-logic data errors.
#[derive(thiserror::Error, serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub enum ForwardRequestError {
    #[error(transparent)]
    CanNotForward(#[from] CanNotForwardError),

    #[error(transparent)]
    NetworkError(#[from] MetaNetworkError),

    #[error(transparent)]
    DataError(#[from] MetaDataError),
}

impl From<ForwardRequestError> for MetaAPIError {
    fn from(e: ForwardRequestError) -> Self {
        match e {
            ForwardRequestError::CanNotForward(e) => MetaAPIError::CanNotForward(e.0),
            ForwardRequestError::NetworkError(e) => MetaAPIError::NetworkError(e),
            ForwardRequestError::DataError(e) => MetaAPIError::DataError(e),
        }
    }
}

impl From<InvalidReply> for ForwardRequestError {
    fn from(e: InvalidReply) -> Self {
        ForwardRequestError::NetworkError(MetaNetworkError::from(e))
    }
}

impl From<ForwardRPCError> for ForwardRequestError {
    fn from(e: ForwardRPCError) -> Self {
        match e {
            ForwardRPCError::NetworkError(e) => ForwardRequestError::NetworkError(e),
            ForwardRPCError::RemoteError(api_err) => match api_err {
                MetaAPIError::DataError(e) | MetaAPIError::RemoteError(e) => {
                    ForwardRequestError::DataError(e)
                }
                MetaAPIError::NetworkError(e) => ForwardRequestError::NetworkError(e),
                MetaAPIError::CanNotForward(e) => {
                    ForwardRequestError::CanNotForward(CanNotForwardError(e))
                }
                MetaAPIError::ForwardToLeader(e) => {
                    log::warn!(
                        "unexpected ForwardToLeader in ForwardRPCError::RemoteError: {}",
                        e
                    );
                    ForwardRequestError::CanNotForward(CanNotForwardError(AnyError::new(&e)))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use anyerror::AnyError;
    use databend_meta_types::ConnectionError;
    use databend_meta_types::MetaAPIError;
    use databend_meta_types::MetaDataError;
    use databend_meta_types::MetaDataReadError;
    use databend_meta_types::MetaNetworkError;
    use databend_meta_types::raft_types::ForwardToLeader;

    use super::*;

    // -- From<ForwardRPCError> for ForwardRequestError tests --

    #[test]
    fn test_from_rpc_network_error() {
        let net = MetaNetworkError::GetNodeAddrError("no addr".into());
        let fwd = ForwardRPCError::NetworkError(net);
        let res: ForwardRequestError = fwd.into();
        assert!(matches!(res, ForwardRequestError::NetworkError(_)));
    }

    #[test]
    fn test_from_rpc_remote_data_error() {
        let read_err = MetaDataReadError::new("read kv", "not found", &io::Error::other("io"));
        let fwd = ForwardRPCError::RemoteError(MetaAPIError::DataError(MetaDataError::ReadError(
            read_err,
        )));
        let res: ForwardRequestError = fwd.into();
        assert!(matches!(res, ForwardRequestError::DataError(_)));
        assert!(res.to_string().contains("read kv"));
    }

    #[test]
    fn test_from_rpc_remote_remote_error() {
        let read_err = MetaDataReadError::new("read", "msg", &io::Error::other("io"));
        let fwd = ForwardRPCError::RemoteError(MetaAPIError::RemoteError(
            MetaDataError::ReadError(read_err),
        ));
        let res: ForwardRequestError = fwd.into();
        assert!(matches!(res, ForwardRequestError::DataError(_)));
    }

    #[test]
    fn test_from_rpc_remote_can_not_forward() {
        let fwd =
            ForwardRPCError::RemoteError(MetaAPIError::CanNotForward(AnyError::error("quota")));
        let res: ForwardRequestError = fwd.into();
        assert!(matches!(res, ForwardRequestError::CanNotForward(_)));
    }

    #[test]
    fn test_from_rpc_remote_network_error() {
        let net =
            MetaNetworkError::ConnectionError(ConnectionError::new(io::Error::other("x"), "y"));
        let fwd = ForwardRPCError::RemoteError(MetaAPIError::NetworkError(net));
        let res: ForwardRequestError = fwd.into();
        assert!(matches!(res, ForwardRequestError::NetworkError(_)));
    }

    #[test]
    fn test_from_rpc_remote_forward_to_leader() {
        let ftl = ForwardToLeader {
            leader_id: Some(1),
            leader_node: None,
        };
        let fwd = ForwardRPCError::RemoteError(MetaAPIError::ForwardToLeader(ftl));
        let res: ForwardRequestError = fwd.into();
        assert!(matches!(res, ForwardRequestError::CanNotForward(_)));
    }

    // -- From<ForwardRequestError> for MetaAPIError tests --

    #[test]
    fn test_into_meta_api_error_can_not_forward() {
        let e = ForwardRequestError::CanNotForward(CanNotForwardError(AnyError::error("max")));
        let api: MetaAPIError = e.into();
        assert_eq!(api.name(), "CanNotForward");
    }

    #[test]
    fn test_into_meta_api_error_network() {
        let net = MetaNetworkError::GetNodeAddrError("addr".into());
        let e = ForwardRequestError::NetworkError(net);
        let api: MetaAPIError = e.into();
        assert_eq!(api.name(), "NetworkError");
    }

    #[test]
    fn test_into_meta_api_error_data() {
        let read_err = MetaDataReadError::new("op", "msg", &io::Error::other("io"));
        let e = ForwardRequestError::DataError(MetaDataError::ReadError(read_err));
        let api: MetaAPIError = e.into();
        assert_eq!(api.name(), "DataError");
    }

    /// Verify that serializing `ForwardRequestError` produces JSON
    /// that an old client can deserialize as `MetaAPIError`.
    ///
    /// This matters because `RaftReply` serializes the error as JSON,
    /// and `reply_to_api_result()` deserializes it as `MetaAPIError`.
    mod serde_compat {
        use anyerror::AnyError;
        use databend_meta_types::MetaAPIError;
        use databend_meta_types::MetaDataError;
        use databend_meta_types::MetaDataReadError;
        use databend_meta_types::MetaNetworkError;

        use super::*;

        fn round_trip(e: &ForwardRequestError) -> MetaAPIError {
            let json = serde_json::to_string(e).unwrap();
            serde_json::from_str::<MetaAPIError>(&json).unwrap_or_else(|de_err| {
                panic!(
                    "failed to deserialize ForwardRequestError JSON as MetaAPIError:\n  json: {}\n  error: {}",
                    json, de_err
                )
            })
        }

        #[test]
        fn test_can_not_forward_compat() {
            let e =
                ForwardRequestError::CanNotForward(CanNotForwardError(AnyError::error("quota")));
            let api_err = round_trip(&e);
            assert_eq!(api_err.name(), "CanNotForward");
            assert!(api_err.to_string().contains("quota"), "{}", api_err);
        }

        #[test]
        fn test_network_error_compat() {
            let e = ForwardRequestError::NetworkError(MetaNetworkError::GetNodeAddrError(
                "no addr".into(),
            ));
            let api_err = round_trip(&e);
            assert_eq!(api_err.name(), "NetworkError");
        }

        #[test]
        fn test_data_error_compat() {
            let read_err = MetaDataReadError::new("read kv", "not found", &io::Error::other("io"));
            let e = ForwardRequestError::DataError(MetaDataError::ReadError(read_err));
            let api_err = round_trip(&e);
            assert_eq!(api_err.name(), "DataError");
        }
    }
}
