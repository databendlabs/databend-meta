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
use databend_meta_types::MetaNetworkError;

/// Errors raised when invoking an RPC.
///
/// It includes two sub errors:
/// - Error that occurs when sending the RPC.
/// - Error that is returned by remove service.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ForwardRPCError {
    #[error(transparent)]
    NetworkError(#[from] MetaNetworkError),

    #[error(transparent)]
    RemoteError(#[from] MetaAPIError),
}

#[cfg(test)]
mod tests {
    use std::io;

    use databend_meta_types::MetaAPIError;
    use databend_meta_types::MetaDataError;
    use databend_meta_types::MetaDataReadError;
    use databend_meta_types::MetaNetworkError;

    use super::ForwardRPCError;

    #[test]
    fn test_from_network_error() {
        let net = MetaNetworkError::GetNodeAddrError("no addr".into());
        let e = ForwardRPCError::NetworkError(net);
        assert!(e.to_string().contains("no addr"), "{}", e);
    }

    #[test]
    fn test_from_remote_error() {
        let read_err = MetaDataReadError::new("read kv", "key not found", &io::Error::other("io"));
        let data_err = MetaDataError::ReadError(read_err);
        let api_err = MetaAPIError::DataError(data_err);
        let e = ForwardRPCError::RemoteError(api_err);
        assert!(e.to_string().contains("read kv"), "{}", e);
    }
}
