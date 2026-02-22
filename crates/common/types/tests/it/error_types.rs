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

mod test_seq_num {
    use databend_meta_types::SeqNum;

    #[test]
    fn test_display() {
        assert_eq!(SeqNum(0).to_string(), "0");
        assert_eq!(SeqNum(42).to_string(), "42");
    }

    #[test]
    fn test_add() {
        assert_eq!((SeqNum(3) + 5).0, 8);
        assert_eq!((SeqNum(0) + 0).0, 0);
    }

    #[test]
    fn test_into_u64() {
        let v: u64 = SeqNum(99).into();
        assert_eq!(v, 99);
    }

    #[test]
    fn test_default() {
        assert_eq!(SeqNum::default().0, 0);
    }
}

mod test_meta_network_errors {
    use std::io;

    use anyerror::AnyError;
    use databend_meta_types::ConnectionError;
    use databend_meta_types::InvalidArgument;
    use databend_meta_types::InvalidReply;
    use databend_meta_types::MetaNetworkError;
    use databend_meta_types::errors::IncompleteStream;

    #[test]
    fn test_connection_error_display() {
        let e = ConnectionError::new(io::Error::other("conn failed"), "connecting to node");
        let s = e.to_string();
        assert!(s.contains("connecting to node"), "{}", s);
        assert!(s.contains("conn failed"), "{}", s);
    }

    #[test]
    fn test_connection_error_add_context() {
        let e = ConnectionError::new(io::Error::other("timeout"), "rpc");
        let e = e.add_context("during handshake");
        let s = e.to_string();
        assert!(s.contains("during handshake"), "{}", s);
    }

    #[test]
    fn test_invalid_argument_display() {
        let e = InvalidArgument::new(io::Error::other("bad key"), "key validation");
        let s = e.to_string();
        assert!(s.contains("key validation"), "{}", s);
        assert!(s.contains("bad key"), "{}", s);
    }

    #[test]
    fn test_invalid_argument_add_context() {
        let e = InvalidArgument::new(io::Error::other("bad"), "arg");
        let e = e.add_context("in request");
        let s = e.to_string();
        assert!(s.contains("in request"), "{}", s);
    }

    #[test]
    fn test_invalid_reply_display() {
        let e = InvalidReply::new("decode failed", &io::Error::other("corrupt"));
        let s = e.to_string();
        assert!(s.contains("decode failed"), "{}", s);
        assert!(s.contains("corrupt"), "{}", s);
    }

    #[test]
    fn test_invalid_reply_add_context() {
        let e = InvalidReply::new("bad", &io::Error::other("x"));
        let e = e.add_context("stream read");
        let s = e.to_string();
        assert!(s.contains("stream read"), "{}", s);
    }

    #[test]
    fn test_invalid_reply_into_io_error() {
        let e = InvalidReply::new("bad reply", &io::Error::other("x"));
        let io_err: io::Error = e.into();
        assert_eq!(io_err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_incomplete_stream_into_invalid_reply() {
        let e = IncompleteStream::new(10, 5);
        let reply: InvalidReply = e.into();
        let s = reply.to_string();
        assert!(s.contains("Invalid reply"), "{}", s);
    }

    #[test]
    fn test_meta_network_error_from_tonic_status_invalid_argument() {
        let status = tonic::Status::invalid_argument("bad arg");
        let e: MetaNetworkError = status.into();
        assert_eq!(e.name(), "InvalidArgument");
    }

    #[test]
    fn test_meta_network_error_from_tonic_status_other() {
        let status = tonic::Status::unavailable("server down");
        let e: MetaNetworkError = status.into();
        assert_eq!(e.name(), "ConnectionError");
    }

    #[test]
    fn test_meta_network_error_from_addr_parse_error() {
        let e: MetaNetworkError = "not-an-addr"
            .parse::<std::net::SocketAddr>()
            .unwrap_err()
            .into();
        assert_eq!(e.name(), "BadAddressFormat");
    }

    #[test]
    fn test_meta_network_error_from_incomplete_stream() {
        let e = IncompleteStream::new(10, 3);
        let net_err: MetaNetworkError = e.into();
        assert_eq!(net_err.name(), "InvalidReply");
    }

    #[test]
    fn test_meta_network_error_name_variants() {
        let conn =
            MetaNetworkError::ConnectionError(ConnectionError::new(io::Error::other("x"), "y"));
        assert_eq!(conn.name(), "ConnectionError");

        let dns = MetaNetworkError::DnsParseError("bad dns".into());
        assert_eq!(dns.name(), "DnsParseError");

        let get_addr = MetaNetworkError::GetNodeAddrError("no addr".into());
        assert_eq!(get_addr.name(), "GetNodeAddrError");

        let tls = MetaNetworkError::TLSConfigError(AnyError::error("tls fail"));
        assert_eq!(tls.name(), "TLSConfigError");

        let bad_addr = MetaNetworkError::BadAddressFormat(AnyError::error("bad"));
        assert_eq!(bad_addr.name(), "BadAddressFormat");
    }

    #[test]
    fn test_meta_network_error_add_context() {
        let e = MetaNetworkError::GetNodeAddrError("no addr".into());
        let e = e.add_context("while forwarding");
        let s = e.to_string();
        assert!(s.contains("while forwarding"), "{}", s);

        let e = MetaNetworkError::DnsParseError("bad".into());
        let e = e.add_context("lookup");
        let s = e.to_string();
        assert!(s.contains("lookup"), "{}", s);

        let e = MetaNetworkError::TLSConfigError(AnyError::error("tls fail"));
        let e = e.add_context("configuring TLS");
        let s = e.to_string();
        assert!(s.contains("configuring TLS"), "{}", s);

        let e = MetaNetworkError::BadAddressFormat(AnyError::error("bad"));
        let e = e.add_context("parsing address");
        let s = e.to_string();
        assert!(s.contains("parsing address"), "{}", s);
    }

    #[test]
    fn test_meta_network_error_into_io_error() {
        let cases: Vec<(MetaNetworkError, io::ErrorKind)> = vec![
            (
                MetaNetworkError::ConnectionError(ConnectionError::new(io::Error::other("x"), "y")),
                io::ErrorKind::NotConnected,
            ),
            (
                MetaNetworkError::GetNodeAddrError("no addr".into()),
                io::ErrorKind::NotFound,
            ),
            (
                MetaNetworkError::DnsParseError("bad dns".into()),
                io::ErrorKind::NotConnected,
            ),
            (
                MetaNetworkError::TLSConfigError(AnyError::error("tls")),
                io::ErrorKind::NotConnected,
            ),
            (
                MetaNetworkError::BadAddressFormat(AnyError::error("bad")),
                io::ErrorKind::InvalidInput,
            ),
            (
                MetaNetworkError::InvalidArgument(InvalidArgument::new(io::Error::other("x"), "y")),
                io::ErrorKind::InvalidInput,
            ),
            (
                MetaNetworkError::InvalidReply(InvalidReply::new("x", &io::Error::other("y"))),
                io::ErrorKind::InvalidData,
            ),
        ];

        for (net_err, expected_kind) in cases {
            let io_err: io::Error = net_err.into();
            assert_eq!(io_err.kind(), expected_kind);
        }
    }
}

mod test_meta_startup_errors {
    use std::io;

    use anyerror::AnyError;
    use databend_meta_types::MetaNetworkError;
    use databend_meta_types::MetaStartupError;

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

mod test_rpc_errors {
    use std::io;

    use anyerror::AnyError;
    use databend_meta_types::ConnectionError;
    use databend_meta_types::ForwardRPCError;
    use databend_meta_types::MetaAPIError;
    use databend_meta_types::MetaDataError;
    use databend_meta_types::MetaDataReadError;
    use databend_meta_types::MetaNetworkError;

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

    #[test]
    fn test_forward_rpc_network_error_into_meta_api_error() {
        let net =
            MetaNetworkError::ConnectionError(ConnectionError::new(io::Error::other("x"), "y"));
        let fwd = ForwardRPCError::NetworkError(net);
        let api_err: MetaAPIError = fwd.into();
        assert_eq!(api_err.name(), "NetworkError");
    }

    #[test]
    fn test_forward_rpc_remote_data_error_becomes_remote_error() {
        let read_err = MetaDataReadError::new("read", "msg", &io::Error::other("io"));
        let data_err = MetaDataError::ReadError(read_err);
        let api_err = MetaAPIError::DataError(data_err);
        let fwd = ForwardRPCError::RemoteError(api_err);
        let result: MetaAPIError = fwd.into();
        assert_eq!(result.name(), "RemoteError");
    }

    #[test]
    fn test_forward_rpc_remote_non_data_passes_through() {
        let net = MetaNetworkError::GetNodeAddrError("addr".into());
        let api_err = MetaAPIError::NetworkError(net);
        let fwd = ForwardRPCError::RemoteError(api_err);
        let result: MetaAPIError = fwd.into();
        assert_eq!(result.name(), "NetworkError");
    }

    #[test]
    fn test_forward_rpc_remote_can_not_forward_passes_through() {
        let api_err = MetaAPIError::CanNotForward(AnyError::error("no leader"));
        let fwd = ForwardRPCError::RemoteError(api_err);
        let result: MetaAPIError = fwd.into();
        assert_eq!(result.name(), "CanNotForward");
    }
}

mod test_meta_api_errors {
    use std::io;

    use anyerror::AnyError;
    use databend_meta_types::ConnectionError;
    use databend_meta_types::InvalidArgument;
    use databend_meta_types::InvalidReply;
    use databend_meta_types::MetaAPIError;
    use databend_meta_types::MetaDataError;
    use databend_meta_types::MetaDataReadError;
    use databend_meta_types::MetaNetworkError;
    use databend_meta_types::MetaOperationError;
    use databend_meta_types::errors::IncompleteStream;
    use databend_meta_types::raft_types::ForwardToLeader;

    #[test]
    fn test_name_variants() {
        let ftl = MetaAPIError::ForwardToLeader(ForwardToLeader {
            leader_id: None,
            leader_node: None,
        });
        assert_eq!(ftl.name(), "ForwardToLeader");

        let cnf = MetaAPIError::CanNotForward(AnyError::error("no leader"));
        assert_eq!(cnf.name(), "CanNotForward");

        let net = MetaAPIError::NetworkError(MetaNetworkError::GetNodeAddrError("addr".into()));
        assert_eq!(net.name(), "NetworkError");

        let read_err = MetaDataReadError::new("r", "m", &io::Error::other("e"));
        let de = MetaAPIError::DataError(MetaDataError::ReadError(read_err));
        assert_eq!(de.name(), "DataError");

        let read_err2 = MetaDataReadError::new("r", "m", &io::Error::other("e"));
        let re = MetaAPIError::RemoteError(MetaDataError::ReadError(read_err2));
        assert_eq!(re.name(), "RemoteError");
    }

    #[test]
    fn test_is_retryable() {
        let ftl = MetaAPIError::ForwardToLeader(ForwardToLeader {
            leader_id: None,
            leader_node: None,
        });
        assert!(ftl.is_retryable());

        let cnf = MetaAPIError::CanNotForward(AnyError::error("x"));
        assert!(cnf.is_retryable());

        let net = MetaAPIError::NetworkError(MetaNetworkError::GetNodeAddrError("x".into()));
        assert!(net.is_retryable());

        let read_err = MetaDataReadError::new("r", "m", &io::Error::other("e"));
        let data = MetaAPIError::DataError(MetaDataError::ReadError(read_err));
        assert!(!data.is_retryable());

        let read_err2 = MetaDataReadError::new("r", "m", &io::Error::other("e"));
        let remote_read = MetaAPIError::RemoteError(MetaDataError::ReadError(read_err2));
        assert!(!remote_read.is_retryable());
    }

    #[test]
    fn test_from_tonic_status() {
        let status = tonic::Status::unavailable("server down");
        let e: MetaAPIError = status.into();
        assert_eq!(e.name(), "NetworkError");
    }

    #[test]
    fn test_from_invalid_argument() {
        let inv = InvalidArgument::new(io::Error::other("bad"), "test");
        let e: MetaAPIError = inv.into();
        assert_eq!(e.name(), "NetworkError");
    }

    #[test]
    fn test_from_invalid_reply() {
        let inv = InvalidReply::new("bad reply", &io::Error::other("x"));
        let e: MetaAPIError = inv.into();
        assert_eq!(e.name(), "NetworkError");
    }

    #[test]
    fn test_from_incomplete_stream() {
        let inc = IncompleteStream::new(10, 5);
        let e: MetaAPIError = inc.into();
        assert_eq!(e.name(), "NetworkError");
    }

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
        let op_err = MetaOperationError::DataError(MetaDataError::ReadError(read_err));
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
    fn test_meta_data_read_error_display() {
        let e = MetaDataReadError::new("get_kv", "key=foo", &io::Error::other("disk"));
        let s = e.to_string();
        assert!(s.contains("get_kv"), "{}", s);
        assert!(s.contains("key=foo"), "{}", s);
        assert!(s.contains("disk"), "{}", s);
    }

    #[test]
    fn test_meta_api_error_into_io_error() {
        let ftl = MetaAPIError::ForwardToLeader(ForwardToLeader {
            leader_id: None,
            leader_node: None,
        });
        let io_err: io::Error = ftl.into();
        assert_eq!(io_err.kind(), io::ErrorKind::Interrupted);

        let cnf = MetaAPIError::CanNotForward(AnyError::error("no leader"));
        let io_err: io::Error = cnf.into();
        assert_eq!(io_err.kind(), io::ErrorKind::Interrupted);

        let net = MetaAPIError::NetworkError(MetaNetworkError::ConnectionError(
            ConnectionError::new(io::Error::other("x"), "y"),
        ));
        let io_err: io::Error = net.into();
        assert_eq!(io_err.kind(), io::ErrorKind::NotConnected);

        let read_err = MetaDataReadError::new("r", "m", &io::Error::other("e"));
        let data = MetaAPIError::DataError(MetaDataError::ReadError(read_err));
        let io_err: io::Error = data.into();
        assert_eq!(io_err.kind(), io::ErrorKind::Other);

        let read_err2 = MetaDataReadError::new("r", "m", &io::Error::other("e"));
        let remote = MetaAPIError::RemoteError(MetaDataError::ReadError(read_err2));
        let io_err: io::Error = remote.into();
        assert_eq!(io_err.kind(), io::ErrorKind::Other);
    }
}

mod test_meta_client_errors {
    use std::io;

    use databend_meta_types::ConnectionError;
    use databend_meta_types::InvalidReply;
    use databend_meta_types::MetaClientError;
    use databend_meta_types::MetaHandshakeError;
    use databend_meta_types::MetaNetworkError;

    #[test]
    fn test_name() {
        let net =
            MetaClientError::NetworkError(MetaNetworkError::GetNodeAddrError("no addr".into()));
        assert_eq!(net.name(), "GetNodeAddrError");

        let hs = MetaClientError::HandshakeError(MetaHandshakeError::new("auth failed"));
        assert_eq!(hs.name(), "MetaHandshakeError");
    }

    #[test]
    fn test_from_tonic_status() {
        let status = tonic::Status::unavailable("down");
        let e: MetaClientError = status.into();
        assert_eq!(e.name(), "ConnectionError");
    }

    #[test]
    fn test_from_connection_error() {
        let conn = ConnectionError::new(io::Error::other("x"), "y");
        let e: MetaClientError = conn.into();
        assert_eq!(e.name(), "ConnectionError");
    }

    #[test]
    fn test_from_invalid_reply() {
        let inv = InvalidReply::new("bad", &io::Error::other("x"));
        let e: MetaClientError = inv.into();
        assert_eq!(e.name(), "InvalidReply");
    }

    #[test]
    fn test_into_io_error_network() {
        let net = MetaClientError::NetworkError(MetaNetworkError::ConnectionError(
            ConnectionError::new(io::Error::other("x"), "y"),
        ));
        let io_err: io::Error = net.into();
        assert_eq!(io_err.kind(), io::ErrorKind::NotConnected);
    }

    #[test]
    fn test_into_io_error_handshake() {
        let hs = MetaClientError::HandshakeError(MetaHandshakeError::new("auth"));
        let io_err: io::Error = hs.into();
        assert_eq!(io_err.kind(), io::ErrorKind::NotConnected);
    }
}

mod test_meta_errors {
    use std::io;

    use anyerror::AnyError;
    use databend_meta_types::ConnectionError;
    use databend_meta_types::InvalidArgument;
    use databend_meta_types::InvalidReply;
    use databend_meta_types::MetaAPIError;
    use databend_meta_types::MetaClientError;
    use databend_meta_types::MetaError;
    use databend_meta_types::MetaHandshakeError;
    use databend_meta_types::MetaNetworkError;
    use databend_meta_types::errors::IncompleteStream;

    #[test]
    fn test_name() {
        let net = MetaError::NetworkError(MetaNetworkError::GetNodeAddrError("x".into()));
        assert_eq!(net.name(), "GetNodeAddrError");

        let storage = MetaError::StorageError(AnyError::error("disk"));
        assert_eq!(storage.name(), "StorageError");

        let client = MetaError::ClientError(MetaClientError::HandshakeError(
            MetaHandshakeError::new("auth"),
        ));
        assert_eq!(client.name(), "MetaHandshakeError");

        let api = MetaError::APIError(MetaAPIError::CanNotForward(AnyError::error("x")));
        assert_eq!(api.name(), "CanNotForward");
    }

    #[test]
    fn test_from_io_error() {
        let io_err = io::Error::other("disk fail");
        let e: MetaError = io_err.into();
        assert_eq!(e.name(), "StorageError");
        assert!(e.to_string().contains("disk fail"), "{}", e);
    }

    #[test]
    fn test_from_tonic_status() {
        let status = tonic::Status::unavailable("down");
        let e: MetaError = status.into();
        assert_eq!(e.name(), "ConnectionError");
    }

    #[test]
    fn test_from_invalid_argument() {
        let inv = InvalidArgument::new(io::Error::other("bad"), "test");
        let e: MetaError = inv.into();
        assert_eq!(e.name(), "NetworkError");
    }

    #[test]
    fn test_from_invalid_reply() {
        let inv = InvalidReply::new("bad", &io::Error::other("x"));
        let e: MetaError = inv.into();
        assert_eq!(e.name(), "NetworkError");
    }

    #[test]
    fn test_from_incomplete_stream() {
        let inc = IncompleteStream::new(10, 5);
        let e: MetaError = inc.into();
        assert_eq!(e.name(), "InvalidReply");
    }

    #[test]
    fn test_from_meta_handshake_error() {
        let hs = MetaHandshakeError::new("auth failed");
        let e: MetaError = hs.into();
        assert_eq!(e.name(), "MetaHandshakeError");
    }

    #[test]
    fn test_into_io_error() {
        let cases: Vec<(MetaError, io::ErrorKind)> = vec![
            (
                MetaError::NetworkError(MetaNetworkError::ConnectionError(ConnectionError::new(
                    io::Error::other("x"),
                    "y",
                ))),
                io::ErrorKind::NotConnected,
            ),
            (
                MetaError::StorageError(AnyError::error("disk")),
                io::ErrorKind::Other,
            ),
            (
                MetaError::ClientError(MetaClientError::HandshakeError(MetaHandshakeError::new(
                    "auth",
                ))),
                io::ErrorKind::NotConnected,
            ),
            (
                MetaError::APIError(MetaAPIError::CanNotForward(AnyError::error("x"))),
                io::ErrorKind::Interrupted,
            ),
        ];

        for (meta_err, expected_kind) in cases {
            let io_err: io::Error = meta_err.into();
            assert_eq!(io_err.kind(), expected_kind);
        }
    }
}

mod test_meta_raft_errors {
    use databend_meta_types::MetaDataError;
    use databend_meta_types::MetaOperationError;
    use databend_meta_types::raft_types::ChangeMembershipError;
    use databend_meta_types::raft_types::ClientWriteError;
    use databend_meta_types::raft_types::Fatal;
    use databend_meta_types::raft_types::ForwardToLeader;
    use databend_meta_types::raft_types::RaftError;
    use openraft::error::EmptyMembership;

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
            MetaOperationError::DataError(MetaDataError::ChangeMembershipError(_))
        ));
    }

    #[test]
    fn test_raft_error_fatal_into_meta_operation_error() {
        let fatal = Fatal::Panicked;
        let raft_err: RaftError<ClientWriteError> = RaftError::Fatal(fatal);
        let op_err: MetaOperationError = raft_err.into();
        assert!(matches!(
            op_err,
            MetaOperationError::DataError(MetaDataError::WriteError(_))
        ));
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
}

mod test_incomplete_stream {
    use std::io;

    use databend_meta_types::errors::IncompleteStream;

    #[test]
    fn test_display() {
        let e = IncompleteStream::new(10, 5);
        assert_eq!(
            e.to_string(),
            "IncompleteStream: expect: 10 items, got: 5 items"
        );
    }

    #[test]
    fn test_display_with_context() {
        let e = IncompleteStream::new(10, 5).context("; reading snapshot");
        assert!(e.to_string().contains("reading snapshot"), "{}", e);
    }

    #[test]
    fn test_into_io_error() {
        let e = IncompleteStream::new(10, 5);
        let io_err: io::Error = e.into();
        assert_eq!(io_err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
