// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_runtime_api::TokioRuntime;
use futures::StreamExt;
use futures::future::poll_fn;
use parking_lot::Mutex;
use prost::Message;
use tonic::Code;
use tonic::Response;
use tonic::Status;
use tonic::transport::Channel;

use super::MetaGrpcClient;
use super::RPC_RETRIES;
use crate::DEFAULT_GRPC_MESSAGE_SIZE;
use crate::MIN_SERVER_VERSION;
use crate::MetaChannelManager;
use crate::endpoints::Endpoints;
use crate::established_client::EstablishedClient;
use crate::grpc_action::MGetKVReq;
use crate::grpc_action::MetaGrpcReadReq;
use crate::pool::Pool;
use crate::rpc_handler::ResponseAction;
use crate::rpc_handler::RpcHandler;
use crate::rpc_handler::is_local_h2_goaway;
use crate::rpc_handler::observe_read_stream;
use crate::types::protobuf::HandshakeResponse;
use crate::types::protobuf::StreamItem;

fn worker(address: &str, budget: Duration) -> MetaGrpcClient<TokioRuntime> {
    let endpoints = Arc::new(Mutex::new(Endpoints::new([address])));
    let manager = MetaChannelManager::new(
        "root",
        "",
        Some(budget),
        None,
        endpoints.clone(),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    );
    MetaGrpcClient {
        conn_pool: Pool::new(manager, Duration::from_millis(1)),
        endpoints,
        endpoints_str: vec![address.to_string()],
        auto_sync_interval: None,
        read_response_timeout: budget,
        _phantom: PhantomData,
    }
}

fn connection(client: &MetaGrpcClient<TokioRuntime>) -> EstablishedClient {
    let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
    let (client_rpc, _) = MetaChannelManager::<TokioRuntime>::new_real_client_for_testing(channel);
    EstablishedClient::new(
        client_rpc,
        MIN_SERVER_VERSION.to_digit(),
        &client.endpoints_str[0],
        client.endpoints.clone(),
    )
}

/// A deterministic, actual Library GOAWAY, not h2::Error::from(Reason).
/// With a zero internal-reset budget a content-length violation immediately
/// closes the client connection with ENHANCE_YOUR_CALM.
async fn local_goaway() -> Status {
    goaway(true).await
}

async fn goaway(local: bool) -> Status {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let server = async move {
            let mut connection = h2::server::handshake(server_io).await.unwrap();
            let (_, mut response) = connection.accept().await.unwrap().unwrap();
            if local {
                let headers = http::Response::builder()
                    .header("content-length", "0")
                    .body(())
                    .unwrap();
                let mut send = response.send_response(headers, false).unwrap();
                send.send_data(Bytes::from_static(b"x"), false).unwrap();
            } else {
                connection.abrupt_shutdown(h2::Reason::ENHANCE_YOUR_CALM);
            }
            let _ = poll_fn(|cx| connection.poll_closed(cx)).await;
        };
        let client = async move {
            let (mut sender, drive) = h2::client::Builder::new()
                .max_local_error_reset_streams(Some(0))
                .handshake::<_, Bytes>(client_io)
                .await
                .unwrap();
            let read = async move {
                let (response, _) = sender.send_request(http::Request::new(()), true).unwrap();
                if let Ok(response) = response.await {
                    let _ = response.into_body().data().await;
                }
            };
            let (_, result) = tokio::join!(read, drive);
            result.unwrap_err()
        };
        let (_, error) = tokio::join!(server, client);
        assert!(error.is_go_away());
        assert_eq!(error.is_library(), local);
        assert_eq!(error.is_remote(), !local);
        let status = Status::from_error(Box::new(error));
        assert_eq!(status.code(), Code::ResourceExhausted);
        status
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn test_only_structured_local_goaway_is_recognized() {
    let status = local_goaway().await;
    assert!(is_local_h2_goaway(&status));
    assert!(!is_local_h2_goaway(&goaway(false).await));
    assert!(!is_local_h2_goaway(&Status::resource_exhausted(
        "too_many_internal_resets ENHANCE_YOUR_CALM Library"
    )));
    let reason_only = Status::from_error(Box::new(h2::Error::from(h2::Reason::ENHANCE_YOUR_CALM)));
    assert!(!is_local_h2_goaway(&reason_only));
    let mut wrapped = Status::resource_exhausted("wrapped");
    wrapped.set_source(Arc::new(std::io::Error::other(status)));
    assert!(is_local_h2_goaway(&wrapped));
}

#[tokio::test]
async fn test_goaway_retry_is_read_only_and_invalidates_connection() {
    let client = worker("127.0.0.1:1", Duration::from_secs(1));
    let status = local_goaway().await;
    for read_only in [false, true] {
        let established = connection(&client);
        let mut handler = if read_only {
            RpcHandler::new_read(&client)
        } else {
            RpcHandler::new(&client)
        };
        handler.established_client = Some((established.clone(), Instant::now()));
        let result =
            handler.process_response_result(&"request", Err::<Response<()>, _>(status.clone()));
        if read_only {
            assert!(matches!(result, Ok(ResponseAction::ShouldRetry)));
            assert!(established.take_error().is_some());
        } else {
            assert!(result.is_err());
        }
    }
    let mut handler = RpcHandler::new_read(&client);
    handler.established_client = Some((connection(&client), Instant::now()));
    assert!(
        handler
            .process_response_result(
                &"request",
                Err::<Response<()>, _>(Status::resource_exhausted("quota"))
            )
            .is_err()
    );
    assert!(handler.rpc_failures.is_empty());
}

#[tokio::test]
async fn test_read_retry_attempts_and_total_deadline_are_bounded() {
    let client = worker("127.0.0.1:1", Duration::ZERO);
    let status = local_goaway().await;
    let mut handler = RpcHandler::new_read(&client);
    for _ in 0..RPC_RETRIES {
        handler.established_client = Some((connection(&client), Instant::now()));
        assert!(matches!(
            handler.process_response_result(&"request", Err::<Response<()>, _>(status.clone())),
            Ok(ResponseAction::ShouldRetry)
        ));
    }
    assert_eq!(handler.rpc_failures.len(), 4);
    assert!(
        handler
            .create_network_error()
            .to_string()
            .contains("after 4 retries")
    );
    // A retry is delayed before another connection is obtained.
    {
        let next_attempt = handler.new_established_client();
        tokio::pin!(next_attempt);
        assert!(futures::poll!(&mut next_attempt).is_pending());
    }
    let error = client
        .kv_read_v1(MetaGrpcReadReq::MGetKV(MGetKVReq::new(["key"])))
        .await
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("retry budget exhausted"),
        "{error}"
    );
}

#[tokio::test]
async fn test_partial_stream_is_not_replayed() {
    let client = worker("127.0.0.1:1", Duration::from_secs(1));
    let established = connection(&client);
    let status = local_goaway().await;
    let source = futures::stream::iter([Ok(StreamItem::new("key".to_string(), None)), Err(status)]);
    let results = observe_read_stream(source, established.clone())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap().key, "key");
    assert!(results[1].is_err());
    assert!(established.take_error().is_some());
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A handshake-capable peer that fails read response establishment.
async fn failing_peer(code: Code) -> (String, Arc<AtomicUsize>, AbortOnDrop<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let task = TokioRuntime::spawn(
        async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let count = count.clone();
                        tasks.spawn(async move {
                            let mut connection = h2::server::handshake(socket).await.unwrap();
                            while let Some(request) = connection.accept().await {
                                let (request, mut response) = request.unwrap();
                                let handshake = request.uri().path().ends_with("/Handshake");
                                let mut body = request.into_body();
                                let read_request = async {
                                    while let Some(data) = body.data().await {
                                        let data = data.unwrap();
                                        body.flow_control().release_capacity(data.len()).unwrap();
                                    }
                                };
                                tokio::select! {
                                    _ = read_request => {}
                                    _ = poll_fn(|cx| connection.poll_closed(cx)) => { return; }
                                }
                                if !handshake {
                                    count.fetch_add(1, Ordering::SeqCst);
                                    let headers = http::Response::builder()
                                        .header("content-type", "application/grpc")
                                        .header("grpc-status", (code as i32).to_string())
                                        .body(()).unwrap();
                                    response.send_response(headers, true).unwrap();
                                    continue;
                                }
                                let headers = http::Response::builder().header("content-type", "application/grpc").body(()).unwrap();
                                let mut send = response.send_response(headers, false).unwrap();
                                if handshake {
                                    let message = HandshakeResponse { protocol_version: MIN_SERVER_VERSION.to_digit(), payload: vec![] };
                                    let mut data = BytesMut::new();
                                    data.put_u8(0);
                                    data.put_u32(message.encoded_len() as u32);
                                    message.encode(&mut data).unwrap();
                                    send.send_data(data.freeze(), false).unwrap();
                                }
                                let mut trailers = http::HeaderMap::new();
                                trailers.insert("grpc-status", http::HeaderValue::from_str(&(if handshake { Code::Ok } else { code } as i32).to_string()).unwrap());
                                send.send_trailers(trailers).unwrap();
                            }
                        });
                    }
                    Some(result) = tasks.join_next() => { result.unwrap(); }
                }
            }
        },
        None,
    );
    (address, requests, AbortOnDrop(task))
}

#[tokio::test]
async fn test_actual_read_loop_exhausts_attempts_without_retrying_quota() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for (code, expected) in [
            (Code::Unavailable, RPC_RETRIES),
            (Code::ResourceExhausted, 1),
        ] {
            let (address, requests, _server) = failing_peer(code).await;
            let client = worker(&address, Duration::from_secs(2));
            let error = client
                .kv_read_v1(MetaGrpcReadReq::MGetKV(MGetKVReq::new(["key"])))
                .await
                .err()
                .unwrap();
            assert_eq!(requests.load(Ordering::SeqCst), expected, "{error}");
            if code == Code::Unavailable {
                assert!(error.to_string().contains("after 4 retries"));
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_total_deadline_includes_reconnect_and_backoff() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (address, requests, _server) = failing_peer(Code::Unavailable).await;
        let client = worker(&address, Duration::from_millis(30));
        let error = client
            .kv_read_v1(MetaGrpcReadReq::MGetKV(MGetKVReq::new(["key"])))
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("retry budget exhausted"),
            "{error}"
        );
        assert!(requests.load(Ordering::SeqCst) < RPC_RETRIES);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_bad_pooled_connection_is_rebuilt() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = AbortOnDrop(TokioRuntime::spawn(
            async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut connection = h2::server::handshake(socket).await.unwrap();
                let (request, mut response) = connection.accept().await.unwrap().unwrap();
                assert!(request.uri().path().ends_with("/Handshake"));
                let message = HandshakeResponse {
                    protocol_version: MIN_SERVER_VERSION.to_digit(),
                    payload: vec![],
                };
                let mut data = BytesMut::new();
                data.put_u8(0);
                data.put_u32(message.encoded_len() as u32);
                message.encode(&mut data).unwrap();
                let mut send = response
                    .send_response(
                        http::Response::builder()
                            .header("content-type", "application/grpc")
                            .body(())
                            .unwrap(),
                        false,
                    )
                    .unwrap();
                send.send_data(data.freeze(), false).unwrap();
                let mut trailers = http::HeaderMap::new();
                trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                send.send_trailers(trailers).unwrap();
                let _ = poll_fn(|cx| connection.poll_closed(cx)).await;
            },
            None,
        ));
        let client = worker(&address, Duration::from_secs(1));
        let stale = connection(&client);
        let old_id = stale.to_string();
        stale.set_error(local_goaway().await);
        client
            .conn_pool
            .items
            .lock()
            .unwrap()
            .insert(address, Arc::new(tokio::sync::Mutex::new(Some(stale))));
        let rebuilt = client.get_established_client().await.unwrap();
        assert_ne!(rebuilt.to_string(), old_id);
        assert!(rebuilt.take_error().is_none());
        drop(server);
    })
    .await
    .unwrap();
}
