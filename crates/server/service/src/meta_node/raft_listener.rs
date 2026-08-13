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

//! The raft service listeners a node serves, and their lifecycle: binding a
//! port, serving the service on it, and stopping when the node stops.

use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;

use anyerror::AnyError;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_types::Endpoint;
use databend_meta_types::MetaNetworkError;
use databend_meta_types::protobuf::raft_service_server::RaftServiceServer;
use log::info;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::ServerTlsConfig;
use tonic::transport::server::Router;
use tonic::transport::server::TcpIncoming;

use crate::meta_node::meta_node::MetaNode;
use crate::meta_service::raft_service_impl::RaftServiceImpl;
use crate::raft_secret::RaftSecretChecker;
use crate::raft_transport::install_crypto_provider;
use crate::raft_transport::server_tls_config;

/// A raft service that already holds its port but is not yet served.
struct RaftListener {
    socket_addr: SocketAddr,
    /// `http` or `https`, for logging and for naming the serving task.
    scheme: &'static str,
    incoming: TcpIncoming,
    server: Router,
}

/// Start the grpc service for raft communication and meta operation API.
///
/// A node with a configured TLS identity serves raft on a second port as
/// well. Both listeners run the same service and the same secret check, so
/// they differ only in transport, and the plaintext one stays open: peers
/// that cannot dial TLS have to keep reaching this node throughout the
/// migration. Which port a peer picks is decided by the TLS address this
/// node publishes in its own record, not by anything here.
#[fastrace::trace]
pub(crate) async fn start_raft_service<SP: SpawnApi>(
    meta_node: Arc<MetaNode<SP>>,
    endpoint: &Endpoint,
) -> Result<(), MetaNetworkError> {
    info!("Start raft service listening on: {}", endpoint);

    let max_msg_size = meta_node.raft_store.config.raft_grpc_max_message_size();
    info!(
        "RaftService gRPC message size limit: {}MB",
        max_msg_size / (1024 * 1024)
    );

    let socket_addr = resolve_listen_addr::<SP>(endpoint).await?;

    // Every listener is built, port included, before any of them is
    // spawned. A spawned listener cannot be called back: its service owns
    // an `Arc<MetaNode>`, so it keeps the node and its port alive after
    // this function returns an error, with nobody left holding a handle to
    // stop it. Building first leaves a failure with nothing but bound
    // sockets to drop.
    let plaintext_listener = build_raft_listener(&meta_node, socket_addr, None)?;

    let config = &meta_node.raft_store.config;

    let tls_listener = match config.raft_tls_listen_host_endpoint() {
        Some(tls_endpoint) => {
            info!("Start raft TLS service listening on: {}", tls_endpoint);

            let tls = server_tls_config(config).await?;
            let tls_socket_addr = resolve_listen_addr::<SP>(&tls_endpoint).await?;
            let listener = build_raft_listener(&meta_node, tls_socket_addr, Some(tls))?;

            Some(listener)
        }
        None => None,
    };

    spawn_raft_listener(&meta_node, plaintext_listener).await;

    if let Some(tls_listener) = tls_listener {
        spawn_raft_listener(&meta_node, tls_listener).await;
    }

    Ok(())
}

/// Resolve a listen endpoint to a socket address, looking up the host when
/// it is a name rather than an address.
async fn resolve_listen_addr<SP: SpawnApi>(
    endpoint: &Endpoint,
) -> Result<SocketAddr, MetaNetworkError> {
    let host = endpoint.addr();
    let port = endpoint.port();

    let ipv4_addr = host.parse::<Ipv4Addr>();
    let ip_port = match ipv4_addr {
        Ok(addr) => format!("{}:{}", addr, port),
        Err(_) => {
            let ip_addrs = SP::resolve(host).await.map_err(|e| {
                MetaNetworkError::GetNodeAddrError(format!("resolve addr {} error: {}", host, e))
            })?;
            format!("{}:{}", ip_addrs[0], port)
        }
    };

    let socket_addr = ip_port.parse::<SocketAddr>()?;

    Ok(socket_addr)
}

/// Build one raft listener, taking its port, but leave it unserved.
///
/// Serving plaintext when `tls` is `None`.
fn build_raft_listener<SP: SpawnApi>(
    meta_node: &Arc<MetaNode<SP>>,
    socket_addr: SocketAddr,
    tls: Option<ServerTlsConfig>,
) -> Result<RaftListener, MetaNetworkError> {
    // One service instance per listener: `RaftServiceImpl` is not `Clone`,
    // and creating a second one costs nothing but another handle.
    let raft_service_impl = RaftServiceImpl::create(meta_node.clone());

    let max_msg_size = meta_node.raft_store.config.raft_grpc_max_message_size();

    let raft_server = InterceptedService::new(
        RaftServiceServer::new(raft_service_impl)
            .max_decoding_message_size(max_msg_size)
            .max_encoding_message_size(max_msg_size),
        RaftSecretChecker::new(&meta_node.raft_store.config),
    );

    let scheme = if tls.is_some() { "https" } else { "http" };

    info!("about to build raft grpc on: {}://{}", scheme, socket_addr);

    // Bind here rather than inside the spawned task: if the port is taken,
    // startup must fail loudly. `serve_with_shutdown()` binds inside the
    // task, where the error is only observed when the task is joined at
    // shutdown. Until then the node reports a successful start while having
    // no raft service at all.
    let incoming = TcpIncoming::bind(socket_addr)
        .map_err(|e| {
            MetaNetworkError::BadAddressFormat(
                AnyError::new(&e).add_context(|| format!("bind raft service to {}", socket_addr)),
            )
        })?
        .with_nodelay(Some(true));

    let mut builder = tonic::transport::Server::builder();
    // .concurrency_limit_per_connection()
    // .timeout(Duration::from_secs(60))

    if let Some(tls) = tls {
        install_crypto_provider();

        builder = builder
            .tls_config(tls)
            .map_err(|e| MetaNetworkError::TLSConfigError(AnyError::new(&e)))?;
    }

    let server = builder.add_service(raft_server);

    Ok(RaftListener {
        socket_addr,
        scheme,
        incoming,
        server,
    })
}

/// Serve a built listener, and register its task so that shutdown joins it.
async fn spawn_raft_listener<SP: SpawnApi>(meta_node: &Arc<MetaNode<SP>>, listener: RaftListener) {
    let RaftListener {
        socket_addr,
        scheme,
        incoming,
        server,
    } = listener;

    let mut running_rx = meta_node.running_rx.clone();
    let node_id = meta_node.raft_store.id;

    info!("about to serve raft grpc on: {}://{}", scheme, socket_addr);

    let h = SP::spawn(
        async move {
            server
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = running_rx.changed().await;
                    info!(
                        "running_rx for Raft server received, shutting down: id={} {}://{} ",
                        node_id, scheme, socket_addr
                    );
                })
                .await
                .map_err(|e| {
                    AnyError::new(&e).add_context(|| "when serving meta-service raft service")
                })?;

            Ok::<(), AnyError>(())
        },
        Some(format!("raft-server-{}", scheme)),
    );

    let mut jh = meta_node.join_handles.lock().await;
    jh.push(h);
}
