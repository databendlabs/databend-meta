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

//! Standalone binary for backward-compatibility testing of databend-meta.
//!
//! Provides three subcommands:
//! - `serve`  — Start a MetaNode + gRPC server + admin HTTP (for trigger_snapshot)
//! - `upsert` — Write a single KV pair via gRPC client
//! - `export` — Export state machine data via gRPC and print JSON lines to stdout

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use clap::Subcommand;
use databend_meta::api::GrpcServer;
use databend_meta::configs;
use databend_meta::meta_node::meta_worker::MetaWorker;
use databend_meta_client::MetaGrpcClient;
use databend_meta_runtime_api::RuntimeApi;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_types::UpsertKV;
use databend_meta_types::protobuf as pb;
use log::info;
use openraft::async_runtime::WatchReceiver as _;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;

#[derive(Parser)]
#[command(name = "databend-meta-compat")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
enum Commands {
    Serve(ServeArgs),
    Upsert(UpsertArgs),
    Export(ExportArgs),
}

#[derive(Parser)]
struct ServeArgs {
    #[arg(long)]
    id: u64,

    #[arg(long)]
    admin_api_address: String,

    #[arg(long)]
    grpc_api_address: String,

    #[arg(long, default_value = "127.0.0.1")]
    raft_listen_host: String,

    #[arg(long, default_value = "127.0.0.1")]
    raft_advertise_host: String,

    #[arg(long)]
    raft_api_port: u16,

    #[arg(long)]
    raft_dir: String,

    #[arg(long, default_value = "1000")]
    max_applied_log_to_keep: u64,

    #[arg(long)]
    single: bool,

    #[arg(long)]
    join: Vec<String>,

    #[arg(long)]
    log_dir: Option<String>,

    #[arg(long)]
    log_stderr_on: bool,

    #[arg(long)]
    log_stderr_level: Option<String>,

    #[arg(long)]
    log_stderr_format: Option<String>,

    #[arg(long)]
    log_file_on: bool,

    #[arg(long)]
    log_file_level: Option<String>,

    #[arg(long)]
    log_file_format: Option<String>,
}

#[derive(Parser)]
struct UpsertArgs {
    #[arg(long)]
    grpc_api_address: String,

    #[arg(long)]
    key: String,

    #[arg(long)]
    value: String,
}

#[derive(Parser)]
struct ExportArgs {
    #[arg(long)]
    grpc_api_address: String,
}

fn main() {
    let cli = Cli::parse();

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async move {
        match cli.command {
            Commands::Serve(args) => run_serve(args).await,
            Commands::Upsert(args) => run_upsert(args).await,
            Commands::Export(args) => run_export(args).await,
        }
    })
    .expect("Command failed");
}

async fn run_serve(args: ServeArgs) -> anyhow::Result<()> {
    init_logging(&args);

    let (grpc_host, grpc_port) = parse_host_port(&args.grpc_api_address)?;

    let mut config = configs::MetaServiceConfig::default();
    config.raft_config.id = args.id;
    config.raft_config.raft_listen_host = args.raft_listen_host;
    config.raft_config.raft_advertise_host = args.raft_advertise_host;
    config.raft_config.raft_api_port = args.raft_api_port;
    config.raft_config.raft_dir = args.raft_dir;
    config.raft_config.max_applied_log_to_keep = args.max_applied_log_to_keep;
    config.raft_config.single = args.single;
    config.raft_config.join = args.join;

    config.grpc.listen_host = grpc_host;
    config.grpc.listen_port = Some(grpc_port);

    let runtime = TokioRuntime::new_testing("meta-io-rt-compat");
    let mh = MetaWorker::create_meta_worker(config.clone(), Arc::new(runtime)).await?;
    let mh = Arc::new(mh);

    // Join cluster if needed
    {
        let c = config.clone();
        let _ = mh
            .request(move |mn| {
                let fu = async move { mn.join_cluster(&c).await };
                Box::pin(fu)
            })
            .await??;
    }

    let mut srv = GrpcServer::create(&config, mh.clone());
    srv.do_start().await?;

    info!(
        "gRPC server started on {}",
        config.grpc.api_address().unwrap_or_default()
    );

    // Start admin HTTP server for trigger_snapshot
    let admin_addr: SocketAddr = args.admin_api_address.parse()?;
    let admin_handle = mh.clone();
    tokio::spawn(run_admin_http(admin_addr, admin_handle));

    info!("Admin HTTP server started on {}", admin_addr);

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Received shutdown signal");

    srv.do_stop(None).await;
    Ok(())
}

async fn run_admin_http(
    addr: SocketAddr,
    meta_handle: Arc<databend_meta::meta_node::meta_handle::MetaHandle<TokioRuntime>>,
) {
    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind admin HTTP");

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                log::warn!("Admin HTTP accept error: {}", e);
                continue;
            }
        };

        let meta_handle = meta_handle.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            let n = match stream.read(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };

            let request = String::from_utf8_lossy(&buf[..n]);

            let (status, body) = if request.contains("/v1/ctrl/trigger_snapshot") {
                match meta_handle.handle_trigger_snapshot().await {
                    Ok(Ok(())) => ("200 OK", "OK".to_string()),
                    Ok(Err(e)) => {
                        log::error!("trigger_snapshot fatal: {}", e);
                        ("500 Internal Server Error", "Fatal error".to_string())
                    }
                    Err(e) => {
                        log::error!("trigger_snapshot stopped: {}", e);
                        ("503 Service Unavailable", "Node stopped".to_string())
                    }
                }
            } else if request.contains("/v1/ctrl/status") {
                match meta_handle.handle_raft_metrics().await {
                    Ok(rx) => {
                        let json =
                            serde_json::to_string(&NodeStatus::from_metrics(&rx.borrow_watched()))
                                .expect("NodeStatus is always serializable");
                        ("200 OK", json)
                    }
                    Err(e) => {
                        log::error!("status: {}", e);
                        ("503 Service Unavailable", "Node stopped".to_string())
                    }
                }
            } else {
                ("404 Not Found", "Not Found".to_string())
            };

            let response = format!(
                "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status,
                body.len(),
                body
            );

            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

/// Stable JSON shape for `/v1/ctrl/status`.
///
/// Identical wire contract to `bin-new`. Field names form the protocol the
/// Python orchestrator depends on; they intentionally do not mirror
/// `RaftMetrics` so the JSON output stays stable across openraft versions.
#[derive(Serialize)]
struct NodeStatus {
    id: u64,
    state: String,
    current_term: u64,
    current_leader: Option<u64>,
    last_log_index: Option<u64>,
    last_applied_index: Option<u64>,
    snapshot_index: Option<u64>,
    purged_index: Option<u64>,
    voters: Vec<u64>,
}

impl NodeStatus {
    fn from_metrics(m: &databend_meta_types::raft_types::RaftMetrics) -> Self {
        Self {
            id: m.id,
            state: format!("{:?}", m.state),
            current_term: m.current_term,
            current_leader: m.current_leader,
            last_log_index: m.last_log_index,
            last_applied_index: m.last_applied.as_ref().map(|l| l.index()),
            snapshot_index: m.snapshot.as_ref().map(|l| l.index()),
            purged_index: m.purged.as_ref().map(|l| l.index()),
            voters: m.membership_config.voter_ids().collect(),
        }
    }
}

async fn run_upsert(args: UpsertArgs) -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("WARN")).init();

    let client = MetaGrpcClient::<TokioRuntime>::try_create(
        vec![args.grpc_api_address],
        "root",
        "xxx",
        Some(Duration::from_secs(10)),
        Some(Duration::from_secs(10)),
        None,
        32 * 1024 * 1024,
    )?;

    client
        .upsert_kv(UpsertKV::update(&args.key, args.value.as_bytes()))
        .await?;

    Ok(())
}

async fn run_export(args: ExportArgs) -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("WARN")).init();

    let client = MetaGrpcClient::<TokioRuntime>::try_create(
        vec![args.grpc_api_address],
        "root",
        "xxx",
        Some(Duration::from_secs(10)),
        Some(Duration::from_secs(10)),
        None,
        32 * 1024 * 1024,
    )?;

    let mut established = client.make_established_client().await?;
    let resp = established
        .export_v1(tonic::Request::new(pb::ExportRequest {
            chunk_size: Some(32),
        }))
        .await?;

    let mut stream = resp.into_inner();
    while let Some(chunk_res) = stream.next().await {
        let chunk = chunk_res?;
        for line in chunk.data {
            println!("{}", line);
        }
    }

    Ok(())
}

fn parse_host_port(addr: &str) -> anyhow::Result<(String, u16)> {
    let sa: SocketAddr = addr.parse()?;
    Ok((sa.ip().to_string(), sa.port()))
}

fn init_logging(args: &ServeArgs) {
    let level = args.log_stderr_level.as_deref().unwrap_or("WARN");

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level)).init();
}
