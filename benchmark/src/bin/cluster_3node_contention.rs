//! 3-node Raft cluster contention benchmark.
//!
//! Step 1 of investigating the AppendEntries delay regression observed in
//! production after upgrading one follower to v1.2.896 (raft-log 0.3.0).
//!
//! This benchmark spins up a complete 3-voter meta-service cluster, all
//! running the current code (raft-log 0.3.0 with the `WriteRequest`-based
//! flush worker and the `save_committed` `log.flush(None)` line), warms up
//! the state machine, and runs concurrent write+read load against the
//! leader for 60s. It reports per-RPC latency percentiles (p50/p90/p99
//! /p99.9) and throughput.
//!
//! Goal: produce a reproducible baseline number that later revert
//! experiments can be compared against:
//!   - Step 1 (this run): all-current code, no fixes applied.
//!   - Step 2: drop `log.flush(None)` from `save_committed`. Re-run.
//!   - Step 3: also revert the write-in-worker WAL change. Re-run.
//!
//! Compare write p50/p99 across the three runs to attribute latency to
//! each commit.
//!
//! Note on disk: `MetaSrvTestContext` uses a temp dir on the developer's
//! local disk. Local NVMe fsync is fast enough (~30-80us) that the 0.3.0
//! architectural change may be hard to see. To better mimic production
//! (EBS-class storage with ~1-3ms fsync baseline), point the temp dir at a
//! slower volume or inject artificial fsync latency before running.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::Operation;
use databend_meta_client::types::UpsertKV;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_test_harness::MetaSrvTestContext;
use databend_meta_test_harness::start_metasrv_with_context;
use rand::Rng;
use rand::SeedableRng;

const NUM_WRITERS: usize = 4;
const NUM_READERS: usize = 16;
const BENCH_DURATION_SECS: u64 = 60;
const WARMUP_KEYS: usize = 1000;
const VALUE_SIZE: usize = 4096;
/// Keep snapshot rotation out of the steady-state window. Production runs
/// with much larger thresholds; the test-context default (1024) would fire
/// every ~4s under our write rate and pollute the latency tail.
const SNAPSHOT_LOGS_SINCE_LAST: u64 = 10_000_000;

fn make_value(seed: u64) -> Vec<u8> {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut buf = vec![0u8; VALUE_SIZE];
    rng.fill(&mut buf[..]);
    buf
}

fn make_key(i: u64) -> String {
    format!("bench/{:08}", i)
}

async fn grpc_client(addr: &str) -> Arc<ClientHandle<TokioRuntime>> {
    MetaGrpcClient::<TokioRuntime>::try_create(
        vec![addr.to_string()],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )
    .expect("create grpc client")
}

async fn warmup(client: &Arc<ClientHandle<TokioRuntime>>, n: usize) {
    eprintln!(
        "Warming up: writing {} keys ({} bytes each)...",
        n, VALUE_SIZE
    );
    for i in 0..n as u64 {
        let key = make_key(i);
        let val = make_value(i);
        client
            .upsert_kv(UpsertKV::new(
                &key,
                MatchSeq::Any,
                Operation::Update(val),
                None,
            ))
            .await
            .expect("warmup upsert");

        if (i + 1) % 200 == 0 {
            eprintln!("  warm-up: {}/{}", i + 1, n);
        }
    }
    eprintln!("Warm-up done.\n");
}

async fn writer_task(
    client: Arc<ClientHandle<TokioRuntime>>,
    stop: Arc<AtomicBool>,
    key_counter: Arc<AtomicU64>,
) -> Vec<Duration> {
    let mut latencies = Vec::new();

    while !stop.load(Ordering::Relaxed) {
        let i = key_counter.fetch_add(1, Ordering::Relaxed);
        let key = make_key(i);
        let val = make_value(i);

        let t = Instant::now();
        let res = client
            .upsert_kv(UpsertKV::new(
                &key,
                MatchSeq::Any,
                Operation::Update(val),
                None,
            ))
            .await;
        let elapsed = t.elapsed();

        if res.is_ok() {
            latencies.push(elapsed);
        }
    }

    latencies
}

async fn reader_task(
    client: Arc<ClientHandle<TokioRuntime>>,
    stop: Arc<AtomicBool>,
    max_key: Arc<AtomicU64>,
) -> Vec<Duration> {
    let mut latencies = Vec::new();
    let mut rng = rand::rngs::SmallRng::from_entropy();

    while !stop.load(Ordering::Relaxed) {
        let hi = max_key.load(Ordering::Relaxed);
        if hi == 0 {
            tokio::task::yield_now().await;
            continue;
        }
        let i = rng.gen_range(0..hi);
        let key = make_key(i);

        let t = Instant::now();
        let res = client.get_kv(&key).await;
        let elapsed = t.elapsed();

        if res.is_ok() {
            latencies.push(elapsed);
        }
    }

    latencies
}

fn report(label: &str, mut latencies: Vec<Duration>, duration: Duration) {
    if latencies.is_empty() {
        eprintln!("{}: no samples", label);
        return;
    }
    latencies.sort();
    let n = latencies.len();
    let p = |pct: f64| -> Duration {
        let idx = ((n as f64) * pct / 100.0).ceil() as usize;
        latencies[idx.min(n - 1)]
    };
    let qps = (n as f64) / duration.as_secs_f64();
    eprintln!(
        "{:>8}: {:>7} ops  {:>7.0}/s  |  p50 {:>9.2?}  p90 {:>9.2?}  p99 {:>9.2?}  p999 {:>9.2?}  max {:>9.2?}",
        label,
        n,
        qps,
        p(50.0),
        p(90.0),
        p(99.0),
        p(99.9),
        latencies[n - 1],
    );
}

/// Build a 3-voter cluster: node 0 bootstraps as single-leader, then nodes
/// 1 and 2 join. After this returns, all three are voters.
async fn build_cluster() -> anyhow::Result<Vec<MetaSrvTestContext<TokioRuntime>>> {
    eprintln!("Starting 3-node cluster (nodes 0, 1, 2)...");

    let mut tc0 = MetaSrvTestContext::<TokioRuntime>::new(0);
    tc0.config.raft_config.single = true;
    tc0.config.raft_config.snapshot_logs_since_last = SNAPSHOT_LOGS_SINCE_LAST;
    start_metasrv_with_context(&mut tc0).await?;

    let leader_raft_addr = tc0
        .config
        .raft_config
        .raft_api_addr::<TokioRuntime>()
        .await?
        .to_string();

    let mut tc1 = MetaSrvTestContext::<TokioRuntime>::new(1);
    tc1.config.raft_config.single = false;
    tc1.config.raft_config.snapshot_logs_since_last = SNAPSHOT_LOGS_SINCE_LAST;
    tc1.config.raft_config.join = vec![leader_raft_addr.clone()];
    start_metasrv_with_context(&mut tc1).await?;

    let mut tc2 = MetaSrvTestContext::<TokioRuntime>::new(2);
    tc2.config.raft_config.single = false;
    tc2.config.raft_config.snapshot_logs_since_last = SNAPSHOT_LOGS_SINCE_LAST;
    tc2.config.raft_config.join = vec![leader_raft_addr];
    start_metasrv_with_context(&mut tc2).await?;

    Ok(vec![tc0, tc1, tc2])
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp_millis()
        .init();

    eprintln!("=== 3-node cluster contention benchmark ===\n");
    eprintln!(
        "Writers: {}, Readers: {}, Duration: {}s, Value size: {}B",
        NUM_WRITERS, NUM_READERS, BENCH_DURATION_SECS, VALUE_SIZE
    );
    eprintln!("snapshot_logs_since_last: {}\n", SNAPSHOT_LOGS_SINCE_LAST);

    let tcs = build_cluster().await?;
    let leader_grpc = tcs[0]
        .config
        .grpc
        .api_address()
        .expect("leader grpc address");

    eprintln!("Cluster up. Leader gRPC: {}", leader_grpc);
    for (i, tc) in tcs.iter().enumerate() {
        let grpc = tc.config.grpc.api_address().unwrap_or_default();
        let raft_port = tc.config.raft_config.raft_api_port;
        eprintln!("  node {}: grpc={} raft_port={}", i, grpc, raft_port);
    }
    eprintln!();

    // All clients hit the leader directly. (Production has clients spread
    // across all 3 via a K8s service; for step-1 reproducibility we keep
    // it deterministic and stress only the leader's path -- which still
    // forces full AE replication to followers, the path we care about.)
    let warmup_client = grpc_client(&leader_grpc).await;
    warmup(&warmup_client, WARMUP_KEYS).await;

    let key_counter = Arc::new(AtomicU64::new(WARMUP_KEYS as u64));
    let stop = Arc::new(AtomicBool::new(false));

    eprintln!("Running benchmark for {}s ...\n", BENCH_DURATION_SECS);
    let bench_start = Instant::now();

    let mut handles = Vec::new();

    for _ in 0..NUM_WRITERS {
        let c = grpc_client(&leader_grpc).await;
        let s = stop.clone();
        let kc = key_counter.clone();
        handles.push(tokio::spawn(async move {
            ("write", writer_task(c, s, kc).await)
        }));
    }

    for _ in 0..NUM_READERS {
        let c = grpc_client(&leader_grpc).await;
        let s = stop.clone();
        let mk = key_counter.clone();
        handles.push(tokio::spawn(async move {
            ("read", reader_task(c, s, mk).await)
        }));
    }

    tokio::time::sleep(Duration::from_secs(BENCH_DURATION_SECS)).await;
    stop.store(true, Ordering::Relaxed);
    let actual_duration = bench_start.elapsed();

    let mut all_writes = Vec::new();
    let mut all_reads = Vec::new();

    for h in handles {
        let (label, lats) = h.await?;
        match label {
            "write" => all_writes.extend(lats),
            "read" => all_reads.extend(lats),
            _ => {}
        }
    }

    eprintln!("=== Results (wall: {:?}) ===\n", actual_duration);
    report("Writes", all_writes, actual_duration);
    report("Reads", all_reads, actual_duration);
    eprintln!();

    // Hold the cluster until reports are emitted; dropping tcs shuts them
    // down via temp-dir cleanup.
    drop(tcs);

    Ok(())
}
