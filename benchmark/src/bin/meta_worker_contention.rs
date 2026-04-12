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
const NUM_READERS: usize = 8;
const BENCH_DURATION_SECS: u64 = 30;
const WARMUP_KEYS: usize = 500;
const VALUE_SIZE: usize = 4096;
const SNAPSHOT_LOGS_SINCE_LAST: u64 = 100;

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

        if (i + 1) % 100 == 0 {
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

fn report(label: &str, mut latencies: Vec<Duration>) {
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
    eprintln!(
        "{:>8}: {:>6} ops  |  p50 {:>8.2?}  p90 {:>8.2?}  p99 {:>8.2?}  p999 {:>8.2?}  max {:>8.2?}",
        label,
        n,
        p(50.0),
        p(90.0),
        p(99.0),
        p(99.9),
        latencies[n - 1],
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp_millis()
        .init();

    eprintln!("=== MetaWorker contention benchmark ===\n");
    eprintln!(
        "Writers: {}, Readers: {}, Duration: {}s, Value size: {}B",
        NUM_WRITERS, NUM_READERS, BENCH_DURATION_SECS, VALUE_SIZE
    );
    eprintln!("snapshot_logs_since_last: {}\n", SNAPSHOT_LOGS_SINCE_LAST);

    let mut tc = MetaSrvTestContext::<TokioRuntime>::new(0);
    tc.config.raft_config.snapshot_logs_since_last = SNAPSHOT_LOGS_SINCE_LAST;
    tc.config.raft_config.max_applied_log_to_keep = 0;
    tc.config.raft_config.single = true;

    start_metasrv_with_context(&mut tc).await?;

    let addr = tc.config.grpc.api_address().expect("grpc address");

    eprintln!("Meta-service started at {}\n", addr);

    let client = grpc_client(&addr).await;

    warmup(&client, WARMUP_KEYS).await;

    // key_counter starts after warmup range so writers create new keys
    let key_counter = Arc::new(AtomicU64::new(WARMUP_KEYS as u64));
    let stop = Arc::new(AtomicBool::new(false));

    eprintln!("Running benchmark for {}s ...\n", BENCH_DURATION_SECS);

    let mut handles = Vec::new();

    for _ in 0..NUM_WRITERS {
        let c = grpc_client(&addr).await;
        let s = stop.clone();
        let kc = key_counter.clone();
        handles.push(tokio::spawn(async move {
            ("write", writer_task(c, s, kc).await)
        }));
    }

    for _ in 0..NUM_READERS {
        let c = grpc_client(&addr).await;
        let s = stop.clone();
        let mk = key_counter.clone();
        handles.push(tokio::spawn(async move {
            ("read", reader_task(c, s, mk).await)
        }));
    }

    tokio::time::sleep(Duration::from_secs(BENCH_DURATION_SECS)).await;
    stop.store(true, Ordering::Relaxed);

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

    eprintln!("=== Results ===\n");
    report("Writes", all_writes);
    report("Reads", all_reads);
    eprintln!();

    Ok(())
}
