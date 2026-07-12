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

//! A structured, typed view over the whole meta-service metric registry.
//!
//! `prometheus-client` metrics are write-only: `Counter`/`Gauge`/`Histogram`
//! (and labeled `Family`s of them) expose no way to read their current value
//! back except through an encoder. So this typed snapshot is populated by
//! encoding the registry to the OpenMetrics protobuf [`MetricSet`] and mapping
//! every metric family, by name, into a named field. It is the programmatic
//! counterpart of the Prometheus text exposition: a caller reads
//! `metrics.server.current_term` instead of grepping a metric-name string.
//!
//! Because the mapping is by metric name, a field is only populated while a
//! metric of the matching name is registered; adding a new metric to
//! [`crate::metrics`] means adding a field here.

use std::collections::BTreeMap;
use std::collections::HashMap;

use prometheus_client::encoding::protobuf::openmetrics_data_model as om;

/// Typed snapshot of every metric in the meta-service registry.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct MetaMetrics {
    pub server: ServerMetrics,
    pub raft_network: RaftNetworkMetrics,
    pub raft_storage: RaftStorageMetrics,
    pub meta_network: MetaNetworkMetrics,
}

/// `metasrv_server_*` metrics: node role, raft progress and local storage.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct ServerMetrics {
    pub current_leader_id: i64,
    pub is_leader: bool,
    pub node_is_health: bool,
    pub leader_changes: u64,
    pub applying_snapshot: i64,
    pub snapshot_key_count: i64,
    pub snapshot_primary_index_count: i64,
    pub snapshot_expire_index_count: i64,
    pub snapshot_block_count: i64,
    pub snapshot_data_size: i64,
    pub snapshot_index_size: i64,
    pub snapshot_avg_block_size: i64,
    pub snapshot_avg_keys_per_block: i64,
    pub snapshot_read_block: i64,
    pub snapshot_read_block_from_cache: i64,
    pub snapshot_read_block_from_disk: i64,
    pub raft_log_cache_items: i64,
    pub raft_log_cache_used_size: i64,
    pub raft_log_wal_open_chunk_size: i64,
    pub raft_log_wal_offset: i64,
    pub raft_log_wal_closed_chunk_count: i64,
    pub raft_log_wal_closed_chunk_total_size: i64,
    pub raft_log_size: i64,
    pub proposals_applied: i64,
    pub last_log_index: i64,
    pub last_seq: i64,
    pub current_term: i64,
    pub proposals_pending: i64,
    pub proposals_failed: u64,
    pub read_failed: u64,
    pub watchers: i64,
    /// Build version, keyed by the joined label set (e.g. `component=metasrv,semver=v1.2`).
    pub version: BTreeMap<String, i64>,
}

/// `metasrv_raft_network_*` metrics. Every metric is per-peer; each map is
/// keyed by the joined label set (e.g. `to=2` or `id=2,addr=1.2.3.4:9191`).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct RaftNetworkMetrics {
    pub active_peers: BTreeMap<String, i64>,
    pub fail_connect_to_peer: BTreeMap<String, i64>,
    pub sent_bytes: BTreeMap<String, u64>,
    pub recv_bytes: BTreeMap<String, u64>,
    pub sent_failures: BTreeMap<String, u64>,
    pub append_sent_seconds: BTreeMap<String, HistogramMetrics>,
    pub snapshot_send_success: BTreeMap<String, u64>,
    pub snapshot_send_failure: BTreeMap<String, u64>,
    pub snapshot_send_inflights: BTreeMap<String, i64>,
    pub snapshot_recv_inflights: BTreeMap<String, i64>,
    pub snapshot_sent_seconds: BTreeMap<String, HistogramMetrics>,
    pub snapshot_recv_seconds: BTreeMap<String, HistogramMetrics>,
    pub snapshot_recv_success: BTreeMap<String, u64>,
    pub snapshot_recv_failures: BTreeMap<String, u64>,
}

/// `metasrv_raft_storage_*` metrics.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct RaftStorageMetrics {
    /// Keyed by the failing function name (`func=...`).
    pub raft_store_write_failed: BTreeMap<String, u64>,
    /// Keyed by the failing function name (`func=...`).
    pub raft_store_read_failed: BTreeMap<String, u64>,
    pub snapshot_building: i64,
    pub snapshot_written_entries: u64,
}

/// `metasrv_meta_network_*` metrics: the client-facing RPC service.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct MetaNetworkMetrics {
    pub rpc_delay_ms: HistogramMetrics,
    pub rpc_delay_read_ms: HistogramMetrics,
    pub rpc_delay_write_ms: HistogramMetrics,
    pub sent_bytes: u64,
    pub recv_bytes: u64,
    pub req_inflights: i64,
    pub req_success: u64,
    pub req_failed: u64,
    pub watch_initialization: u64,
    pub watch_change: u64,
    pub stream_get_item_sent: u64,
    pub stream_mget_item_sent: u64,
    pub stream_list_item_sent: u64,
}

/// A summary of a histogram: total `count`/`sum` plus a few percentiles
/// (in the histogram's own unit) derived from its buckets.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct HistogramMetrics {
    pub count: u64,
    pub sum: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

impl MetaMetrics {
    /// Map an OpenMetrics protobuf [`MetricSet`] (the encoded registry) into the
    /// typed snapshot. Missing metrics leave their field at the default (0 / empty).
    pub fn from_metric_set(set: &om::MetricSet) -> Self {
        let fams: HashMap<&str, &om::MetricFamily> = set
            .metric_families
            .iter()
            .map(|f| (f.name.as_str(), f))
            .collect();

        MetaMetrics {
            server: ServerMetrics {
                current_leader_id: gauge(&fams, "metasrv_server_current_leader_id"),
                is_leader: gauge(&fams, "metasrv_server_is_leader") != 0,
                node_is_health: gauge(&fams, "metasrv_server_node_is_health") != 0,
                leader_changes: counter(&fams, "metasrv_server_leader_changes"),
                applying_snapshot: gauge(&fams, "metasrv_server_applying_snapshot"),
                snapshot_key_count: gauge(&fams, "metasrv_server_snapshot_key_count"),
                snapshot_primary_index_count: gauge(
                    &fams,
                    "metasrv_server_snapshot_primary_index_count",
                ),
                snapshot_expire_index_count: gauge(
                    &fams,
                    "metasrv_server_snapshot_expire_index_count",
                ),
                snapshot_block_count: gauge(&fams, "metasrv_server_snapshot_block_count"),
                snapshot_data_size: gauge(&fams, "metasrv_server_snapshot_data_size"),
                snapshot_index_size: gauge(&fams, "metasrv_server_snapshot_index_size"),
                snapshot_avg_block_size: gauge(&fams, "metasrv_server_snapshot_avg_block_size"),
                snapshot_avg_keys_per_block: gauge(
                    &fams,
                    "metasrv_server_snapshot_avg_keys_per_block",
                ),
                snapshot_read_block: gauge(&fams, "metasrv_server_snapshot_read_block"),
                snapshot_read_block_from_cache: gauge(
                    &fams,
                    "metasrv_server_snapshot_read_block_from_cache",
                ),
                snapshot_read_block_from_disk: gauge(
                    &fams,
                    "metasrv_server_snapshot_read_block_from_disk",
                ),
                raft_log_cache_items: gauge(&fams, "metasrv_server_raft_log_cache_items"),
                raft_log_cache_used_size: gauge(&fams, "metasrv_server_raft_log_cache_used_size"),
                raft_log_wal_open_chunk_size: gauge(
                    &fams,
                    "metasrv_server_raft_log_wal_open_chunk_size",
                ),
                raft_log_wal_offset: gauge(&fams, "metasrv_server_raft_log_wal_offset"),
                raft_log_wal_closed_chunk_count: gauge(
                    &fams,
                    "metasrv_server_raft_log_wal_closed_chunk_count",
                ),
                raft_log_wal_closed_chunk_total_size: gauge(
                    &fams,
                    "metasrv_server_raft_log_wal_closed_chunk_total_size",
                ),
                raft_log_size: gauge(&fams, "metasrv_server_raft_log_size"),
                proposals_applied: gauge(&fams, "metasrv_server_proposals_applied"),
                last_log_index: gauge(&fams, "metasrv_server_last_log_index"),
                last_seq: gauge(&fams, "metasrv_server_last_seq"),
                current_term: gauge(&fams, "metasrv_server_current_term"),
                proposals_pending: gauge(&fams, "metasrv_server_proposals_pending"),
                proposals_failed: counter(&fams, "metasrv_server_proposals_failed"),
                read_failed: counter(&fams, "metasrv_server_read_failed"),
                watchers: gauge(&fams, "metasrv_server_watchers"),
                version: labeled_gauge(&fams, "metasrv_server_version"),
            },
            raft_network: RaftNetworkMetrics {
                active_peers: labeled_gauge(&fams, "metasrv_raft_network_active_peers"),
                fail_connect_to_peer: labeled_gauge(
                    &fams,
                    "metasrv_raft_network_fail_connect_to_peer",
                ),
                sent_bytes: labeled_counter(&fams, "metasrv_raft_network_sent_bytes"),
                recv_bytes: labeled_counter(&fams, "metasrv_raft_network_recv_bytes"),
                sent_failures: labeled_counter(&fams, "metasrv_raft_network_sent_failures"),
                append_sent_seconds: labeled_histogram(
                    &fams,
                    "metasrv_raft_network_append_sent_seconds",
                ),
                snapshot_send_success: labeled_counter(
                    &fams,
                    "metasrv_raft_network_snapshot_send_success",
                ),
                snapshot_send_failure: labeled_counter(
                    &fams,
                    "metasrv_raft_network_snapshot_send_failure",
                ),
                snapshot_send_inflights: labeled_gauge(
                    &fams,
                    "metasrv_raft_network_snapshot_send_inflights",
                ),
                snapshot_recv_inflights: labeled_gauge(
                    &fams,
                    "metasrv_raft_network_snapshot_recv_inflights",
                ),
                snapshot_sent_seconds: labeled_histogram(
                    &fams,
                    "metasrv_raft_network_snapshot_sent_seconds",
                ),
                snapshot_recv_seconds: labeled_histogram(
                    &fams,
                    "metasrv_raft_network_snapshot_recv_seconds",
                ),
                snapshot_recv_success: labeled_counter(
                    &fams,
                    "metasrv_raft_network_snapshot_recv_success",
                ),
                snapshot_recv_failures: labeled_counter(
                    &fams,
                    "metasrv_raft_network_snapshot_recv_failures",
                ),
            },
            raft_storage: RaftStorageMetrics {
                raft_store_write_failed: labeled_counter(
                    &fams,
                    "metasrv_raft_storage_raft_store_write_failed",
                ),
                raft_store_read_failed: labeled_counter(
                    &fams,
                    "metasrv_raft_storage_raft_store_read_failed",
                ),
                snapshot_building: gauge(&fams, "metasrv_raft_storage_snapshot_building"),
                snapshot_written_entries: counter(
                    &fams,
                    "metasrv_raft_storage_snapshot_written_entries",
                ),
            },
            meta_network: MetaNetworkMetrics {
                rpc_delay_ms: histogram(&fams, "metasrv_meta_network_rpc_delay_ms"),
                rpc_delay_read_ms: histogram(&fams, "metasrv_meta_network_rpc_delay_read_ms"),
                rpc_delay_write_ms: histogram(&fams, "metasrv_meta_network_rpc_delay_write_ms"),
                sent_bytes: counter(&fams, "metasrv_meta_network_sent_bytes"),
                recv_bytes: counter(&fams, "metasrv_meta_network_recv_bytes"),
                req_inflights: gauge(&fams, "metasrv_meta_network_req_inflights"),
                req_success: counter(&fams, "metasrv_meta_network_req_success"),
                req_failed: counter(&fams, "metasrv_meta_network_req_failed"),
                watch_initialization: counter(&fams, "metasrv_meta_network_watch_initialization"),
                watch_change: counter(&fams, "metasrv_meta_network_watch_change"),
                stream_get_item_sent: counter(&fams, "metasrv_meta_network_stream_get_item_sent"),
                stream_mget_item_sent: counter(&fams, "metasrv_meta_network_stream_mget_item_sent"),
                stream_list_item_sent: counter(&fams, "metasrv_meta_network_stream_list_item_sent"),
            },
        }
    }
}

type Families<'a> = HashMap<&'a str, &'a om::MetricFamily>;

fn metric_value(m: &om::Metric) -> Option<&om::metric_point::Value> {
    m.metric_points.first()?.value.as_ref()
}

fn as_gauge(m: &om::Metric) -> i64 {
    match metric_value(m) {
        Some(om::metric_point::Value::GaugeValue(g)) => match &g.value {
            Some(om::gauge_value::Value::IntValue(v)) => *v,
            Some(om::gauge_value::Value::DoubleValue(v)) => *v as i64,
            None => 0,
        },
        _ => 0,
    }
}

fn as_counter(m: &om::Metric) -> u64 {
    match metric_value(m) {
        Some(om::metric_point::Value::CounterValue(c)) => match &c.total {
            Some(om::counter_value::Total::IntValue(v)) => *v,
            Some(om::counter_value::Total::DoubleValue(v)) => *v as u64,
            None => 0,
        },
        _ => 0,
    }
}

fn as_histogram(m: &om::Metric) -> HistogramMetrics {
    let Some(om::metric_point::Value::HistogramValue(h)) = metric_value(m) else {
        return HistogramMetrics::default();
    };

    let sum = match &h.sum {
        Some(om::histogram_value::Sum::DoubleValue(v)) => *v,
        Some(om::histogram_value::Sum::IntValue(v)) => *v as f64,
        None => 0.0,
    };

    HistogramMetrics {
        count: h.count,
        sum: if sum.is_finite() { sum } else { 0.0 },
        p50: percentile(h.count, &h.buckets, 0.50),
        p90: percentile(h.count, &h.buckets, 0.90),
        p99: percentile(h.count, &h.buckets, 0.99),
    }
}

/// The `q`-quantile upper bound, derived from per-bucket counts.
///
/// `prometheus-client` stores non-cumulative bucket counts and a trailing
/// `f64::MAX` catch-all bucket; this accumulates the counts and never returns
/// the sentinel bound (which would not be JSON-serializable), falling back to
/// the largest finite upper bound instead.
fn percentile(count: u64, buckets: &[om::histogram_value::Bucket], q: f64) -> f64 {
    if count == 0 {
        return 0.0;
    }

    let threshold = q * count as f64;
    let mut cumulative = 0u64;
    let mut last_finite = 0.0;

    for b in buckets {
        let finite = b.upper_bound.is_finite() && b.upper_bound < f64::MAX;
        if finite {
            last_finite = b.upper_bound;
        }
        cumulative += b.count;
        if cumulative as f64 >= threshold {
            return if finite { b.upper_bound } else { last_finite };
        }
    }

    last_finite
}

/// The joined label set, e.g. `to=2` or `id=2,addr=1.2.3.4:9191`.
fn label_key(m: &om::Metric) -> String {
    m.labels
        .iter()
        .map(|l| format!("{}={}", l.name, l.value))
        .collect::<Vec<_>>()
        .join(",")
}

fn gauge(fams: &Families, name: &str) -> i64 {
    fams.get(name)
        .and_then(|f| f.metrics.first())
        .map(as_gauge)
        .unwrap_or(0)
}

fn counter(fams: &Families, name: &str) -> u64 {
    fams.get(name)
        .and_then(|f| f.metrics.first())
        .map(as_counter)
        .unwrap_or(0)
}

fn histogram(fams: &Families, name: &str) -> HistogramMetrics {
    fams.get(name)
        .and_then(|f| f.metrics.first())
        .map(as_histogram)
        .unwrap_or_default()
}

fn labeled_gauge(fams: &Families, name: &str) -> BTreeMap<String, i64> {
    labeled(fams, name, as_gauge)
}

fn labeled_counter(fams: &Families, name: &str) -> BTreeMap<String, u64> {
    labeled(fams, name, as_counter)
}

fn labeled_histogram(fams: &Families, name: &str) -> BTreeMap<String, HistogramMetrics> {
    labeled(fams, name, as_histogram)
}

fn labeled<V>(fams: &Families, name: &str, extract: fn(&om::Metric) -> V) -> BTreeMap<String, V> {
    match fams.get(name) {
        None => BTreeMap::new(),
        Some(f) => f
            .metrics
            .iter()
            .map(|m| (label_key(m), extract(m)))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(name: &str, value: &str) -> om::Label {
        om::Label {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn point(value: om::metric_point::Value) -> om::Metric {
        om::Metric {
            labels: vec![],
            metric_points: vec![om::MetricPoint {
                value: Some(value),
                ..Default::default()
            }],
        }
    }

    fn family(name: &str, metrics: Vec<om::Metric>) -> om::MetricFamily {
        om::MetricFamily {
            name: name.to_string(),
            r#type: 0,
            unit: String::new(),
            help: String::new(),
            metrics,
        }
    }

    fn gauge_point(v: i64) -> om::metric_point::Value {
        om::metric_point::Value::GaugeValue(om::GaugeValue {
            value: Some(om::gauge_value::Value::IntValue(v)),
        })
    }

    fn counter_point(v: u64) -> om::metric_point::Value {
        om::metric_point::Value::CounterValue(om::CounterValue {
            total: Some(om::counter_value::Total::IntValue(v)),
            ..Default::default()
        })
    }

    fn histogram_point(sum: f64, buckets: &[(f64, u64)]) -> om::metric_point::Value {
        let count = buckets.iter().map(|(_, c)| c).sum();
        om::metric_point::Value::HistogramValue(om::HistogramValue {
            sum: Some(om::histogram_value::Sum::DoubleValue(sum)),
            count,
            created: None,
            buckets: buckets
                .iter()
                .map(|(upper_bound, count)| om::histogram_value::Bucket {
                    upper_bound: *upper_bound,
                    count: *count,
                    exemplar: None,
                })
                .collect(),
        })
    }

    #[test]
    fn test_from_metric_set_scalars_labels_and_histogram() {
        let mut sent = point(counter_point(100));
        sent.labels = vec![label("to", "2")];
        let mut sent2 = point(counter_point(200));
        sent2.labels = vec![label("to", "3")];

        let set = om::MetricSet {
            metric_families: vec![
                family("metasrv_server_current_term", vec![point(gauge_point(7))]),
                family("metasrv_server_is_leader", vec![point(gauge_point(1))]),
                family("metasrv_server_leader_changes", vec![point(counter_point(
                    3,
                ))]),
                family("metasrv_raft_network_sent_bytes", vec![sent, sent2]),
                family(
                    "metasrv_meta_network_rpc_delay_ms",
                    // 10 observations: 5 in <=1.0, 4 in <=2.0, 1 in the +Inf catch-all.
                    vec![point(histogram_point(23.0, &[
                        (1.0, 5),
                        (2.0, 4),
                        (f64::MAX, 1),
                    ]))],
                ),
            ],
        };

        let m = MetaMetrics::from_metric_set(&set);

        assert_eq!(m.server.current_term, 7);
        assert!(m.server.is_leader);
        assert!(!m.server.node_is_health); // absent -> default false
        assert_eq!(m.server.leader_changes, 3);

        assert_eq!(m.raft_network.sent_bytes.get("to=2"), Some(&100));
        assert_eq!(m.raft_network.sent_bytes.get("to=3"), Some(&200));

        let h = &m.meta_network.rpc_delay_ms;
        assert_eq!(h.count, 10);
        assert_eq!(h.sum, 23.0);
        assert_eq!(h.p50, 1.0); // rank 5.0 reached at the <=1.0 bucket
        assert_eq!(h.p90, 2.0); // rank 9.0 reached at the <=2.0 bucket
        assert_eq!(h.p99, 2.0); // rank 9.9 falls in +Inf -> last finite bound

        // The whole snapshot must be JSON-serializable (no NaN/Inf leaking in).
        assert!(serde_json::to_string(&m).is_ok());
    }

    /// End-to-end test of what `MetaNode::get_metrics()` evaluates: drive real
    /// metrics into the global registry, then encode + map it. This guards the
    /// hard-coded metric names against drift from [`crate::metrics`], which a
    /// hand-built `MetricSet` cannot catch.
    #[test]
    fn test_get_metrics_maps_live_registry() {
        use std::time::Duration;

        use crate::metrics::meta_metrics_to_metric_set;
        use crate::metrics::network_metrics;
        use crate::metrics::raft_metrics;
        use crate::metrics::server_metrics;

        let peer: databend_meta_types::raft_types::NodeId = 424242;

        // Exercise every extraction path against real metric families:
        // plain gauge, bool gauge, single-label counter, multi-label gauge, histogram.
        server_metrics::set_current_term(4242);
        server_metrics::set_last_log_index(7777);
        server_metrics::set_last_seq(555);
        server_metrics::set_is_leader(true);
        raft_metrics::network::incr_sendto_bytes(&peer, 512);
        raft_metrics::network::incr_active_peers(&peer, "127.0.0.1:29003", 1);
        network_metrics::sample_rpc_read_delay(Duration::from_millis(5));

        // Exactly the expression behind `MetaNode::get_metrics()`.
        let m = MetaMetrics::from_metric_set(&meta_metrics_to_metric_set());

        // Gauges are absolute and only `report_metrics_loop` else writes them
        // (it does not run under `cargo test --lib`), so these are exact.
        assert_eq!(m.server.current_term, 4242);
        assert_eq!(m.server.last_log_index, 7777);
        assert_eq!(m.server.last_seq, 555);
        assert!(m.server.is_leader);

        // A distinct peer id makes the labeled entries unambiguous.
        assert_eq!(m.raft_network.sent_bytes.get("to=424242"), Some(&512));
        let active = m
            .raft_network
            .active_peers
            .iter()
            .find(|(k, _)| k.contains("id=424242"));
        assert_eq!(active.map(|(_, v)| *v), Some(1));

        // Histograms accumulate across the process, so assert presence.
        assert!(m.meta_network.rpc_delay_ms.count >= 1);
        assert!(m.meta_network.rpc_delay_read_ms.count >= 1);

        // A live snapshot must still serialize (real histogram percentiles are finite).
        assert!(serde_json::to_string(&m).is_ok());
    }

    /// Completeness guard against silent information loss.
    ///
    /// `from_metric_set` maps families by hard-coded name, so a metric registered
    /// in [`crate::metrics`] but not wired into `MetaMetrics` would be silently
    /// dropped from `get_metrics()` — losing information the Prometheus-string
    /// exposition still carries. This pins the whole registry inventory: adding,
    /// removing, or renaming a metric fails here until `from_metric_set` and this
    /// list are updated together. Names in `registered` only mean an unmapped
    /// metric (information dropped); names in `mapped` only mean a dead mapping.
    #[test]
    fn test_every_registry_metric_is_mapped() {
        use std::collections::BTreeSet;

        use crate::metrics::meta_metrics_to_metric_set;
        use crate::metrics::network_metrics;
        use crate::metrics::raft_metrics;
        use crate::metrics::server_metrics;

        // Families register lazily, one `LazyLock` per group; touch one metric in
        // each group to force every family to register. These specific calls are
        // chosen not to perturb any value `test_get_metrics_maps_live_registry`
        // asserts under parallel execution (the registry is process-global): the
        // server op is `inc_by(0)`, the peer id is private to this test, and the
        // storage/meta counters bumped here are asserted by no other test.
        let peer: databend_meta_types::raft_types::NodeId = 1;
        server_metrics::incr_proposals_pending(0);
        raft_metrics::network::incr_sendto_bytes(&peer, 0);
        raft_metrics::storage::incr_snapshot_written_entries();
        network_metrics::incr_recv_bytes(0);

        let set = meta_metrics_to_metric_set();
        let registered: BTreeSet<&str> = set
            .metric_families
            .iter()
            .map(|f| f.name.as_str())
            .collect();

        let mapped: BTreeSet<&str> = [
            // server_metrics
            "metasrv_server_applying_snapshot",
            "metasrv_server_current_leader_id",
            "metasrv_server_current_term",
            "metasrv_server_is_leader",
            "metasrv_server_last_log_index",
            "metasrv_server_last_seq",
            "metasrv_server_leader_changes",
            "metasrv_server_node_is_health",
            "metasrv_server_proposals_applied",
            "metasrv_server_proposals_failed",
            "metasrv_server_proposals_pending",
            "metasrv_server_raft_log_cache_items",
            "metasrv_server_raft_log_cache_used_size",
            "metasrv_server_raft_log_size",
            "metasrv_server_raft_log_wal_closed_chunk_count",
            "metasrv_server_raft_log_wal_closed_chunk_total_size",
            "metasrv_server_raft_log_wal_offset",
            "metasrv_server_raft_log_wal_open_chunk_size",
            "metasrv_server_read_failed",
            "metasrv_server_snapshot_avg_block_size",
            "metasrv_server_snapshot_avg_keys_per_block",
            "metasrv_server_snapshot_block_count",
            "metasrv_server_snapshot_data_size",
            "metasrv_server_snapshot_expire_index_count",
            "metasrv_server_snapshot_index_size",
            "metasrv_server_snapshot_key_count",
            "metasrv_server_snapshot_primary_index_count",
            "metasrv_server_snapshot_read_block",
            "metasrv_server_snapshot_read_block_from_cache",
            "metasrv_server_snapshot_read_block_from_disk",
            "metasrv_server_version",
            "metasrv_server_watchers",
            // raft_metrics::network
            "metasrv_raft_network_active_peers",
            "metasrv_raft_network_append_sent_seconds",
            "metasrv_raft_network_fail_connect_to_peer",
            "metasrv_raft_network_recv_bytes",
            "metasrv_raft_network_sent_bytes",
            "metasrv_raft_network_sent_failures",
            "metasrv_raft_network_snapshot_recv_failures",
            "metasrv_raft_network_snapshot_recv_inflights",
            "metasrv_raft_network_snapshot_recv_seconds",
            "metasrv_raft_network_snapshot_recv_success",
            "metasrv_raft_network_snapshot_send_failure",
            "metasrv_raft_network_snapshot_send_inflights",
            "metasrv_raft_network_snapshot_send_success",
            "metasrv_raft_network_snapshot_sent_seconds",
            // raft_metrics::storage
            "metasrv_raft_storage_raft_store_read_failed",
            "metasrv_raft_storage_raft_store_write_failed",
            "metasrv_raft_storage_snapshot_building",
            "metasrv_raft_storage_snapshot_written_entries",
            // network_metrics
            "metasrv_meta_network_recv_bytes",
            "metasrv_meta_network_req_failed",
            "metasrv_meta_network_req_inflights",
            "metasrv_meta_network_req_success",
            "metasrv_meta_network_rpc_delay_ms",
            "metasrv_meta_network_rpc_delay_read_ms",
            "metasrv_meta_network_rpc_delay_write_ms",
            "metasrv_meta_network_sent_bytes",
            "metasrv_meta_network_stream_get_item_sent",
            "metasrv_meta_network_stream_list_item_sent",
            "metasrv_meta_network_stream_mget_item_sent",
            "metasrv_meta_network_watch_change",
            "metasrv_meta_network_watch_initialization",
        ]
        .into_iter()
        .collect();

        assert_eq!(registered, mapped);
    }
}
