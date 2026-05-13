# Databend Meta Stream Append Benchmark 结果

日期：2026-05-12

## 范围

- Benchmark 脚本：`scripts/benchmark/meta-cluster-bench.py`（位于 Databend 仓库）
- Databend 仓库：本地 checkout
- Databend-meta 仓库：本地 checkout
- Baseline meta 版本：`260428.3.0`
- Patched meta 版本：`260512.0.0`
- Patched commit：`09decba feat: use AppendV002 for stream append`
- Workload RPC：`upsert_kv`
- 每个并发点重复次数：3
- 执行方式：串行执行，没有并发运行多个 benchmark
- 每次运行总操作数：约 `40000`

## Median 结果

| clients | ops/client | total ops | baseline qps | patched qps | qps delta | baseline wall ms | patched wall ms | wall delta |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 | 10000 | 40000 | 172.3 | 168.6 | -2.15% | 232120 | 237274 | +2.22% |
| 16 | 2500 | 40000 | 704.8 | 655.9 | -6.94% | 56750 | 60983 | +7.46% |
| 64 | 625 | 40000 | 2877.5 | 2720.5 | -5.46% | 13901 | 14703 | +5.77% |
| 128 | 313 | 40064 | 5117.4 | 5700.6 | +11.40% | 7829 | 7028 | -10.23% |

## 单次 QPS

| clients | baseline qps values | patched qps values |
|---:|---|---|
| 4 | 172.3, 171.4, 175.1 | 167.4, 169.5, 168.6 |
| 16 | 701.1, 704.8, 710.2 | 675.8, 655.9, 640.2 |
| 64 | 2877.5, 2747.8, 3046.9 | 2773.5, 2720.5, 2714.6 |
| 128 | 4364.7, 6324.2, 5117.4 | 5774.6, 4957.8, 5700.6 |

## 慢请求与慢 IO 信号

| clients | baseline slow-RPC median | patched slow-RPC median | baseline slow-IO total median | patched slow-IO total median |
|---:|---:|---:|---:|---:|
| 4 | 82 | 72 | 354 | 602 |
| 16 | 75 | 81 | 124 | 356 |
| 64 | 66 | 70 | 5 | 0 |
| 128 | 149 | 0 | 25 | 0 |

## AppendV002 使用情况

Patched runs 使用了 `AppendV002`；baseline runs 没有使用。

| clients | baseline append_v002 counts | patched append_v002 counts |
|---:|---|---|
| 4 | 0, 0, 0 | 321, 316, 318 |
| 16 | 0, 0, 0 | 81, 84, 87 |
| 64 | 0, 0, 0 | 15, 16, 16 |
| 128 | 0, 0, 0 | 7, 8, 7 |

## 结论

在这个 benchmark 中，`AppendV002` 在低并发和中等并发下没有提升，反而略慢。

在 `128` clients 下，patched build 的 median throughput 提升 `+11.40%`，wall time 降低 `-10.23%`，slow-RPC median 从 `149` 降到 `0`。

`128` clients 的单次结果波动较大，因此这个高并发结果应视为正向信号，而不是最终性能数字。

## 原始产物

- Raw JSON：`/tmp/databend-meta-stream-append-bench/matrix-total40000/results.json`
- Benchmark runs：`/tmp/databend-meta-stream-append-bench/matrix-total40000`
- Baseline binaries：`/tmp/databend-meta-stream-append-bench/bins/baseline`
- Patched binaries：`/tmp/databend-meta-stream-append-bench/bins/patched`
