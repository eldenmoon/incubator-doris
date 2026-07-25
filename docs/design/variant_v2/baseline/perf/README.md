<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Variant V1/V2 JSONBench baseline

This directory is the reproducible T0.1/T7.1 comparison of JSONB, legacy `ColumnVariant` (V1),
and `ColumnVariantV2`. It uses the public 1M-row NDJSON dataset from
[ClickHouse/JSONBench](https://github.com/ClickHouse/JSONBench). JSONBench does not measure import
time and does not contain a whole-root query, so this harness adds both while retaining its native
path-query style.

## Fixed comparison contract

- Dataset: `file_0001.json.gz`, decompressed once before timing; 1,000,000 NDJSON rows,
  480,778,277 bytes, SHA-256
  `7beb29f6c036fe784754ff34d68d1f216c6cc89de12155da06f725bdf5c8536e`.
  The 10M follow-up streams this file ten times, for 10,000,000 rows and 4,807,782,770 logical
  input bytes, without materializing a second copy.
- Hardware: one FE and one BE on the same host; one engine version runs at a time.
- Builds: `BUILD_TYPE=RELEASE ./build.sh --be`, with no explicit `-j`.
- V1 source: FE and BE at design base `6bf4327a7a58bcff9d2b869869f7ae3067a0f80f`.
- V2 source: FE `99d3dd1ba37`, BE `50077170ee9322a5428bf17cb7b9cd6ae0aa8eff`.
- Tables: identical duplicate-key/random-8-bucket layout, ZSTD, replication 1; the only DDL
  difference is `JSONB` versus `VARIANT`.
- Import: three fresh-table stream loads; report the median wall time and Doris `LoadTimeMs`.
- Query: two unmeasured warmups plus five measured runs; report the median wall time. OS page cache
  is not dropped because this is a shared host, so the result is explicitly a warm-page-cache
  baseline. The current harness disables Doris SQL/query result caches on every MySQL connection.
- Correctness: every final table must contain the expected 1M or 10M rows. Query scalar outputs and
  their SHA-256 values are retained in `raw.tsv` or `raw_10m.tsv`.

The four query cases are:

| Case | Purpose | JSONB expression | Variant expression |
|-|-|-|-|
| `root` | Materialize every complete document | `CAST(data AS STRING)` | `CAST(data AS STRING)` |
| `shallow_path` | Filter on `$.kind` | `get_json_string` | `data['kind']` |
| `deep_path` | Filter on `$.commit.operation` | `get_json_string` | `data['commit']['operation']` |
| `path_group` | JSONBench-style group on `$.commit.collection` | `get_json_string` | nested `element_at` |

## Reproduce

Download and decompress the official 1M dataset, start the target Doris cluster, then run:

```shell
docs/design/variant_v2/baseline/perf/run_jsonbench.sh \
  --label v2_release \
  --type variant \
  --dataset /absolute/path/to/file_0001.json \
  --dataset-repetitions 10 \
  --output /absolute/path/to/results \
  --host 127.0.0.1 \
  --query-port 9030 \
  --http-port 8030
```

Run the same command with `--type jsonb`. For V1, use the Release binary built from the design base
and labels `v1_release`/`v1_jsonb_control`. Each output directory contains `environment.txt`,
`raw.tsv`, and the original stream-load JSON responses.

## Result

Run date: 2026-07-20. Hardware: Intel Xeon Platinum 8457C, 2 sockets, 48 cores/socket,
2 threads/core. The V1 and V2 clusters ran sequentially. Lower latency is better.

### 1M rows

| Case | JSONB on V2 HEAD (ms) | ColumnVariant V1 (ms) | ColumnVariantV2 (ms) | V2 vs V1 | V2 vs JSONB |
|-|-:|-:|-:|-|-|
| import wall time | 2,056.264 | 2,590.206 | 6,528.858 | 2.52x slower | 3.18x slower |
| root materialization | 109.698 | 1,394.290 | 488.120 | 2.86x faster | 4.45x slower |
| shallow path filter | 71.354 | 39.003 | 59.266 | 1.52x slower | 1.20x faster |
| deep path filter | 77.543 | 39.775 | 57.998 | 1.46x slower | 1.34x faster |
| path group | 78.587 | 44.043 | 61.386 | 1.39x slower | 1.28x faster |

The import row is the median of three fresh-table stream-load wall times. Query rows are the median
of five warm measurements. The corresponding median Doris server load times are 2,039 ms for
JSONB, 2,571 ms for V1, and 6,511 ms for V2. `results.tsv` contains both JSONB controls and all
medians; `raw.tsv` retains every measured trial.

All twelve loads reported `Success` and exactly 1,000,000 loaded rows. The shallow-path count
(`994672`), deep-path count (`954611`), and group count (`15`) have identical result hashes across
JSONB, V1, and V2. Root results are length aggregates, not semantic hashes: JSONB, V1, and V2 can
emit different valid JSON key orders or scalar spellings, so their aggregate lengths are expected
to differ slightly.

The V1-build JSONB control is close to the V2-build JSONB control: import differs by 2.4%, root by
4.0%, shallow path by 2.5%, and group by 0.6%; deep path differs by 10.2%. This bounds some of the
shared-host/version noise, but does not explain the 39%--152% V2 regressions against V1.

### 10M rows

| Case | JSONB on V2 HEAD (ms) | ColumnVariant V1 (ms) | ColumnVariantV2 (ms) | V2 vs V1 | V2 vs JSONB |
|-|-:|-:|-:|-|-|
| import wall time | 18,953.119 | 47,732.666 | 67,804.674 | 1.42x slower | 3.58x slower |
| root materialization | 405.551 | 12,952.534 | 4,158.279 | 3.12x faster | 10.25x slower |
| shallow path filter | 282.192 | 66.763 | 200.061 | 3.00x slower | 1.41x faster |
| deep path filter | 319.271 | 67.813 | 195.140 | 2.88x slower | 1.64x faster |
| path group | 313.702 | 70.986 | 224.332 | 3.16x slower | 1.40x faster |

All twelve 10M loads reported `Success` and exactly 10,000,000 loaded rows. Path result hashes are
identical across JSONB, V1, and V2; the shallow and deep counts are exactly ten times the 1M counts,
while the number of groups remains 15. `results_10m.tsv` contains all medians and both JSONB
controls; `raw_10m.tsv` contains the 92 accepted trials.

The first 10M query pass is intentionally excluded: V1 Variant returned the 4.8 GB root aggregate
in about 18 ms, proving that it hit Doris SQL cache while V2 did not. The accepted query pass sets
`enable_sql_cache=false`, `enable_query_cache=false`, and `query_cache_force_refresh=true` on every
connection. With caches disabled, V1 root latency is 12.95 seconds. Import results were never
served by SQL cache and remain from the original three fresh-table loads.

At 10M, the query-side JSONB controls remain close across builds (within 7%), but import controls do
not: V1-build JSONB takes 52.64 seconds versus 18.95 seconds on V2 HEAD. The direct 1.42x V2/V1
Variant import ratio therefore includes unrelated cross-build load-stack improvements and should
not be treated as pure column-format attribution. The within-build signal is unambiguous: V1
Variant is 9% faster than its JSONB control, while V2 is 3.58x slower than its JSONB control.

### 10M optimized follow-up

The profile-guided P0 remediation in the commit containing this section was rerun against the same
10M dataset and table layout with a Release BE. Import remains the median of three fresh loads.
Root is the median of seven runs after two warmups; each path result is the median of 21 runs after
ten warmups. Query caches were disabled, result hashes were checked on every trial, and the final
path run was taken while the host averaged 92% idle CPU.

| Case | JSONB on V2 baseline (ms) | ColumnVariant V1 (ms) | Original V2 (ms) | Optimized V2 (ms) | Optimized V2 vs V1 | Change from original V2 |
|-|-:|-:|-:|-:|-:|-:|
| import wall time | 18,953.119 | 47,732.666 | 67,804.674 | 42,940.792 | 10.04% faster | 36.67% faster |
| root materialization | 405.551 | 12,952.534 | 4,158.279 | 4,035.636 | 68.84% faster | 2.95% faster |
| shallow path filter | 282.192 | 66.763 | 200.061 | 70.078 | 4.97% slower | 64.97% faster |
| deep path filter | 319.271 | 67.813 | 195.140 | 68.183 | 0.55% slower | 65.06% faster |
| path group | 313.702 | 70.986 | 224.332 | 72.556 | 2.21% slower | 67.66% faster |

All optimized loads reported `Success` and exactly 10,000,000 rows. The optimized path results and
SHA-256 values match V1: shallow count `9946720`, deep count `9546110`, and group count `15`.
`results_10m_optimized.tsv` records the medians and hashes. The import trials predate only the final
query-side null-map reuse change; that change is outside the load call path.

### B8 root-output investigation

The B8 follow-up on 2026-07-21 used the same 10M tables and a Release BE. The measured SQL was
`SELECT SUM(LENGTH(CAST(data AS STRING)))`: it forces complete Variant-to-JSON materialization in
the BE without transferring roughly 4.8 GB to the client. It is therefore a reproducible B8-like
serialization proxy, not a substitute for the literal `SELECT v` client/result-sink measurement.
Every connection disabled SQL/query caches. Each accepted batch used two warmups followed by seven
interleaved V2/JSONB runs; `results_10m_b8.tsv` retains the accepted raw wall times.

| Build | V2 median (ms) | JSONB median (ms) | V2 / JSONB | V2 change |
|-|-:|-:|-:|-:|
| before this follow-up | 4,150 | 420 | 9.88x | — |
| optimized batch A | 4,038 | 422 | 9.57x | 2.70% faster |
| optimized final-binary batch | 4,160 | 420 | 9.90x | 0.24% slower |
| optimized combined, 14 runs | 4,094 | 420 | 9.75x | 1.35% faster |

The output aggregates stayed stable at `4797090110` for V2 and `4797090470` for JSONB. Their
difference is expected because valid canonical formatting, key order, and number text can differ.
Two shared-host intervals were excluded before drawing conclusions: one compiler-saturated pass
produced roughly 13-second V2 and 5-second JSONB results, and a later pass produced 17.84-second V2
and 5.22-second JSONB warmups while host idle CPU fell to about 10%. Accepted batches started near
90% idle CPU.

The retained implementation has four changes:

- scan JSON strings a machine word at a time, bulk-copy strings that need no escaping, and skip a
  second UTF-8 validation pass for pure ASCII while preserving control, quote, backslash,
  U+2028/U+2029, invalid UTF-8, and word-boundary behavior;
- return the already-produced nullable string column directly for homogeneous encoded or fallback
  blocks instead of concatenating and permuting a second copy. Mixed encoded/typed blocks retain
  the original ordering path;
- remove the redundant JSON-length traversal before single-row string serialization; callers that
  require all-or-nothing publication, including `serialize_column_to_json`, retain their preflight;
- validate encoded metadata and exact value boundaries once when they enter `ColumnVariantV2`, then
  let `ReadView` and the concrete encoded output SerDe borrow that column invariant. The direct
  single-row output path no longer constructs and validates a generic `ReadInput` per row. Const,
  typed, nullable, and other fallback routes retain the generic path. The MySQL result writer is
  unchanged from the parent commit.

The pre-change CPU sample was dominated by string escaping (`28.55%`), UTF-8 validation (`10.22%`),
and `memcpy` (`9.50%`). After the retained changes, the string family fell from approximately
`38.8%` to `28.1%`; the visible hot functions were string scan (`12.57%`), UTF-8 validation
(`8.43%`), escaped write (`7.12%`), and repeated container/metadata parsing (approximately `32%`).
A third experiment cached container and metadata layouts inside the printer. It reduced the layout
share to approximately `26%`, but its seven-run median regressed to 4,160 ms and its scan profile
regressed from 3.404 s to 3.584 s, so that experiment and its low-level friend coupling were
removed.

The accepted profiles report a V2 scan-operator peak of 32.51 MB versus 32.25 MB for JSONB
(`1.008x`) and an aggregated scanner/local-exchange peak of 260.07 MB versus 310.41 MB (`0.838x`).
Neither satisfies B8's `<=0.7x` memory requirement. The optimized V2 scan/projection time was
3.404 s versus 182.393 ms for JSONB. The final correctness gate was an ASAN targeted union of 81/81
column, exchange, Variant-to-string CAST, output SerDe, and MySQL writer tests. It includes
malformed metadata and trailing-value rejection at the column boundary, legal noncanonical input,
escaping boundaries, batch output, and unchanged writer behavior.

### Literal B8 root output

The formal follow-up used the literal query
`SELECT data FROM variant_v2_jsonbench.<10M-row-table>` through the MySQL text protocol and sent the
roughly 4.49 GB result to `/dev/null`. It therefore includes scan/assembly, complete JSON printing,
result-sink framing, socket transfer, and client consumption. Every connection disabled SQL/query
caches and enabled the server profile. After an unmeasured warmup, accepted runs were interleaved;
`results_10m_b8_literal.tsv` retains accepted and rejected samples with their profile ids and
rejection reasons.

| Build stage | V2 median (s) | JSONB median (s) | V2 / JSONB latency | V2 change |
|-|-:|-:|-:|-:|
| before B8 changes | 44.82 | 25.05 | 1.789x | — |
| single-pass row serialization | 31.63 | 25.41 | 1.245x | 29.43% faster |
| block-shared result serialization (experiment, removed) | 25.11 | 25.12 | 0.9996x | 43.98% faster |
| writerless metadata-only validation (non-formal) | 22.66 | 19.74 | 1.148x | 49.44% faster |
| writerless column invariant (retained) | 20.24 | 19.52 | 1.037x | 54.84% faster |

The final accepted writerless V2 wall times were 20.50, 20.24, and 19.95 seconds; JSONB was 19.01,
19.55, and 19.52 seconds. Every run required at least 80% average host idle CPU both before and
during the query; the accepted pre-run intervals were 84.59%--90.04% idle and the complete query
intervals were 84.24%--87.67% idle. A V2 attempt at 62.24% pre-run idle was rejected without running
the query. Client peak RSS was 7,680 KB in every accepted run. The retained Release binary SHA-256
was `06ebd9d79baf6eb2ff9a959eb3875220a8f0ec5128887c1be747b17b4e9f7bcb`.

The writer-batching result is retained only as experiment history. It was removed after the scope
decision to keep `vmysql_result_writer.cpp` unchanged. A first writerless version that moved only
metadata validation to column ingress produced a directional 1.148x ratio while the shared host
averaged roughly 70% idle; those runs are marked non-formal. Moving exact value-boundary validation
to the same ingress invariant allowed the encoded SerDe to borrow `get_value_ref()` directly and
produced the formal 1.037x result. `results_10m_b8_writerless.tsv` retains the accepted, rejected,
warmup, and non-formal samples with load telemetry and profile ids.

The load-accepted writerless warmup profiles show V2 total time at 20.308 s, result-sink execution
at 19.762 s, `TupleConvertTime` at 17.155 s, and scanner CPU at 37.894 s. The matching JSONB warmup
used 19.486 s total, 14.282 s in the result sink, 13.617 s in tuple conversion, and 9.918 s of
scanner CPU. Literal single-stream wall throughput therefore passes B8 at 1.037x JSONB, but V2
still has substantially less CPU/concurrency headroom.

Moving validation to column ingress does add load work. Three fresh 10M Release imports took
47.278, 45.577, and 46.359 seconds, for a 46.359-second median and 46.344-second median Doris server
time. Each loaded exactly 10,000,000 rows; the accepted pre-run/full-run CPU-idle ranges were
85.22%--88.80% and 84.65%--85.73%, with no active `clang`, `clang++`, `cc1`, `ninja`, or Maven build
sample. This is 7.96% slower than the earlier optimized V2 import (42.941 s), but remains 2.88%
faster than V1 (47.733 s) and 31.63% faster than the original V2 baseline (67.805 s).
`results_10m_b8_writerless_import.tsv` retains accepted and conservatively excluded load samples.

The scan-operator peak remains 32.51 MB for V2 versus 32.25 MB for JSONB (`1.008x`), and its total
output-block bytes are 3.51 GB versus 4.48 GB (`0.783x`). The available profile does not charge the
result sink's temporary text column, so it cannot be used to claim a lower end-to-end peak. The
literal B8 memory requirement (`<=0.7x` JSONB) remains failed and needs a dedicated tracked-memory
run or the original VariantWideBench Test2 dataset before any memory-layout change is justified.

## BE Variant microbenchmarks

The reusable CPU-kernel coverage now lives in `be/benchmark/benchmark_variant.hpp`. The former
disabled timing tests in `variant_json_test.cpp` and `variant_jsonb_test.cpp` were removed from BE
UT: correctness stays in UT, while timing, repetitions, counters, and JSON output are owned by
Google Benchmark.

The suite covers the retained optimizations and the performance problems observed in this
investigation:

| Area | Benchmarks | Regression signal |
|-|-|-|
| migrated MB1 parse | `BM_VariantJson_ParseToV2_Mixed`, `BM_VariantJson_ParseToJsonb_Mixed` | V2 JSON import against a same-corpus JSONB control |
| migrated shared-block JSONB | `BM_VariantJsonbToV2_PerRow`, `BM_VariantJsonbToV2_SharedBlock` | old per-row metadata/builders against shared block encoding |
| V2 import and shredding | `BM_VariantV2_InsertEncodedBlock`, `BM_VariantV2_ImportPathBuilderHomogeneousInt` | ingress validation cost and homogeneous path type-inference fast path |
| JSONB container import | `BM_VariantV2_FlatJsonbCastToVariant`, `BM_VariantV2_ArrayJsonbCastToVariant` | the known `ARRAY<JSONB>` flatten/rebuild hotspot against the same number of flat leaves |
| whole-root output | `BM_VariantV2_RootSerdeRow`, `BM_VariantV2_RootSerdeBatch`, `BM_VariantV2_EncodedRootCastToString` | direct row SerDe, batch SerDe, ASCII/escape/UTF-8 scan, and homogeneous fallback output |
| concrete scalar output | `BM_VariantV2_TypedPathCastToString`, `BM_VariantV2_EncodedScalarCastToString` | typed and concrete encoded CAST fast paths |
| path query | `BM_VariantV2_ExtractPath_Shallow`, `BM_VariantV2_ExtractPath_Deep` | root metadata/container traversal for shallow and deep paths |
| exchange | `BM_VariantV2_ExchangeSerializedSize`, `BM_VariantV2_ExchangeSerialize` | allocation-free encoded frame sizing and full validated serialization |

Build and run from the Doris root:

```shell
BUILD_TYPE=RELEASE ./build.sh --benchmark
be/benchmark/run_variant_benchmark.sh \
  --output-dir /absolute/new/result/directory
```

Performance results are valid only on a relatively idle host. The runner verifies that the CMake
build is `RELEASE` with `BUILD_BENCHMARK=ON`, defaults to five repetitions and 0.5 seconds minimum
time, and rejects both a pre-run sample and a completed run below 80% aggregate CPU idle. It
records the complete command, binary and source SHA-256 values, CPU/kernel/build metadata,
load-average snapshots, one-second CPU-idle samples, and the highest-CPU processes seen before the
run and after it. Rejected runs are retained for diagnosis but must not be used as a baseline.
Timings are observational baselines, not correctness assertions or cross-host thresholds.

Legacy `ColumnVariant` is absent from the current V2 BE binary, so a meaningful V1 microbenchmark
cannot be linked into this executable without restoring the forbidden dual implementation. The
cross-build 10M JSONBench results above remain the V1/V2 import and query baseline; this suite is
the same-build, source-level guard for the hot kernels that produced those end-to-end results.

### 2026-07-23 BE baseline

The first retained microbenchmark baseline used the Release binary with SHA-256
`7e592420daff3fb401adbc5bc6d59ad1728feeb9c9e5cec63fdcfca6a2393e77` on an Intel Xeon Platinum
8457C host with 192 logical CPUs and the `powersave` scaling governor. Each case has five
repetitions and a 0.5-second minimum time. The runner admitted the run at 84.29% idle CPU and
accepted it at 83.29% idle over the complete benchmark interval. Its one-second samples averaged
83.31% idle, with a 70.22% minimum over 68 samples. The load average moved from
`38.88/127.12/169.11` to `30.64/106.22/158.80`; aggregate CPU idle, rather than load average alone,
is the admission metric on this 192-CPU shared host.

The following values are median real time. Rows are the work per benchmark iteration, not Google
Benchmark's adaptive iteration count.

| Area | Case | Rows | Median | CV |
|-|-|-:|-:|-:|
| mixed JSON import | V2 parse | 65,536 | 190.498 ms | 0.59% |
| mixed JSON import | JSONB control | 65,536 | 113.005 ms | 0.29% |
| JSONB-to-V2 import | old per-row builders | 65,536 | 531.246 ms | 9.97% |
| JSONB-to-V2 import | shared block encoder | 65,536 | 397.615 ms | 2.78% |
| encoded ingress | insert encoded block | 4,096 | 67.851 us | 0.74% |
| root output | row SerDe | 4,096 | 3.508 ms | 1.64% |
| root output | batch SerDe | 4,096 | 3.561 ms | 4.62% |
| exchange | serialized size | 4,096 | 57.173 us | 1.29% |
| exchange | serialize | 4,096 | 2.476 ms | 0.55% |
| root output | encoded root CAST | 4,096 | 3.635 ms | 0.92% |
| path query | shallow `kind` | 4,096 | 517.701 us | 1.81% |
| path query | deep `commit.operation` | 4,096 | 733.096 us | 1.43% |
| scalar output | typed path CAST | 4,096 | 0.104 us | 0.22% |
| scalar output | encoded scalar CAST | 4,096 | 586.150 us | 3.32% |
| JSONB-to-V2 import | flat leaves | 8,192 | 5.990 ms | 16.68% |
| JSONB-to-V2 import | 1,024 arrays x 8 leaves | 8,192 leaves | 18.708 ms | 1.05% |
| path builder | homogeneous integers | 4,096 | 403.311 us | 1.43% |

On this exact binary and host, V2 mixed JSON parsing is 1.686x the JSONB control time. Shared-block
JSONB conversion is 1.336x faster than the old per-row builder path. Rebuilding
`ARRAY<JSONB>` into Variant is 3.123x the flat-input time for the same 8,192 JSONB leaves, retaining
that container path as a visible import hotspot. Deep path extraction is 1.416x the shallow path.
Batch root SerDe is within 1.5% of row SerDe, so this run does not show an independent batch
advantage.

The typed-path CAST result measures the retained zero-copy return of an already materialized typed
string column; it must not be compared as JSON rendering throughput against the encoded cases. The
per-row builder and flat JSONB cases each contain one slow sample, which inflates their CV; use
their medians directionally and require another load-accepted run before claiming a small
regression or improvement in either case.

Raw JSON, console output, exact command, binary/source hashes, source status, process snapshots, and
load telemetry are retained in `be_variant_benchmark_20260723/`. `summary.tsv` is the compact
median/CV view; `benchmark.json` is authoritative.

## Gate decision

The original baseline failed the P0 performance target. The retained 10M implementation passes the
scoped requirement that import and each measured path case remain within 10% of V1: the final
writerless import is 2.88% faster than V1, while shallow, deep, and grouped paths are respectively
4.97%, 0.55%, and 2.21% slower. Root materialization remains 3.21x faster than V1.

This does not complete the broader `05-performance.md` gate. The full B1--B8/MB1--MB8 matrix and
remaining work still belongs to T7.1. The aggregation proxy remains 9.75x slower because it does
not traverse the literal single-row output SerDe path. The literal B8 single-stream throughput gate
now passes at 1.037x JSONB with no MySQL writer change, while its `<=0.7x` memory gate and V2 CPU
headroom remain open.
