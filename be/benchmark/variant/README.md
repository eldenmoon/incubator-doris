# Variant benchmarks

Google Benchmark suites here include:

- `BM_VariantCumulativeCompaction`: a source-version-neutral, high-level cumulative compaction
  workload. It writes 10 overlapping rowsets of synthetic Variant rows, runs a real
  `Compaction`/`VerticalSegmentWriter` merge over them, and validates the merged output's semantics
  and physical layout (materialized/sparse/doc-bucket subcolumn counts) before reporting timing.
  It has no Variant-version selector: the same workload should be comparable commit-to-commit as
  the underlying storage format evolves.
- `BM_VariantSparseImport`: an import workload for the segment writer's Variant shredder, which the
  compaction workload does not reach because it only writes flat BIGINT objects. Each row carries a
  Poisson(20) subset (clipped to [5, 60]) of 2,000 possible keys, with a key's value kind fixed by
  its index (50% BIGINT, 25% short strings, 10% doubles, 5% booleans, 5% BIGINT arrays of 1-4
  elements, 5% `{"x": BIGINT, "y": string}` objects) — matching the dataset used for end-to-end JSON
  and Parquet VARIANT import measurements. Input generation and `parse_to_variant` are untimed; the
  timed region is `VerticalSegmentWriter` init/append/finalize, which shreds every value through the
  Variant path builders.

- `BM_VariantEncodedAppend` and `BM_VariantEncodedBatch`: encoded storage microbenchmarks with
  128 B, 1 KiB, 64 KiB and 1 MiB string payloads, each totalling 64 MiB. Append repeats a one-row
  source through the same column range-insertion interface used by join build-block merges; Batch
  includes scalar collection, encoding and insertion into the destination column. Compare identical
  cases and Release build settings across commits. `allocated_bytes` reports the final append
  destination's retained capacity, not process peak memory or total query memory.
- `BM_VariantEncodedCapacity`: one 4,096-row append with 1 MiB payloads. Run this separately to check
  the real 4 GiB boundary and measure process peak RSS. It requires several GiB of available memory;
  a failure on the old implementation is an expected capacity result, not a timing sample.

The files are:

- `benchmark_variant_binary.hpp`: encoded append, batch and capacity microbenchmarks.
- `benchmark_variant_segment.hpp`: both workloads' scenario definitions, synthetic data generation,
  correctness checks, and Google Benchmark registration.
- `run_variant_segment_benchmark.sh`: wraps a benchmark run with an `environment.txt` snapshot
  (git commit, dirty diff, thirdparty fingerprints, host/CPU/memory info) so results can be tied to
  the exact code and machine that produced them, plus JSON output for later parsing.
- `README.md`: this file.
- `BASELINE.md`: a recorded baseline run with reproduction steps, for commit-to-commit comparison.

## Build

Performance results must come from a Release build:

```shell
BUILD_TYPE=RELEASE ./build.sh --be --benchmark
```

This installs `be/output/lib/benchmark_test`.

## List and run cases

```shell
be/output/lib/benchmark_test --benchmark_list_tests | grep '^BM_Variant'
```

`BM_VariantCumulativeCompaction` registers 7 scenarios x 5 samples = 35 cases:

| Scenario | Layout | Notes |
|---|---|---|
| `Sparse16/FlatPhysicalColumns` | 16-bucket sparse | subcolumns compacted, flat workload |
| `Sparse16/MixedPhysicalPlacement` | 16-bucket sparse | subcolumns compacted, mixed physical placement workload |
| `Sparse16/WholeVariant` | 16-bucket sparse | whole-Variant column compacted (no subcolumn compaction) |
| `Full/WholeVariant` | fully materialized | whole-Variant column compacted |
| `Doc16/DocBuckets` | 16-bucket Doc Mode | doc-bucket subcolumns compacted |
| `Doc16/DocBucketsMaterialized` | 16-bucket Doc Mode | doc buckets compacted and materialized |
| `Doc16/WholeVariant` | 16-bucket Doc Mode | whole-Variant column compacted |

`BM_VariantSparseImport` registers 4 scenarios x 5 samples = 20 cases: `MixedTypes` vs. `NoArrays`
(arrays replaced with BIGINT, isolating per-array type inference) crossed with `Writers1` vs.
`Writers8` (1 vs. 8 concurrent `VerticalSegmentWriter`s, matching parallel memtable flushes of
several tablets — this contends process-wide shared state such as static `DataTypePtr` reference
counts the way a real import does).

Run the full suite and capture an environment-tagged, reproducible result:

```shell
source custom_env.sh   # sets JAVA_HOME
bash be/benchmark/variant/run_variant_segment_benchmark.sh \
    be/output/lib/benchmark_test <result_dir> '^BM_Variant'
```

`<result_dir>/raw.json` has the Google Benchmark JSON; `<result_dir>/environment.txt` has the
commit, working-tree diff, thirdparty fingerprints, and host info needed to interpret it. Both
suites read `DORIS_VARIANT_BENCHMARK_ROWS` (default 1,000,000) for the row count and
`DORIS_VARIANT_BENCHMARK_ROOT` (default `/tmp`) for scratch space; `DORIS_BENCHMARK_CPU` pins the
run with `taskset` when set.

To run only one suite, pass its own filter as the third argument, e.g.
`'^BM_VariantSparseImport/'`.

## Interpret the result

- `BM_VariantCumulativeCompaction` cases are `->UseManualTime()` with a single iteration per
  registered case (`sample1`..`sample5` are separate named cases, not Google Benchmark
  repetitions); use `real_time` in milliseconds. Lower is faster.
- `BM_VariantSparseImport` reports `cpu_s_per_1m_rows` (also `append_ns_per_row`,
  `finalize_ns_per_row`, `segment_bytes_per_row`, `input_json_bytes_per_row`) alongside
  `real_time`/`cpu_time`. `cpu_s_per_1m_rows` is thread CPU time summed across writers, so
  `Writers8` is expected to cost more than `Writers1` even with no regression; compare `Writers8` to
  `Writers8` and `Writers1` to `Writers1` across commits, not to each other.
- Both suites validate output before returning: a compaction case checks the merged segment's
  subcolumn layout and rewritten values, and an import case checks each writer's row/segment count.
  A failing case aborts benchmark execution (`error_occurred` in the JSON) rather than silently
  reporting bad numbers.
- This is a workload-level benchmark against real `Compaction`/`VerticalSegmentWriter` code, not a
  microbenchmark of an isolated function — treat regressions as signals to investigate with a
  profiler, not as a precise attribution of which code path changed.
- On a shared/busy host, run-to-run noise can be large (see `BASELINE.md` for a measured example).
  Prefer the median of several samples, and re-run before concluding a change regressed or improved
  performance.

### VARIANT `element_at` extraction

`benchmark_variant_element.hpp` invokes the registered `element_at` IFunction, including
nullable handling, constant-key materialization, path resolution, lookup, result copying,
and result-column construction. Input building and first/last-row checks are outside timing;
JSON parsing, storage scanning, RPC, and SQL planning are excluded.

```bash
BUILD_TYPE=RELEASE ./build.sh --benchmark
taskset -c 0 be/output/lib/benchmark_test \
  --benchmark_filter='^BM_VariantElement' \
  --benchmark_min_time=0.25s --benchmark_repetitions=5 \
  --benchmark_out=element-at.json --benchmark_out_format=json
```

Names end in `/rows/payload_bytes`. Scalar/missing cases use the payload as an unselected
sibling; string/array cases return that many string bytes. Object/chain cases include that
payload in the selected object; chain measures `element_at(element_at(v, 'outer'), 'target')`.
The source is nullable with a zero null map. TypedMissing covers a typed scalar root with
an absent object key; it does not represent a shredded typed-leaf hit. Divide CPU time by
rows to compare ns/row, and use the same CPU and interleaved repeated runs for before/after.

### Encoded storage costs and controls

`benchmark_variant_cost.hpp` covers batch construction, row/batch append, clone, 50%/1%
selection, in-place selection, permutation, comparison, sorting, hash/CRC, binary size
calculation, binary serialization/deserialization, row-key serialization and JSON output.
Fixtures include encoded integers, short strings, objects, per-row high-entropy strings and a
typed integer control. Inputs and binary round-trip checks are outside timing; the build case
includes fixture construction. Binary buffers are preallocated for serialization timing.

```bash
BUILD_TYPE=RELEASE ./build.sh --benchmark
DORIS_HOME="$PWD" be/output/lib/benchmark_test \
  --benchmark_filter='^BM_Variant(Cost|Raw)' \
  --benchmark_repetitions=5 --benchmark_enable_random_interleaving=true \
  --benchmark_out=variant-cost.json --benchmark_out_format=json
```

`BM_VariantRawString` and `BM_VariantRawBinary` compare the two containers in one process;
the final argument selects continuous import (0), clone (1), 50% filter (2) or read (3).
The fixtures report logical bytes, allocated bytes and actual wire bytes separately.
`BM_VariantCostRetainAppend` keeps 4096 logical rows while repeatedly erasing/appending half a
batch, exposing arena retention that a one-shot append benchmark does not measure.
`BM_VariantCostDefaults` isolates default-row construction.

Encoded Variant children use one `DataTypeVarbinary` transport layout, with no legacy
`ColumnString` conversion or execution-version gate. Round trips under multiple version
arguments test the current layout, not interoperability with an older peer.
