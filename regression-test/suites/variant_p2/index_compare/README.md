# GitHub Events VARIANT index comparison

This Doris-only performance harness compares four index layouts on the real
GitHub Events rows used by `variant_p2`:

- no inverted index;
- one logical Root index per analyzer;
- one logical AllValues index per analyzer;
- materialized-child indexes.

The four target tables have the original `variant_p2` schema and the same
`variant_max_subcolumns_count=1024` setting. Only `payload` is indexed. Each
indexed table has both an exact (`parser=none`) and an English token index, so
write, compaction, and storage numbers represent the complete schema needed by
the query matrix. The Google Benchmark remains the source for isolated
exact-versus-English attribution.

## Measurement boundary

The local JSON files are first stream-loaded into eight unmeasured staging
tables. Target write time is then measured with eight `INSERT SELECT` chunks in
a rotated layout order. This excludes S3/network and client JSON parsing noise
while retaining real GitHub Events Variant values and Doris' production write
and index-building paths. It must be described as a real-data SQL write
benchmark, not as an S3 end-to-end import benchmark.

Auto compaction is disabled. The eight chunks create comparable multi-rowset
inputs before full compaction. The harness rejects results unless row counts,
full logical fingerprints, query fingerprints, and active-rowset topology pass
before and after compaction.

SNII writes are gated at the Doris default
`inverted_index_ram_buffer_size=512` MiB. Immediately before the full-compaction
phase, the runner changes this mutable BE setting with `persist=false` to 2,048
MiB and reads it back. Every layout is compacted under the same 2,048 MiB
condition, with another read-back immediately before and after its timed window.
This is an explicit tuned-compaction condition, not Doris' default. On this
44,273,863-row corpus, Root native merge at the 512 MiB default deterministically
failed for all eight tablets at its first rejected reservation; that
completability result is reported separately and none of its timings are mixed
with the tuned comparison. A BE restart restores the non-persistent setting to
512 MiB, which the query-resume preflight also asserts.

GitHub Event `id` is retained exactly as supplied and is not assumed unique.
The query oracle fingerprints the full matching-row multiset (count, id
sum/xor, and complete logical-row hash sum/xor), so source duplicate rows remain
part of the real population without weakening cross-layout or pre/post
compaction correctness checks. The source distinct-id and duplicate-row counts
are retained in the event log as the duplicate-id group count, excess row count,
and maximum id multiplicity.

Queries pin `inverted_index_skip_threshold=50`; the runner does not force index
admission in timed samples. SQL, query samples, EXPLAIN,
profiles, process CPU counters, active `.dat`/`.idx` bytes, DDL, binary hashes,
Git state, and the validated source manifest are retained in the result
directory.

Write and full-compaction measurements use at most 64 physical cores; the formal
`--dataset-scope full` run requires exactly 64. Query measurements require
exactly eight physical cores. Both CPU sets are explicit runner inputs, and the
query set must be a subset of the write set. The runner rejects
offline/disallowed CPUs, overlapping ranges, and any set containing two SMT
threads from the same `(physical_package_id, core_id)`.

The runner is an affinity gate, not an affinity mutator. It enumerates every
FE/BE thread repeatedly until two consecutive snapshots are stable, then
requires every thread's `Cpus_allowed_list` to equal the phase CPU set. It also
binds the supplied PIDs to the actual FE MySQL/HTTP sockets and requires the
unique alive `SHOW BACKENDS` row to advertise a local host whose heartbeat, BE,
HTTP, and BRPC listening socket inodes all belong to the supplied BE PID. It
also verifies the live BE `/api/show_config` `num_cores` value equals the phase
CPU count. The complete gate runs both before and after write, full compaction,
and query. Full per-thread masks, socket ownership, service identity, PIDs,
process start times, timestamps, `num_cores`, and gate outcomes are saved under
`cpu_affinity/` and summarized in `events.jsonl`.

Do not shrink a 64-core process in place for query measurement. Its BE thread
pools were sized at startup and its memory placement reflects the write phase.
After `storage_post`, the first invocation intentionally stops at the query
restart gate. Stop both FE and BE, set `be.conf` `num_cores=8`, restart both on
the eight-core query set, and resume with the new PIDs. Resume permits this
runtime identity change only after all pre-query phases have completed; binary,
Git, source, database, port, and storage identity checks remain strict.
Before any timed query, the restarted cluster must also reproduce the complete
post-compaction logical/query fingerprints and the exact persisted
tablet/schema/version/rowset/file identity. Every warmup, timed count, and
profile count is checked against the retained post-compaction oracle.

`CAST(payload AS STRING) LIKE ...` and whole-root `payload MATCH_* ...` are
different semantics and are always reported separately. The former is a brute
reconstruction/string scan. Only AllValues can use a whole-root MATCH index.
An explicit `USING ANALYZER english` also requires matching index metadata at
FE translation time, so physical NoIndex is N/A for English MATCH cases. Their
scan baseline is the same indexed table with `enable_inverted_index_query=false`;
NoIndex still participates in EQ/IN, path LIKE, startsWith, and whole-root LIKE.
Phrase search is unsupported because the compared indexes use
`support_phrase=false`. The 2015 corpus has no validated scalar-array path in
the main population, so array search is reported as N/A rather than replaced
with synthetic rows.

Keyword selectivity is classified before timing with Doris' own analyzer and
the complete row population. Sparse means nonzero and at most 0.01%; dense
means at least 0.1% and below the 50% skip threshold. The fixed-path English
`MATCH_ANY` pair is `sonar` versus `the`; the whole-root English `MATCH_ANY`
pair is `xgboost` versus `update`. A full run fails before reporting if either
pair falls outside its declared band. Partial smoke runs record but do not
enforce the bands.

The query session asserts SQL/query/condition/inverted-query caches are off and
inverted searcher/page caches are on. Every `(query, layout, index-mode)` gets
two fixed warmups followed by the TSV-declared interleaved timed samples. Five
sample groups are labeled stable only when CV is at most 10% and span is at
most 25%; one-sample whole-root LIKE remains directional. No outlier is removed.

Storage is measured only from active rowset files and split into `.dat`, `.idx`,
and other bytes. Shared BE RocksDB metadata is not attributable per table and is
reported as N/A. Full compaction is accepted only when every tablet consumes the
complete `[0-9]` version range and leaves one active `[0-9]` rowset, with Profile,
BE rowset metadata, filesystem bytes, and FE tablet reports reconciled.

## Run

### Prepare the complete corpus

The complete `variant_p2` source is the public OSS prefix
`https://doris-regression-hk.oss-cn-hongkong.aliyuncs.com/regression/github_events_dataset`.
It contains the suite's 2015 January through March objects for hours 1 through
23: 90 days, 2,070 JSON files, and the golden 44,273,863 JSONEachRow records.
Prepare it once outside the timed benchmark:

```bash
python3 bin/prepare_full_corpus.py \
  --raw-dir /mnt/disk6/common/variant-p2-full/dataset/raw
```

The default output is
`/mnt/disk6/common/variant-p2-full/dataset/manifests/full.validated.tsv` and has
the runner's `batch,file,rows,bytes,sha256` schema. The tool first validates the
complete public inventory with OSS ListObjects and a HEAD request per expected
object. Downloads use eight workers by default. Each object is written to a
`.part` file, resumed with HTTP Range, fsynced, and atomically renamed. A final
file whose size already matches OSS is reused without being overwritten; a
wrong-sized final file is rejected for manual inspection. Interrupted `.part`
files are retained for the next invocation.

After all files are ready, the tool independently counts rows and computes
SHA-256 for every file. It publishes the formal manifest atomically only after
both the 2,070-object and 44,273,863-row gates pass. Batch assignment is a
deterministic row-balanced schedule with 258 or 259 files per batch. A failed
run before the atomic publish step never replaces an existing formal manifest.
If OSS access reports HTTP 502, unset `HTTP_PROXY`, `HTTPS_PROXY`, `http_proxy`,
and `https_proxy`, then rerun in the physical environment.

Start an isolated Release FE/BE first, then run a smoke manifest before the
validated 1/8 population:

For example, on a 2x48-core/SMT2 host where CPUs `0-95` are one hardware thread
per core, use `0-31,48-79` for the balanced 64-core write set and `48-55` for the
single-NUMA-node eight-core query set. Verify these IDs against the current
host's sysfs topology; the runner will reject an invalid mapping. Start the
initial FE and BE with `taskset -c 0-31,48-79` and `be.conf` `num_cores=64`.

```bash
python3 bin/run_doris.py \
  --manifest /path/to/smoke.validated.tsv \
  --dataset-scope partial \
  --raw-dir /path/to/raw \
  --database variant_p2_index_compare_smoke \
  --mysql-port 24869 --fe-http-port 24859 --be-http-port 24889 \
  --storage-root /path/to/output/be/storage \
  --be-pid BE_PID --fe-pid FE_PID \
  --write-cpu-list 0-31,48-79 --query-cpu-list 48-55 \
  --write-snii-ram-buffer-mib 512 \
  --compaction-snii-ram-buffer-mib 2048 \
  --results /tmp/variant-p2-index-smoke --fresh
```

When that invocation reaches the query restart gate, stop both processes and
restart them with `taskset -c 48-55`, retaining the same storage and ports, and
with `be.conf` `num_cores=8`. Then continue with the new process IDs:

Runtime changes to `enable_variant_v2` are not persisted. After FE restarts it
is loaded again from `fe.conf`, or from its default (`false`) when the file does
not set it. The resume preflight re-enables and verifies it before post-restart
fingerprints or query measurements.

```bash
python3 bin/run_doris.py \
  --manifest /path/to/smoke.validated.tsv \
  --dataset-scope partial \
  --raw-dir /path/to/raw \
  --database variant_p2_index_compare_smoke \
  --mysql-port 24869 --fe-http-port 24859 --be-http-port 24889 \
  --storage-root /path/to/output/be/storage \
  --be-pid NEW_BE_PID --fe-pid NEW_FE_PID \
  --write-cpu-list 0-31,48-79 --query-cpu-list 48-55 \
  --write-snii-ram-buffer-mib 512 \
  --compaction-snii-ram-buffer-mib 2048 \
  --results /tmp/variant-p2-index-smoke --resume
```

The earlier one-round gate uses `1_8.validated.tsv` (259 files and 5,510,792
rows) with `--dataset-scope partial`. The complete formal run must use
`full.validated.tsv` with `--dataset-scope full`; this makes the runner itself
reject anything other than the exact 2,070-file name set and 44,273,863 rows.
Import and compaction each have one aggregate observation per layout and are
directional results. Query cases have two warmups plus repeated interleaved
samples; `query_stability.json` retains every group's classification.
