# Variant V2 segment snapshots

This directory contains two independent T0.2 C3 gates:

- `samples/` is the canonical V2 **new-write** baseline produced by the current public
  `SegmentWriter` path. A reviewed writer change may intentionally change its physical layout and
  replace these bytes with `regenerate.sh --update`.
- `legacy_samples/` is the **old persisted readability** contract. Its three `.dat` files are the
  exact bytes extracted with `git show` from commit
  `b344b5580bd34e50c340c76b34d7de4eb8aa79f8`; the current writer must never regenerate or update
  them. `legacy_samples/PROVENANCE.tsv` records the source commit and path for each file.

Each case uses a `DUP_KEYS` schema with an `INT` key and a `VARIANT` value, V2 inline column
metadata, LZ4 compression, and 4 KiB data pages. The `.dat` files are complete segment files,
including data pages, indexes, the inline `SegmentFooterPB`, checksum, and trailer.

## Cases

- `ordinary_sparse.dat`: 13 rows with the top-level VARIANT root stream, two materialized BIGINT
  paths, and one sparse map. One row contains only a scalar root. The new-write baseline stores
  the nested object as `object.x`; the legacy baseline stores an `object` JSONB cell. Reading both
  must assemble the same `"object":{"x":...}` semantics. JSON null and the empty path are absent.
- `doc.dat`: 8 rows routed through one doc-value bucket. It pins BOOL, BIGINT,
  STRING, nested BIGINT, and DOUBLE cells plus the `HIERARCHICAL_DOC` read
  route; JSON null is absent.
- `bucketed_sparse.dat`: 12 rows with one materialized BIGINT path and three
  sparse buckets. Each bucket has a fixed path, FieldType, count, shard, and
  persisted `VariantStatisticsPB` entry.

`samples/source.tsv` is a deterministic logical/legacy-provenance comparison, with rows sorted
within each fixed-order case. It is not a byte-exact description of the current V2 in-memory
input: the current canonical V2 generator encodes only assembled-visible semantics, while entries
for the old root sidecar and suppressed/null fields remain in `source.tsv` solely as provenance.
Only `legacy_samples/` preserves those old persisted root-sidecar bytes. `samples/manifest.tsv`
records the decoded sparse/doc cells, physical column metadata, assembled rows, read-route
observations, file sizes, and the explicitly deferred nested-group case.
`SHA256SUMS` covers the five current-writer artifacts. The separate `LEGACY_SHA256SUMS` covers the
three immutable legacy segments and their provenance file.

Nested-group is not part of this user-approved C3 scope. It remains a
`T5.9` TODO and is recorded as deferred in the manifest.

## Verify or regenerate

From this directory, verify in a fresh ASAN unit-test process that the current writer reproduces
`samples/`, that the current V2 reader opens and semantically validates every `legacy_samples/`
segment, and that both independent checksum sets match:

```bash
./regenerate.sh --check
```

To intentionally replace only the current-writer baseline after reviewing a storage change:

```bash
./regenerate.sh --update
./regenerate.sh --check
```

The update path generates `samples/` in two independent empty temporary directories and requires
full-directory diff plus per-file byte comparison before installing the complete artifact set. It
verifies `LEGACY_SHA256SUMS` before and after generation and never writes `legacy_samples/` or
`LEGACY_SHA256SUMS`. Do not hand-edit generated sample `.dat`, TSV, or checksum files. Legacy
segments may only be re-extracted byte-for-byte from their recorded source commit after explicit
review.

The V2 inline footer is serialized with protobuf deterministic map ordering so
equal messages produced by the same binary are stable across processes. This
is not a permanent canonical format across protobuf implementations, builds,
or schema revisions. V3 external `ColumnMetaPB` bytes and repeated variant
external-metadata ordering are not covered here and remain a separate
correctness TODO. Performance measurement is also deferred and must use a
Release build; these scripts are correctness-only ASAN checks.
