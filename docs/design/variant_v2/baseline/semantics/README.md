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

# Variant V2 legacy semantics snapshot

The legacy snapshot is intentionally immutable. The final V2 comparison, intentional-difference
list, and Phase 2 interface coverage index are recorded in `column_variant_v2.md`.

`internal.tsv` is a deterministic characterization of current BE behavior at base HEAD
`e8a13279055752c7c698b772adecda3e6f5673a0`. It records legacy `JSONDataParser` Field/path
shapes, legacy `ColumnVariant` default rows, canonical Variant codec bytes, and current depth-limit
evidence. It is evidence for T0.2; it is not the target Variant V2 semantics specification.

The snapshot format is `variant-semantics-internal-v1` with exactly eight columns:
`case_id`, `source`, `input`, `config`, `path`, `path_parts`, `field_shape`, and `observation`.
Records are sorted deterministically by `(case_id, path, path_parts)` before the remaining fields;
the three-field primary key must be unique. Within a field, backslash, tab, LF, and CR are encoded
as `\\`, `\t`, `\n`, and `\r`; other C0 controls and DEL use uppercase `\xHH`. The verifier
decodes and re-encodes every field to reject unknown or noncanonical escapes.

Normal verification is read-only:

```shell
BUILD_TYPE_UT=ASAN ./run-be-ut.sh --run --filter='VariantSemanticsSnapshotTest.VerifyInternalGolden'
```

Regeneration is an explicit opt-in and writes through a temporary file plus atomic rename:

```shell
DORIS_REGEN_VARIANT_SEMANTICS_SNAPSHOT=1 BUILD_TYPE_UT=ASAN ./run-be-ut.sh --run --filter='VariantSemanticsSnapshotTest.GenerateInternalGolden'
```

After reviewing the generated diff, run the verifier and then the full focused filter:

```shell
BUILD_TYPE_UT=ASAN ./run-be-ut.sh --run --filter='VariantSemanticsSnapshotTest.*'
```

Without the environment variable, the generator skips; any value other than exactly `1` fails.

## O2 legacy default evidence

- `ColumnVariant::insert_default` in `be/src/core/column/column_variant.cpp` inserts a default into
  every materialized subcolumn and both internal map columns, then increments the row count.
- `ColumnVariant::get` first constructs a `VariantMap`, omits null fields, and replaces an empty map
  with a default `Field`, whose type is `TYPE_NULL`.
- `ColumnVariant::serialize_one_row_to_json_format` emits an object delimiter pair when no visible
  root, document value, sparse value, or materialized path exists, so the same row serializes as
  `{}` through the direct JSON path.
- `DataTypeVariantSerDe::serialize_one_cell_to_json` and `DataTypeVariantSerDe::to_string` delegate
  to the legacy column serializer; the snapshot invokes both APIs independently and records their
  equality with the direct JSON result.

## SQL, JDBC, and CSV snapshot

`variant_semantics_snapshot.groovy` characterizes the old FE/BE through the regression
framework's MySQL Connector/J result path at production evidence base
`2ec5f1514d79df6850885f1d6f92c249f9811be6`; the earlier `internal.tsv` intentionally retains
its separately recorded `e8a13279055752c7c698b772adecda3e6f5673a0` base. The generated SQL
golden is committed twice:

- `regression-test/data/variant_p0/variant_semantics_snapshot.out` is the executable regression
  expectation and is generated only by `-forceGenOut`;
- `sql.out` is a byte-for-byte copy kept beside the internal snapshot for downstream consumers.

The suite fixes doc mode off and records SQL NULL, object-path JSON null, missing paths, scalar
roots, direct and `CAST(... AS STRING)` text, the actual path-to-type object returned by
`variant_type`, `json_length` scalar behavior, escaped quotes/backslashes/Unicode, scientific
notation, duplicate-key policy, and LARGEINT boundaries. A normal quick-test block is executed
through `JdbcUtils.executeToStringList`, so its direct Variant cells are the MySQL/JDBC text
surface rather than a separately reconstructed rendering.

The local CSV case is intentionally non-concurrent. It disables parallel OUTFILE, requires exactly
one generated `.csv` file, and stream-loads that exact file first into
`(id INT, v_raw STRING)`. The golden records both `v_raw` and `HEX(v_raw)` before the same file
is loaded into a Variant table. This prevents Variant reload from hiding a textual change. The
object, string, and root-JSON-null input cases are all present. Absolute paths, generated filenames,
OUTFILE URLs, labels, timings, and load transaction ids are assertions only and never enter the
golden output.

The generated C2 evidence records several legacy asymmetries without normalizing them:

- SQL NULL remains `IS NULL = true`. In an object, a JSON-null member is omitted from rendered
  text and its path result is SQL NULL, the same observable result as a missing path.
- A root `'null'` inserted into a nullable Variant renders as `{}`; empty object and empty array
  inputs render the same way. The same root `'null'` input in the NOT NULL CSV source renders as
  `""`, whose raw CSV bytes are `2222`, and Variant reload preserves `""`.
- `variant_type` returns a path-to-type object, not a single vocabulary word. Scientific
  `1.25e3` renders as `1250` while `1e-6` renders as `1e-06`.
- Duplicate checking disabled rejects a duplicate path; enabled keeps the first value. JSON object
  input containing a `10^38-1` or `10^38` integer falls back to a quoted string root, while
  explicitly cast LARGEINT values retain their numeric text after Variant storage.

Local OUTFILE is an immutable FE startup configuration. Before running the suite,
`enable_outfile_to_local=true` must be set in the active runtime FE config, not a tracked
`conf/fe.conf`, and that FE must be restarted. The regeneration wrapper accepts a temporary
regression config and requires the backend host so FE-to-BE stream-load redirects bypass proxies:

```shell
docs/design/variant_v2/baseline/semantics/regenerate.sh \
  --conf tmp/regression-conf.auto.groovy \
  --backend-host <backend-host>
```

The wrapper runs generation and then a normal no-force verification, copies the generated golden
to `sql.out`, and updates `SHA256SUMS`. Checksums cover `internal.tsv`, `sql.out`, and the
generated regression output; the two SQL-output hashes must remain equal.

## O2 decision: canonical empty object

O2 is resolved to the canonical empty object: metadata `110000`, value `020000`.

- The reachable public `IColumn` default surface of old `ColumnVariant::insert_default` emits
  `{}` through direct JSON serialization, `DataTypeVariantSerDe::serialize_one_cell_to_json`,
  and `DataTypeVariantSerDe::to_string`; C1 invokes and compares all three.
- The legacy `ColumnVariant::get` projection produces an empty `Field` with `TYPE_NULL`, but
  that internal projection is not a JSON-null public value and is not used to overturn the
  observable `{}` behavior.
- The two implicit-default SQL entry points characterized by C2 do not expose a NOT NULL Variant
  default row. Omitting an existing Variant column from INSERT fails with
  `Column has no default value, column=v`; adding a NOT NULL Variant column without a DEFAULT
  clause fails with `Field 'v' doesn't have a default value`. The regression fixes both as
  expected errors. Consequently, C2 found no reachable SELECT/MySQL/CSV implicit-default row
  through those DML/DDL entry points and no contrary SQL-surface evidence.
- Explicit `DEFAULT '{}'`, `DEFAULT NULL`, or a literal inserted by the test would test the
  literal, not the type's implicit default, and is deliberately excluded. CSV evidence in C2 is
  general Variant text/export/reload evidence, not an implicit-default claim.

The choice is therefore a compatibility decision for the old column's reachable public behavior,
not a claim that every legacy internal representation is an object. LARGEINT values at and beyond
`10^38` are characterized but remain O3 decision debt. T0.2 is not complete until the C3 legacy
segment-byte samples and dump tooling are accepted.
