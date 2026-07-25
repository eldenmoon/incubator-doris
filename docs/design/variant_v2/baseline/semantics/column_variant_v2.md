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

# ColumnVariantV2 semantic closure

This is the T2.6 comparison record for the final V2 runtime. The immutable legacy evidence is
`internal.tsv` plus `sql.out`; the executable V2 matrix is
`ColumnVariantV2Test.T26*` in `be/test/core/column/column_variant_v2_test.cpp`.

## Legacy comparison and intentional differences

| Surface | T0.2 legacy evidence | ColumnVariantV2 contract | Decision |
|-|-|-|-|
| Field representation | A row projected to a path-oriented `VariantMap`; arrays of objects could appear as `Array(JSONB)` or flattened arrays of fields. | One self-contained `VariantField` containing `[u32 metadata_size][metadata][value]`; arrays and objects stay structural. | Intentional D15 replacement; public JSON is compared, not the removed internal representation. |
| Default row | Legacy `get` returned an empty/NULL Field, while direct JSON, SerDe JSON, and string output all emitted `{}`. | `insert_default` and `insert_many_defaults` encode canonical `{}`; `get` returns a `VariantField` for `{}`. | O2 is resolved from the reachable public output. An explicitly inserted empty Field remains JSON null. |
| Root JSON null | Nullable legacy SQL input could render `{}` and the CSV NOT NULL case could render `""`. | JSON null remains primitive id 0 and renders `null`; SQL NULL remains an outer nullable bit. | Intentional V2 null-model correction. |
| Empty containers | Legacy SQL rendered both root `{}` and root `[]` as `{}` in the captured old path. | Empty object renders `{}` and empty array renders `[]`. | Intentional structural preservation. |
| Object JSON-null member | The legacy SQL snapshot omitted the member and made its path indistinguishable from a missing path. | V2 preserves the member as JSON null; missing path remains SQL NULL at the extraction boundary. | Intentional V2 null-model correction. |
| Nested objects and arrays | Legacy non-flatten mode stored object arrays as JSONB fields; flatten mode changed path/Field shapes. | V2 preserves the complete nested value in Variant encoding and has no path-flattened Field form. | Intentional removal of trie/Field flattening. |
| Scalar roots | Legacy storage commonly surfaced them as JSONB/root fields. | V2 uses native primitive headers or the typed scalar state. | Representation change; JSON text and canonical equality remain aligned. |
| Duplicate keys | The captured policy rejects when duplicate checking is disabled and keeps the first complete member when enabled. | Same policy: reject without the per-object duplicate set; first complete member wins when enabled. | Compatible and covered in the T2.6 matrix. |
| Maximum nesting | T0.2 records legacy parser behavior at depths 128/129 and the independent simdjson limit. | V2 accepts `VARIANT_MAX_NESTING_DEPTH` and rejects the next level. | V2 codec limit is authoritative for V2 bytes. |

## Phase 2 interface coverage

| 02 §3.3 surface | Test evidence |
|-|-|
| `size`, `get_name`, variable-length, byte/allocation accounting, `structure_equals`, no-op finalize | `EmptySkeleton`, `TypedFactoriesAndNullableInvariant` |
| `insert(Field)`, `operator[]`, `get` and empty Field → JSON null | `EncodedFieldApiAndUnsupportedInterfaces`, `TypedFieldApiAndUnsupportedInterfaces`, `T26FieldRoundTripMatchesT02SemanticMatrix` |
| `insert_from`, range, indices, selector and metadata remap | `CopyInterfacesUseSharedMetadataFastPath`, `RemapsDistinctAndDuplicateMetadataBlobs`, `SelfInsertRangeAndIndicesSnapshotAliases` |
| block/bulk append | `BulkAppendSharedMetadataAtLeast64KRows`, `InsertsCodecOwnedBlocksDirectly`, `InsertsCodecOwnedBlockAtLeast64KRows` |
| `insert_data` canonical arena cell | `BulkNoOpAndCanonicalInsertData` |
| defaults and resolved O2 | `DefaultRowsUseCanonicalEmptyObjectAndReuseMetadata`, `T26ResolvedDefaultAndDuplicateKeySemantics` |
| canonical arena serialize/deserialize and serialized-key adapters | `CanonicalArenaAndSerializedKeyAdaptersRoundTrip` |
| hash, xxHash, CRC and CRC32C families | `CanonicalHashFamiliesRespectEncodingsSeedsAndMasks`, `TypedCanonicalHashCrcAndArenaMatchEncoded` |
| comparison/sort/data-at/replace rejection | both `*FieldApiAndUnsupportedInterfaces` tests |
| filter and in-place filter | `ConstFiltersMatchColumnStringAndKeepMetadataReadOnly`, `InPlaceFilterPrevalidatesAndPreservesMetadataIndex` |
| permute, pop, resize | `PermuteMatchesColumnStringAndRejectsInvalidInputs`, `PopBackAndResizeCoverBoundsShrinkAndGrowth` |
| clone family, COW, clear and subcolumn traversal | `CloneFamilyPreservesCowAndMetadataContracts`, `CowCloneForEachAndClearRebuildMetadataIndex`, `CowDetachAndClear` |
| E/T state invariants and conversion | `EncodedRowCountInvariant`, `MetadataIdInvariant`, `TypedFactoriesAndNullableInvariant`, `TypedEnsureEncodedAllScalarMappings` |
| E/T canonical equivalence | the three `ETCrossCheck*` tests and `TypedEncodedInsertMatrixKeepsConstSource` |
| T2.6 scalar/nested/object-array/empty/null/depth/duplicate matrix | both `T26*` tests |

The Phase 2 exit command is:

```shell
BUILD_TYPE_UT=ASAN ./run-be-ut.sh --run --filter='ColumnVariantV2Test.*'
```
