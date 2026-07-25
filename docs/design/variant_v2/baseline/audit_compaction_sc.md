# Variant V2 compaction / schema-change data-flow audit

## 1. Scope and evidence rules

- Task: T0.3, revision-49 `03 §4–§5` audit delta.
- Audited code baseline: `d6ce45ecba883188cb1175afb1d54cccf1355a6d`.
- Storage persistence is out of scope for change and remains frozen: segment layout, root/typed/sparse/doc bytes, statistics, read plan, budgets, PB and Thrift are unchanged.
- Compute and SerDe V2-only policy does not alter this storage audit.
- Nested-group (NG) is a user-approved deferred gap. NG observations below describe the current code only; they are not current-scope acceptance evidence.
- Performance work is deferred. No performance result is used here; future performance tests must use RELEASE.

Evidence levels used below:

| Level | Meaning |
|-|-|
| Code | The baseline call chain and branch conditions establish the statement. |
| Test | An existing focused UT or regression source exercises the stated behavior. A test is counted as executed evidence only when §10 records it as passed. |
| Gap | Code identifies the route, but no deterministic end-to-end test proves the complete claim. The gap is assigned to the T5.4 matrix. |

Delta status is deliberately restricted to `已证实`, `已证伪`, and `待验证`:

- `已证实`: code establishes the scoped statement and relevant existing test coverage is identified.
- `已证伪`: the revision-49 statement is unconditional or names a consumer that the baseline does not have; the replacement statement is given.
- `待验证`: the route is known, but the claimed V2 behavior or complete integration still needs a T5.4 regression.

`docs/source/03-storage-integration.md` is a read-only snapshot. This memo is its revision-49 audit delta and is the permitted equivalent of writing the conclusions back into that snapshot.

## 2. Compaction topology and actual block slots

### 2.1 Top-level route

```text
Compaction::merge_input_rowsets
  -> construct_output_rowset_writer
  -> unordered horizontal: Merger::vmerge_rowsets
       -> BlockReader::next_block_with_aggregation
       -> RowsetWriter::add_block
  -> unordered vertical: Merger::vertical_merge_rowsets
       -> Merger::vertical_compact_one_group
       -> VerticalBlockReader::next_block_with_aggregation
       -> RowsetWriter::add_columns
  -> RowsetWriter::build
  -> Compaction::check_correctness
       -> VariantCompactionUtil::check_path_stats

CompactionMixin::do_compact_ordered_rowsets
  -> Rowset::link_files_to
  -> RowsetWriter::manual_build
  -> VariantCompactionUtil::check_path_stats
```

Ordered link compaction has no data Block and does not append Variant rows to a column writer. Therefore any statement that every compaction materializes a Variant slot is false.

### 2.2 Local versus Cloud setup

For local compaction, `CompactionMixin::build_basic_info(bool is_ordered_compaction)` calls
`VariantCompactionUtil::get_extended_compaction_schema` only when
`enable_vertical_compact_variant_subcolumns` is enabled and the operation is not ordered. This
condition is independent of `_is_vertical`: an unordered horizontal merge can also receive the
extended schema. Local ordered link compaction supplies `true` and skips extension.

Cloud follows `CloudCompactionMixin::execute_compact_impl -> build_basic_info ->
Compaction::merge_input_rowsets`, but its `build_basic_info()` has no ordered parameter and extends
the schema whenever `enable_vertical_compact_variant_subcolumns` is enabled. This audit found no
Cloud equivalent of the local ordered-link branch in that route. The common merge/read/write code
still applies after setup, but no focused Cloud Variant E2E was executed; C08 records this gap.

### 2.3 Slot-shape matrix

Horizontal and vertical merge use the same `VariantColumnReader` plans. Their difference is Block
transport (`add_block` versus grouped `add_columns`), not the slot representation.

| Mode / target | Read plan | Actual Block slot on the audited baseline | Output writer route | Evidence |
|-|-|-|-|-|
| ordered link | none | no Block / no slot | file link + `manual_build` | Code |
| unordered, non-extended, non-doc root | `HIERARCHICAL` | nullable as declared, nested payload `ColumnVariant` | ordinary `VariantColumnWriterImpl` | Code; existing reader/writer UT |
| unordered, non-extended, doc root | `HIERARCHICAL_DOC` | nullable as declared, nested payload `ColumnVariant` | ordinary `VariantColumnWriterImpl` | Code; doc hierarchical UT |
| unordered, extended root | `ROOT_FLAT` | nullable as declared, nested payload `ColumnVariant` containing persisted root | ordinary root writer, or NG streaming route under §2.4 | Code |
| extended regular extracted path | `LEAF` | iterator reads the segment's concrete physical type; `SegmentIterator::_convert_to_expected_type` then converts to the generated compaction-schema type. Most targets are concrete typed columns, while a heterogeneous/unresolved target can remain `VARIANT` and become `ColumnVariant`. | concrete target uses its normal scalar/array/map writer; `VARIANT` fallback uses `VariantSubcolumnWriter` | Code; extended-schema UT |
| extended single/bucket sparse path | `SPARSE_MERGE` | physical sparse `ColumnMap(String,String)` shape described by the target column | normal map writer | Code; rowset compaction UT |
| extended doc bucket | `DOC_COMPACT` | special `ColumnVariant` carrying that doc bucket | `VariantDocCompactWriter` | Code; focused doc-compact UT |

`BINARY_EXTRACT`, `DEFAULT_NESTED`, and `DEFAULT_FILL` remain planner fallbacks, but they are not
the expected slots produced by a valid generated compaction schema for the six required cases
above. T5.4 must fail loudly if a generated target unexpectedly falls into one of those fallbacks.

### 2.4 Ordinary versus streaming writer

The ordinary root path is:

```text
VariantColumnWriterImpl::append_data
  -> copy a range from the input ColumnVariant into `_column`
  -> VariantColumnWriterImpl::finalize
  -> root / regular / sparse / doc write pipelines
```

The current streaming route is enabled only when all of these are true:

```text
write_type == TYPE_COMPACTION
variant_enable_nested_group == true
variant_enable_doc_mode == false
input_rs_readers is not empty
```

It delegates from `VariantColumnWriterImpl::append_data` to
`VariantStreamingCompactionWriter::append_data`. For every input slice,
`VariantStreamingCompactionWriter::_append_input` copies a legacy `ColumnVariant` chunk, sanitizes
it, calls `chunk_variant->finalize()`, and only then calls `_append_chunk`. The distinction is that
it avoids the ordinary writer's whole-column buffer/finalize, not that it avoids finalization.
Existing source tests cover plan construction and multi-batch writing, but both NG tests were
skipped in the selected build because the NG write path is unavailable; no complete production
compaction proves route selection. NG is user-deferred, so this remains a recorded gap rather than
current-scope completion evidence.

## 3. Revision-49 `03 §5` delta

| 03 §5 statement | Status | Audited conclusion and direct call chain | Existing test evidence / gap |
|-|-|-|-|
| Opening: compaction mainly reads flat leaves; most data does not pass through an execution Variant column. | 已证实 | With an extended schema, `_need_read_flat_leaves` selects `_build_read_plan_flat_leaves`; regular paths use `LEAF`, sparse paths use `SPARSE_MERGE`, and doc buckets use `DOC_COMPACT`. Root, doc buckets, and heterogeneous regular-path fallbacks can be `ColumnVariant`; concrete regular leaves and sparse maps are not. | `SchemaUtilRowsetTest.*`, doc-compact reader/writer tests; horizontal/vertical matrix remains in T5.4. |
| §5 item 1: the root Variant enters the compaction Block and follows normal `append_data`, requiring no separate adapter. | 已证伪 | Ordered compaction has no Block. Unordered non-extended root uses `HIERARCHICAL`/`HIERARCHICAL_DOC`; extended root uses `ROOT_FLAT`; NG can delegate to the streaming writer rather than the ordinary buffered path. V2 writer adaptation is not proven by the current legacy route. | Existing root/doc/streaming UT are partial; C01–C06 in §7 close the integration gap. |
| §5 item 2: `VariantCompactionUtil::get_extended_compaction_schema` is unchanged. | 已证实 | Local `CompactionMixin::build_basic_info` invokes it for configured unordered compaction; Cloud `CloudCompactionMixin::build_basic_info` invokes it whenever the switch is enabled. It aggregates segment metadata through `aggregate_variant_extended_info`, constructs a temporary schema, and records path-set information; it does not require an execution-column adapter or change persistence. | `SchemaUtilTest.TestGetCompactionSchema`, `SchemaUtilRowsetTest.collect_path_stats_and_get_extended_compaction_schema`, and the uid-0 vertical merge test. Cloud E2E is C08. |
| §5 item 3: vertical grouping and `aggregate_variant_extended_info` are unchanged. | 已证实 | The aggregator is a segment-reader/schema-metadata consumer used by `get_extended_compaction_schema`. `Merger::vertical_split_columns` groups the resulting root and concrete extracted columns, then `vertical_compact_one_group` transports them with `add_columns`. | Existing vertical rowset/uid-0 tests cover a subset; doc, bucket sparse, horizontal-with-extended and exact grouping remain in T5.4. |
| Streaming writer must consume V2 and share shredder path/budget logic. | 待验证 | The baseline still consumes legacy `ColumnVariant`. T5.7 must change only the input/building adapter and reuse the T5.5 `PathBuilder` plus the same path ranking/budget selection. A second streaming-only path planner or budget implementation is prohibited. | Existing focused streaming UT proves the old incremental route only. NG integration is user-deferred. |
| T5.4 matrix covers ordered/unordered, horizontal/vertical, doc/NG/bucket sparse and path-stat checks. | 待验证 | The known chain is `VariantColumnReader::_build_read_plan -> _create_iterator_from_plan -> Merger::{vmerge_rowsets,vertical_merge_rowsets} -> RowsetWriter`; existing tests are not a deterministic full matrix. §7 is the required driver input. NG is listed but user-deferred, not counted as current completion. | Add the rows in §7 under T5.4/T5.9 ownership. |

## 4. Revision-49 `03 §4` consumer delta

| 03 §4 consumer statement | Status | Audited conclusion and direct call chain | Existing test evidence / gap |
|-|-|-|-|
| Inverted-index construction/search is based on typed storage subcolumns and is unchanged. | 待验证 | Writing resolves inherited indexes while preparing physical subcolumn writers; reading binds extracted paths in `SegmentIterator` through `VariantColumnReader::find_subcolumn_tablet_indexes`; search then uses the bound index iterator. A schema index difference makes schema change direct via `has_schema_index_diff`. These are storage-schema consumers, not permission to change persistence. | Existing subcolumn-index and function-search UT plus Variant index regressions exist; post-compaction and all three schema-change routes need I01–I03. |
| Schema change obtains V2 compatibility automatically through read/write boundaries. | 已证伪 | Linked schema change bypasses both boundaries and links the rowset. Direct and sorting changes do cross reader/Block/writer boundaries, but the baseline creates legacy `ColumnVariant`, and no V2 cutover integration is proven. Each strategy needs separate acceptance. | Existing schema-change regressions are partial; S01–S05 are required. |
| `get_least_common_schema` / rowset-meta merge is pure schema handling. | 已证实 | `VariantCompactionUtil::get_extended_compaction_schema -> aggregate_variant_extended_info` consumes TabletSchema, path statistics and segment metadata; it does not require a runtime `ColumnVariant` instance for schema aggregation. | Existing `SchemaUtilTest` and `SchemaUtilRowsetTest` coverage. |
| MoW delete bitmap and key-column behavior is unaffected because Variant is not a key. | 待验证 | `SchemaChangeJob::_do_process_alter_tablet -> _convert_historical_rowsets -> _calc_delete_bitmap_for_mow_table` keeps post-conversion MoW catch-up outside Variant slot construction; compaction row-id conversion is likewise separate. This audit found no Variant key route, but no focused V2 storage regression closes the end-to-end claim. | S05/T5.4 must cover delete predicates and MoW catch-up without treating skipped `check_path_stats` as success evidence. |
| Compaction `check_path_stats` is a path-stat reconciliation consumer. | 已证实 | Ordered link calls it after `manual_build`; rewritten compaction calls it from `Compaction::check_correctness`. It performs an actual comparison only under its supported conditions, notably DUP_KEYS without delete predicates and with path-stat recording enabled. | Positive comparison: `SchemaUtilRowsetTest.collect_path_stats_and_get_extended_compaction_schema` and `typed_path_to_sparse_column`; negative comparison: `check_path_stats_agg_delete`. `check_path_stats_agg_key` is only a non-DUP early-return test. |
| `check_before_quit` is a path-stat reconciliation consumer. | 已证伪 | `regression-test/suites/check_before_quit/check_before_quit.groovy` is selected last by the regression framework and recreates/cleans tables. It does not call or inspect `VariantCompactionUtil::check_path_stats` and supplies no path-stat oracle. | Do not count suite completion as path-stat coverage; use P01–P03. |

## 5. Schema-change chain

The BE conversion chain is:

```text
SchemaChangeJob::process_alter_tablet
  -> SchemaChangeJob::_do_process_alter_tablet
       -> capture rowset readers
       -> compute return_columns before merge_dropped_columns
       -> RowsetReaderContext.reader_type = READER_ALTER_TABLE
  -> SchemaChangeJob::_convert_historical_rowsets
       -> SchemaChangeJob::parse_request
       -> SchemaChangeJob::_get_sc_procedure
```

The constructor copies the new tablet schema without Variant extracted columns. The reader must
therefore reconstruct the complete Variant root; temporary extracted compaction slots are not part
of the persisted target schema.

Strategy-specific routes are:

| Strategy | Route | Variant consequence | Status |
|-|-|-|-|
| Linked | `LinkedSchemaChange::process -> add_rowset_for_linked_schema_change` | No Block, no Variant reader/writer conversion; existing segment files are linked. | 已证实 |
| Direct | `VSchemaChangeDirectly::_inner_process -> next_batch -> BlockChanger::change_block -> RowsetWriter::add_block` | Full root is read into the baseline execution column and written through the ordinary schema-change writer. | 已证实 |
| Sorting | `VBaseSchemaChangeWithSorting::_inner_process -> BlockChanger -> _internal_sorting -> MultiBlockMerger`; external merge uses `Merger::vmerge_rowsets` or `vertical_merge_rowsets` with `READER_ALTER_TABLE` | Intermediate and final rewrites use the target schema without persisted extracted columns. | 已证实 |

`TYPE_SCHEMA_CHANGE` does not satisfy the streaming-writer condition (`TYPE_COMPACTION`), so the
current schema-change rewrite uses the ordinary writer. Index differences are detected in
`SchemaChangeJob::parse_request` by `variant_util::has_schema_index_diff` and force a direct
rewrite. After historical conversion, MoW tablets run the existing delete-bitmap catch-up before
the new tablet becomes running.

## 6. Index and reconciliation touchpoints

### 6.1 Indexes

```text
write/rewrite
  -> physical Variant subcolumn selection
  -> inherit_index / prepare_subcolumn_writer_target
  -> concrete ColumnWriter::write_inverted_index

read/search
  -> SegmentIterator::init_iterators
  -> VariantColumnReader::infer_data_type_for_path
  -> VariantColumnReader::find_subcolumn_tablet_indexes
  -> variant_inverted_index_search / function-search index evaluation
```

File-level `Compaction::construct_index_compaction_columns` and
`Compaction::do_inverted_index_compaction` are a separate optimization. They must not be used as
proof that Variant path indexes were rebuilt or rebound correctly; T5.4 checks both logical path
binding and results before/after rewrite.

### 6.2 `check_path_stats`

The function is not a universal oracle. It returns without comparing in several intentional cases,
including no Variant columns, disabled path-stat recording, extracted columns in inputs, non-DUP
key models, delete predicates, and doc-mode outputs. T5.4 must record whether the comparison branch
was actually eligible; a returned OK from a skip branch is not proof of preservation.

### 6.3 `check_before_quit`

The suite is useful as a global cleanup/recreate guard only. It has no direct production caller and
no path-stat assertion, so it must remain separate from P01–P03.

## 7. Deterministic T5.4 regression matrix

All result queries must use `order_qt_*` or explicit `ORDER BY`; tables must be dropped before
setup; expected failures must use `test { sql; exception }`; `.out` files must be generated by the
regression runner.

| ID | Required case and forced route | Deterministic oracle | Owner/status |
|-|-|-|-|
| C01 | ordered link, regular non-doc Variant | Prove ordered link was selected; identical row count and ordered canonical string rows; path stats eligible and preserved. | T5.4 / 待验证 |
| C02 | unordered horizontal, non-extended, non-doc | Force `HIERARCHICAL`; compare all rows and typed/sparse paths before/after. | T5.4 / 待验证 |
| C03 | unordered horizontal with extended schema | Force root `ROOT_FLAT`, concrete and heterogeneous-fallback `LEAF`, plus sparse `SPARSE_MERGE`; compare rows, target slot types and footer/path inventory. | T5.4 / 待验证 |
| C04 | unordered vertical with extended schema and single sparse column | Assert grouped root/typed/map outputs, row equality and eligible `check_path_stats`. | T5.4 / 待验证 |
| C05 | unordered vertical with bucketized sparse columns | Exercise every bucket with fixed keys; compare per-bucket inventory and reconstructed rows. | T5.4 / 待验证 |
| C06 | doc mode with extended schema | Exercise `ROOT_FLAT` plus every `DOC_COMPACT` bucket; compare reconstructed rows and materialized paths. | T5.4 / 待验证 |
| C07 | NG streaming compaction | Full production compaction must prove streaming selection and output equivalence. | user-deferred T5.9 / 待验证; not current gate |
| C08 | Cloud unordered horizontal/vertical with extension switch on/off | Prove `CloudCompactionMixin::build_basic_info` schema selection and common merge output; compare rows and path inventory. | T5.4 Cloud / 待验证 |
| S01 | linked schema change | Prove linked strategy and byte/row equivalence without claiming a Block conversion. | T5.4 / 待验证 |
| S02 | direct schema change | Force Variant column/index change; prove `READER_ALTER_TABLE`, result rows, nullability and index results. | T5.4 / 待验证 |
| S03 | sorting schema change | Force key reorder/type strategy and both internal/external sorting; compare deterministic rows. | T5.4 / 待验证 |
| S04 | concurrent/double-write schema change | Fixed inserts during RUNNING; compare exact key set and Variant values after publish. | T5.4 / 待验证 |
| S05 | dropped-column delete predicate and MoW catch-up | Prove dropped predicate evaluation, final rows and delete bitmap; do not count skipped path stats. | T5.4 / 待验证 |
| I01 | parent Variant inverted index across compaction | Same search result/profile binding before and after horizontal and vertical rewrite. | T5.4 / 待验证 |
| I02 | typed exact/glob path index across schema change | Same exact path, suffix and query result after direct rewrite. | T5.4 / 待验证 |
| I03 | sparse/unmaterialized path | Explicitly prove supported fallback behavior; never claim a physical index that was not built. | T5.4 / 待验证 |
| P01 | eligible DUP_KEYS path-stat comparison | Input/output counts equal and comparison branch is eligible. | T5.4 / 待验证 |
| P02 | truncated statistics and multi-segment output | Exercise the bounded comparison rules deterministically. | T5.4 / 待验证 |
| P03 | skip branches | One case each for delete predicate, non-DUP and doc mode; assert the reason is a skip, not successful reconciliation. Existing `check_path_stats_agg_key` proves only the non-DUP early return and is not positive reconciliation evidence. | T5.4 / 待验证 |

The matrix is intentionally scenario-based rather than a full Cartesian product. It covers every
required route once while sharing fixtures for typed, sparse, doc, index and path-stat assertions.

## 8. Bloat and reuse decision

The current storage Variant code already has several closely related planning/building surfaces:

- `VariantCompactionUtil` selects materialized and sparse paths from persisted statistics;
- `VariantColumnWriterImpl` has ordinary root/subcolumn/sparse/doc pipelines;
- `VariantStreamingCompactionWriter` independently streams root and regular subcolumns;
- T5.5 will introduce the native V2 shredder.

The main duplication risk is making T5.7 a second V2 shredder. The required boundary is:

1. T5.5 owns the reusable `PathBuilder`, path type promotion, materialized/sparse ranking and budget
   decision.
2. T5.7 owns only incremental chunk/state management and physical writer flushing.
3. Both routes consume the same selection result and storage writer helpers.
4. No streaming-only path hash/ranking/budget implementation, no second persistent format, no
   legacy/V2 switch, and no temporary trie/`VariantMap` shim are allowed.

This refactor target meaningfully reduces duplicated path semantics and divergence risk; merely
moving the existing streaming functions or adding another adapter class would not.

## 9. Evidence index

| Topic | Stable production path and symbol | Existing test/source evidence | Executed status in this audit |
|-|-|-|-|
| local ordered link | `be/src/storage/compaction/compaction.cpp` — `CompactionMixin::do_compact_ordered_rowsets` | `be/test/storage/compaction/ordered_data_compaction_test.cpp` — `OrderedDataCompactionTest.test_01` (not Variant-specific) | not selected; C01 gap |
| common unordered merge | `be/src/storage/compaction/compaction.cpp` — `Compaction::merge_input_rowsets` | `be/test/storage/segment/variant_column_writer_reader_test.cpp` — `test_compaction_nokey_variant_uid0` | passed |
| horizontal/vertical transport | `be/src/storage/merger.cpp` — `Merger::vmerge_rowsets`, `vertical_merge_rowsets`, `vertical_compact_one_group` | `SchemaUtilRowsetTest.typed_path_to_sparse_column`; `VariantColumnWriterReaderTest.test_compaction_nokey_variant_uid0` | passed |
| Cloud setup | `be/src/storage/compaction/compaction.cpp` — `CloudCompactionMixin::execute_compact_impl`, `build_basic_info`, `construct_output_rowset_writer` | no focused Cloud Variant E2E identified | C08 gap |
| Variant read plans | `be/src/storage/segment/variant/variant_column_reader.cpp` — `_need_read_flat_leaves`, `_build_read_plan_flat_leaves`, `_build_read_plan`, `_create_iterator_from_plan` | `test_read_doc_compact_from_doc_value_bucket`, `test_write_doc_compact_writer_and_read_doc_compact` | passed |
| extended schema | `be/src/exec/common/variant_util.cpp` — `VariantCompactionUtil::aggregate_variant_extended_info`, `get_extended_compaction_schema` | `SchemaUtilRowsetTest.collect_path_stats_and_get_extended_compaction_schema`; `SchemaUtilTest.TestGetCompactionSchema` | passed |
| ordinary/extracted/doc writer dispatch | `be/src/storage/segment/column_writer.cpp` — `ColumnWriter::create`, `create_variant_writer`; `be/src/storage/segment/variant/variant_column_writer_impl.cpp` — `VariantColumnWriterImpl`, `VariantSubcolumnWriter`, `VariantDocCompactWriter` | `SchemaUtilRowsetTest.some_test_for_subcolumn_writer`; two doc-compact tests above | passed |
| NG streaming writer | `be/src/storage/segment/variant/variant_streaming_compaction_writer.cpp` — `_append_input`, `_append_chunk` | `test_streaming_write_plan_collects_regular_paths_from_rowset_metadata`; `test_streaming_compaction_writer_streams_regular_array_paths_across_batches` | both skipped: NG path unavailable; not coverage |
| schema-change dispatch | `be/src/storage/schema_change/schema_change.cpp` — `SchemaChangeJob::process_alter_tablet`, `_do_process_alter_tablet`, `_convert_historical_rowsets`, `parse_request` | `regression-test/suites/variant_p0/schema_change/schema_change.groovy`; `test_double_write_when_schema_change.groovy` | source inventory only; S01–S05 gap |
| Linked/Direct/Sorting | `be/src/storage/schema_change/schema_change.cpp` — `LinkedSchemaChange::process`, `VSchemaChangeDirectly::_inner_process`, `VBaseSchemaChangeWithSorting::_inner_process` | same schema-change regression inventory | not executed |
| path-index binding | `be/src/storage/segment/segment_iterator.cpp` — `SegmentIterator::init_iterators`; `variant_column_reader.cpp` — `find_subcolumn_tablet_indexes`; `be/src/exprs/function/variant_inverted_index_search.cpp` | `VariantColumnWriterReaderTest.test_find_subcolumn_tablet_indexes_*`; FunctionSearch Variant tests; Variant index regressions | not selected; I01–I03 gap |
| path-stat comparison | `be/src/exec/common/variant_util.cpp` — `VariantCompactionUtil::check_path_stats`; `compaction.cpp` — `do_compact_ordered_rowsets`, `Compaction::check_correctness` | positive `collect_path_stats_and_get_extended_compaction_schema`, `typed_path_to_sparse_column`; negative `check_path_stats_agg_delete`; skip-only `check_path_stats_agg_key` | all four passed with their stated roles |
| final regression cleanup | `regression-test/framework/src/main/groovy/org/apache/doris/regression/RegressionTest.groovy` — last-case selection; `regression-test/suites/check_before_quit/check_before_quit.groovy` | no path-stat assertion | not a reconciliation test |

## 10. Focused validation results

The audit commit changes Markdown only, so no BE build or formatter is required. On 2026-07-13,
the closest existing ASAN tests were run to validate the cited current behavior:

```bash
BUILD_TYPE_UT=ASAN ./run-be-ut.sh --run --filter='VariantColumnWriterReaderTest.test_read_doc_compact_from_doc_value_bucket:VariantColumnWriterReaderTest.test_write_doc_compact_writer_and_read_doc_compact:VariantColumnWriterReaderTest.test_streaming_write_plan_collects_regular_paths_from_rowset_metadata:VariantColumnWriterReaderTest.test_streaming_compaction_writer_streams_regular_array_paths_across_batches:VariantColumnWriterReaderTest.test_compaction_nokey_variant_uid0:SchemaUtilRowsetTest.*:SchemaUtilTest.TestGetCompactionSchema:SchemaUtilTest.get_extended_compaction_schema_nested_group_ignores_existing_extracted_subcolumns'
git diff --check
projects/column-variant-v2/scripts/validate_project.sh
```

Result: 13 selected tests from 3 suites; 11 passed, 2 skipped, 0 failed, and 0 disabled among the
selected filter. The skipped tests were:

- `VariantColumnWriterReaderTest.test_streaming_write_plan_collects_regular_paths_from_rowset_metadata`
- `VariantColumnWriterReaderTest.test_streaming_compaction_writer_streams_regular_array_paths_across_batches`

Both reported `NestedGroup write path is not available in this build`. They are not counted as
passing evidence and remain the user-deferred NG gap. The other 11 selected tests passed under
`ASAN_UT`.

Existing regression sources used only as coverage inventory, not claimed as executed by T0.3:

- `variant_p0/compaction/test_compaction.groovy`
- `variant_p0/compaction/test_compaction_nokey_variant.groovy`
- `variant_p0/schema_change/schema_change.groovy`
- `variant_p0/schema_change/test_double_write_when_schema_change.groovy`
- Variant predefined/index suites
- `check_before_quit/check_before_quit.groovy` (cleanup guard only)

Final document gates:

- `git diff --check`: passed.
- `projects/column-variant-v2/scripts/validate_project.sh`: passed at audit baseline
  `d6ce45ecba883188cb1175afb1d54cccf1355a6d` (44 task cards, 14 source snapshots).
- `git diff --cached --check`: passed.
- `git diff --cached --name-only`: exactly
  `docs/design/variant_v2/baseline/audit_compaction_sc.md`.

## 11. Audit outcome and status semantics

T0.3 can close in one document commit because the task delivers an audited data-flow map and a
deterministic T5.4 driver matrix; it does not implement the pending storage routes.

Marking T0.3 `done` means only:

- all revision-49 `03 §4/§5` judgments have one of the three allowed statuses;
- every conclusion names its direct code path and distinguishes existing tests from gaps;
- T5.4 has deterministic inputs for all current-scope gaps;
- NG remains visibly user-deferred.

It does not mean that T5.4, T5.5, T5.6, T5.7, T5.9, V2 cutover, performance, or any storage
implementation is complete.
