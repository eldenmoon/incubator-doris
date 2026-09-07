# VARIANT Root / AllValues query contract

Scope: experimental PR #10. Capacity and latency recommendations require a fresh
Release benchmark with matching row fingerprints; ASAN correctness runs are not
performance evidence.

## Values and SQL behavior

Whole-root MATCH visits scalar leaves recursively through objects and arrays.
Object keys and container syntax are not searchable values. Strings contribute
their contents without JSON quotes; booleans and numbers use Doris' existing
VARIANT scalar JSON formatter. These textual representations are not a numeric
equality encoding: for example, textual negative zero may differ from zero.

MATCH_ANY accepts any query token in any leaf. MATCH_ALL requires every query
token somewhere in the row and may match across fields or array elements. It
does not express same-element association. SQL NULL retains SQL null behavior;
JSON null, missing values and empty containers contribute no terms. Empty
strings contribute an empty value in the exact domain and no English tokens.
Analyzer properties continue to define case handling and tokenization.

Doris path normalization applies to dotted names; the tested layouts reject a
document containing both `a.b` and an overlapping nested `a.b` path.

An explicit CAST applies SQL cast semantics before MATCH. In particular,
CAST(array/object AS STRING) searches the resulting string; it must not silently
inherit the leaf-only contract of whole-root VARIANT MATCH. A sparse VARIANT
reader whose cast cannot use an index retains scan evaluation.

| Query | Index behavior |
| --- | --- |
| Whole-root MATCH_ANY / MATCH_ALL | AllValues v2 can answer from its whole-root value/token domain. |
| Path MATCH using Root or AllValues | Candidate set followed by the original scalar predicate. |
| Path numeric EQ / IN | Existing physical-type and cast compatibility gates apply. Root keeps its canonical numeric domain. AllValues FLOAT/DOUBLE equality falls back to scalar evaluation. |
| Declared typed path in doc layout | Parent Root/AllValues postings are bypassed because doc retains original values while a materialized path can be converted. Direct child indexes retain their existing rules. Whole-root AllValues remains usable. |
| Exact value longer than `ignore_above` | Scan fallback, using the serialized byte length for AllValues values. One unsupported IN element makes the whole predicate fall back. |
| NOT, inequality, NOT IN and boolean combinations | A candidate superset cannot be complemented or counted as final matches. Existing residual protections remain. |
| MATCH without a usable index | Requires `enable_match_without_inverted_index=true`. This round validates only `true`; candidate-residual behavior with `false`, including OR short-circuiting, remains unresolved. |

Ordinary typed paths are indexed after the existing storage conversion, so a
stored INT `1` converted from `"001"` has the same numeric postings as INT `1`.
Doc layout instead retains its original whole-root values, including failed
path casts. This distinction follows existing storage reads.

Ordinary scalar/path phrase and regexp evaluation retains its existing scalar
conversion. Array phrase evaluation stays inside each scalar element; ANY/ALL
keep their cross-element token semantics. New whole-root phrase, scoring and
nested element correlation remain outside this change.

## Stored format and lifecycle

New AllValues indexes use the existing `variant_root_format_version` property
with value `2`, identifying recursive scalar leaves in arrays. Root remains at
version `1`; term tags and postings encoding are unchanged.

Reload experimental tables whose typed-path indexes were built before the
storage-conversion fix; the new writer does not repair existing postings.

Deploy the updated FE and BE before creating v2 indexes; mixed old/new binary
rollout is not covered by the local validation.

Legacy AllValues v1 indexes may contain serialized arrays and object-key tokens.
Their queries fall back before using postings. Existing tables keep their stored
version through writes and compaction; compaction does not silently claim a
format upgrade. Recreate/load a table with the new default to obtain v2 indexes
(the experimental PR does not support ALTER BUILD/DROP of a Root index).

## Evaluation gate

Preserve DDL, source and binary hashes, analyzer settings, materialization,
caches, CPU affinity/quota, logical row fingerprints and compaction state for
every layout. Compare ordered IDs or complete stable row fingerprints before
including a query in latency comparisons. Record candidate rows, final rows and
COUNT separately. Zero-hit queries report candidate rows directly.

Candidate configurations for subsequent measurement are existing Root,
AllValues and Children layouts, Root exact plus AllValues text, and small hot
path coverage. No capacity or performance default is selected here. Separate
exact/text index bytes, dictionaries, postings and metadata before evaluating
full duplication. The historical `the` discrepancy still needs its original
result manifest; this contract does not infer a cause from stopword assumptions.
