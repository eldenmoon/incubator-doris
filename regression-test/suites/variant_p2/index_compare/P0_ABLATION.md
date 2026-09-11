# Root / AllValues functional ablation

Control: `811aa64cc7483c97c10b9c49c41f135f27694ae9`, based on PR #10 HEAD
`d89a0ac1d178fa3fc3608a5bff3e64fa93c0a4d5`. Run in the isolated
`/tmp/doris-variant-root-p0-20260907` worktree with ASAN. This is a functional
and code-complexity experiment, not a Release performance comparison.

| Experiment | Evidence | Decision |
| --- | --- | --- |
| Unmodified control | 46 BE tests pass. | Control. |
| Replace per-value AllValues prefix/value wrapper with owned strings and one document-local prefix | Same 46 BE tests pass, including direct postings and document/NULL counts. | Retain; remove one struct and one prefix string per buffered scalar. |
| Also remove array recursion from writer | Direct postings test fails: exact `database`, `doris`, `leafvalue` postings are missing and token `secretkey` incorrectly hits row 1. | Reject; restore recursive scalar semantics. |
| Restore recursion, reuse codec type family for reader eligibility, remove family forwarding function and factor analyzer condition | 73 BE tests pass, including index selection, COUNT fallback and cache/NULL domain. | Retain; eliminate duplicate type list and forwarding function. |

The rejected variant is never deployed to the SQL cluster. Expected outputs
are unchanged. Shared prefixes and serialized values remain alive until the
existing synchronous SNII `add_document` returns; path prefixes retain their
owning strings. Analyzer consumption and rowid advancement remain in SNII.

The type-set comparison checks the old explicit scalar types plus
`is_string_type` against `query_value_family`; sparse VARIANT eligibility still
depends on the selected `BinaryColumnExtractIterator`. Floating AllValues EQ,
`ignore_above`, legacy-format fallback, residual MATCH permission and candidate
COUNT/negation protections are retained.

## Reproduction

Use the repository test scripts with the worktree environment initialized:

```bash
./run-be-ut.sh --run --filter='*VariantRoot*:FunctionMatchTest.*'
./run-be-ut.sh --run --filter='VariantRootIndexWriterTest.AllValuesIndexesRootScalarsArraysAndCrossPathTokens'
./run-be-ut.sh --run --filter='*VariantRoot*:FunctionMatchTest.*:InvertedIndexIteratorTest.*:SniiIndexReaderCountFallback.RootPreparedTermsReuseCommonCacheAndNullDomain'
./build.sh --be
```

The negative experiment deletes only the ARRAY branch in `append_all_values`,
then restores it before the final combined run. SQL validation uses the six
suites listed below, with the fresh BE, isolated ports and existing FE. No
`-forceGenOut` is used:

```bash
./run-regression-test.sh --run --conf tmp/regression-conf.auto.groovy \
  -d variant_p0/v2/with_index \
  -s test_variant_all_values_p0_contract,test_variant_all_values_float_equality,test_variant_all_values_legacy_format,test_variant_all_values_mow_lifecycle,test_variant_all_values_index_correctness,test_variant_root_index_correctness \
  -parallel 1
```

These suites compare ordered IDs and COUNT with index queries enabled/disabled,
and enforce index filtering or safe fallback where intended. They cover
materialized/sparse paths, analyzer/cache isolation, MOW updates/deletes and
compaction. The final six suites pass (zero failed, fatal or skipped scripts); BE build,
clang-format 16 and header hygiene pass. FE sources are unchanged, so FE unit
tests are not rerun for this ablation. Clang-tidy fails on baseline diagnostics, including unmatched `NOLINTEND` in
`be/src/core/types.h:576` and existing function-size warnings. No diagnostic
intersects an added line; this is not a clang-tidy pass. No throughput, latency or total-storage improvement is claimed.

Local raw evidence: `tmp/ablation/` (control, each ablation, final UT, build,
SQL verification, binary/source hashes and self-review). Formal storage/CPU
and complementary-layout evaluation remain separate work.
