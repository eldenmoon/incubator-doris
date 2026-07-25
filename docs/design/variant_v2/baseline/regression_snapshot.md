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

# Variant regression snapshot

This is the T0.1 comparison set for the M2 regression gate. It records what was actually exercised,
including environment failures, rather than treating an unexecuted or externally blocked suite as
green.

## Environment

- Run date: 2026-07-20.
- Build: Release, FE `99d3dd1ba37`, BE
  `50077170ee9322a5428bf17cb7b9cd6ae0aa8eff`.
- Topology: one local FE and one local BE, replication 1.
- FE local-file outfile was enabled only for the targeted semantics-snapshot retry. The shared OSS
  access key in the regression configuration was disabled at run time.

The common command form was:

```shell
./run-regression-test.sh --conf <local-release-regression-conf> --run -d <suite-directory>
```

## Snapshot

| Directory or test | Executed result | Classification | Local log |
|-|-|-|-|
| `variant_p0` | Full run: 147/150 suites passed. Targeted environment-corrected retry of `variant_semantics_snapshot`: 1/1 passed, so 148/150 suites have a passing result on this build. | Two suites remain non-green; details below. | `doris-regression-test.20260720.034520.log`; retry `doris-regression-test.20260720.035124.log` |
| `jsonb_p0` | 12/13 suites passed. | One external credential failure; details below. | `doris-regression-test.20260720.035202.log` |
| `variant_p1` | 4/4 suites passed. | Public GHArchive load, root extraction, and compaction coverage all completed. | `doris-regression-test.20260720.035227.log` |
| `variant_p2` | Not run in this local P0 snapshot. | Capacity suite, not a bounded local regression: `load.groovy` submits January--March 2015 hourly files through 20 workers, while `performance.groovy` creates 20 million wide Variant rows. It requires a provisioned benchmark environment and external data credentials. | n/a |
| `ColumnVariantV2Test.*` | 38/38 passed under ASAN. | Codec/column correctness gate. | BE UT output retained with the T2.6 task evidence. |

### Non-green cases

1. `variant_p0/column_name.groovy` expects the legacy diagnostic `Duplicate Variant object key`.
   The implementation rejects the conflicting input with
   `Variant paths with distinct structures collide at dotted path a.b`. The rejection behavior is
   present, but the asserted diagnostic contract has drifted; update or deliberately preserve the
   expected message, then rerun before M2.
2. `variant_p0/doc_mode/test_outfile_csv_variant_type.groovy` failed while writing its OSS outfile:
   `InvalidAccessKeyId` / `FORBIDDEN`. This is an external test-infrastructure failure and did not
   exercise the expected outfile comparison.
3. The first `variant_semantics_snapshot` attempt found
   `enable_outfile_to_local=false`. After enabling it in the local Release FE and restarting, the
   same test passed 1/1. This case is counted as verified green above.
4. `jsonb_p0/test_json_reader_without_object.groovy` failed while listing its OSS input because the
   access key was disabled. The expected `DATA_QUALITY_ERROR` product path was never reached.

## M2 use

This file is a coverage snapshot, not a claim that the M2 gate is green. Before M2:

- rerun the complete `variant_p0` directory with local outfile enabled and a working OSS credential;
- resolve the `column_name` diagnostic expectation and require 150/150;
- rerun `jsonb_p0` and require 13/13;
- preserve the 4/4 `variant_p1` result when the M2 target build is rerun;
- provision the external dataset/capacity environment for `variant_p2`, or split out and explicitly
  approve a bounded correctness subset before using it as a gate.
