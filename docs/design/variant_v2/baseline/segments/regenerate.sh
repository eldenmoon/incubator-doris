#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "${script_dir}/../../../../.." && pwd)
samples_dir="${script_dir}/samples"
generate_filter=VariantSegmentSnapshotTest.GenerateOrVerifyGolden
check_filter="${generate_filter}:VariantSegmentSnapshotTest.LegacyPersistedSegmentsRemainReadable"
legacy_checksums=LEGACY_SHA256SUMS
files=(
    ordinary_sparse.dat
    doc.dat
    bucketed_sparse.dat
    source.tsv
    manifest.tsv
)

usage() {
    echo "Usage: $0 --check|--update" >&2
    exit 2
}

mode=${1:---check}
[[ $# -le 1 ]] || usage

case "${mode}" in
--check)
    (
        cd -- "${repo_root}"
        env -u DORIS_REGEN_VARIANT_SEGMENT_SNAPSHOT_OUTPUT \
            BUILD_TYPE_UT=ASAN \
            ./run-be-ut.sh --run --filter="${check_filter}"
    )
    (cd -- "${script_dir}" && sha256sum --check SHA256SUMS)
    (cd -- "${script_dir}" && sha256sum --check "${legacy_checksums}")
    ;;
--update)
    # Legacy samples are an immutable old-persisted readability contract. Verify them before and
    # after regenerating the independent current-writer baseline; never install into their tree.
    (cd -- "${script_dir}" && sha256sum --check "${legacy_checksums}")
    outputs=()
    cleanup() { rm -rf -- "${outputs[@]}"; }
    trap cleanup EXIT
    for generation in one two; do
        output=$(mktemp -d "${TMPDIR:-/tmp}/variant-segment-snapshot.${generation}.XXXXXX")
        outputs+=("${output}")
        (
            cd -- "${repo_root}"
            BUILD_TYPE_UT=ASAN \
                DORIS_REGEN_VARIANT_SEGMENT_SNAPSHOT_OUTPUT="${output}" \
                ./run-be-ut.sh --run --filter="${generate_filter}"
        )
    done
    diff -ru --no-dereference "${outputs[0]}" "${outputs[1]}"
    mkdir -p -- "${samples_dir}"
    checksum_paths=()
    for file in "${files[@]}"; do
        cmp -- "${outputs[0]}/${file}" "${outputs[1]}/${file}"
        install -m 0644 -- "${outputs[0]}/${file}" "${samples_dir}/${file}"
        checksum_paths+=("samples/${file}")
    done
    (cd -- "${script_dir}" && sha256sum "${checksum_paths[@]}" >SHA256SUMS)
    (cd -- "${script_dir}" && sha256sum --check "${legacy_checksums}")
    ;;
*)
    usage
    ;;
esac
