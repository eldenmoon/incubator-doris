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

usage() {
    cat <<'USAGE'
Usage: regenerate.sh [--conf <regression-conf>] [--backend-host <host>]

The optional regression config may also be supplied through
DORIS_REGRESSION_CONF. The backend host may also be supplied through
DORIS_BACKEND_HOST and is required so stream-load redirects bypass proxies.
USAGE
}

regression_conf="${DORIS_REGRESSION_CONF:-}"
backend_host="${DORIS_BACKEND_HOST:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
    --conf)
        [[ $# -ge 2 ]] || {
            usage >&2
            exit 2
        }
        regression_conf="$2"
        shift 2
        ;;
    --backend-host)
        [[ $# -ge 2 ]] || {
            usage >&2
            exit 2
        }
        backend_host="$2"
        shift 2
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        echo "Unknown argument: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
done

[[ -n "${backend_host}" ]] || {
    echo "DORIS_BACKEND_HOST or --backend-host is required" >&2
    exit 2
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "${script_dir}" rev-parse --show-toplevel)"
cd "${repo_root}"

if [[ -n "${regression_conf}" && ! -f "${regression_conf}" ]]; then
    echo "Regression config does not exist: ${regression_conf}" >&2
    exit 2
fi

unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
export NO_PROXY="127.0.0.1,localhost,${backend_host}"
export no_proxy="${NO_PROXY}"

runner=(./run-regression-test.sh --run)
if [[ -n "${regression_conf}" ]]; then
    runner+=(--conf "${regression_conf}")
fi
runner+=(-d variant_p0 -s variant_semantics_snapshot)

"${runner[@]}" -forceGenOut
"${runner[@]}"

generated_out="regression-test/data/variant_p0/variant_semantics_snapshot.out"
sql_out="${script_dir}/sql.out"
relative_generated="../../../../../${generated_out}"

cp -- "${generated_out}" "${sql_out}"
cmp -- "${generated_out}" "${sql_out}"

checksum_tmp="$(mktemp "${script_dir}/.SHA256SUMS.XXXXXX")"
trap 'rm -f -- "${checksum_tmp}"' EXIT
(
    cd "${script_dir}"
    sha256sum internal.tsv sql.out "${relative_generated}"
) >"${checksum_tmp}"
mv -- "${checksum_tmp}" "${script_dir}/SHA256SUMS"
trap - EXIT
(
    cd "${script_dir}"
    sha256sum --check SHA256SUMS
)

echo "Regenerated ${generated_out}, sql.out, and SHA256SUMS"
