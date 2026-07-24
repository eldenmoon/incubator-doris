#!/usr/bin/env bash

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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DORIS_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

BUILD_DIR="${DORIS_ROOT}/be/build_RELEASE"
BINARY=""
OUTPUT_DIR="/tmp/variant_be_benchmark_$(date +%Y%m%d_%H%M%S)"
MIN_IDLE_PCT="80"
PRE_SAMPLE_SECONDS="5"
REPETITIONS="5"
MIN_TIME="0.5s"
BENCHMARK_FILTER='^BM_Variant'

usage() {
    echo "Usage: $0 [options]"
    echo
    echo "Build first with: BUILD_TYPE=RELEASE ./build.sh --benchmark"
    echo
    echo "Options:"
    echo "  --binary PATH              benchmark_test binary"
    echo "  --build-dir DIR            CMake build directory (default: be/build_RELEASE)"
    echo "  --output-dir DIR           new or empty result directory"
    echo "  --min-idle-pct PCT         minimum pre-run and whole-run CPU idle (default: 80)"
    echo "  --pre-sample-seconds SEC   pre-run CPU sample interval (default: 5)"
    echo "  --repetitions N            Google Benchmark repetitions (default: 5)"
    echo "  --min-time VALUE           Google Benchmark minimum time (default: 0.5s)"
    echo "  --filter REGEX             benchmark filter (default: ^BM_Variant)"
    echo "  --help                     show this help"
}

while (($# > 0)); do
    case "$1" in
    --binary)
        BINARY="$2"
        shift 2
        ;;
    --build-dir)
        BUILD_DIR="$2"
        shift 2
        ;;
    --output-dir)
        OUTPUT_DIR="$2"
        shift 2
        ;;
    --min-idle-pct)
        MIN_IDLE_PCT="$2"
        shift 2
        ;;
    --pre-sample-seconds)
        PRE_SAMPLE_SECONDS="$2"
        shift 2
        ;;
    --repetitions)
        REPETITIONS="$2"
        shift 2
        ;;
    --min-time)
        MIN_TIME="$2"
        shift 2
        ;;
    --filter)
        BENCHMARK_FILTER="$2"
        shift 2
        ;;
    --help)
        usage
        exit 0
        ;;
    *)
        echo "Unknown option: $1" >&2
        usage >&2
        exit 1
        ;;
    esac
done

if [[ -z "${BINARY}" ]]; then
    BINARY="${BUILD_DIR}/bin/benchmark_test"
fi

if [[ ! -x "${BINARY}" ]]; then
    echo "Benchmark binary is missing or not executable: ${BINARY}" >&2
    echo "Build it with: BUILD_TYPE=RELEASE ./build.sh --benchmark" >&2
    exit 1
fi

CMAKE_CACHE="${BUILD_DIR}/CMakeCache.txt"
if [[ ! -f "${CMAKE_CACHE}" ]]; then
    echo "CMake cache is missing: ${CMAKE_CACHE}" >&2
    echo "Build first with: BUILD_TYPE=RELEASE ./build.sh --benchmark" >&2
    exit 1
fi
CMAKE_BUILD_TYPE="$(awk -F '=' '/^CMAKE_BUILD_TYPE:STRING=/ {print $2}' "${CMAKE_CACHE}")"
BUILD_BENCHMARK="$(awk -F '=' '/^BUILD_BENCHMARK:BOOL=/ {print $2}' "${CMAKE_CACHE}")"
if [[ "${CMAKE_BUILD_TYPE}" != "RELEASE" ]] || [[ "${BUILD_BENCHMARK}" != "ON" ]]; then
    echo "Rejected: benchmark requires a RELEASE benchmark build, found" >&2
    echo "  CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-missing}" >&2
    echo "  BUILD_BENCHMARK=${BUILD_BENCHMARK:-missing}" >&2
    exit 1
fi

if [[ -n "${JAVA_HOME:-}" ]] && [[ -f "${JAVA_HOME}/lib/server/libjvm.so" ]]; then
    export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH:-}"
fi
MISSING_LIBRARIES="$(ldd "${BINARY}" | awk '/not found/ {print $1}' | paste -sd ',' -)"
if [[ -n "${MISSING_LIBRARIES}" ]]; then
    echo "Benchmark runtime libraries are missing: ${MISSING_LIBRARIES}" >&2
    echo "Set JAVA_HOME/LD_LIBRARY_PATH to the same runtime used by the Release build." >&2
    exit 1
fi

if [[ -d "${OUTPUT_DIR}" ]] && [[ -n "$(find "${OUTPUT_DIR}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Output directory must be new or empty: ${OUTPUT_DIR}" >&2
    exit 1
fi
mkdir -p "${OUTPUT_DIR}"

cpu_snapshot() {
    local user nice system idle iowait irq softirq steal
    read -r _ user nice system idle iowait irq softirq steal _ </proc/stat
    local idle_all=$((idle + iowait))
    local total=$((user + nice + system + idle + iowait + irq + softirq + steal))
    printf '%s %s\n' "${idle_all}" "${total}"
}

cpu_idle_between() {
    local before_idle="$1"
    local before_total="$2"
    local after_idle="$3"
    local after_total="$4"
    awk -v bi="${before_idle}" -v bt="${before_total}" -v ai="${after_idle}" -v at="${after_total}" \
        'BEGIN {
            delta_total = at - bt;
            if (delta_total <= 0) {
                print "0.00";
                exit;
            }
            printf "%.2f", 100.0 * (ai - bi) / delta_total;
        }'
}

sample_cpu_idle() {
    local seconds="$1"
    local before_idle before_total after_idle after_total
    read -r before_idle before_total < <(cpu_snapshot)
    sleep "${seconds}"
    read -r after_idle after_total < <(cpu_snapshot)
    cpu_idle_between "${before_idle}" "${before_total}" "${after_idle}" "${after_total}"
}

meets_idle_requirement() {
    local actual="$1"
    awk -v actual="${actual}" -v minimum="${MIN_IDLE_PCT}" \
        'BEGIN { exit !((actual + 0.0) >= (minimum + 0.0)) }'
}

sample_host_load() {
    printf 'timestamp\tload1\tload5\tload15\tcpu_idle_pct\n'
    while true; do
        local before_idle before_total after_idle after_total load1 load5 load15
        read -r before_idle before_total < <(cpu_snapshot)
        sleep 1
        read -r after_idle after_total < <(cpu_snapshot)
        read -r load1 load5 load15 _ </proc/loadavg
        printf '%s\t%s\t%s\t%s\t%s\n' \
            "$(date --iso-8601=seconds)" "${load1}" "${load5}" "${load15}" \
            "$(cpu_idle_between "${before_idle}" "${before_total}" \
                "${after_idle}" "${after_total}")"
    done
}

START_TIME="$(date --iso-8601=seconds)"
GIT_HEAD="$(git -C "${DORIS_ROOT}" rev-parse HEAD)"
BINARY_SHA256="$(sha256sum "${BINARY}" | awk '{print $1}')"
CPU_MODEL="$(awk -F ': ' '/model name/ {print $2; exit}' /proc/cpuinfo)"
LOGICAL_CPUS="$(getconf _NPROCESSORS_ONLN)"
KERNEL="$(uname -srmo)"
CPU_GOVERNOR="unavailable"
if [[ -r /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]]; then
    read -r CPU_GOVERNOR </sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
fi

SOURCE_FILES=(
    "be/benchmark/benchmark_main.cpp"
    "be/benchmark/benchmark_variant.hpp"
    "be/benchmark/run_variant_benchmark.sh"
    "be/src/core/column/variant_v2/column_variant_v2.cpp"
    "be/src/core/column/variant_v2/column_variant_v2_read_view.cpp"
    "be/src/core/column/variant_v2/column_variant_v2_typed_column.cpp"
    "be/src/core/data_type_serde/data_type_variant_v2_serde.cpp"
    "be/src/core/value/variant/variant_batch_builder.cpp"
    "be/src/exprs/function/cast/variant_v2/cast_array_to_variant.cpp"
    "be/src/exprs/function/cast/variant_v2/cast_variant_to_string.cpp"
    "be/src/exprs/function/function_variant_element_v2.cpp"
    "be/src/storage/segment/variant/variant_path_builder.cpp"
    "be/src/exprs/function/parse/variant_string_parse.cpp"
    "be/test/util/variant/variant_string_parse_test.cpp"
    "be/test/util/variant/variant_jsonb_parse_test.cpp"
)
git -C "${DORIS_ROOT}" status --short -- "${SOURCE_FILES[@]}" \
    >"${OUTPUT_DIR}/source_git_status.txt"
(
    cd "${DORIS_ROOT}"
    sha256sum "${SOURCE_FILES[@]}"
) >"${OUTPUT_DIR}/source_sha256.txt"
ps -eo pid,comm,%cpu,%mem --sort=-%cpu | sed -n '1,21p' >"${OUTPUT_DIR}/top_processes_before.tsv"

# Collect potentially cache- and CPU-active metadata above. This idle sample is intentionally the
# final substantive action before the benchmark gate so a stale, pre-hash quiet interval cannot
# admit a run on a newly busy host.
LOADAVG_BEFORE="$(</proc/loadavg)"
PRE_IDLE_PCT="$(sample_cpu_idle "${PRE_SAMPLE_SECONDS}")"

{
    printf 'key\tvalue\n'
    printf 'start_time\t%s\n' "${START_TIME}"
    printf 'git_head\t%s\n' "${GIT_HEAD}"
    printf 'binary\t%s\n' "${BINARY}"
    printf 'binary_sha256\t%s\n' "${BINARY_SHA256}"
    printf 'build_dir\t%s\n' "${BUILD_DIR}"
    printf 'cmake_build_type\t%s\n' "${CMAKE_BUILD_TYPE}"
    printf 'build_benchmark\t%s\n' "${BUILD_BENCHMARK}"
    printf 'java_home\t%s\n' "${JAVA_HOME:-unset}"
    printf 'kernel\t%s\n' "${KERNEL}"
    printf 'cpu_model\t%s\n' "${CPU_MODEL}"
    printf 'logical_cpus\t%s\n' "${LOGICAL_CPUS}"
    printf 'cpu_scaling_governor\t%s\n' "${CPU_GOVERNOR}"
    printf 'loadavg_before\t%s\n' "${LOADAVG_BEFORE}"
    printf 'pre_sample_seconds\t%s\n' "${PRE_SAMPLE_SECONDS}"
    printf 'pre_cpu_idle_pct\t%s\n' "${PRE_IDLE_PCT}"
    printf 'minimum_cpu_idle_pct\t%s\n' "${MIN_IDLE_PCT}"
    printf 'repetitions\t%s\n' "${REPETITIONS}"
    printf 'benchmark_min_time\t%s\n' "${MIN_TIME}"
    printf 'benchmark_filter\t%s\n' "${BENCHMARK_FILTER}"
} >"${OUTPUT_DIR}/environment.tsv"

if ! meets_idle_requirement "${PRE_IDLE_PCT}"; then
    printf 'accepted_pre_run\tfalse\n' >>"${OUTPUT_DIR}/environment.tsv"
    echo "Rejected: pre-run CPU idle ${PRE_IDLE_PCT}% is below ${MIN_IDLE_PCT}%." >&2
    echo "Environment was recorded in ${OUTPUT_DIR}/environment.tsv." >&2
    exit 2
fi
printf 'accepted_pre_run\ttrue\n' >>"${OUTPUT_DIR}/environment.tsv"

COMMAND=(
    "${BINARY}"
    "--benchmark_filter=${BENCHMARK_FILTER}"
    "--benchmark_repetitions=${REPETITIONS}"
    "--benchmark_min_time=${MIN_TIME}"
    "--benchmark_display_aggregates_only=true"
    "--benchmark_out=${OUTPUT_DIR}/benchmark.json"
    "--benchmark_out_format=json"
)
printf '%q ' "${COMMAND[@]}" >"${OUTPUT_DIR}/command.txt"
printf '\n' >>"${OUTPUT_DIR}/command.txt"

read -r RUN_BEFORE_IDLE RUN_BEFORE_TOTAL < <(cpu_snapshot)
sample_host_load >"${OUTPUT_DIR}/load_samples.tsv" &
SAMPLER_PID=$!
cleanup_sampler() {
    if kill -0 "${SAMPLER_PID}" 2>/dev/null; then
        kill "${SAMPLER_PID}" 2>/dev/null || true
        wait "${SAMPLER_PID}" 2>/dev/null || true
    fi
}
trap cleanup_sampler EXIT

set +e
"${COMMAND[@]}" 2>&1 | tee "${OUTPUT_DIR}/console.txt"
BENCHMARK_STATUS=${PIPESTATUS[0]}
set -e

cleanup_sampler
trap - EXIT
read -r RUN_AFTER_IDLE RUN_AFTER_TOTAL < <(cpu_snapshot)
RUN_IDLE_PCT="$(cpu_idle_between "${RUN_BEFORE_IDLE}" "${RUN_BEFORE_TOTAL}" \
    "${RUN_AFTER_IDLE}" "${RUN_AFTER_TOTAL}")"
LOADAVG_AFTER="$(</proc/loadavg)"
END_TIME="$(date --iso-8601=seconds)"
ps -eo pid,comm,%cpu,%mem --sort=-%cpu | sed -n '1,21p' >"${OUTPUT_DIR}/top_processes_after.tsv"
SAMPLE_SUMMARY="$(awk -F '\t' '
    NR > 1 {
        sum += $5;
        count += 1;
        if (count == 1 || $5 < minimum) {
            minimum = $5;
        }
    }
    END {
        if (count == 0) {
            printf "none";
        } else {
            printf "average=%.2f,min=%.2f,count=%d", sum / count, minimum, count;
        }
    }' "${OUTPUT_DIR}/load_samples.tsv")"

{
    printf 'end_time\t%s\n' "${END_TIME}"
    printf 'loadavg_after\t%s\n' "${LOADAVG_AFTER}"
    printf 'whole_run_cpu_idle_pct\t%s\n' "${RUN_IDLE_PCT}"
    printf 'per_second_cpu_idle_summary\t%s\n' "${SAMPLE_SUMMARY}"
    printf 'benchmark_exit_status\t%s\n' "${BENCHMARK_STATUS}"
} >>"${OUTPUT_DIR}/environment.tsv"

if ((BENCHMARK_STATUS != 0)); then
    printf 'accepted_run\tfalse\n' >>"${OUTPUT_DIR}/environment.tsv"
    echo "Benchmark failed with status ${BENCHMARK_STATUS}; results retained in ${OUTPUT_DIR}." >&2
    exit "${BENCHMARK_STATUS}"
fi

if ! meets_idle_requirement "${RUN_IDLE_PCT}"; then
    printf 'accepted_run\tfalse\n' >>"${OUTPUT_DIR}/environment.tsv"
    echo "Benchmark completed, but whole-run CPU idle ${RUN_IDLE_PCT}% is below ${MIN_IDLE_PCT}%." >&2
    echo "Results are retained but must not be used as the baseline: ${OUTPUT_DIR}" >&2
    exit 3
fi

printf 'accepted_run\ttrue\n' >>"${OUTPUT_DIR}/environment.tsv"
echo "Accepted Variant benchmark run: ${OUTPUT_DIR}"
