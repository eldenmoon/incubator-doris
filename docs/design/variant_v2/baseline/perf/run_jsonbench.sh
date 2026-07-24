#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: run_jsonbench.sh --label NAME --type jsonb|variant --dataset FILE --output DIR [options]

Options:
  --host HOST             FE host (default: 127.0.0.1)
  --query-port PORT       MySQL query port (default: 9030)
  --http-port PORT        FE HTTP port (default: 8030)
  --user USER             Doris user (default: root)
  --password PASSWORD     Doris password (default: empty)
  --database NAME         Database name (default: variant_v2_jsonbench)
  --dataset-repetitions N Repeat the NDJSON input N times per load (default: 1)
  --load-repetitions N    Fresh-table stream loads (default: 3)
  --query-repetitions N   Measured warm query runs (default: 5)
  --query-warmups N       Unmeasured warmups per query (default: 2)

The input must be the uncompressed NDJSON file from ClickHouse/JSONBench.
The final fresh table is intentionally retained for profile inspection.
EOF
}

label=
column_type=
dataset=
output_dir=
host=127.0.0.1
query_port=9030
http_port=8030
user=root
password=
database=variant_v2_jsonbench
dataset_repetitions=1
load_repetitions=3
query_repetitions=5
query_warmups=2

while [[ $# -gt 0 ]]; do
    case "$1" in
    --label)
        label="$2"
        shift 2
        ;;
    --type)
        column_type="$2"
        shift 2
        ;;
    --dataset)
        dataset="$2"
        shift 2
        ;;
    --output)
        output_dir="$2"
        shift 2
        ;;
    --host)
        host="$2"
        shift 2
        ;;
    --query-port)
        query_port="$2"
        shift 2
        ;;
    --http-port)
        http_port="$2"
        shift 2
        ;;
    --user)
        user="$2"
        shift 2
        ;;
    --password)
        password="$2"
        shift 2
        ;;
    --database)
        database="$2"
        shift 2
        ;;
    --dataset-repetitions)
        dataset_repetitions="$2"
        shift 2
        ;;
    --load-repetitions)
        load_repetitions="$2"
        shift 2
        ;;
    --query-repetitions)
        query_repetitions="$2"
        shift 2
        ;;
    --query-warmups)
        query_warmups="$2"
        shift 2
        ;;
    --help | -h)
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

if [[ -z "${label}" || -z "${column_type}" || -z "${dataset}" || -z "${output_dir}" ]]; then
    usage >&2
    exit 2
fi
if [[ "${column_type}" != jsonb && "${column_type}" != variant ]]; then
    echo "--type must be jsonb or variant" >&2
    exit 2
fi
if [[ ! -f "${dataset}" ]]; then
    echo "Dataset does not exist: ${dataset}" >&2
    exit 2
fi
for value in "${dataset_repetitions}" "${load_repetitions}" "${query_repetitions}" "${query_warmups}"; do
    if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
        echo "Repetition counts must be non-negative integers" >&2
        exit 2
    fi
done
if [[ "${dataset_repetitions}" -eq 0 ]]; then
    echo "--dataset-repetitions must be greater than zero" >&2
    exit 2
fi

command -v mysql >/dev/null
command -v curl >/dev/null
command -v python3 >/dev/null
command -v sha256sum >/dev/null

safe_label="$(printf '%s' "${label}" | tr -c '[:alnum:]_' '_')"
table="jsonbench_${safe_label}_${column_type}"
mkdir -p "${output_dir}/responses"
results="${output_dir}/raw.tsv"
environment="${output_dir}/environment.txt"
dataset_lines="$(wc -l <"${dataset}")"
dataset_bytes="$(stat -c %s "${dataset}")"
effective_dataset_lines="$((dataset_lines * dataset_repetitions))"
effective_dataset_bytes="$((dataset_bytes * dataset_repetitions))"

mysql_args=(
    --batch
    --skip-column-names
    --init-command="SET enable_sql_cache=false, enable_query_cache=false, query_cache_force_refresh=true"
    -h "${host}"
    -P "${query_port}"
    -u "${user}"
)
if [[ -n "${password}" ]]; then
    mysql_args+=("--password=${password}")
fi

run_sql() {
    mysql "${mysql_args[@]}" "$@"
}

now_ns() {
    date +%s%N
}

elapsed_ms() {
    python3 - "$1" "$2" <<'PY'
import sys
print(f"{(int(sys.argv[2]) - int(sys.argv[1])) / 1_000_000:.3f}")
PY
}

if [[ ! -f "${results}" ]]; then
    printf 'label\tcolumn_type\tphase\tcase\titeration\twall_ms\tserver_ms\trows\tbytes\tresult\tresult_sha256\n' >"${results}"
fi

{
    printf 'label=%s\n' "${label}"
    printf 'column_type=%s\n' "${column_type}"
    printf 'dataset=%s\n' "$(realpath "${dataset}")"
    printf 'dataset_bytes=%s\n' "${dataset_bytes}"
    printf 'dataset_lines=%s\n' "${dataset_lines}"
    printf 'dataset_sha256=%s\n' "$(sha256sum "${dataset}" | awk '{print $1}')"
    printf 'dataset_repetitions=%s\n' "${dataset_repetitions}"
    printf 'effective_dataset_bytes=%s\n' "${effective_dataset_bytes}"
    printf 'effective_dataset_lines=%s\n' "${effective_dataset_lines}"
    printf 'host=%s\nquery_port=%s\nhttp_port=%s\n' "${host}" "${query_port}" "${http_port}"
    printf 'database=%s\ntable=%s\n' "${database}" "${table}"
    printf 'load_repetitions=%s\nquery_warmups=%s\nquery_repetitions=%s\n' \
        "${load_repetitions}" "${query_warmups}" "${query_repetitions}"
    printf 'captured_at=%s\n' "$(date --iso-8601=seconds)"
    uname -a
    lscpu | grep -E '^(Model name|Socket\(s\)|Core\(s\) per socket|Thread\(s\) per core|CPU\(s\)):'
    run_sql -e 'SHOW BACKENDS' || true
} >"${environment}"

run_sql -e "CREATE DATABASE IF NOT EXISTS \`${database}\`;"

sql_type=VARIANT
if [[ "${column_type}" == jsonb ]]; then
    sql_type=JSONB
fi

create_fresh_table() {
    run_sql "${database}" <<SQL
DROP TABLE IF EXISTS \`${table}\`;
CREATE TABLE \`${table}\` (
    row_id BIGINT NULL,
    data ${sql_type} NOT NULL
)
ENGINE=OLAP
DUPLICATE KEY(row_id)
DISTRIBUTED BY RANDOM BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "compression" = "zstd"
);
SQL
}

for iteration in $(seq 1 "${load_repetitions}"); do
    create_fresh_table
    response_file="${output_dir}/responses/load_${iteration}.json"
    start_ns="$(now_ns)"
    curl_args=(
        --silent --show-error --fail --location-trusted
        -u "${user}:${password}"
        -H 'max_filter_ratio: 0'
        -H 'Expect:100-continue'
        -H 'columns: data'
        -X PUT
    )
    endpoint="http://${host}:${http_port}/api/${database}/${table}/_stream_load"
    if [[ "${dataset_repetitions}" -eq 1 ]]; then
        response="$(curl "${curl_args[@]}" -T "${dataset}" "${endpoint}")"
    else
        response="$({
            for ((copy = 0; copy < dataset_repetitions; copy++)); do
                cat "${dataset}"
            done
        } | curl "${curl_args[@]}" -T - "${endpoint}")"
    fi
    end_ns="$(now_ns)"
    printf '%s\n' "${response}" >"${response_file}"

    read -r status server_ms rows bytes < <(
        python3 - "${response_file}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    result = json.load(handle)
status = result.get("Status", "")
if status not in {"Success", "Publish Timeout"}:
    raise SystemExit(f"stream load failed: {result}")
print(status.replace(" ", "_"), result.get("LoadTimeMs", ""),
      result.get("NumberLoadedRows", ""), result.get("LoadBytes", ""))
PY
    )
    wall_ms="$(elapsed_ms "${start_ns}" "${end_ns}")"
    printf '%s\t%s\tload\tstream_load\t%s\t%s\t%s\t%s\t%s\t%s\t-\n' \
        "${label}" "${column_type}" "${iteration}" "${wall_ms}" "${server_ms}" "${rows}" "${bytes}" "${status}" >>"${results}"
done

loaded_rows="$(run_sql "${database}" -e "SELECT COUNT(*) FROM \`${table}\`;")"
if [[ "${loaded_rows}" != "${effective_dataset_lines}" ]]; then
    echo "Row-count mismatch: table=${loaded_rows} dataset=${effective_dataset_lines}" >&2
    exit 1
fi

if [[ "${column_type}" == jsonb ]]; then
    root_expr='CAST(data AS STRING)'
    shallow_expr="get_json_string(data, '\$.kind')"
    deep_expr="get_json_string(data, '\$.commit.operation')"
    group_expr="get_json_string(data, '\$.commit.collection')"
else
    root_expr='CAST(data AS STRING)'
    shallow_expr="CAST(data['kind'] AS STRING)"
    deep_expr="CAST(data['commit']['operation'] AS STRING)"
    group_expr="CAST(data['commit']['collection'] AS STRING)"
fi

query_names=(root shallow_path deep_path path_group)
queries=(
    "SELECT SUM(LENGTH(${root_expr})) FROM \`${table}\`"
    "SELECT COUNT(*) FROM \`${table}\` WHERE ${shallow_expr} = 'commit'"
    "SELECT COUNT(*) FROM \`${table}\` WHERE ${deep_expr} = 'create'"
    "SELECT COUNT(*) FROM (SELECT ${group_expr} AS event, COUNT(*) AS n FROM \`${table}\` GROUP BY event) grouped_events"
)

for index in "${!query_names[@]}"; do
    query_name="${query_names[${index}]}"
    query="${queries[${index}]}"
    for _ in $(seq 1 "${query_warmups}"); do
        run_sql "${database}" -e "${query}" >/dev/null
    done
    for iteration in $(seq 1 "${query_repetitions}"); do
        start_ns="$(now_ns)"
        result="$(run_sql "${database}" -e "${query}")"
        end_ns="$(now_ns)"
        wall_ms="$(elapsed_ms "${start_ns}" "${end_ns}")"
        result_sha="$(printf '%s' "${result}" | sha256sum | awk '{print $1}')"
        printf '%s\t%s\tquery\t%s\t%s\t%s\t-\t%s\t%s\t%s\t%s\n' \
            "${label}" "${column_type}" "${query_name}" "${iteration}" "${wall_ms}" "${loaded_rows}" \
            "${effective_dataset_bytes}" "${result}" "${result_sha}" >>"${results}"
    done
done

printf 'Results: %s\nEnvironment: %s\n' "${results}" "${environment}"
