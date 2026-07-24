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

readonly EXPECTED_SHA256="daecf8161e7bba63f7ba9fd62c1e8a77730c9a9d76a335191dc9d0a0fcaaec52"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
readonly DEFAULT_JAR="${HOME}/.m2/repository/org/apache/parquet/parquet-variant/1.17.0/parquet-variant-1.17.0.jar"
readonly PARQUET_JAR="${PARQUET_VARIANT_JAR:-${DEFAULT_JAR}}"
readonly MODE="${1:---check}"

case "${MODE}" in
--check | --update | --extended) ;;
*)
    echo "usage: $0 [--check|--update|--extended]" >&2
    exit 2
    ;;
esac

if [[ ! -f "${PARQUET_JAR}" ]]; then
    echo "missing parquet-variant jar: ${PARQUET_JAR}" >&2
    exit 1
fi

actual_sha256="$(sha256sum "${PARQUET_JAR}" | awk '{print $1}')"
if [[ "${actual_sha256}" != "${EXPECTED_SHA256}" ]]; then
    echo "unexpected parquet-variant jar SHA256: ${actual_sha256}" >&2
    echo "expected: ${EXPECTED_SHA256}" >&2
    exit 1
fi

tmp_dir="$(mktemp -d "${SCRIPT_DIR}/.regenerate.XXXXXX")"
trap 'rm -rf "${tmp_dir}"' EXIT
mkdir -p "${tmp_dir}/classes" "${tmp_dir}/generated"

javac -encoding UTF-8 -cp "${PARQUET_JAR}" -d "${tmp_dir}/classes" \
    "${SCRIPT_DIR}/ParquetVariantGolden.java"

if [[ "${MODE}" == "--extended" ]]; then
    java -Xmx512m -cp "${tmp_dir}/classes:${PARQUET_JAR}" ParquetVariantGolden extended
    echo "verified real metadata and value-offset 3-to-4-byte boundaries"
    exit 0
fi

java -Xmx512m -cp "${tmp_dir}/classes:${PARQUET_JAR}" \
    ParquetVariantGolden generate "${tmp_dir}/generated"

readonly CORPORA=(
    parquet_java_vectors.tsv
    doris_java_verified_vectors.tsv
)

if [[ "${MODE}" == "--update" ]]; then
    for corpus in "${CORPORA[@]}"; do
        cp "${tmp_dir}/generated/${corpus}" "${SCRIPT_DIR}/${corpus}"
    done
    echo "updated ${#CORPORA[@]} deterministic Variant golden corpora"
    exit 0
fi

for corpus in "${CORPORA[@]}"; do
    if [[ ! -f "${SCRIPT_DIR}/${corpus}" ]]; then
        echo "missing checked-in corpus: ${SCRIPT_DIR}/${corpus}" >&2
        echo "run $0 --update after reviewing the generator" >&2
        exit 1
    fi
    if ! cmp "${tmp_dir}/generated/${corpus}" "${SCRIPT_DIR}/${corpus}"; then
        echo "golden corpus is stale: ${corpus}" >&2
        echo "run $0 --update and review the exact diff" >&2
        exit 1
    fi
done

echo "verified ${#CORPORA[@]} deterministic Variant golden corpora"
