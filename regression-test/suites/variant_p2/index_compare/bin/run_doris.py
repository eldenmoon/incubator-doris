#!/usr/bin/env python3

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

import argparse
import base64
import csv
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import statistics
import subprocess
import time
import urllib.error
import uuid
from typing import Any

from benchmark_lib import (
    DorisHttp,
    EventLog,
    MysqlSession,
    mysql_tsv,
    proc_delta,
    proc_snapshot,
    read_validated_manifest,
    sha256_file,
    write_json,
)


LAYOUTS = ("no_index", "root", "all_values", "children")
LATIN_ORDERS = tuple(
    tuple(LAYOUTS[(start + offset) % len(LAYOUTS)] for offset in range(len(LAYOUTS)))
    for start in range(len(LAYOUTS))
)
ROWSET_PATTERN = re.compile(
    r"^\[(\d+)-(\d+)\] (\d+) (DATA|DELETE) ([A-Z_]+) ([0-9a-f]+) "
    r"(.+?) level=(\d+)$"
)
CPU_LIST_PATTERN = re.compile(
    r"(?:0|[1-9]\d*)(?:-(?:0|[1-9]\d*))?"
    r"(?:,(?:0|[1-9]\d*)(?:-(?:0|[1-9]\d*))?)*")
SPARSE_MAX_SELECTIVITY = 0.0001
DENSE_MIN_SELECTIVITY = 0.001
DENSE_MAX_SELECTIVITY = 0.5
QUERY_WARMUPS = 2
STABLE_CV_MAX = 0.10
STABLE_SPAN_MAX = 0.25
SNII_RAM_BUFFER_CONFIG = "inverted_index_ram_buffer_size"
FORMAL_WRITE_SNII_RAM_BUFFER_MIB = 512
FORMAL_COMPACTION_SNII_RAM_BUFFER_MIB = 2048


def parse_linux_cpu_list(value: str) -> tuple[int, ...]:
    if not CPU_LIST_PATTERN.fullmatch(value):
        raise ValueError(f"invalid Linux CPU list: {value!r}")
    cpus: set[int] = set()
    for item in value.split(","):
        first_text, separator, last_text = item.partition("-")
        first = int(first_text)
        last = int(last_text) if separator else first
        if last < first:
            raise ValueError(f"descending CPU range is not allowed: {item!r}")
        item_cpus = set(range(first, last + 1))
        overlap = cpus & item_cpus
        if overlap:
            raise ValueError(f"duplicate CPUs in {value!r}: {format_linux_cpu_list(overlap)}")
        cpus.update(item_cpus)
    if not cpus:
        raise ValueError("CPU list must not be empty")
    return tuple(sorted(cpus))


def format_linux_cpu_list(cpus: Any) -> str:
    ordered = sorted(set(cpus))
    if not ordered:
        return ""
    ranges: list[str] = []
    first = previous = ordered[0]
    for cpu in ordered[1:]:
        if cpu == previous + 1:
            previous = cpu
            continue
        ranges.append(str(first) if first == previous else f"{first}-{previous}")
        first = previous = cpu
    ranges.append(str(first) if first == previous else f"{first}-{previous}")
    return ",".join(ranges)


def cpu_list_argument(value: str) -> tuple[int, ...]:
    try:
        return parse_linux_cpu_list(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def validate_cpu_configuration(
        write_cpus: tuple[int, ...], query_cpus: tuple[int, ...]) -> dict[str, Any]:
    online = set(parse_linux_cpu_list(
        Path("/sys/devices/system/cpu/online").read_text(encoding="utf-8").strip()))
    allowed = set(os.sched_getaffinity(0))
    available = online & allowed
    write = set(write_cpus)
    query = set(query_cpus)
    if len(query) != 8:
        raise ValueError(f"query CPU list must contain exactly 8 CPUs, got {len(query)}")
    if len(write) > 64:
        raise ValueError(f"write CPU list must contain at most 64 CPUs, got {len(write)}")
    if not query <= write:
        raise ValueError(
            "query CPU list must be a subset of write CPU list: "
            f"query={format_linux_cpu_list(query)}, write={format_linux_cpu_list(write)}")
    for label, selected in (("write", write), ("query", query)):
        unavailable = selected - available
        if unavailable:
            raise ValueError(
                f"{label} CPU list contains offline or disallowed CPUs: "
                f"{format_linux_cpu_list(unavailable)}; "
                f"online={format_linux_cpu_list(online)}, "
                f"allowed={format_linux_cpu_list(allowed)}")

    topology: dict[str, dict[str, int]] = {}
    seen_cores: dict[tuple[int, int], int] = {}
    for cpu in sorted(write):
        topology_root = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        try:
            package = int((topology_root / "physical_package_id").read_text().strip())
            core = int((topology_root / "core_id").read_text().strip())
        except (FileNotFoundError, OSError, ValueError) as error:
            raise ValueError(f"cannot read topology for CPU {cpu}: {error}") from error
        key = (package, core)
        sibling = seen_cores.get(key)
        if sibling is not None:
            raise ValueError(
                "SMT siblings are not allowed in write CPU list: "
                f"CPU {sibling} and CPU {cpu} both map to package={package}, core={core}")
        seen_cores[key] = cpu
        topology[str(cpu)] = {"physical_package_id": package, "core_id": core}

    return {
        "online_cpus": format_linux_cpu_list(online),
        "allowed_cpus": format_linux_cpu_list(allowed),
        "write_cpus": format_linux_cpu_list(write),
        "write_cpu_ids": sorted(write),
        "query_cpus": format_linux_cpu_list(query),
        "query_cpu_ids": sorted(query),
        "topology": topology,
        "smt_siblings_selected": False,
    }


def _process_starttime_ticks(pid: int) -> int:
    fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
    if len(fields) <= 21:
        raise RuntimeError(f"invalid /proc stat for pid {pid}")
    return int(fields[21])


def process_listening_port_evidence(pid: int, ports: tuple[int, ...]) -> dict[str, Any]:
    socket_inodes: dict[int, set[str]] = {port: set() for port in ports}
    for protocol in ("tcp", "tcp6"):
        path = Path(f"/proc/{pid}/net/{protocol}")
        for line in path.read_text(encoding="utf-8").splitlines()[1:]:
            fields = line.split()
            if len(fields) <= 9 or fields[3] != "0A":
                continue
            local_port = int(fields[1].rsplit(":", 1)[1], 16)
            if local_port in socket_inodes:
                socket_inodes[local_port].add(fields[9])
    owned_inodes: set[str] = set()
    for descriptor in Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = os.readlink(descriptor)
        except (FileNotFoundError, OSError):
            continue
        match = re.fullmatch(r"socket:\[(\d+)\]", target)
        if match:
            owned_inodes.add(match.group(1))
    evidence = {
        str(port): {
            "listening_socket_inodes": sorted(socket_inodes[port]),
            "owned_listening_socket_inodes": sorted(socket_inodes[port] & owned_inodes),
        }
        for port in ports
    }
    missing = [port for port in ports if not evidence[str(port)]["owned_listening_socket_inodes"]]
    if missing:
        raise RuntimeError(
            f"pid {pid} does not own listening sockets for ports {missing}: {evidence}")
    return evidence


def resolved_ip_addresses(host: str) -> set[str]:
    try:
        return {
            item[4][0].split("%", 1)[0]
            for item in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        }
    except socket.gaierror as error:
        raise RuntimeError(f"cannot resolve host {host!r}: {error}") from error


def local_ip_addresses() -> set[str]:
    addresses = {"127.0.0.1", "::1"}
    hostnames = {"localhost", socket.gethostname()}
    for hostname in hostnames:
        try:
            addresses.update(resolved_ip_addresses(hostname))
        except RuntimeError:
            pass
    result = subprocess.run(
        ["hostname", "-I"], check=False, text=True, capture_output=True)
    if result.returncode == 0:
        addresses.update(item.split("%", 1)[0] for item in result.stdout.split())
    return addresses


def _task_ids(pid: int) -> tuple[int, ...]:
    return tuple(sorted(
        int(path.name) for path in Path(f"/proc/{pid}/task").iterdir()
        if path.name.isdigit()))


def _thread_cpu_mask(pid: int, tid: int) -> str:
    status = Path(f"/proc/{pid}/task/{tid}/status").read_text(encoding="utf-8")
    for line in status.splitlines():
        if line.startswith("Cpus_allowed_list:"):
            value = line.split(":", 1)[1].strip()
            return format_linux_cpu_list(parse_linux_cpu_list(value))
    raise RuntimeError(f"Cpus_allowed_list is missing for pid={pid}, tid={tid}")


def stable_thread_affinity_snapshot(
        role: str, pid: int, expected_starttime: int,
        configured_cpus: tuple[int, ...], timeout_seconds: float = 10.0) -> dict[str, Any]:
    configured = format_linux_cpu_list(configured_cpus)
    deadline = time.monotonic() + timeout_seconds
    previous: tuple[tuple[int, ...], tuple[tuple[int, str], ...]] | None = None
    attempts = 0
    last_error = "thread set did not stabilize"
    while time.monotonic() < deadline:
        attempts += 1
        try:
            starttime_before = _process_starttime_ticks(pid)
            tids_before = _task_ids(pid)
            masks = {tid: _thread_cpu_mask(pid, tid) for tid in tids_before}
            tids_after = _task_ids(pid)
            starttime_after = _process_starttime_ticks(pid)
        except (FileNotFoundError, ProcessLookupError, OSError) as error:
            previous = None
            last_error = str(error)
            time.sleep(0.05)
            continue
        if starttime_before != expected_starttime or starttime_after != expected_starttime:
            raise RuntimeError(
                f"{role} pid {pid} changed or was reused while checking affinity: "
                f"expected starttime={expected_starttime}, "
                f"observed={starttime_before}/{starttime_after}")
        if tids_before != tids_after:
            previous = None
            last_error = f"thread set changed: before={tids_before}, after={tids_after}"
            time.sleep(0.05)
            continue
        current = (tids_after, tuple(sorted(masks.items())))
        if current == previous:
            mismatched = {
                str(tid): mask for tid, mask in masks.items() if mask != configured
            }
            return {
                "role": role,
                "pid": pid,
                "starttime_ticks": expected_starttime,
                "configured_cpus": configured,
                "configured_cpu_ids": list(configured_cpus),
                "thread_count": len(masks),
                "thread_masks": {str(tid): mask for tid, mask in sorted(masks.items())},
                "unique_thread_masks": sorted(set(masks.values())),
                "mismatched_threads": mismatched,
                "enumeration_attempts": attempts,
                "stable_enumerations": 2,
            }
        previous = current
        time.sleep(0.05)
    raise RuntimeError(
        f"{role} pid {pid} thread set did not stabilize within {timeout_seconds}s: {last_error}")


def table_name(layout: str) -> str:
    return f"github_events_{layout}"


def index_clause(layout: str) -> str:
    if layout == "no_index" or layout.startswith("stage_"):
        return ""
    mode = "" if layout == "children" else f', "variant_index_mode" = "{layout}"'
    return f""",
    INDEX idx_payload_exact(payload) USING INVERTED PROPERTIES(
        "parser" = "none",
        "support_phrase" = "false"{mode}),
    INDEX idx_payload_english(payload) USING INVERTED PROPERTIES(
        "parser" = "english",
        "lower_case" = "true",
        "support_phrase" = "false"{mode})"""


def render_ddl(template: str, table: str, layout: str) -> str:
    result = template.replace("${TABLE}", table).replace("${INDEXES}", index_clause(layout))
    if "${" in result:
        raise RuntimeError(f"unexpanded DDL placeholder for {table}")
    return result


def session_settings() -> str:
    return """
        SET default_variant_enable_doc_mode = false;
        SET default_variant_max_subcolumns_count = 1024;
        SET default_variant_enable_typed_paths_to_sparse = false;
        SET enable_sql_cache = false;
        SET enable_query_cache = false;
        SET enable_condition_cache = false;
        SET enable_inverted_index_query_cache = false;
        SET enable_inverted_index_searcher_cache = true;
        SET enable_page_cache = true;
        SET inverted_index_skip_threshold = 50;
        SET enable_match_without_inverted_index = true;
        SET enable_count_on_index_pushdown = false;
    """


def select_hint(enable_index: bool) -> str:
    if enable_index:
        return "/*+ SET_VAR(enable_inverted_index_query=true, enable_count_on_index_pushdown=false) */"
    return (
        "/*+ SET_VAR(enable_inverted_index_query=false, "
        "enable_match_without_inverted_index=true, enable_count_on_index_pushdown=false) */"
    )


def query_signature_sql(table: str, predicate: str, enable_index: bool) -> str:
    return f"""
        SELECT {select_hint(enable_index)} COUNT(*),
               COALESCE(SUM(CAST(id AS LARGEINT)), 0),
               COALESCE(GROUP_BIT_XOR(id), 0),
               COALESCE(SUM(CAST({row_hash_expression()} AS LARGEINT)), 0),
               COALESCE(GROUP_BIT_XOR({row_hash_expression()}), 0)
        FROM `{table}` WHERE {predicate}
    """


def timed_count_sql(table: str, predicate: str, enable_index: bool, tag: str | None = None) -> str:
    comment = f"/* {tag} */" if tag else ""
    return (
        f"SELECT {select_hint(enable_index)} {comment} COUNT(*) "
        f"FROM `{table}` WHERE {predicate}"
    )


def row_hash_expression() -> str:
    return """XXHASH_64(
        CAST(id AS STRING),
        IF(type IS NULL, 'N', 'V'), COALESCE(type, ''),
        IF(actor IS NULL, 'N', 'V'), COALESCE(CAST(actor AS STRING), ''),
        IF(repo IS NULL, 'N', 'V'), COALESCE(CAST(repo AS STRING), ''),
        IF(payload IS NULL, 'N', 'V'), COALESCE(CAST(payload AS STRING), ''),
        IF(public IS NULL, 'N', 'V'), COALESCE(CAST(public AS STRING), ''),
        IF(created_at IS NULL, 'N', 'V'), COALESCE(CAST(created_at AS STRING), ''),
        IF(org IS NULL, 'N', 'V'), COALESCE(CAST(org AS STRING), '')
    )"""


def fingerprint_sql(source: str) -> str:
    return f"""
        WITH canon AS (
            SELECT MOD(id, 256) AS shard,
                   id,
                   actor IS NULL AS actor_null,
                   repo IS NULL AS repo_null,
                   payload IS NULL AS payload_null,
                   org IS NULL AS org_null,
                   LENGTH(COALESCE(CAST(actor AS STRING), '')) AS actor_len,
                   LENGTH(COALESCE(CAST(repo AS STRING), '')) AS repo_len,
                   LENGTH(COALESCE(CAST(payload AS STRING), '')) AS payload_len,
                   LENGTH(COALESCE(CAST(org AS STRING), '')) AS org_len,
                   {row_hash_expression()} AS row_hash
            FROM {source}
        )
        SELECT shard, COUNT(*) AS row_count,
               COALESCE(SUM(CAST(id AS LARGEINT)), 0) AS id_sum,
               COALESCE(GROUP_BIT_XOR(id), 0) AS id_xor,
               COALESCE(SUM(CAST(row_hash AS LARGEINT)), 0) AS hash_sum,
               COALESCE(GROUP_BIT_XOR(row_hash), 0) AS hash_xor,
               SUM(IF(actor_null, 1, 0)) AS actor_nulls,
               SUM(IF(repo_null, 1, 0)) AS repo_nulls,
               SUM(IF(payload_null, 1, 0)) AS payload_nulls,
               SUM(IF(org_null, 1, 0)) AS org_nulls,
               SUM(CAST(actor_len AS LARGEINT)) AS actor_bytes,
               SUM(CAST(repo_len AS LARGEINT)) AS repo_bytes,
               SUM(CAST(payload_len AS LARGEINT)) AS payload_bytes,
               SUM(CAST(org_len AS LARGEINT)) AS org_bytes
        FROM canon GROUP BY shard ORDER BY shard
    """


def source_id_union() -> str:
    return "(" + " UNION ALL ".join(
        f"SELECT id FROM `github_events_stage_{batch}`" for batch in range(8)
    ) + ") AS source_ids"


def parse_single_row(rows: list[str], columns: int, context: str) -> list[str]:
    if len(rows) != 1:
        raise RuntimeError(f"{context}: expected one row, got {rows}")
    values = rows[0].split("\t")
    if len(values) != columns:
        raise RuntimeError(f"{context}: expected {columns} columns, got {values}")
    return values


def file_chunks(files: list[Path]):
    for path in files:
        last = b""
        with path.open("rb") as source:
            while chunk := source.read(4 * 1024 * 1024):
                last = chunk[-1:]
                yield chunk
        if last != b"\n":
            yield b"\n"


def stream_load_batch(
        host: str, port: int, database: str, table: str, files: list[Path],
        expected_rows: int) -> dict[str, Any]:
    connection = http.client.HTTPConnection(host, port, timeout=7200)
    headers = {
        "Authorization": "Basic " + base64.b64encode(b"root:").decode(),
        "label": f"variant_p2_real_{table}_{uuid.uuid4().hex}",
        "format": "json",
        "read_json_by_line": "true",
        "strict_mode": "true",
        "max_filter_ratio": "0",
        "columns": "id,type,actor,repo,payload,public,created_at,org",
    }
    started = time.monotonic()
    connection.request(
        "PUT", f"/api/{database}/{table}/_stream_load",
        body=file_chunks(files), headers=headers, encode_chunked=True)
    response = connection.getresponse()
    payload = response.read().decode()
    connection.close()
    wall_seconds = time.monotonic() - started
    if response.status != 200:
        raise RuntimeError(f"stream load HTTP {response.status}: {payload}")
    result = json.loads(payload)
    loaded = int(result.get("NumberLoadedRows", -1))
    total = int(result.get("NumberTotalRows", -1))
    filtered = int(result.get("NumberFilteredRows", -1))
    unselected = int(result.get("NumberUnselectedRows", -1))
    if result.get("Status", "").lower() != "success":
        raise RuntimeError(f"stream load failed: {result}")
    if (loaded, total, filtered, unselected) != (expected_rows, expected_rows, 0, 0):
        raise RuntimeError(
            f"stream load row gate failed for {table}: "
            f"loaded={loaded}, total={total}, filtered={filtered}, "
            f"unselected={unselected}, expected={expected_rows}")
    return {**result, "client_wall_seconds": wall_seconds}


def normalize_key(row: dict[str, str], wanted: str) -> str:
    matches = [key for key in row if key.lower().replace("_", "") == wanted.lower().replace("_", "")]
    if len(matches) != 1:
        raise RuntimeError(f"expected one {wanted} column in {list(row)}, got {matches}")
    return matches[0]


def parse_size(value: str) -> int:
    stripped = value.strip()
    if re.fullmatch(r"\d+", stripped):
        return int(stripped)
    match = re.fullmatch(r"([0-9.]+)\s*([KMGTPE]?B)", stripped, re.IGNORECASE)
    if not match:
        raise ValueError(f"cannot parse size {value!r}")
    factors = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3,
               "TB": 1024 ** 4, "PB": 1024 ** 5, "EB": 1024 ** 6}
    return round(float(match.group(1)) * factors[match.group(2).upper()])


def parse_rowset(value: str) -> dict[str, Any]:
    match = ROWSET_PATTERN.fullmatch(value)
    if not match:
        raise RuntimeError(f"unrecognized active rowset: {value!r}")
    start, end, segments, kind, overlap, rowset_id, pretty_size, level = match.groups()
    return {
        "start_version": int(start),
        "end_version": int(end),
        "segments": int(segments),
        "kind": kind,
        "overlap": overlap,
        "rowset_id": rowset_id,
        "pretty_size": pretty_size,
        "level": int(level),
        "raw": value,
    }


def locate_tablet_dir(storage_root: Path, tablet_id: int, schema_hash: str | None) -> Path:
    candidates = [path for path in storage_root.glob(f"data/*/{tablet_id}/*") if path.is_dir()]
    if schema_hash:
        exact = [path for path in candidates if path.name == schema_hash]
        if exact:
            candidates = exact
    if len(candidates) != 1:
        raise RuntimeError(f"tablet {tablet_id}: expected one schema directory, got {candidates}")
    return candidates[0]


def table_state(
        args: argparse.Namespace, be_http: DorisHttp, table: str) -> dict[str, Any]:
    tablet_rows = mysql_tsv(
        args.host, args.mysql_port, args.user,
        f"SHOW TABLETS FROM `{args.database}`.`{table}`", headers=True)
    if not isinstance(tablet_rows, list) or not tablet_rows:
        raise RuntimeError(f"{table}: SHOW TABLETS returned no rows")
    tablets = []
    unique_inodes: set[tuple[int, int]] = set()
    aggregate = {
        "rows": 0,
        "local_data_size": 0,
        "active_rowsets": 0,
        "nonempty_rowsets": 0,
        "segments": 0,
        "data_files": 0,
        "index_files": 0,
        "other_files": 0,
        "data_bytes": 0,
        "index_bytes": 0,
        "other_bytes": 0,
        "active_allocated_bytes": 0,
    }
    for row in tablet_rows:
        assert isinstance(row, dict)
        tablet_id = int(row[normalize_key(row, "TabletId")])
        schema_hash_key = next(
            (key for key in row if key.lower().replace("_", "") == "schemahash"), None)
        schema_hash = row[schema_hash_key] if schema_hash_key else None
        row_count = int(row[normalize_key(row, "RowCount")])
        local_size = parse_size(row[normalize_key(row, "LocalDataSize")])
        detail = be_http.json("GET", f"/api/compaction/show?tablet_id={tablet_id}")
        if detail.get("missing_rowsets") not in (None, [], ""):
            raise RuntimeError(f"tablet {tablet_id}: missing rowsets {detail.get('missing_rowsets')}")
        parsed = [parse_rowset(value) for value in detail.get("rowsets", [])]
        tablet_dir = locate_tablet_dir(args.storage_root, tablet_id, schema_hash)
        files = []
        for rowset in parsed:
            prefix = rowset["rowset_id"] + "_"
            rowset_files = [path for path in tablet_dir.iterdir()
                            if path.is_file() and path.name.startswith(prefix)]
            if rowset["segments"] > 0:
                dat = [path for path in rowset_files if path.suffix == ".dat"]
                if len(dat) != rowset["segments"]:
                    raise RuntimeError(
                        f"tablet {tablet_id} rowset {rowset['rowset_id']}: "
                        f"segments={rowset['segments']}, dat_files={dat}")
            for path in rowset_files:
                stat = path.stat()
                inode = (stat.st_dev, stat.st_ino)
                allocated = 0
                if inode not in unique_inodes:
                    unique_inodes.add(inode)
                    allocated = stat.st_blocks * 512
                kind = "data" if path.suffix == ".dat" else "index" if path.suffix == ".idx" else "other"
                files.append({
                    "path": str(path), "kind": kind, "bytes": stat.st_size,
                    "allocated_bytes": allocated,
                })
        tablet = {
            "tablet_id": tablet_id,
            "schema_hash": schema_hash,
            "row_count": row_count,
            "local_data_size": local_size,
            "rowsets": parsed,
            "files": files,
            "compaction": detail,
        }
        tablets.append(tablet)
        aggregate["rows"] += row_count
        aggregate["local_data_size"] += local_size
        aggregate["active_rowsets"] += len(parsed)
        aggregate["nonempty_rowsets"] += sum(item["segments"] > 0 for item in parsed)
        aggregate["segments"] += sum(item["segments"] for item in parsed)
        for file in files:
            aggregate[f"{file['kind']}_files"] += 1
            aggregate[f"{file['kind']}_bytes"] += file["bytes"]
            aggregate["active_allocated_bytes"] += file["allocated_bytes"]
    aggregate["active_total_bytes"] = (
        aggregate["data_bytes"] + aggregate["index_bytes"] + aggregate["other_bytes"])
    if abs(aggregate["active_total_bytes"] - aggregate["local_data_size"]) > max(
            len(tablets) * 1024, aggregate["active_total_bytes"] * 0.001):
        raise RuntimeError(
            f"{table}: active files={aggregate['active_total_bytes']} disagree with "
            f"LocalDataSize={aggregate['local_data_size']}")
    return {"table": table, "aggregate": aggregate, "tablets": tablets}


def topology_signature(state: dict[str, Any]) -> list[tuple[Any, ...]]:
    result = []
    for tablet in state["tablets"]:
        versions = tuple(
            (rowset["start_version"], rowset["end_version"], rowset["segments"], rowset["kind"])
            for rowset in tablet["rowsets"])
        result.append((tablet["row_count"], versions))
    return sorted(result)


def persistent_storage_identity(states: dict[str, Any]) -> dict[str, Any]:
    identity: dict[str, Any] = {}
    aggregate_fields = (
        "rows", "local_data_size", "active_rowsets", "nonempty_rowsets", "segments",
        "data_files", "index_files", "other_files", "data_bytes", "index_bytes",
        "other_bytes", "active_total_bytes",
    )
    rowset_fields = (
        "start_version", "end_version", "segments", "kind", "overlap", "rowset_id", "level",
    )
    for layout in LAYOUTS:
        state = states[layout]
        tablets = []
        for tablet in sorted(state["tablets"], key=lambda item: item["tablet_id"]):
            tablets.append({
                "tablet_id": tablet["tablet_id"],
                "schema_hash": tablet["schema_hash"],
                "row_count": tablet["row_count"],
                "local_data_size": tablet["local_data_size"],
                "rowsets": [
                    {field: rowset[field] for field in rowset_fields}
                    for rowset in sorted(
                        tablet["rowsets"],
                        key=lambda item: (item["start_version"], item["end_version"]))
                ],
                "files": sorted(
                    ({"path": item["path"], "kind": item["kind"], "bytes": item["bytes"]}
                     for item in tablet["files"]),
                    key=lambda item: item["path"]),
            })
        identity[layout] = {
            "table": state["table"],
            "aggregate": {
                field: state["aggregate"][field] for field in aggregate_fields
            },
            "tablets": tablets,
        }
    return identity


def tablet_active_bytes(tablet: dict[str, Any]) -> dict[str, int]:
    result = {"data": 0, "index": 0, "other": 0}
    for file in tablet["files"]:
        result[file["kind"]] += int(file["bytes"])
    result["total"] = result["data"] + result["index"] + result["other"]
    return result


def assert_version_chain(
        tablet: dict[str, Any], expected_last_version: int, compacted: bool) -> None:
    rowsets = sorted(
        tablet["rowsets"], key=lambda item: (item["start_version"], item["end_version"]))
    if compacted:
        expected = [(0, expected_last_version)]
        actual = [(item["start_version"], item["end_version"]) for item in rowsets]
        if actual != expected or len(rowsets) != 1:
            raise RuntimeError(
                f"tablet {tablet['tablet_id']}: expected one active {expected}, got {actual}")
        if rowsets[0]["kind"] != "DATA" or rowsets[0]["segments"] <= 0:
            raise RuntimeError(
                f"tablet {tablet['tablet_id']}: invalid compacted rowset {rowsets[0]}")
        return

    expected_versions = [(0, 1)] + [
        (version, version) for version in range(2, expected_last_version + 1)]
    actual_versions = [
        (item["start_version"], item["end_version"]) for item in rowsets]
    if actual_versions != expected_versions:
        raise RuntimeError(
            f"tablet {tablet['tablet_id']}: expected versions {expected_versions}, "
            f"got {actual_versions}")
    if rowsets[0]["kind"] != "DATA" or rowsets[0]["segments"] != 0:
        raise RuntimeError(
            f"tablet {tablet['tablet_id']}: invalid initial rowset {rowsets[0]}")
    if any(item["kind"] != "DATA" or item["segments"] <= 0 for item in rowsets[1:]):
        raise RuntimeError(
            f"tablet {tablet['tablet_id']}: invalid pre-compaction rowsets {rowsets[1:]}")


def completed_compaction_profiles(response: dict[str, Any]) -> list[dict[str, Any]]:
    profiles = response.get("compaction_profiles")
    if not isinstance(profiles, list):
        raise RuntimeError(f"invalid compaction profile response: {response}")
    return profiles


def query_rows(session: MysqlSession, statement: str) -> list[str]:
    rows, error = session.query(statement)
    if error:
        raise RuntimeError(error)
    return rows


def fingerprint_relation(session: MysqlSession, source: str) -> list[str]:
    return query_rows(session, fingerprint_sql(source))


def read_queries(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    expected = {
        "id", "category", "execution", "layouts", "repetitions", "predicate",
        "expected_indexed_layouts", "note",
    }
    if set(rows[0]) != expected:
        raise RuntimeError(f"unexpected query TSV columns: {list(rows[0])}")
    for query in rows:
        expected_layouts = expected_indexed_layouts(query)
        selected_layouts = set(layouts_for_query(query))
        if not expected_layouts <= selected_layouts:
            raise RuntimeError(
                f"{query['id']}: expected indexed layouts {sorted(expected_layouts)} "
                f"are not selected by layouts={query['layouts']}")
        if expected_layouts and query["execution"] == "index_off":
            raise RuntimeError(f"{query['id']}: index_off cannot expect index admission")
    return rows


def query_hit_class(query: dict[str, str]) -> str:
    category = query["category"]
    if category.endswith("_sparse"):
        return "sparse"
    if category.endswith("_dense"):
        return "dense"
    return "none"


def query_population(
        query: dict[str, str], result_count: int, total_rows: int,
        enforce: bool) -> dict[str, Any]:
    if total_rows <= 0:
        raise RuntimeError(f"invalid population row count: {total_rows}")
    hit_class = query_hit_class(query)
    selectivity = result_count / total_rows
    passed = True
    expected = "not classified"
    if hit_class == "sparse":
        expected = f"0 < selectivity <= {SPARSE_MAX_SELECTIVITY}"
        passed = result_count > 0 and selectivity <= SPARSE_MAX_SELECTIVITY
    elif hit_class == "dense":
        expected = (
            f"{DENSE_MIN_SELECTIVITY} <= selectivity < {DENSE_MAX_SELECTIVITY}")
        passed = DENSE_MIN_SELECTIVITY <= selectivity < DENSE_MAX_SELECTIVITY
    record = {
        "hit_class": hit_class,
        "result_count": result_count,
        "total_rows": total_rows,
        "selectivity": selectivity,
        "expected": expected,
        "enforced": enforce and hit_class != "none",
        "passed": passed,
    }
    if record["enforced"] and not passed:
        raise RuntimeError(
            f"{query['id']} {hit_class} population gate failed: "
            f"count={result_count}, total={total_rows}, selectivity={selectivity:.9%}, "
            f"expected {expected}")
    return record


def assert_query_session_variables(rows: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for row in rows:
        values = row.split("\t")
        if len(values) != 2:
            raise RuntimeError(f"invalid SHOW VARIABLES row: {row!r}")
        parsed[values[0]] = values[1].lower()
    expected = {
        "enable_sql_cache": "false",
        "enable_query_cache": "false",
        "enable_condition_cache": "false",
        "enable_inverted_index_query_cache": "false",
        "enable_inverted_index_searcher_cache": "true",
        "enable_page_cache": "true",
        "inverted_index_skip_threshold": "50",
        "enable_count_on_index_pushdown": "false",
    }
    drift = {
        name: {"actual": parsed.get(name), "expected": value}
        for name, value in expected.items() if parsed.get(name) != value
    }
    if drift:
        raise RuntimeError(
            "query session cache/config gate failed: "
            + json.dumps(drift, sort_keys=True))
    return parsed


def layouts_for_query(query: dict[str, str]) -> tuple[str, ...]:
    return LAYOUTS if query["layouts"] == "all" else tuple(query["layouts"].split(","))


def modes_for_query(query: dict[str, str], layout: str) -> tuple[bool, ...]:
    execution = query["execution"]
    if layout == "no_index":
        return (False,)
    if execution == "index_off":
        return (False,)
    if execution == "index_on_and_off":
        return (True, False)
    if execution == "index_on":
        return (True,)
    raise RuntimeError(f"unknown execution {execution}")


def expected_indexed_layouts(query: dict[str, str]) -> set[str]:
    value = query["expected_indexed_layouts"]
    if value == "none":
        return set()
    layouts = set(value.split(","))
    invalid = layouts - set(LAYOUTS[1:])
    if not layouts or invalid:
        raise RuntimeError(
            f"{query['id']}: invalid expected_indexed_layouts={value!r}")
    return layouts


def expected_index_admission(
        query: dict[str, str], layout: str, enable_index: bool) -> bool:
    return enable_index and layout in expected_indexed_layouts(query)


def parse_profile_unit_value(value: str) -> float:
    """Parse a Doris TUnit::UNIT pretty-printed value into its displayed magnitude."""
    value = value.strip()
    raw = re.fullmatch(
        r"[+-]?[0-9][0-9,]*(?:\.[0-9]+)?[KMB]?\s+"
        r"\(([+-]?[0-9][0-9,]*)\)", value)
    if raw:
        return float(raw.group(1).replace(",", ""))
    match = re.fullmatch(r"([+-]?[0-9][0-9,]*(?:\.[0-9]+)?)([KMB]?)", value)
    if not match:
        raise RuntimeError(f"cannot parse profile unit counter value {value!r}")
    multiplier = {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9}[match.group(2)]
    return float(match.group(1).replace(",", "")) * multiplier


def parse_profile_time_ns(value: str) -> float:
    """Parse Doris TIME_NS output, including compound h/m/s/ms values, into ns."""
    value = re.sub(r"\s+", "", value)
    if value == "0":
        return 0.0
    factors = {
        "h": 60.0 * 60 * 1e9,
        "m": 60.0 * 1e9,
        "s": 1e9,
        "ms": 1e6,
        "us": 1e3,
        "ns": 1.0,
    }
    total = 0.0
    position = 0
    for match in re.finditer(r"([0-9]+(?:\.[0-9]+)?)(ms|us|ns|h|m|s)", value):
        if match.start() != position:
            raise RuntimeError(f"cannot parse profile time counter value {value!r}")
        total += float(match.group(1)) * factors[match.group(2)]
        position = match.end()
    if position != len(value) or position == 0:
        raise RuntimeError(f"cannot parse profile time counter value {value!r}")
    return total


def profile_counter_values(
        profile: str, name: str, parser: Any) -> tuple[list[str], list[float]]:
    texts = []
    values = []
    pattern = re.compile(rf"(?:^|\s){re.escape(name)}:\s*(.+?)\s*$")
    for line in profile.splitlines():
        match = pattern.search(line)
        if match:
            text_value = match.group(1)
            texts.append(text_value)
            values.append(parser(text_value))
    return texts, values


def profile_index_admission(profile: str) -> dict[str, Any]:
    rows_text, rows_values = profile_counter_values(
        profile, "RowsInvertedIndexFiltered", parse_profile_unit_value)
    time_text, time_values = profile_counter_values(
        profile, "InvertedIndexQueryTime", parse_profile_time_ns)
    downgrade_text, downgrade_values = profile_counter_values(
        profile, "InvertedIndexDowngradeCount", parse_profile_unit_value)
    query_cache: dict[str, float] = {}
    for name in (
            "InvertedIndexQueryCacheLookup", "InvertedIndexQueryCacheHit",
            "InvertedIndexQueryCacheMiss", "InvertedIndexQueryCacheInsert"):
        _, values = profile_counter_values(profile, name, parse_profile_unit_value)
        query_cache[name] = sum(values)
    rows_sum = sum(rows_values)
    time_sum = sum(time_values)
    downgrade_sum = sum(downgrade_values)
    return {
        "rows_inverted_index_filtered_text": rows_text,
        "rows_inverted_index_filtered_values": rows_values,
        "rows_inverted_index_filtered_sum": rows_sum,
        "inverted_index_query_time_text": time_text,
        "inverted_index_query_time_ns_values": time_values,
        "inverted_index_query_time_ns_sum": time_sum,
        "inverted_index_downgrade_count_text": downgrade_text,
        "inverted_index_downgrade_count_values": downgrade_values,
        "inverted_index_downgrade_count_sum": downgrade_sum,
        "inverted_index_query_cache": query_cache,
        "actual_index_admission": rows_sum > 0 and time_sum > 0 and downgrade_sum == 0,
    }


def write_fingerprint(path: Path, rows: list[str]) -> dict[str, Any]:
    payload = "\n".join(rows) + "\n"
    path.write_text(payload, encoding="utf-8")
    return {"rows": len(rows), "sha256": hashlib.sha256(payload.encode()).hexdigest()}


def combine_fingerprint_rows(batch_results: list[list[str]]) -> list[str]:
    """Combine independently aggregated 256-shard fingerprints.

    Doris currently fails to plan the full Variant expression over an eight-way
    UNION ALL with an empty error message. Aggregating each staging table once
    and combining the associative counters is equivalent and avoids that planner
    limitation. Target tables still use one direct scan.
    """
    combined: dict[int, list[int]] = {}
    xor_positions = {3, 5}
    mask = (1 << 64) - 1
    for rows in batch_results:
        for row in rows:
            values = [int(value) for value in row.split("\t")]
            if len(values) != 14:
                raise RuntimeError(f"fingerprint row must have 14 columns: {row}")
            shard = values[0]
            aggregate = combined.setdefault(shard, [shard] + [0] * 13)
            for position in range(1, 14):
                if position in xor_positions:
                    aggregate[position] = (
                        (aggregate[position] & mask) ^ (values[position] & mask))
                else:
                    aggregate[position] += values[position]
    output = []
    for shard in sorted(combined):
        values = combined[shard]
        for position in xor_positions:
            if values[position] >= 1 << 63:
                values[position] -= 1 << 64
        output.append("\t".join(str(value) for value in values))
    return output


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


class Runner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.root = Path(__file__).resolve().parents[1]
        self.template = (self.root / "ddl" / "doris.sql.tmpl").read_text(encoding="utf-8")
        self.queries = read_queries(self.root / "queries" / "query_set.tsv")
        self.fe_http = DorisHttp(args.host, args.fe_http_port, args.user)
        self.be_http = DorisHttp(args.host, args.be_http_port, args.user)
        self.run_id = uuid.uuid4().hex
        self.state: dict[str, Any] = {"run_id": self.run_id, "completed": []}
        if args.resume:
            self.state = json.loads((args.results / "state.json").read_text(encoding="utf-8"))
            self.run_id = self.state["run_id"]
        self.events = EventLog(args.results / "events.jsonl", self.run_id)
        self.batches: dict[int, list[Path]] = {}
        self.batch_rows: dict[int, int] = {}
        self.expected_rows = 0
        self.source_bytes = 0
        self.current_runtime: dict[str, Any] | None = None
        self.query_runtime_approved = False

    def done(self, phase: str) -> bool:
        return phase in self.state["completed"]

    def complete(self, phase: str, **values: Any) -> None:
        if phase not in self.state["completed"]:
            self.state["completed"].append(phase)
        self.state.update(values)
        write_json(self.args.results / "state.json", self.state)

    def _query_restart_boundary(self) -> bool:
        required = {
            "create", "stage", "write", "correctness_pre", "storage_pre", "compact",
            "correctness_post", "storage_post",
        }
        return required <= set(self.state["completed"])

    def _be_num_cores(self) -> int:
        response = self.be_http.json("GET", "/api/show_config?conf_item=num_cores")
        if (not isinstance(response, list) or len(response) != 1
                or not isinstance(response[0], list) or len(response[0]) != 4
                or response[0][0] != "num_cores"):
            raise RuntimeError(f"invalid BE num_cores response: {response}")
        try:
            return int(response[0][2])
        except (TypeError, ValueError) as error:
            raise RuntimeError(f"invalid BE num_cores value: {response[0][2]!r}") from error

    def _be_config_value(self, name: str) -> str:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise RuntimeError(f"invalid BE config name: {name!r}")
        response = self.be_http.json("GET", f"/api/show_config?conf_item={name}")
        if (not isinstance(response, list) or len(response) != 1
                or not isinstance(response[0], list) or len(response[0]) != 4
                or response[0][0] != name):
            raise RuntimeError(f"invalid BE config response for {name}: {response}")
        return str(response[0][2])

    def _gate_snii_ram_buffer(
            self, phase: str, expected_mib: int, allow_update: bool) -> dict[str, Any]:
        before = self._be_config_value(SNII_RAM_BUFFER_CONFIG)
        try:
            before_mib = float(before)
        except ValueError as error:
            raise RuntimeError(
                f"invalid {SNII_RAM_BUFFER_CONFIG} value: {before!r}") from error
        changed = not math.isclose(before_mib, expected_mib, rel_tol=0, abs_tol=1e-9)
        update_response: Any = None
        if changed:
            if not allow_update:
                raise RuntimeError(
                    f"{phase}: expected {SNII_RAM_BUFFER_CONFIG}={expected_mib} MiB, "
                    f"got {before}")
            update_response = self.be_http.json(
                "POST", f"/api/update_config?{SNII_RAM_BUFFER_CONFIG}={expected_mib}"
                        "&persist=false")
            if (not isinstance(update_response, list) or len(update_response) != 1
                    or update_response[0].get("config_name") != SNII_RAM_BUFFER_CONFIG
                    or update_response[0].get("status") != "OK"):
                raise RuntimeError(
                    f"failed to update {SNII_RAM_BUFFER_CONFIG}: {update_response}")
        after = self._be_config_value(SNII_RAM_BUFFER_CONFIG)
        try:
            after_mib = float(after)
        except ValueError as error:
            raise RuntimeError(
                f"invalid {SNII_RAM_BUFFER_CONFIG} value after update: {after!r}") from error
        if not math.isclose(after_mib, expected_mib, rel_tol=0, abs_tol=1e-9):
            raise RuntimeError(
                f"{phase}: failed to gate {SNII_RAM_BUFFER_CONFIG}={expected_mib} MiB, "
                f"got {after}")
        evidence = {
            "phase": phase,
            "config": SNII_RAM_BUFFER_CONFIG,
            "expected_mib": expected_mib,
            "before": before,
            "after": after,
            "changed": changed,
            "persist": False,
            "update_response": update_response,
        }
        self.events.append("be_config_gate", **evidence)
        return evidence

    def _record_affinity_gate(
            self, phase: str, configured_cpus: tuple[int, ...]) -> dict[str, Any]:
        if self.current_runtime is None:
            raise RuntimeError("runtime identity is unavailable before CPU affinity gate")
        timestamp_ns = time.time_ns()
        processes = [
            stable_thread_affinity_snapshot(
                "fe", self.args.fe_pid, self.current_runtime["fe"]["starttime_ticks"],
                configured_cpus),
            stable_thread_affinity_snapshot(
                "be", self.args.be_pid, self.current_runtime["be"]["starttime_ticks"],
                configured_cpus),
        ]
        processes[0]["listening_ports"] = process_listening_port_evidence(
            self.args.fe_pid, (self.args.mysql_port, self.args.fe_http_port))
        processes[1]["listening_ports"] = process_listening_port_evidence(
            self.args.be_pid, (self.args.be_http_port,))
        service = self._service_endpoint_identity()
        service_identity = {
            key: service[key] for key in (
                "alive_backend", "backend_host_addresses", "local_addresses",
                "backend_ports", "be_listening_ports")
        }
        expected_num_cores = len(configured_cpus)
        actual_num_cores = self._be_num_cores()
        for process in processes:
            observed_starttime = _process_starttime_ticks(process["pid"])
            if observed_starttime != process["starttime_ticks"]:
                raise RuntimeError(
                    f"{process['role']} pid {process['pid']} changed during affinity/port gate: "
                    f"expected starttime={process['starttime_ticks']}, "
                    f"observed={observed_starttime}")
        passed = (
            not any(process["mismatched_threads"] for process in processes)
            and actual_num_cores == expected_num_cores)
        evidence = {
            "run_id": self.run_id,
            "phase": phase,
            "timestamp_ns": timestamp_ns,
            "configured_cpus": format_linux_cpu_list(configured_cpus),
            "configured_cpu_ids": list(configured_cpus),
            "expected_be_num_cores": expected_num_cores,
            "actual_be_num_cores": actual_num_cores,
            "service_identity": service_identity,
            "passed": passed,
            "processes": processes,
        }
        evidence_root = self.args.results / "cpu_affinity"
        evidence_root.mkdir(parents=True, exist_ok=True)
        evidence_path = evidence_root / f"{timestamp_ns}_{phase}.json"
        write_json(evidence_path, evidence)
        event_processes = [{
            "role": process["role"],
            "pid": process["pid"],
            "starttime_ticks": process["starttime_ticks"],
            "thread_count": process["thread_count"],
            "unique_thread_masks": process["unique_thread_masks"],
            "mismatched_thread_count": len(process["mismatched_threads"]),
        } for process in processes]
        self.events.append(
            "cpu_affinity_gate", phase=phase,
            configured_cpus=evidence["configured_cpus"], passed=passed,
            expected_be_num_cores=expected_num_cores,
            actual_be_num_cores=actual_num_cores,
            service_identity=service_identity,
            evidence=str(evidence_path), processes=event_processes)
        if not passed:
            mismatches = {
                process["role"]: process["mismatched_threads"]
                for process in processes if process["mismatched_threads"]
            }
            raise RuntimeError(
                f"{phase} CPU affinity gate failed; expected every FE/BE thread on "
                f"{evidence['configured_cpus']} and BE num_cores={expected_num_cores}; "
                f"actual num_cores={actual_num_cores}, thread mismatches={mismatches}; "
                f"evidence={evidence_path}")
        return evidence

    def _record_query_runtime_transition(
            self, original: dict[str, Any], affinity: dict[str, Any]) -> None:
        if self.current_runtime is None:
            raise RuntimeError("runtime identity is unavailable for query transition")
        timestamp_ns = time.time_ns()
        record = {
            "run_id": self.run_id,
            "timestamp_ns": timestamp_ns,
            "from": {
                "fe": {
                    "pid": original["fe"]["pid"],
                    "starttime_ticks": original["fe"]["starttime_ticks"],
                },
                "be": {
                    "pid": original["be"]["pid"],
                    "starttime_ticks": original["be"]["starttime_ticks"],
                },
            },
            "to": self.current_runtime,
            "affinity_evidence_timestamp_ns": affinity["timestamp_ns"],
        }
        transition_root = self.args.results / "query_runtime"
        transition_root.mkdir(parents=True, exist_ok=True)
        path = transition_root / f"{timestamp_ns}.json"
        write_json(path, record)
        self.events.append(
            "query_runtime_transition", path=str(path),
            from_fe_pid=record["from"]["fe"]["pid"],
            from_be_pid=record["from"]["be"]["pid"],
            to_fe_pid=self.args.fe_pid, to_be_pid=self.args.be_pid,
            configured_cpus=affinity["configured_cpus"])

    def _gate_measured_phase(self, phase: str) -> None:
        if phase in ("write", "compact"):
            self._record_affinity_gate(phase, self.args.write_cpu_list)
        elif phase == "query":
            if not self.query_runtime_approved:
                self.events.append(
                    "query_runtime_restart_required",
                    configured_cpus=format_linux_cpu_list(self.args.query_cpu_list),
                    be_num_cores=len(self.args.query_cpu_list))
                raise RuntimeError(
                    "query phase requires an external FE/BE restart after storage_post: "
                    f"start both processes on {format_linux_cpu_list(self.args.query_cpu_list)}, "
                    f"set be.conf num_cores={len(self.args.query_cpu_list)}, then rerun with "
                    "--resume and the new --fe-pid/--be-pid")
            self._record_affinity_gate(phase, self.args.query_cpu_list)

    def run(self) -> None:
        self.preflight()
        phases = (
            ("create", self.create_tables),
            ("stage", self.load_staging),
            ("write", self.write_targets),
            ("correctness_pre", lambda: self.correctness("pre_compaction")),
            ("storage_pre", lambda: self.storage("post_import")),
            ("compact", self.compact),
            ("correctness_post", lambda: self.correctness("post_compaction")),
            ("storage_post", lambda: self.storage("post_compaction")),
            ("query", self.measure_queries),
            ("report", self.report),
        )
        for name, function in phases:
            if self.done(name):
                print(f"[resume] skip completed phase {name}", flush=True)
                continue
            print(f"[phase] {name}", flush=True)
            self._gate_measured_phase(name)
            function()
            if name in ("write", "compact"):
                self._record_affinity_gate(f"{name}_after", self.args.write_cpu_list)
            elif name == "query":
                self._record_affinity_gate("query_after", self.args.query_cpu_list)
            self.complete(name)
        write_json(self.args.results / "validation.json", {
            "status": "pass",
            "run_id": self.run_id,
            "completed": self.state["completed"],
            "expected_rows": self.expected_rows,
        })

    def _benchmark_source_hashes(self) -> dict[str, str]:
        relative_paths = (
            "README.md",
            "ddl/doris.sql.tmpl",
            "queries/query_set.tsv",
            "bin/benchmark_lib.py",
            "bin/prepare_full_corpus.py",
            "bin/run_doris.py",
        )
        return {
            relative_path: sha256_file(self.root / relative_path)
            for relative_path in relative_paths
        }

    def _snapshot_benchmark_sources(self, source_hashes: dict[str, str]) -> None:
        snapshot_root = self.args.results / "sources"
        for relative_path, expected_sha256 in source_hashes.items():
            source = self.root / relative_path
            destination = snapshot_root / relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            if sha256_file(destination) != expected_sha256:
                raise RuntimeError(f"benchmark source changed while copying: {source}")
            if sha256_file(source) != expected_sha256:
                raise RuntimeError(f"benchmark source changed during preflight: {source}")

    def _fe_process_identity(self) -> dict[str, Any]:
        proc = Path(f"/proc/{self.args.fe_pid}")
        try:
            stat = (proc / "stat").read_text(encoding="utf-8").split()
            executable = (proc / "exe").resolve(strict=True)
            cwd = (proc / "cwd").resolve(strict=True)
            cmdline_parts = [
                value.decode(errors="replace")
                for value in (proc / "cmdline").read_bytes().split(b"\0") if value
            ]
            environment_entries = [
                value for value in (proc / "environ").read_bytes().split(b"\0") if value
            ]
        except (FileNotFoundError, ProcessLookupError, PermissionError, OSError) as error:
            raise RuntimeError(
                f"cannot inspect live FE pid {self.args.fe_pid}: {error}") from error
        if len(stat) <= 21:
            raise RuntimeError(f"invalid /proc stat for FE pid {self.args.fe_pid}")
        environment: dict[str, str] = {}
        for entry in environment_entries:
            key, separator, value = entry.partition(b"=")
            if separator:
                environment[key.decode(errors="replace")] = value.decode(errors="replace")

        expected_home = (self.args.repo_root / "output" / "fe").resolve(strict=True)
        expected_jar = self.args.fe_jar.resolve(strict=True)
        doris_home_raw = environment.get("DORIS_HOME")
        doris_home = (
            str(Path(doris_home_raw).resolve(strict=True)) if doris_home_raw else None
        )
        if doris_home and Path(doris_home) != expected_home:
            raise RuntimeError(
                f"FE pid {self.args.fe_pid} DORIS_HOME={doris_home}, expected={expected_home}")

        cmdline = " ".join(cmdline_parts)
        cwd_matches = cwd in (self.args.repo_root, expected_home) or expected_home in cwd.parents
        environment_matches = doris_home is not None and Path(doris_home) == expected_home
        cmdline_matches = str(expected_home) in cmdline or str(expected_jar) in cmdline
        bindings = [
            name for name, matches in (
                ("cwd", cwd_matches),
                ("DORIS_HOME", environment_matches),
                ("cmdline", cmdline_matches),
            ) if matches
        ]
        if "org.apache.doris.DorisFE" not in cmdline:
            raise RuntimeError(
                f"pid {self.args.fe_pid} is not a Doris FE Java process: {cmdline}")
        if not bindings:
            raise RuntimeError(
                f"FE pid {self.args.fe_pid} is not bound to {expected_home}: "
                f"cwd={cwd}, DORIS_HOME={doris_home_raw!r}, cmdline={cmdline}")
        return {
            "pid": self.args.fe_pid,
            "starttime_ticks": int(stat[21]),
            "exe": str(executable),
            "cwd": str(cwd),
            "cmdline": cmdline,
            "doris_home": doris_home,
            "expected_home": str(expected_home),
            "bindings": bindings,
        }

    def _runtime_identity(self) -> dict[str, Any]:
        before = proc_snapshot(self.args.be_pid)
        expected_be = (
            self.args.repo_root / "output" / "be" / "lib" / "doris_be"
        ).resolve(strict=True)
        actual_be = Path(before["exe"]).resolve(strict=True)
        if actual_be != expected_be:
            raise RuntimeError(
                f"BE pid {self.args.be_pid} exe={actual_be}, expected={expected_be}")
        fe = self._fe_process_identity()
        fe_ports = process_listening_port_evidence(
            self.args.fe_pid, (self.args.mysql_port, self.args.fe_http_port))
        be_ports = process_listening_port_evidence(
            self.args.be_pid, (self.args.be_http_port,))
        return {
            "be": before,
            "be_expected_executable": str(expected_be),
            "be_sha256": sha256_file(expected_be),
            "be_listening_ports": be_ports,
            "fe": fe,
            "fe_listening_ports": fe_ports,
            "fe_jar": str(self.args.fe_jar),
            "fe_jar_sha256": sha256_file(self.args.fe_jar),
        }

    def _service_endpoint_identity(self) -> dict[str, Any]:
        local_addresses = local_ip_addresses()
        fe_addresses = resolved_ip_addresses(self.args.host)
        if not fe_addresses & local_addresses:
            raise RuntimeError(
                f"--host {self.args.host!r} is not local to --fe-pid {self.args.fe_pid}: "
                f"resolved={sorted(fe_addresses)}, local={sorted(local_addresses)}")
        health = mysql_tsv(
            self.args.host, self.args.mysql_port, self.args.user,
            "SELECT VERSION() AS version", headers=True)
        backends = mysql_tsv(
            self.args.host, self.args.mysql_port, self.args.user,
            "SHOW BACKENDS", headers=True)
        alive = [row for row in backends if row.get("Alive", "").lower() == "true"]
        if len(alive) != 1:
            raise RuntimeError(f"expected exactly one alive BE, got {alive}")
        backend_host = alive[0][normalize_key(alive[0], "Host")]
        backend_addresses = resolved_ip_addresses(backend_host)
        if not backend_addresses & local_addresses:
            raise RuntimeError(
                f"SHOW BACKENDS host {backend_host!r} is not local to --be-pid "
                f"{self.args.be_pid}: resolved={sorted(backend_addresses)}, "
                f"local={sorted(local_addresses)}")
        backend_ports = {
            name: int(alive[0][normalize_key(alive[0], name)])
            for name in ("HeartbeatPort", "BePort", "HttpPort", "BrpcPort")
        }
        invalid_ports = {name: port for name, port in backend_ports.items() if port <= 0}
        if invalid_ports:
            raise RuntimeError(f"SHOW BACKENDS contains invalid service ports: {invalid_ports}")
        http_port = backend_ports["HttpPort"]
        if http_port != self.args.be_http_port:
            raise RuntimeError(
                f"SHOW BACKENDS HTTP port {http_port} does not match "
                f"--be-http-port {self.args.be_http_port}")
        be_listening_ports = process_listening_port_evidence(
            self.args.be_pid, tuple(sorted(set(backend_ports.values()))))
        return {
            "health": health,
            "backends": backends,
            "alive_backend": alive[0],
            "backend_host_addresses": sorted(backend_addresses),
            "local_addresses": sorted(local_addresses),
            "backend_ports": backend_ports,
            "be_listening_ports": be_listening_ports,
        }

    def _git_metadata(self) -> dict[str, Any]:
        git_status = subprocess.run(
            ["git", "status", "--short"], cwd=self.args.repo_root,
            check=True, text=True, capture_output=True).stdout
        git_diff = subprocess.run(
            ["git", "diff", "HEAD", "--binary"], cwd=self.args.repo_root,
            check=True, capture_output=True).stdout
        git_head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=self.args.repo_root,
            check=True, text=True, capture_output=True).stdout.strip()
        untracked = subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=self.args.repo_root, check=True, text=True,
            capture_output=True).stdout.splitlines()
        return {
            "git_head": git_head,
            "git_status": git_status,
            "git_diff_sha256": hashlib.sha256(git_diff).hexdigest(),
            "git_untracked": untracked,
        }

    def _validate_resume_environment(
            self, original: dict[str, Any], manifest_sha256: str,
            runtime: dict[str, Any], git: dict[str, Any],
            source_hashes: dict[str, str], allow_query_runtime_transition: bool) -> None:
        required_original_fields = {
            "benchmark_sources", "cpu_affinity", "dataset_scope", "fe", "fe_jar",
            "be_expected_executable", "be_listening_ports", "fe_listening_ports",
            "git_untracked", "service_identity", "write_snii_ram_buffer_mib",
            "compaction_snii_ram_buffer_mib",
        }
        missing = sorted(required_original_fields - original.keys())
        if missing:
            raise RuntimeError(
                "resume environment predates strict runtime/source identity metadata; "
                f"missing={missing}; restart this benchmark with --fresh")

        checks: dict[str, tuple[Any, Any]] = {
            "database": (self.args.database, original.get("database")),
            "repo_root": (str(self.args.repo_root), original.get("repo_root")),
            "manifest": (str(self.args.manifest), original.get("manifest")),
            "manifest_sha256": (manifest_sha256, original.get("manifest_sha256")),
            "dataset_scope": (self.args.dataset_scope, original.get("dataset_scope")),
            "raw_dir": (str(self.args.raw_dir), original.get("raw_dir")),
            "expected_rows": (self.expected_rows, original.get("expected_rows")),
            "source_bytes": (self.source_bytes, original.get("source_bytes")),
            "BE executable": (
                runtime["be_expected_executable"], original.get("be_expected_executable")),
            "BE sha256": (runtime["be_sha256"], original.get("be_sha256")),
            "FE expected home": (
                runtime["fe"]["expected_home"], original.get("fe", {}).get("expected_home")),
            "FE jar": (runtime["fe_jar"], original.get("fe_jar")),
            "FE jar sha256": (runtime["fe_jar_sha256"], original.get("fe_jar_sha256")),
            "git head": (git["git_head"], original.get("git_head")),
            "git diff sha256": (git["git_diff_sha256"], original.get("git_diff_sha256")),
            "git untracked": (git["git_untracked"], original.get("git_untracked")),
            "benchmark sources": (source_hashes, original.get("benchmark_sources")),
            "ports": ({
                "mysql": self.args.mysql_port,
                "fe_http": self.args.fe_http_port,
                "be_http": self.args.be_http_port,
            }, original.get("ports")),
            "storage_root": (str(self.args.storage_root), original.get("storage_root")),
            "cpu affinity": (self.args.cpu_affinity, original.get("cpu_affinity")),
            "write SNII RAM buffer MiB": (
                self.args.write_snii_ram_buffer_mib,
                original.get("write_snii_ram_buffer_mib")),
            "compaction SNII RAM buffer MiB": (
                self.args.compaction_snii_ram_buffer_mib,
                original.get("compaction_snii_ram_buffer_mib")),
        }
        if not allow_query_runtime_transition:
            checks.update({
                "BE pid": (self.args.be_pid, original.get("be", {}).get("pid")),
                "BE starttime": (
                    runtime["be"]["starttime_ticks"],
                    original.get("be", {}).get("starttime_ticks"),
                ),
                "FE pid": (self.args.fe_pid, original.get("fe", {}).get("pid")),
                "FE starttime": (
                    runtime["fe"]["starttime_ticks"],
                    original.get("fe", {}).get("starttime_ticks"),
                ),
            })
        drift = {
            name: {"current": current, "original": expected}
            for name, (current, expected) in checks.items() if current != expected
        }
        if drift:
            raise RuntimeError(
                "resume preflight identity drift:\n" + json.dumps(
                    drift, indent=2, sort_keys=True, ensure_ascii=False))
        if allow_query_runtime_transition:
            unchanged = []
            for role in ("fe", "be"):
                current_instance = (
                    runtime[role]["pid"], runtime[role]["starttime_ticks"])
                original_instance = (
                    original[role]["pid"], original[role]["starttime_ticks"])
                if current_instance == original_instance:
                    unchanged.append(role)
            if unchanged:
                raise RuntimeError(
                    "query resume requires newly started FE and BE process instances; "
                    f"unchanged={unchanged}. Restart both with query affinity and "
                    f"be.conf num_cores={len(self.args.query_cpu_list)}")
        for relative_path, expected_sha256 in source_hashes.items():
            snapshot = self.args.results / "sources" / relative_path
            if not snapshot.is_file() or sha256_file(snapshot) != expected_sha256:
                raise RuntimeError(f"resume source snapshot missing or corrupt: {snapshot}")

    def _ensure_variant_v2_enabled(
            self, phase: str) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        query = "ADMIN SHOW FRONTEND CONFIG LIKE 'enable_variant_v2'"
        before = mysql_tsv(
            self.args.host, self.args.mysql_port, self.args.user, query, headers=True)
        if not before:
            raise RuntimeError("enable_variant_v2 FE config is unavailable")
        value_key = normalize_key(before[0], "Value")
        changed = before[0][value_key].lower() != "true"
        if changed:
            mysql_tsv(
                self.args.host, self.args.mysql_port, self.args.user,
                'ADMIN SET FRONTEND CONFIG ("enable_variant_v2" = "true")', headers=False)
        after = mysql_tsv(
            self.args.host, self.args.mysql_port, self.args.user, query, headers=True)
        if not after:
            raise RuntimeError("enable_variant_v2 FE config disappeared after update")
        after_value_key = normalize_key(after[0], "Value")
        if after[0][after_value_key].lower() != "true":
            raise RuntimeError(f"failed to enable enable_variant_v2: {after}")
        self.events.append(
            "fe_config_gate", phase=phase, changed=changed,
            enable_variant_v2_before=before[0][value_key],
            enable_variant_v2_after=after[0][after_value_key])
        return before, after

    def preflight(self) -> None:
        started = time.monotonic()
        self.batches, self.batch_rows, self.expected_rows, self.source_bytes = read_validated_manifest(
            self.args.manifest, self.args.raw_dir,
            not self.done("preflight") and not self.args.skip_source_sha256,
            self.args.dataset_scope)
        manifest_sha = sha256_file(self.args.manifest)
        runtime = self._runtime_identity()
        self.current_runtime = runtime
        service = self._service_endpoint_identity()
        git = self._git_metadata()
        benchmark_sources = self._benchmark_source_hashes()
        build_cache = self.args.repo_root / "be" / "build_Release" / "CMakeCache.txt"
        if "CMAKE_BUILD_TYPE:STRING=Release" not in build_cache.read_text(encoding="utf-8"):
            raise RuntimeError(f"not a Release build: {build_cache}")
        if self.done("preflight"):
            original = json.loads(
                (self.args.results / "environment.json").read_text(encoding="utf-8"))
            query_runtime_transition = self._query_restart_boundary()
            self._validate_resume_environment(
                original, manifest_sha, runtime, git, benchmark_sources,
                query_runtime_transition)
            self._ensure_variant_v2_enabled(
                "resume_query_preflight" if query_runtime_transition
                else "resume_write_preflight")
            expected_snii_ram_buffer_mib = (
                self.args.write_snii_ram_buffer_mib if query_runtime_transition
                else self.args.compaction_snii_ram_buffer_mib
                if self.done("compact") else self.args.write_snii_ram_buffer_mib)
            self._gate_snii_ram_buffer(
                "resume_query_preflight" if query_runtime_transition
                else "resume_write_preflight",
                expected_snii_ram_buffer_mib, allow_update=False)
            affinity = self._record_affinity_gate(
                "resume_query_preflight" if query_runtime_transition else "resume_write_preflight",
                self.args.query_cpu_list if query_runtime_transition
                else self.args.write_cpu_list)
            if query_runtime_transition:
                self.correctness("post_restart")
                self.storage("post_restart")
                self._record_query_runtime_transition(original, affinity)
                self.query_runtime_approved = True
            self.events.append(
                "resume_preflight_validated", database=self.args.database,
                manifest_sha256=manifest_sha, be_pid=self.args.be_pid,
                fe_pid=self.args.fe_pid, be_sha256=runtime["be_sha256"],
                fe_jar_sha256=runtime["fe_jar_sha256"],
                query_runtime_transition=query_runtime_transition,
                service_identity={key: service[key] for key in (
                    "alive_backend", "backend_host_addresses", "local_addresses",
                    "backend_ports", "be_listening_ports")},
                affinity_evidence_timestamp_ns=affinity["timestamp_ns"])
            return
        self._record_affinity_gate("fresh_write_preflight", self.args.write_cpu_list)
        fe_config, fe_config_after = self._ensure_variant_v2_enabled("fresh_write_preflight")
        snii_ram_buffer = self._gate_snii_ram_buffer(
            "fresh_write_preflight", self.args.write_snii_ram_buffer_mib,
            allow_update=True)
        variables = mysql_tsv(
            self.args.host, self.args.mysql_port, self.args.user,
            "SHOW VARIABLES WHERE Variable_name IN "
            "('enable_sql_cache','enable_query_cache','enable_condition_cache',"
            "'enable_inverted_index_query_cache','enable_inverted_index_searcher_cache',"
            "'enable_page_cache','inverted_index_skip_threshold','enable_count_on_index_pushdown')",
            headers=True)
        disk = shutil.disk_usage(self.args.storage_root)
        metadata = {
            "run_id": self.run_id,
            "database": self.args.database,
            "repo_root": str(self.args.repo_root),
            **git,
            "manifest": str(self.args.manifest),
            "manifest_sha256": manifest_sha,
            "dataset_scope": self.args.dataset_scope,
            "raw_dir": str(self.args.raw_dir),
            "manifest_files": sum(len(value) for value in self.batches.values()),
            "expected_rows": self.expected_rows,
            "source_bytes": self.source_bytes,
            "source_sha256_reverified": not self.args.skip_source_sha256,
            **runtime,
            "cpu_affinity": self.args.cpu_affinity,
            "write_snii_ram_buffer_mib": self.args.write_snii_ram_buffer_mib,
            "compaction_snii_ram_buffer_mib": self.args.compaction_snii_ram_buffer_mib,
            "benchmark_sources": benchmark_sources,
            "release_cache": str(build_cache),
            "health": service["health"],
            "backends": service["backends"],
            "service_identity": {key: service[key] for key in (
                "alive_backend", "backend_host_addresses", "local_addresses",
                "backend_ports", "be_listening_ports")},
            "fe_config_before": fe_config,
            "fe_config_after": fe_config_after,
            "snii_ram_buffer_preflight": snii_ram_buffer,
            "session_variables_before": variables,
            "ports": {
                "mysql": self.args.mysql_port,
                "fe_http": self.args.fe_http_port,
                "be_http": self.args.be_http_port,
            },
            "storage_root": str(self.args.storage_root),
            "disk_total": disk.total,
            "disk_used": disk.used,
            "disk_free": disk.free,
            "loadavg": os.getloadavg(),
            "preflight_wall_seconds": time.monotonic() - started,
        }
        self._snapshot_benchmark_sources(benchmark_sources)
        write_json(self.args.results / "environment.json", metadata)
        shutil.copyfile(self.args.manifest, self.args.results / "source_manifest.tsv")
        shutil.copyfile(self.root / "queries" / "query_set.tsv", self.args.results / "query_set.tsv")
        self.events.append("preflight", **metadata)
        self.complete("preflight", expected_rows=self.expected_rows, source_bytes=self.source_bytes)

    def create_tables(self) -> None:
        if not re.fullmatch(r"variant_p2_index_compare_[a-zA-Z0-9_]+", self.args.database):
            raise RuntimeError(f"unsafe benchmark database name: {self.args.database}")
        rendered = []
        with MysqlSession(self.args.host, self.args.mysql_port, self.args.user) as session:
            query_rows(session, session_settings())
            if self.args.fresh:
                query_rows(session, f"DROP DATABASE IF EXISTS `{self.args.database}`")
            query_rows(session, f"CREATE DATABASE IF NOT EXISTS `{self.args.database}`")
            query_rows(session, f"USE `{self.args.database}`")
            for batch in range(8):
                name = f"github_events_stage_{batch}"
                ddl = render_ddl(self.template, name, f"stage_{batch}")
                query_rows(session, ddl)
                rendered.append(ddl)
            for layout in LAYOUTS:
                name = table_name(layout)
                ddl = render_ddl(self.template, name, layout)
                query_rows(session, ddl)
                rendered.append(ddl)
        ddl_path = self.args.results / "ddl.sql"
        ddl_path.write_text("\n\n".join(rendered) + "\n", encoding="utf-8")
        creates = {}
        for layout in LAYOUTS:
            table = table_name(layout)
            indexes = mysql_tsv(
                self.args.host, self.args.mysql_port, self.args.user,
                f"SHOW INDEX FROM `{table}`", self.args.database, headers=True)
            expected_indexes = 0 if layout == "no_index" else 2
            if len(indexes) != expected_indexes:
                raise RuntimeError(
                    f"{table}: expected {expected_indexes} indexes, got {indexes}")
            creates[table] = mysql_tsv(
                self.args.host, self.args.mysql_port, self.args.user,
                f"SHOW CREATE TABLE `{table}`", self.args.database, headers=True)
        write_json(self.args.results / "show_create_tables.json", creates)
        self.events.append("create_tables", ddl_sha256=sha256_file(ddl_path))

    def load_staging(self) -> None:
        records = []
        for batch in range(8):
            table = f"github_events_stage_{batch}"
            if not self.batches[batch]:
                records.append({"batch": batch, "table": table, "rows": 0, "skipped": True})
                continue
            existing_rows_raw = mysql_tsv(
                self.args.host, self.args.mysql_port, self.args.user,
                f"SELECT COUNT(*) FROM `{table}`", self.args.database, headers=False)
            existing_rows = int(existing_rows_raw[0][0])
            if existing_rows == self.batch_rows[batch]:
                records.append({
                    "batch": batch, "table": table, "rows": existing_rows,
                    "skipped": True, "reason": "already_loaded_resume",
                })
                print(f"[stage] batch={batch} already loaded; resume", flush=True)
                continue
            if existing_rows != 0:
                raise RuntimeError(
                    f"stage batch {batch} has partial rows={existing_rows}, "
                    f"expected 0 or {self.batch_rows[batch]}")
            print(
                f"[stage] batch={batch} files={len(self.batches[batch])} "
                f"rows={self.batch_rows[batch]}", flush=True)
            result = stream_load_batch(
                self.args.host, self.args.be_http_port, self.args.database, table,
                self.batches[batch], self.batch_rows[batch])
            records.append({"batch": batch, "table": table, "rows": self.batch_rows[batch], **result})
            self.events.append("stage_batch", batch=batch, table=table, result=result)
        with MysqlSession(
                self.args.host, self.args.mysql_port, self.args.user, self.args.database) as session:
            query_rows(session, session_settings())
            counts = {}
            for batch in range(8):
                values = parse_single_row(
                    query_rows(session, f"SELECT COUNT(*) FROM `github_events_stage_{batch}`"),
                    1, f"stage batch {batch}")
                counts[batch] = int(values[0])
                if counts[batch] != self.batch_rows[batch]:
                    raise RuntimeError(
                        f"stage {batch}: rows={counts[batch]}, expected={self.batch_rows[batch]}")
            fingerprint_batches = [
                fingerprint_relation(session, f"`github_events_stage_{batch}`")
                for batch in range(8) if self.batch_rows[batch] > 0
            ]
            fingerprint_rows = combine_fingerprint_rows(fingerprint_batches)
            source_ids = parse_single_row(query_rows(session, f"""
                SELECT COALESCE(SUM(multiplicity), 0), COUNT(*),
                       SUM(IF(multiplicity > 1, 1, 0)),
                       SUM(multiplicity - 1), MAX(multiplicity)
                FROM (
                    SELECT id, COUNT(*) AS multiplicity
                    FROM {source_id_union()}
                    GROUP BY id
                ) AS per_id
            """), 5, "staging ids")
        if int(source_ids[0]) != self.expected_rows:
            raise RuntimeError(f"staging rows={source_ids[0]}, expected={self.expected_rows}")
        fingerprint_meta = write_fingerprint(
            self.args.results / "fingerprint_stage.tsv", fingerprint_rows)
        write_json(self.args.results / "stage_load.json", records)
        self.state["stage_fingerprint_sha256"] = fingerprint_meta["sha256"]
        self.events.append(
            "stage_complete", counts=counts, fingerprint=fingerprint_meta,
            distinct_ids=int(source_ids[1]), duplicate_id_groups=int(source_ids[2]),
            duplicate_id_excess_rows=int(source_ids[3]), max_id_multiplicity=int(source_ids[4]))

    def write_targets(self) -> None:
        self._gate_snii_ram_buffer(
            "write_before", self.args.write_snii_ram_buffer_mib, allow_update=False)
        records = []
        with MysqlSession(
                self.args.host, self.args.mysql_port, self.args.user, self.args.database) as session:
            query_rows(session, session_settings())
            for layout in LAYOUTS:
                count = int(parse_single_row(
                    query_rows(session, f"SELECT COUNT(*) FROM `{table_name(layout)}`"),
                    1, layout)[0])
                if count != 0:
                    raise RuntimeError(f"write benchmark requires empty {layout}, got {count} rows")
            for batch in range(8):
                if self.batch_rows[batch] == 0:
                    continue
                order = LATIN_ORDERS[batch % len(LATIN_ORDERS)]
                for position, layout in enumerate(order):
                    target = table_name(layout)
                    statement = f"""
                        INSERT INTO `{target}`
                        SELECT id,type,actor,repo,payload,public,created_at,org
                        FROM `github_events_stage_{batch}`
                    """
                    before = proc_snapshot(self.args.be_pid)
                    started = time.monotonic()
                    query_rows(session, statement)
                    client_wall = time.monotonic() - started
                    after = proc_snapshot(self.args.be_pid)
                    record = {
                        "batch": batch,
                        "position": position,
                        "order": list(order),
                        "layout": layout,
                        "table": target,
                        "rows": self.batch_rows[batch],
                        "snii_ram_buffer_mib": self.args.write_snii_ram_buffer_mib,
                        "client_wall_seconds": client_wall,
                        **proc_delta(before, after),
                    }
                    records.append(record)
                    self.events.append("target_write", **record)
                    print(
                        f"[write] batch={batch} position={position} layout={layout} "
                        f"rows={self.batch_rows[batch]} wall={client_wall:.3f}s", flush=True)
        write_json(self.args.results / "write_records.json", records)
        self._gate_snii_ram_buffer(
            "write_after", self.args.write_snii_ram_buffer_mib, allow_update=False)

    def correctness(self, label: str) -> None:
        stage_sha = self.state.get("stage_fingerprint_sha256")
        if not stage_sha:
            stage_path = self.args.results / "fingerprint_stage.tsv"
            stage_sha = sha256_file(stage_path)
        result: dict[str, Any] = {
            "label": label, "tables": {}, "queries": {}, "populations": {}}
        with MysqlSession(
                self.args.host, self.args.mysql_port, self.args.user, self.args.database) as session:
            query_rows(session, session_settings())
            for layout in LAYOUTS:
                table = table_name(layout)
                rows = fingerprint_relation(session, f"`{table}`")
                meta = write_fingerprint(
                    self.args.results / f"fingerprint_{label}_{layout}.tsv", rows)
                if meta["sha256"] != stage_sha:
                    raise RuntimeError(
                        f"{label} {layout}: full fingerprint {meta['sha256']} != stage {stage_sha}")
                result["tables"][layout] = meta
            for query in self.queries:
                signatures: dict[str, list[str]] = {}
                for layout in layouts_for_query(query):
                    table = table_name(layout)
                    scan = parse_single_row(
                        query_rows(session, query_signature_sql(table, query["predicate"], False)),
                        5, f"{label} {query['id']} {layout} scan")
                    signatures[f"{layout}:off"] = scan
                    for enable_index in modes_for_query(query, layout):
                        if not enable_index:
                            continue
                        indexed = parse_single_row(
                            query_rows(session, query_signature_sql(table, query["predicate"], True)),
                            5, f"{label} {query['id']} {layout} index")
                        if indexed != scan:
                            raise RuntimeError(
                                f"{label} {query['id']} {layout}: index={indexed}, scan={scan}")
                        signatures[f"{layout}:on"] = indexed
                scan_values = [value for key, value in signatures.items() if key.endswith(":off")]
                if any(value != scan_values[0] for value in scan_values[1:]):
                    raise RuntimeError(
                        f"{label} {query['id']}: cross-layout scans differ: {signatures}")
                result["queries"][query["id"]] = signatures
                result["populations"][query["id"]] = query_population(
                    query, int(scan_values[0][0]), self.expected_rows,
                    self.args.dataset_scope == "full")
                print(f"[correctness] {label} {query['id']} {signatures}", flush=True)
        if label in ("post_compaction", "post_restart"):
            reference_label = "pre_compaction" if label == "post_compaction" else "post_compaction"
            before = json.loads(
                (self.args.results / f"correctness_{reference_label}.json").read_text(
                    encoding="utf-8"))
            if (before["tables"] != result["tables"]
                    or before["queries"] != result["queries"]
                    or before["populations"] != result["populations"]):
                raise RuntimeError(
                    f"logical/query fingerprints changed between {reference_label} and {label}")
        write_json(self.args.results / f"correctness_{label}.json", result)
        self.events.append("correctness", **result)

    def table_state_when_reported(self, table: str) -> dict[str, Any]:
        deadline = time.monotonic() + self.args.tablet_report_timeout
        next_report = 0.0
        last_detail = "no tablet state sampled"
        while time.monotonic() < deadline:
            try:
                state = table_state(self.args, self.be_http, table)
                if state["aggregate"]["rows"] == self.expected_rows:
                    return state
                last_detail = (
                    f"reported rows={state['aggregate']['rows']}, "
                    f"expected={self.expected_rows}")
            except RuntimeError as error:
                if "disagree with LocalDataSize" not in str(error):
                    raise
                last_detail = str(error)
            now = time.monotonic()
            if now >= next_report:
                print(f"[storage] waiting for tablet report: {table}: {last_detail}", flush=True)
                next_report = now + 30.0
            time.sleep(2)
        raise TimeoutError(
            f"{table}: tablet report did not converge within "
            f"{self.args.tablet_report_timeout}s: {last_detail}")

    def storage(self, label: str) -> None:
        states = {}
        signatures = {}
        for layout in LAYOUTS:
            state = self.table_state_when_reported(table_name(layout))
            state["layout"] = layout
            state["phase"] = label
            states[layout] = state
            signatures[layout] = topology_signature(state)
            aggregate = state["aggregate"]
            if aggregate["rows"] != self.expected_rows:
                raise RuntimeError(f"{label} {layout}: rows={aggregate['rows']}")
            if layout == "no_index" and (
                    aggregate["index_files"] != 0 or aggregate["index_bytes"] != 0):
                raise RuntimeError(f"{label} NoIndex has active index files: {aggregate}")
            if layout != "no_index" and aggregate["index_bytes"] <= 0:
                raise RuntimeError(f"{label} {layout} has no active index bytes: {aggregate}")
        reference = signatures["no_index"]
        for layout in LAYOUTS[1:]:
            if signatures[layout] != reference:
                raise RuntimeError(
                    f"{label}: rowset topology differs for {layout}\n"
                    f"no_index={reference}\n{layout}={signatures[layout]}")
        if label == "post_import":
            expected_nonempty = sum(rows > 0 for rows in self.batch_rows.values())
            if expected_nonempty < 2:
                raise RuntimeError("compaction smoke needs at least two non-empty manifest batches")
            expected_last_version = 1 + expected_nonempty
            for layout, state in states.items():
                for tablet in state["tablets"]:
                    assert_version_chain(tablet, expected_last_version, compacted=False)
                    if tablet["compaction"].get("stale_rowsets") not in (None, [], ""):
                        raise RuntimeError(
                            f"{layout} tablet {tablet['tablet_id']}: fresh input has stale "
                            f"rowsets {tablet['compaction']['stale_rowsets']}")
        if label in ("post_compaction", "post_restart"):
            expected_last_version = 1 + sum(
                rows > 0 for rows in self.batch_rows.values())
            for layout, state in states.items():
                for tablet in state["tablets"]:
                    assert_version_chain(tablet, expected_last_version, compacted=True)
        write_json(self.args.results / f"storage_{label}.json", states)
        if label == "post_restart":
            before = json.loads(
                (self.args.results / "storage_post_compaction.json").read_text(
                    encoding="utf-8"))
            before_identity = persistent_storage_identity(before)
            current_identity = persistent_storage_identity(states)
            before_sha256 = hashlib.sha256(json.dumps(
                before_identity, sort_keys=True).encode()).hexdigest()
            current_sha256 = hashlib.sha256(json.dumps(
                current_identity, sort_keys=True).encode()).hexdigest()
            if current_identity != before_identity:
                raise RuntimeError(
                    "persistent tablet/rowset/file identity changed across query restart: "
                    f"before={before_sha256}, current={current_sha256}")
            self.events.append(
                "query_restart_storage_identity", status="pass",
                sha256=current_sha256)
        for layout, state in states.items():
            self.events.append("storage", phase=label, layout=layout, **state["aggregate"])

    def compact(self) -> None:
        records = []
        prior_storage = json.loads(
            (self.args.results / "storage_post_import.json").read_text(encoding="utf-8"))
        live_storage = {}
        for layout in LAYOUTS:
            state = self.table_state_when_reported(table_name(layout))
            state["layout"] = layout
            state["phase"] = "pre_compaction_identity_gate"
            live_storage[layout] = state
        expected_identity = persistent_storage_identity(prior_storage)
        live_identity = persistent_storage_identity(live_storage)
        expected_identity_sha256 = hashlib.sha256(json.dumps(
            expected_identity, sort_keys=True).encode()).hexdigest()
        live_identity_sha256 = hashlib.sha256(json.dumps(
            live_identity, sort_keys=True).encode()).hexdigest()
        if live_identity != expected_identity:
            raise RuntimeError(
                "pre-compaction persistent storage identity drift: "
                f"expected={expected_identity_sha256}, live={live_identity_sha256}")
        self.events.append(
            "pre_compaction_storage_identity", status="pass",
            sha256=live_identity_sha256)
        self._gate_snii_ram_buffer(
            "compaction_prepare", self.args.compaction_snii_ram_buffer_mib,
            allow_update=True)
        expected_last_version = 1 + sum(
            rows > 0 for rows in self.batch_rows.values())
        expected_version = f"[0-{expected_last_version}]"
        expected_input_rowsets = expected_last_version
        compaction_signatures = {}
        for layout in LAYOUTS:
            table = table_name(layout)
            tablets = prior_storage[layout]["tablets"]
            self._gate_snii_ram_buffer(
                f"compaction_{layout}_before",
                self.args.compaction_snii_ram_buffer_mib, allow_update=False)
            prior_ids = {}
            for tablet in tablets:
                tablet_id = tablet["tablet_id"]
                run_status = self.be_http.json(
                    "GET", f"/api/compaction/run_status?tablet_id={tablet_id}")
                if run_status.get("status") != "Success" or run_status.get("run_status"):
                    raise RuntimeError(
                        f"tablet {tablet_id}: compaction already running before benchmark: "
                        f"{run_status}")
                profile = self.be_http.json(
                    "GET", f"/api/compaction/profile?tablet_id={tablet_id}"
                           "&compact_type=full&top_n=1")
                profiles = completed_compaction_profiles(profile)
                prior_ids[tablet_id] = profiles[0]["compaction_id"] if profiles else None
            before_proc = proc_snapshot(self.args.be_pid)
            started = time.monotonic()
            attempted_ids = []
            triggered_ids = []
            unknown_submission_ids = []
            trigger_errors = []
            for tablet in tablets:
                tablet_id = tablet["tablet_id"]
                attempted_ids.append(tablet_id)
                try:
                    response = self.be_http.json(
                        "POST", f"/api/compaction/run?tablet_id={tablet_id}&compact_type=full",
                        timeout=min(600, self.args.compaction_timeout))
                except Exception as error:  # Reconcile unknown submission before surfacing it.
                    unknown_submission_ids.append(tablet_id)
                    trigger_errors.append({
                        "tablet_id": tablet_id,
                        "submission": "unknown",
                        "error_type": type(error).__name__,
                        "error": str(error),
                    })
                    continue
                if not isinstance(response, dict):
                    unknown_submission_ids.append(tablet_id)
                    trigger_errors.append({
                        "tablet_id": tablet_id,
                        "submission": "invalid_response",
                        "response": response,
                    })
                    continue
                status = str(response.get("status", "")).lower()
                if status not in ("success", "already_exist"):
                    trigger_errors.append({
                        "tablet_id": tablet_id,
                        "submission": "rejected",
                        "response": response,
                    })
                    continue
                triggered_ids.append(tablet_id)
                if status == "already_exist":
                    trigger_errors.append({
                        "tablet_id": tablet_id,
                        "submission": "already_exist",
                        "response": response,
                    })
            reconcile_ids = list(dict.fromkeys(triggered_ids + unknown_submission_ids))
            deadline = time.monotonic() + self.args.compaction_timeout
            terminal: dict[int, dict[str, Any]] = {}
            poll_error_counts: dict[int, int] = {}
            last_poll_errors: dict[int, dict[str, str]] = {}
            next_report = 0.0
            while len(terminal) != len(reconcile_ids):
                if time.monotonic() >= deadline:
                    failure = {
                        "layout": layout,
                        "table": table,
                        "reason": "timeout",
                        "trigger_errors": trigger_errors,
                        "attempted_tablets": attempted_ids,
                        "triggered_tablets": triggered_ids,
                        "unknown_submission_tablets": unknown_submission_ids,
                        "reconcile_tablets": reconcile_ids,
                        "poll_error_counts": poll_error_counts,
                        "last_poll_errors": last_poll_errors,
                        "terminal_profiles": list(terminal.values()),
                        "snii_ram_buffer_mib": self.args.compaction_snii_ram_buffer_mib,
                    }
                    write_json(self.args.results / "compaction_failure.json", failure)
                    self.events.append("compaction_failure", **failure)
                    raise TimeoutError(
                        f"{table}: compaction timeout; terminal={list(terminal)}")
                for tablet in tablets:
                    tablet_id = tablet["tablet_id"]
                    if tablet_id not in reconcile_ids or tablet_id in terminal:
                        continue
                    try:
                        response = self.be_http.json(
                            "GET", f"/api/compaction/profile?tablet_id={tablet_id}"
                                   "&compact_type=full&top_n=1")
                    except Exception as error:
                        poll_error_counts[tablet_id] = poll_error_counts.get(tablet_id, 0) + 1
                        last_poll_errors[tablet_id] = {
                            "error_type": type(error).__name__,
                            "error": str(error),
                        }
                        continue
                    profiles = completed_compaction_profiles(response)
                    if profiles and profiles[0]["compaction_id"] != prior_ids[tablet_id]:
                        terminal[tablet_id] = profiles[0]
                now = time.monotonic()
                if len(terminal) != len(reconcile_ids) and now >= next_report:
                    print(
                        f"[compact] {layout}: {len(terminal)}/{len(reconcile_ids)} "
                        "reconciled tablets terminal",
                        flush=True)
                    next_report = now + 30
                if len(terminal) != len(reconcile_ids):
                    time.sleep(2)
            after_proc = proc_snapshot(self.args.be_pid)
            client_wall = time.monotonic() - started
            self._gate_snii_ram_buffer(
                f"compaction_{layout}_after",
                self.args.compaction_snii_ram_buffer_mib, allow_update=False)
            if poll_error_counts:
                self.events.append(
                    "compaction_poll_retries", layout=layout,
                    counts=poll_error_counts, last_errors=last_poll_errors)
            profiles = [terminal[tablet_id] for tablet_id in reconcile_ids]
            failed_profiles = [profile for profile in profiles if not profile.get("success")]
            if trigger_errors or failed_profiles:
                failure = {
                    "layout": layout,
                    "table": table,
                    "reason": "trigger_failure" if trigger_errors else "tablet_failure",
                    "client_wall_seconds": client_wall,
                    "trigger_errors": trigger_errors,
                    "attempted_tablets": attempted_ids,
                    "triggered_tablets": triggered_ids,
                    "unknown_submission_tablets": unknown_submission_ids,
                    "reconcile_tablets": reconcile_ids,
                    "poll_error_counts": poll_error_counts,
                    "last_poll_errors": last_poll_errors,
                    "profiles": profiles,
                    "failed_profiles": failed_profiles,
                    "snii_ram_buffer_mib": self.args.compaction_snii_ram_buffer_mib,
                    **proc_delta(before_proc, after_proc),
                }
                write_json(self.args.results / "compaction_failure.json", failure)
                self.events.append("compaction_failure", **failure)
                raise RuntimeError(
                    f"{layout}: {len(trigger_errors)} trigger errors and "
                    f"{len(failed_profiles)}/{len(profiles)} triggered compactions failed; "
                    "see compaction_failure.json")
            profiles = [terminal[tablet["tablet_id"]] for tablet in tablets]
            post_state = self.table_state_when_reported(table)
            post_tablets = {
                tablet["tablet_id"]: tablet for tablet in post_state["tablets"]}
            for profile, tablet in zip(profiles, tablets):
                tablet_id = tablet["tablet_id"]
                post_tablet = post_tablets[tablet_id]
                assert_version_chain(post_tablet, expected_last_version, compacted=True)
                expected_input_segments = sum(rowset["segments"] for rowset in tablet["rowsets"])
                input_bytes = tablet_active_bytes(tablet)
                output_bytes = tablet_active_bytes(post_tablet)
                if not profile.get("success") or profile.get("trigger_method") != "MANUAL":
                    raise RuntimeError(f"{layout}: invalid compaction profile {profile}")
                if int(profile["input_rowsets_count"]) != expected_input_rowsets:
                    raise RuntimeError(
                        f"{layout}: expected {expected_input_rowsets} input rowsets: {profile}")
                if profile.get("input_version_range") != expected_version:
                    raise RuntimeError(
                        f"{layout}: expected input version {expected_version}: {profile}")
                if profile.get("output_version") != expected_version:
                    raise RuntimeError(
                        f"{layout}: expected output version {expected_version}: {profile}")
                if int(profile["input_row_num"]) != int(tablet["row_count"]):
                    raise RuntimeError(f"{layout}: compaction input row mismatch: {profile}")
                if int(profile["input_segments_num"]) != expected_input_segments:
                    raise RuntimeError(f"{layout}: compaction input segment mismatch: {profile}")
                if int(profile["output_row_num"]) != int(profile["input_row_num"]):
                    raise RuntimeError(f"{layout}: compaction output row mismatch: {profile}")
                if int(profile["filtered_rows"]) != 0 or int(profile["output_segments_num"]) <= 0:
                    raise RuntimeError(f"{layout}: invalid compaction output: {profile}")
                if int(profile["input_data_size"]) + int(profile["input_index_size"]) != int(
                        profile["input_total_size"]):
                    raise RuntimeError(f"{layout}: input size accounting mismatch: {profile}")
                if int(profile["output_data_size"]) + int(profile["output_index_size"]) != int(
                        profile["output_total_size"]):
                    raise RuntimeError(f"{layout}: output size accounting mismatch: {profile}")
                if input_bytes != {
                        "data": int(profile["input_data_size"]),
                        "index": int(profile["input_index_size"]),
                        "other": 0,
                        "total": int(profile["input_total_size"])}:
                    raise RuntimeError(
                        f"{layout}: profile/filesystem input bytes differ: "
                        f"files={input_bytes}, profile={profile}")
                if output_bytes != {
                        "data": int(profile["output_data_size"]),
                        "index": int(profile["output_index_size"]),
                        "other": 0,
                        "total": int(profile["output_total_size"])}:
                    raise RuntimeError(
                        f"{layout}: profile/filesystem output bytes differ: "
                        f"files={output_bytes}, profile={profile}")
                if int(profile["output_segments_num"]) != sum(
                        item["segments"] for item in post_tablet["rowsets"]):
                    raise RuntimeError(
                        f"{layout}: profile/post segment mismatch: {profile}")
                if layout == "no_index" and (
                        int(profile["input_index_size"]) != 0 or int(profile["output_index_size"]) != 0):
                    raise RuntimeError(f"NoIndex compaction contains index bytes: {profile}")
            signature = sorted(
                (profile["input_rowsets_count"], profile["input_row_num"],
                 profile["input_segments_num"], profile["input_version_range"],
                 profile["output_version"], profile["output_row_num"],
                 profile["output_segments_num"], profile["is_ordered_data_compaction"])
                for profile in profiles)
            compaction_signatures[layout] = signature
            record = {
                "layout": layout,
                "table": table,
                "rows": self.expected_rows,
                "snii_ram_buffer_mib": self.args.compaction_snii_ram_buffer_mib,
                "client_wall_seconds": client_wall,
                "profiles": profiles,
                **proc_delta(before_proc, after_proc),
            }
            records.append(record)
            self.events.append("compaction", **record)
            print(f"[compact] {layout} wall={client_wall:.3f}s", flush=True)
        reference = compaction_signatures["no_index"]
        for layout in LAYOUTS[1:]:
            comparable = compaction_signatures[layout]
            if comparable != reference:
                raise RuntimeError(
                    f"compaction topology differs: no_index={reference}, {layout}={comparable}")
        write_json(self.args.results / "compaction_records.json", records)

    def capture_profile(
            self, session: MysqlSession, query: dict[str, str], layout: str,
            enable_index: bool) -> dict[str, Any]:
        mode = "on" if enable_index else "off"
        tag = f"variant_p2_real_{query['id']}_{layout}_{mode}_{uuid.uuid4().hex[:8]}"
        query_rows(session, "SET enable_profile = true; SET profile_level = 2")
        statement = timed_count_sql(
            table_name(layout), query["predicate"], enable_index, tag)
        values = parse_single_row([session.select_one(statement)], 1, tag)
        query_id = session.select_one("SELECT last_query_id()")
        if not re.fullmatch(r"[0-9a-f]{1,16}-[0-9a-f]{1,16}", query_id):
            raise RuntimeError(f"invalid last_query_id for {tag}: {query_id}")
        deadline = time.monotonic() + 30.0
        profile = ""
        attempts = 0
        last_state = "profile not returned"
        while time.monotonic() < deadline:
            attempts += 1
            try:
                profile = self.fe_http.request(
                    "GET", f"/api/profile/text?query_id={query_id}", timeout=5
                ).decode(errors="replace")
                state = re.search(r"Profile Completion State\s*:\s*([A-Z]+)", profile)
                last_state = state.group(1) if state else "completion state missing"
                if last_state == "COMPLETE":
                    break
            except urllib.error.HTTPError as error:
                if error.code not in (404, 500):
                    raise
                last_state = f"HTTP {error.code}"
            time.sleep(0.25)
        else:
            raise RuntimeError(
                f"profile {query_id} did not reach COMPLETE within 30s: {last_state}")
        profile_wait_seconds = 30.0 - max(0.0, deadline - time.monotonic())
        profile_dir = self.args.results / "profiles"
        profile_dir.mkdir(exist_ok=True)
        profile_path = profile_dir / f"{query['id']}.{layout}.{mode}.{query_id}.txt"
        profile_path.write_text(profile, encoding="utf-8")
        explain = query_rows(session, "EXPLAIN " + statement)
        explain_path = profile_dir / f"{query['id']}.{layout}.{mode}.explain.txt"
        explain_path.write_text("\n".join(explain) + "\n", encoding="utf-8")
        counter_lines = [line.strip() for line in profile.splitlines() if any(
            name in line for name in (
                "RowsInvertedIndexFiltered", "InvertedIndexQueryTime",
                "InvertedIndexDowngradeCount", "InvertedIndexQueryCache",
                "ScanRows", "RowsExprPred"))]
        admission = profile_index_admission(profile)
        if any(admission["inverted_index_query_cache"].values()):
            raise RuntimeError(
                f"inverted-index query cache was used for {tag}: "
                f"{admission['inverted_index_query_cache']}")
        expected_admission = expected_index_admission(query, layout, enable_index)
        return {
            "query_id": query_id,
            "result_count": int(values[0]),
            "profile": str(profile_path),
            "profile_sha256": sha256_file(profile_path),
            "explain": str(explain_path),
            "counter_lines": counter_lines,
            "profile_poll_attempts": attempts,
            "profile_wait_seconds": profile_wait_seconds,
            "expected_index_admission": expected_admission,
            **admission,
        }

    def measure_queries(self) -> None:
        samples = []
        profiles = {}
        correctness_oracle = json.loads(
            (self.args.results / "correctness_post_compaction.json").read_text(
                encoding="utf-8"))

        def expected_count(
                query: dict[str, str], layout: str, enable_index: bool) -> int:
            mode = "on" if enable_index else "off"
            try:
                signature = correctness_oracle["queries"][query["id"]][f"{layout}:{mode}"]
            except KeyError as error:
                raise RuntimeError(
                    f"missing post-compaction oracle for {query['id']} {layout}:{mode}") from error
            return int(signature[0])

        with MysqlSession(
                self.args.host, self.args.mysql_port, self.args.user, self.args.database) as session:
            query_rows(session, session_settings())
            query_rows(session, "SET enable_profile = false")
            variables = query_rows(
                session,
                "SHOW VARIABLES WHERE Variable_name IN "
                "('enable_sql_cache','enable_query_cache','enable_condition_cache',"
                "'enable_inverted_index_query_cache','enable_inverted_index_searcher_cache',"
                "'enable_page_cache','inverted_index_skip_threshold','enable_count_on_index_pushdown')")
            parsed_variables = assert_query_session_variables(variables)
            (self.args.results / "query_session_variables.tsv").write_text(
                "\n".join(variables) + "\n", encoding="utf-8")
            self.events.append("query_session_variables", values=parsed_variables)
            for query_index, query in enumerate(self.queries):
                query_rows(session, "SET enable_profile = false")
                entries = [
                    (layout, enable_index)
                    for layout in layouts_for_query(query)
                    for enable_index in modes_for_query(query, layout)
                ]
                for warmup in range(QUERY_WARMUPS):
                    for layout, enable_index in entries:
                        values = parse_single_row([session.select_one(timed_count_sql(
                                table_name(layout), query["predicate"], enable_index))],
                            1, f"warm {warmup} {query['id']} {layout}")
                        expected = expected_count(query, layout, enable_index)
                        if int(values[0]) != expected:
                            raise RuntimeError(
                                f"warm {warmup} {query['id']} {layout} count={values[0]}, "
                                f"post-compaction oracle={expected}")
                        print(
                            f"[query warm] warmup={warmup} {query['id']} {layout} "
                            f"{'on' if enable_index else 'off'} count={values[0]}", flush=True)
                repetitions = int(query["repetitions"])
                for repetition in range(repetitions):
                    start = (query_index + repetition) % len(entries)
                    order = entries[start:] + entries[:start]
                    for position, (layout, enable_index) in enumerate(order):
                        before = proc_snapshot(self.args.be_pid)
                        started = time.monotonic_ns()
                        values = parse_single_row([session.select_one(timed_count_sql(
                                table_name(layout), query["predicate"], enable_index))],
                            1, f"timed {query['id']} {layout}")
                        client_ns = time.monotonic_ns() - started
                        expected = expected_count(query, layout, enable_index)
                        if int(values[0]) != expected:
                            raise RuntimeError(
                                f"timed {query['id']} {layout} count={values[0]}, "
                                f"post-compaction oracle={expected}")
                        after = proc_snapshot(self.args.be_pid)
                        sample = {
                            "query_id": query["id"],
                            "category": query["category"],
                            "layout": layout,
                            "index_enabled": enable_index,
                            "repetition": repetition,
                            "position": position,
                            "order": [f"{item[0]}:{'on' if item[1] else 'off'}" for item in order],
                            "result_count": int(values[0]),
                            "client_wall_seconds": client_ns / 1e9,
                            **proc_delta(before, after),
                        }
                        samples.append(sample)
                        self.events.append("query_sample", **sample)
                        print(
                            f"[query] {query['id']} rep={repetition} layout={layout} "
                            f"index={'on' if enable_index else 'off'} "
                            f"wall={client_ns / 1e6:.3f}ms", flush=True)
                for layout, enable_index in entries:
                    key = f"{query['id']}:{layout}:{'on' if enable_index else 'off'}"
                    profiles[key] = self.capture_profile(session, query, layout, enable_index)
                    expected = expected_count(query, layout, enable_index)
                    if profiles[key]["result_count"] != expected:
                        raise RuntimeError(
                            f"profile {key} count={profiles[key]['result_count']}, "
                            f"post-compaction oracle={expected}")
                query_rows(session, "SET enable_profile = false")
        sample_keys = [
            (sample["query_id"], sample["layout"], sample["index_enabled"],
             sample["repetition"])
            for sample in samples
        ]
        if len(sample_keys) != len(set(sample_keys)):
            raise RuntimeError("duplicate query sample identity detected")
        query_by_id = {query["id"]: query for query in self.queries}
        grouped: dict[tuple[str, str, bool], list[dict[str, Any]]] = {}
        for sample in samples:
            grouped.setdefault(
                (sample["query_id"], sample["layout"], sample["index_enabled"]), []).append(sample)
        summary = []
        stability = []
        for (query_id, layout, enable_index), group in sorted(grouped.items()):
            values = [sample["client_wall_seconds"] for sample in group]
            mean = statistics.mean(values)
            query = query_by_id[query_id]
            expected_samples = int(query["repetitions"])
            if len(group) != expected_samples:
                raise RuntimeError(
                    f"{query_id} {layout} index={enable_index}: samples={len(group)}, "
                    f"expected={expected_samples}")
            profile_key = f"{query_id}:{layout}:{'on' if enable_index else 'off'}"
            profile = profiles[profile_key]
            cv = statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0
            span = (max(values) - min(values)) / min(values) if min(values) else None
            stability_status = "directional" if len(values) < 5 else (
                "stable" if cv <= STABLE_CV_MAX
                and span is not None and span <= STABLE_SPAN_MAX else "unstable")
            population = correctness_oracle["populations"][query_id]
            summary.append({
                "query_id": query_id,
                "category": query["category"],
                "hit_class": population["hit_class"],
                "selectivity": population["selectivity"],
                "layout": layout,
                "index_enabled": enable_index,
                "expected_index_admission": profile["expected_index_admission"],
                "actual_index_admission": profile["actual_index_admission"],
                "rows_inverted_index_filtered_sum":
                    profile["rows_inverted_index_filtered_sum"],
                "inverted_index_query_time_ns_sum":
                    profile["inverted_index_query_time_ns_sum"],
                "inverted_index_downgrade_count_sum":
                    profile["inverted_index_downgrade_count_sum"],
                "inverted_index_query_cache":
                    json.dumps(profile["inverted_index_query_cache"], sort_keys=True),
                "profile_query_id": profile["query_id"],
                "result_count": group[0]["result_count"],
                "samples": len(values),
                "median_seconds": statistics.median(values),
                "p95_seconds": percentile(values, 0.95),
                "min_seconds": min(values),
                "max_seconds": max(values),
                "cv": cv,
                "span": span,
                "stability_status": stability_status,
            })
            stability.append({
                "query_id": query_id,
                "layout": layout,
                "index_enabled": enable_index,
                "samples": len(values),
                "cv": cv,
                "span": span,
                "status": stability_status,
            })
        mismatches = {
            key: {
                "expected": profile["expected_index_admission"],
                "actual": profile["actual_index_admission"],
                "rows_filtered": profile["rows_inverted_index_filtered_sum"],
                "query_time_ns": profile["inverted_index_query_time_ns_sum"],
                "downgrades": profile["inverted_index_downgrade_count_sum"],
            }
            for key, profile in profiles.items()
            if profile["actual_index_admission"] != profile["expected_index_admission"]
        }
        if mismatches:
            raise RuntimeError(f"profile index admission mismatches: {mismatches}")
        write_json(self.args.results / "query_samples.json", samples)
        write_json(self.args.results / "query_profiles.json", profiles)
        write_json(self.args.results / "query_summary.json", summary)
        write_json(self.args.results / "query_stability.json", {
            "cv_max": STABLE_CV_MAX,
            "span_max": STABLE_SPAN_MAX,
            "groups": stability,
            "unstable_groups": [item for item in stability if item["status"] == "unstable"],
        })

    def report(self) -> None:
        write_records = json.loads(
            (self.args.results / "write_records.json").read_text(encoding="utf-8"))
        compaction_records = json.loads(
            (self.args.results / "compaction_records.json").read_text(encoding="utf-8"))
        query_summary = json.loads(
            (self.args.results / "query_summary.json").read_text(encoding="utf-8"))
        storage_rows = []
        for phase in ("post_import", "post_compaction"):
            states = json.loads(
                (self.args.results / f"storage_{phase}.json").read_text(encoding="utf-8"))
            no_index_total = states["no_index"]["aggregate"]["active_total_bytes"]
            for layout in LAYOUTS:
                aggregate = states[layout]["aggregate"]
                storage_rows.append({
                    "phase": phase,
                    "layout": layout,
                    **aggregate,
                    "bytes_per_row": aggregate["active_total_bytes"] / aggregate["rows"],
                    "ratio_to_no_index": aggregate["active_total_bytes"] / no_index_total,
                })
        write_summary = []
        for layout in LAYOUTS:
            group = [record for record in write_records if record["layout"] == layout]
            wall = sum(record["client_wall_seconds"] for record in group)
            cpu = sum(record["server_cpu_seconds"] for record in group)
            write_summary.append({
                "layout": layout,
                "rows": sum(record["rows"] for record in group),
                "chunks": len(group),
                "snii_ram_buffer_mib": self.args.write_snii_ram_buffer_mib,
                "wall_seconds": wall,
                "server_cpu_seconds": cpu,
                "rows_per_wall_second": self.expected_rows / wall,
                "rows_per_cpu_second": self.expected_rows / cpu if cpu else None,
            })
        compaction_summary = [{
            "layout": record["layout"],
            "rows": record["rows"],
            "snii_ram_buffer_mib": record["snii_ram_buffer_mib"],
            "wall_seconds": record["client_wall_seconds"],
            "server_cpu_seconds": record["server_cpu_seconds"],
            "rows_per_wall_second": record["rows"] / record["client_wall_seconds"],
            "rows_per_cpu_second": record["rows"] / record["server_cpu_seconds"]
            if record["server_cpu_seconds"] else None,
        } for record in compaction_records]

        def write_tsv(name: str, rows: list[dict[str, Any]]) -> None:
            if not rows:
                raise RuntimeError(f"cannot write empty {name}")
            with (self.args.results / name).open("w", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=list(rows[0]), delimiter="\t")
                writer.writeheader()
                writer.writerows(rows)

        write_tsv("write.tsv", write_summary)
        write_tsv("compaction.tsv", compaction_summary)
        write_tsv("query.tsv", query_summary)
        write_tsv("storage.tsv", storage_rows)
        self.events.append(
            "report", write=write_summary, compaction=compaction_summary,
            query_cases=len(query_summary), storage=storage_rows)
        artifacts = {}
        for path in sorted(self.args.results.rglob("*")):
            if path.is_file() and path.name not in {
                    "artifact_sha256.json", "events.jsonl", "state.json", "validation.json"}:
                artifacts[str(path.relative_to(self.args.results))] = sha256_file(path)
        write_json(self.args.results / "artifact_sha256.json", artifacts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--dataset-scope", choices=("full", "partial"), required=True)
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--database", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, required=True)
    parser.add_argument("--fe-http-port", type=int, required=True)
    parser.add_argument("--be-http-port", type=int, required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--storage-root", type=Path, required=True)
    parser.add_argument("--be-pid", type=int, required=True)
    parser.add_argument("--fe-pid", type=int, required=True)
    parser.add_argument("--write-cpu-list", type=cpu_list_argument, required=True)
    parser.add_argument("--query-cpu-list", type=cpu_list_argument, required=True)
    parser.add_argument("--fe-jar", type=Path)
    parser.add_argument("--repo-root", type=Path)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--compaction-timeout", type=int, default=21600)
    parser.add_argument("--tablet-report-timeout", type=int, default=300)
    parser.add_argument(
        "--write-snii-ram-buffer-mib", type=int,
        default=FORMAL_WRITE_SNII_RAM_BUFFER_MIB)
    parser.add_argument(
        "--compaction-snii-ram-buffer-mib", type=int,
        default=FORMAL_COMPACTION_SNII_RAM_BUFFER_MIB)
    parser.add_argument("--skip-source-sha256", action="store_true")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--fresh", action="store_true")
    group.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.dataset_scope == "full" and args.skip_source_sha256:
        parser.error("--dataset-scope full does not allow --skip-source-sha256")
    if args.write_snii_ram_buffer_mib <= 0:
        parser.error("--write-snii-ram-buffer-mib must be positive")
    if args.compaction_snii_ram_buffer_mib < args.write_snii_ram_buffer_mib:
        parser.error(
            "--compaction-snii-ram-buffer-mib must be at least "
            "--write-snii-ram-buffer-mib")
    if args.dataset_scope == "full" and (
            args.write_snii_ram_buffer_mib != FORMAL_WRITE_SNII_RAM_BUFFER_MIB
            or args.compaction_snii_ram_buffer_mib != FORMAL_COMPACTION_SNII_RAM_BUFFER_MIB):
        parser.error(
            "--dataset-scope full requires write/compaction SNII RAM buffers "
            f"{FORMAL_WRITE_SNII_RAM_BUFFER_MIB}/"
            f"{FORMAL_COMPACTION_SNII_RAM_BUFFER_MIB} MiB")
    script_root = Path(__file__).resolve().parents[5]
    args.repo_root = (args.repo_root or script_root).resolve()
    args.fe_jar = (args.fe_jar or args.repo_root / "output" / "fe" / "lib" / "doris-fe.jar").resolve()
    args.manifest = args.manifest.resolve()
    args.raw_dir = (args.raw_dir or args.manifest.parents[1] / "raw").resolve()
    args.storage_root = args.storage_root.resolve()
    args.results = args.results.resolve()
    try:
        args.cpu_affinity = validate_cpu_configuration(
            args.write_cpu_list, args.query_cpu_list)
    except ValueError as error:
        parser.error(str(error))
    if args.dataset_scope == "full" and len(args.write_cpu_list) != 64:
        parser.error(
            "--dataset-scope full requires exactly 64 write CPUs; "
            f"got {len(args.write_cpu_list)}")
    if args.fresh:
        if args.results.exists() and any(args.results.iterdir()):
            parser.error(f"fresh result directory is not empty: {args.results}")
        args.results.mkdir(parents=True, exist_ok=True)
    else:
        if not (args.results / "state.json").is_file():
            parser.error(f"resume state does not exist: {args.results / 'state.json'}")
    return args


if __name__ == "__main__":
    Runner(parse_args()).run()
