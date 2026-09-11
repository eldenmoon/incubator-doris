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

import calendar
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Any
import urllib.request


FULL_VARIANT_P2_EXPECTED_OBJECTS = 2070
FULL_VARIANT_P2_EXPECTED_ROWS = 44_273_863
FULL_VARIANT_P2_BATCHES = 8


def full_variant_p2_expected_files() -> list[str]:
    names = [
        f"2015-{month:02d}-{day:02d}-{hour}.json"
        for month in range(1, 4)
        for day in range(1, calendar.monthrange(2015, month)[1] + 1)
        for hour in range(1, 24)
    ]
    if (len(names) != FULL_VARIANT_P2_EXPECTED_OBJECTS
            or len(set(names)) != FULL_VARIANT_P2_EXPECTED_OBJECTS):
        raise AssertionError(f"invalid built-in variant_p2 file enumeration: {len(names)}")
    return names


def full_variant_p2_batch_assignments(
        file_stats: list[tuple[str, int, int]]) -> dict[str, int]:
    if len(file_stats) != FULL_VARIANT_P2_EXPECTED_OBJECTS:
        raise ValueError(
            f"full variant_p2 batch assignment needs {FULL_VARIANT_P2_EXPECTED_OBJECTS} "
            f"files, got {len(file_stats)}")
    base, remainder = divmod(len(file_stats), FULL_VARIANT_P2_BATCHES)
    capacities = [
        base + int(batch < remainder) for batch in range(FULL_VARIANT_P2_BATCHES)
    ]
    counts = [0] * FULL_VARIANT_P2_BATCHES
    rows = [0] * FULL_VARIANT_P2_BATCHES
    sizes = [0] * FULL_VARIANT_P2_BATCHES
    assignments: dict[str, int] = {}
    # Constrained LPT: balance rows first, bytes second, and use names/ids as
    # deterministic tie breakers while fixing batch cardinality at 258/259.
    for name, row_count, size in sorted(
            file_stats, key=lambda value: (-value[1], -value[2], value[0])):
        if name in assignments:
            raise ValueError(f"duplicate file in batch assignment: {name}")
        candidates = [
            batch for batch in range(FULL_VARIANT_P2_BATCHES)
            if counts[batch] < capacities[batch]
        ]
        batch = min(candidates, key=lambda value: (rows[value], sizes[value], value))
        assignments[name] = batch
        counts[batch] += 1
        rows[batch] += row_count
        sizes[batch] += size
    if max(counts) - min(counts) > 1:
        raise AssertionError(f"invalid full variant_p2 batch counts: {counts}")
    return assignments


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def read_validated_manifest(
        path: Path, raw_dir: Path, verify_sha256: bool,
        dataset_scope: str) -> tuple[dict[int, list[Path]], dict[int, int], int, int]:
    if dataset_scope not in {"full", "partial"}:
        raise ValueError(f"dataset_scope must be full or partial, got {dataset_scope!r}")
    batches = {batch: [] for batch in range(FULL_VARIANT_P2_BATCHES)}
    batch_rows = {batch: 0 for batch in range(FULL_VARIANT_P2_BATCHES)}
    manifest_assignments: dict[str, int] = {}
    file_stats: list[tuple[str, int, int]] = []
    seen: set[str] = set()
    total_rows = 0
    total_bytes = 0
    raw_dir = raw_dir.resolve()
    with path.open(encoding="utf-8") as source:
        reader = csv.DictReader(source, delimiter="\t")
        expected_header = ["batch", "file", "rows", "bytes", "sha256"]
        if reader.fieldnames != expected_header:
            raise ValueError(f"unexpected manifest header: {reader.fieldnames}")
        for line_number, row in enumerate(reader, start=2):
            batch = int(row["batch"])
            if batch not in batches:
                raise ValueError(
                    f"line {line_number}: batch must be in "
                    f"[0, {FULL_VARIANT_P2_BATCHES - 1}]")
            name = row["file"]
            if name in seen:
                raise ValueError(f"line {line_number}: duplicate file {name}")
            seen.add(name)
            candidate = (raw_dir / name).resolve()
            if candidate.parent != raw_dir:
                raise ValueError(f"line {line_number}: path escapes raw directory: {name}")
            if not candidate.is_file():
                raise FileNotFoundError(candidate)
            expected_bytes = int(row["bytes"])
            if expected_bytes <= 0:
                raise ValueError(f"line {line_number}: bytes must be positive")
            if candidate.stat().st_size != expected_bytes:
                raise ValueError(
                    f"line {line_number}: {name} has {candidate.stat().st_size} bytes, "
                    f"expected {expected_bytes}")
            if verify_sha256:
                actual_sha256 = sha256_file(candidate)
                if actual_sha256 != row["sha256"]:
                    raise ValueError(
                        f"line {line_number}: {name} sha256={actual_sha256}, "
                        f"expected {row['sha256']}")
            rows = int(row["rows"])
            if rows <= 0:
                raise ValueError(f"line {line_number}: rows must be positive")
            batches[batch].append(candidate)
            batch_rows[batch] += rows
            manifest_assignments[name] = batch
            file_stats.append((name, rows, expected_bytes))
            total_rows += rows
            total_bytes += expected_bytes
    if not seen:
        raise ValueError("manifest contains no data files")
    if dataset_scope == "full":
        expected = set(full_variant_p2_expected_files())
        missing = sorted(expected - seen)
        unexpected = sorted(seen - expected)
        if missing or unexpected or len(seen) != FULL_VARIANT_P2_EXPECTED_OBJECTS:
            raise ValueError(
                "full variant_p2 manifest file-set mismatch: "
                f"expected={FULL_VARIANT_P2_EXPECTED_OBJECTS}, actual={len(seen)}, "
                f"first_missing={missing[:5]}, first_unexpected={unexpected[:5]}")
        if total_rows != FULL_VARIANT_P2_EXPECTED_ROWS:
            raise ValueError(
                "full variant_p2 manifest row-count mismatch: "
                f"expected={FULL_VARIANT_P2_EXPECTED_ROWS}, actual={total_rows}")
        expected_assignments = full_variant_p2_batch_assignments(file_stats)
        batch_mismatches = [
            (name, manifest_assignments[name], expected_assignments[name])
            for name in full_variant_p2_expected_files()
            if manifest_assignments[name] != expected_assignments[name]
        ]
        if batch_mismatches:
            raise ValueError(
                "full variant_p2 manifest does not use the deterministic balanced "
                f"batch assignment; first mismatches={batch_mismatches[:5]}")
    return batches, batch_rows, total_rows, total_bytes


def proc_snapshot(pid: int) -> dict[str, Any]:
    proc = Path(f"/proc/{pid}")
    stat = (proc / "stat").read_text().split()
    io: dict[str, int] = {}
    for line in (proc / "io").read_text().splitlines():
        key, value = line.split(":", 1)
        io[key] = int(value.strip())
    status: dict[str, str] = {}
    for line in (proc / "status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            status[key] = value.strip()
    return {
        "monotonic_ns": time.monotonic_ns(),
        "pid": pid,
        "starttime_ticks": int(stat[21]),
        "clock_ticks": os.sysconf("SC_CLK_TCK"),
        "page_size": os.sysconf("SC_PAGE_SIZE"),
        "utime_ticks": int(stat[13]),
        "stime_ticks": int(stat[14]),
        "minflt": int(stat[9]),
        "majflt": int(stat[11]),
        "rss_pages": int(stat[23]),
        "threads": int(stat[19]),
        "read_bytes": io.get("read_bytes", 0),
        "write_bytes": io.get("write_bytes", 0),
        "cancelled_write_bytes": io.get("cancelled_write_bytes", 0),
        "voluntary_ctxt_switches": int(status.get("voluntary_ctxt_switches", "0")),
        "nonvoluntary_ctxt_switches": int(status.get("nonvoluntary_ctxt_switches", "0")),
        "cpus_allowed_list": status.get("Cpus_allowed_list", ""),
        "exe": str((proc / "exe").resolve()),
        "cmdline": (proc / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace").strip(),
    }


def proc_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    if before["pid"] != after["pid"] or before["starttime_ticks"] != after["starttime_ticks"]:
        raise RuntimeError("measured server PID changed or was reused")
    wall_seconds = (after["monotonic_ns"] - before["monotonic_ns"]) / 1e9
    if wall_seconds <= 0:
        raise RuntimeError(f"non-positive process measurement wall time: {wall_seconds}")
    clock_ticks = before["clock_ticks"]
    user_seconds = (after["utime_ticks"] - before["utime_ticks"]) / clock_ticks
    system_seconds = (after["stime_ticks"] - before["stime_ticks"]) / clock_ticks
    cpu_seconds = user_seconds + system_seconds
    counters = {
        "read_bytes": after["read_bytes"] - before["read_bytes"],
        "write_bytes": after["write_bytes"] - before["write_bytes"],
        "major_faults": after["majflt"] - before["majflt"],
        "minor_faults": after["minflt"] - before["minflt"],
    }
    if any(value < 0 for value in counters.values()):
        raise RuntimeError(f"negative process counter delta: {counters}")
    return {
        "wall_seconds": wall_seconds,
        "user_cpu_seconds": user_seconds,
        "system_cpu_seconds": system_seconds,
        "server_cpu_seconds": cpu_seconds,
        "average_server_cores": cpu_seconds / wall_seconds,
        **counters,
        "rss_bytes_before": before["rss_pages"] * before["page_size"],
        "rss_bytes_after": after["rss_pages"] * after["page_size"],
        "threads_before": before["threads"],
        "threads_after": after["threads"],
        "cpus_allowed_list": after["cpus_allowed_list"],
    }


class MysqlSession:
    def __init__(self, host: str, port: int, user: str, database: str | None = None):
        command = [
            "mysql", f"-h{host}", f"-P{port}", f"-u{user}",
            "--batch", "--raw", "--skip-column-names", "--unbuffered", "--force",
            "--connect-timeout=10",
        ]
        if database:
            command.append(database)
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("failed to create mysql pipes")
        self.sequence = 0

    def query(self, statement: str, allow_error: bool = False) -> tuple[list[str], str | None]:
        self.sequence += 1
        marker = f"codex_variant_p2_{os.getpid()}_{self.sequence}"
        begin = f"{marker}_begin"
        end = f"{marker}_end"
        sql = statement.strip()
        if not sql.endswith(";"):
            sql += ";"
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        self.process.stdin.write(f"SELECT '{begin}';\n{sql}\nSELECT '{end}';\n")
        self.process.stdin.flush()
        rows: list[str] = []
        errors: list[str] = []
        inside = False
        while True:
            line = self.process.stdout.readline()
            if line == "":
                raise RuntimeError(
                    f"mysql exited {self.process.poll()} while executing: {statement[:300]}")
            value = line.rstrip("\r\n")
            if value == begin:
                inside = True
                continue
            if value == end and inside:
                break
            if inside:
                if value.startswith("ERROR "):
                    errors.append(value)
                elif errors:
                    errors.append(value)
                else:
                    rows.append(value)
        error = "\n".join(errors) if errors else None
        if error and not allow_error:
            raise RuntimeError(f"mysql statement failed: {error}\nSQL: {statement}")
        if not error and allow_error:
            raise RuntimeError(f"statement unexpectedly succeeded: {statement}")
        return rows, error

    def select_one(self, statement: str) -> str:
        """Execute one SELECT that is guaranteed to return exactly one row.

        This path deliberately sends no marker queries. It is used for timed COUNT
        statements and profile probes, where even a trivial marker SELECT would
        contaminate latency or replace Doris' last_query_id.
        """
        sql = statement.strip()
        if ";" in sql.rstrip(";"):
            raise ValueError("select_one accepts exactly one SQL statement")
        if not sql.endswith(";"):
            sql += ";"
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        self.process.stdin.write(sql + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if line == "":
            raise RuntimeError(
                f"mysql exited {self.process.poll()} while executing: {statement[:300]}")
        value = line.rstrip("\r\n")
        if value.startswith("ERROR "):
            raise RuntimeError(f"mysql statement failed: {value}\nSQL: {statement}")
        return value

    def close(self) -> None:
        if self.process.poll() is None:
            assert self.process.stdin is not None
            self.process.stdin.write("quit;\n")
            self.process.stdin.flush()
            self.process.wait(timeout=10)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def mysql_tsv(
        host: str, port: int, user: str, statement: str, database: str | None = None,
        headers: bool = True) -> list[dict[str, str]] | list[list[str]]:
    command = ["mysql", f"-h{host}", f"-P{port}", f"-u{user}", "--batch", "--raw"]
    if not headers:
        command.append("--skip-column-names")
    if database:
        command.append(database)
    command.extend(["-e", statement])
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    lines = result.stdout.splitlines()
    if not lines:
        return []
    if not headers:
        return [line.split("\t") for line in lines]
    reader = csv.DictReader(lines, delimiter="\t")
    return [dict(row) for row in reader]


class DorisHttp:
    def __init__(self, host: str, port: int, user: str):
        self.base = f"http://{host}:{port}"
        token = __import__("base64").b64encode(f"{user}:".encode()).decode()
        self.authorization = f"Basic {token}"
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(self, method: str, path: str, timeout: int = 60) -> bytes:
        request = urllib.request.Request(self.base + path, method=method)
        request.add_header("Authorization", self.authorization)
        with self.opener.open(request, timeout=timeout) as response:
            return response.read()

    def json(self, method: str, path: str, timeout: int = 60) -> dict[str, Any]:
        return json.loads(self.request(method, path, timeout).decode())


class EventLog:
    def __init__(self, path: Path, run_id: str):
        self.path = path
        self.run_id = run_id
        self.sequence = 0

    def append(self, kind: str, **values: Any) -> dict[str, Any]:
        self.sequence += 1
        record = {
            "run_id": self.run_id,
            "sequence": self.sequence,
            "timestamp_ns": time.time_ns(),
            "kind": kind,
            **values,
        }
        with self.path.open("a", encoding="utf-8") as output:
            output.write(json.dumps(record, sort_keys=True) + "\n")
        return record


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def strict_rowset_id(rowset: str) -> str:
    fields = rowset.split()
    if len(fields) < 5 or not re.fullmatch(r"[0-9a-f]+", fields[4]):
        raise ValueError(f"unrecognized compaction rowset format: {rowset!r}")
    return fields[4]
