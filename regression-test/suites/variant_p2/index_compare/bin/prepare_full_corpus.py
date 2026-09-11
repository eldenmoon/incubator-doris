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

"""Prepare the complete public variant_p2 GitHub Events corpus."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
from dataclasses import dataclass
import fcntl
import hashlib
import http.client
import os
from pathlib import Path
import stat
import sys
import time
from typing import BinaryIO, Iterable
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ElementTree

from benchmark_lib import (
    FULL_VARIANT_P2_BATCHES,
    FULL_VARIANT_P2_EXPECTED_OBJECTS,
    FULL_VARIANT_P2_EXPECTED_ROWS,
    full_variant_p2_batch_assignments,
    full_variant_p2_expected_files,
)


DEFAULT_BASE_URL = (
    "https://doris-regression-hk.oss-cn-hongkong.aliyuncs.com/"
    "regression/github_events_dataset"
)
CHUNK_BYTES = 8 * 1024 * 1024
USER_AGENT = "apache-doris-variant-p2-corpus-preparer/1"


@dataclass(frozen=True)
class RemoteObject:
    name: str
    key: str
    url: str
    size: int


@dataclass(frozen=True)
class FileStats:
    name: str
    rows: int
    size: int
    sha256: str


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child_text(element: ElementTree.Element, name: str) -> str | None:
    for child in element:
        if _local_name(child.tag) == name:
            return child.text
    return None


def _open_with_retry(
        request: urllib.request.Request, timeout: float, retries: int) -> BinaryIO:
    last_error: BaseException | None = None
    for attempt in range(retries + 1):
        try:
            return urllib.request.urlopen(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            last_error = error
            error.close()
            if error.code not in {408, 429, 500, 502, 503, 504} or attempt == retries:
                break
        except (OSError, urllib.error.URLError) as error:
            last_error = error
            if attempt == retries:
                break
        time.sleep(min(30.0, 0.5 * (2 ** attempt)))
    hint = ""
    if isinstance(last_error, urllib.error.HTTPError) and last_error.code == 502:
        hint = " (HTTP 502: retry in the physical environment after unsetting proxy variables)"
    raise RuntimeError(f"request failed after {retries + 1} attempts: {request.full_url}{hint}") \
        from last_error


def _request(
        url: str, method: str, timeout: float, retries: int,
        headers: dict[str, str] | None = None) -> BinaryIO:
    request_headers = {"User-Agent": USER_AGENT}
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(url, headers=request_headers, method=method)
    return _open_with_retry(request, timeout, retries)


def _parse_base_url(base_url: str) -> tuple[str, str]:
    parsed = urllib.parse.urlsplit(base_url.rstrip("/"))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"base URL must be an HTTP(S) URL: {base_url}")
    if parsed.query or parsed.fragment:
        raise ValueError("base URL must not contain a query or fragment")
    prefix = parsed.path.strip("/")
    if not prefix:
        raise ValueError("base URL must include the object prefix")
    bucket_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))
    return bucket_url, prefix + "/"


def list_remote_objects(
        base_url: str, names: Iterable[str], timeout: float, retries: int) -> list[RemoteObject]:
    bucket_url, prefix = _parse_base_url(base_url)
    ordered_names = list(names)
    expected = set(ordered_names)
    listed: dict[str, tuple[str, int]] = {}
    continuation: str | None = None
    while True:
        query = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if continuation is not None:
            query["continuation-token"] = continuation
        list_url = bucket_url + "?" + urllib.parse.urlencode(query)
        with _request(list_url, "GET", timeout, retries) as response:
            payload = response.read()
        try:
            root = ElementTree.fromstring(payload)
        except ElementTree.ParseError as error:
            raise RuntimeError(f"ListObjects returned invalid XML: {list_url}") from error

        truncated = False
        next_continuation: str | None = None
        for element in root:
            kind = _local_name(element.tag)
            if kind == "Contents":
                key = _child_text(element, "Key")
                size_text = _child_text(element, "Size")
                if key is None or size_text is None:
                    raise RuntimeError("ListObjects entry is missing Key or Size")
                if not key.startswith(prefix):
                    continue
                name = key[len(prefix):]
                if name not in expected:
                    continue
                if name in listed:
                    raise RuntimeError(f"ListObjects returned duplicate expected key: {key}")
                size = int(size_text)
                if size <= 0:
                    raise RuntimeError(f"remote object has non-positive size: {key}={size}")
                listed[name] = (key, size)
            elif kind == "IsTruncated":
                truncated = (element.text or "").strip().lower() == "true"
            elif kind == "NextContinuationToken":
                next_continuation = element.text

        if not truncated:
            break
        if not next_continuation or next_continuation == continuation:
            raise RuntimeError("truncated ListObjects response has no new continuation token")
        continuation = next_continuation

    missing = sorted(expected - listed.keys())
    if missing:
        preview = ", ".join(missing[:5])
        raise RuntimeError(
            f"ListObjects found {len(listed)}/{len(expected)} expected objects; "
            f"first missing: {preview}")
    if len(listed) != FULL_VARIANT_P2_EXPECTED_OBJECTS:
        raise RuntimeError(
            f"expected {FULL_VARIANT_P2_EXPECTED_OBJECTS} listed objects, got {len(listed)}")

    quoted_base = base_url.rstrip("/") + "/"
    return [
        RemoteObject(
            name=name,
            key=listed[name][0],
            url=quoted_base + urllib.parse.quote(name),
            size=listed[name][1],
        )
        for name in ordered_names
    ]


def head_size(remote: RemoteObject, timeout: float, retries: int) -> int:
    with _request(remote.url, "HEAD", timeout, retries) as response:
        length = response.headers.get("Content-Length")
    if length is None:
        raise RuntimeError(f"HEAD response has no Content-Length: {remote.url}")
    size = int(length)
    if size != remote.size:
        raise RuntimeError(
            f"ListObjects/HEAD size mismatch for {remote.name}: {remote.size} != {size}")
    return size


def verify_heads(
        remotes: list[RemoteObject], workers: int, timeout: float, retries: int) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(head_size, remote, timeout, retries): remote
            for remote in remotes
        }
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            remote = futures[future]
            future.result()
            completed += 1
            if completed % 100 == 0 or completed == len(remotes):
                print(f"HEAD verified {completed}/{len(remotes)}", file=sys.stderr)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _regular_file_size(path: Path) -> int | None:
    if path.is_symlink():
        raise RuntimeError(f"refusing to follow a symbolic link: {path}")
    try:
        metadata = path.stat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(metadata.st_mode):
        raise RuntimeError(f"expected a regular file or missing path: {path}")
    return metadata.st_size


def _download_attempt(
        remote: RemoteObject, part: Path, timeout: float, retries: int) -> None:
    offset = _regular_file_size(part) or 0
    if offset > remote.size:
        raise RuntimeError(
            f"partial file is larger than remote object: {part}={offset}, remote={remote.size}")
    if offset == remote.size:
        with part.open("rb") as source:
            os.fsync(source.fileno())
        return

    headers = {"Range": f"bytes={offset}-"} if offset else None
    with _request(remote.url, "GET", timeout, retries, headers) as response:
        status = response.getcode()
        if offset and status == 206:
            content_range = response.headers.get("Content-Range", "")
            expected_content_range = f"bytes {offset}-{remote.size - 1}/{remote.size}"
            if content_range != expected_content_range:
                raise RuntimeError(
                    f"unexpected Content-Range for {remote.name}: {content_range!r}, "
                    f"expected {expected_content_range!r}")
            mode = "ab"
        elif status == 200:
            # The server ignored Range. The .part file is disposable, so restart it.
            mode = "wb"
        else:
            raise RuntimeError(
                f"unexpected HTTP status while downloading {remote.name}: {status}")

        with part.open(mode) as destination:
            try:
                while chunk := response.read(CHUNK_BYTES):
                    destination.write(chunk)
            finally:
                destination.flush()
                os.fsync(destination.fileno())


def download_one(
        remote: RemoteObject, raw_dir: Path, timeout: float, retries: int) -> str:
    target = raw_dir / remote.name
    part = raw_dir / f"{remote.name}.part"
    size = _regular_file_size(target)
    if size is not None:
        if size == remote.size:
            return "existing"
        raise RuntimeError(
            f"existing final file has the wrong size and will not be overwritten: "
            f"{target}={size}, remote={remote.size}")

    last_error: BaseException | None = None
    for attempt in range(retries + 1):
        try:
            _download_attempt(remote, part, timeout, retries)
            size = part.stat().st_size
            if size != remote.size:
                raise RuntimeError(
                    f"partial download has {size} bytes, expected {remote.size}: {part}")
            os.replace(part, target)
            _fsync_directory(raw_dir)
            return "downloaded"
        except (http.client.HTTPException, OSError, RuntimeError, urllib.error.URLError) as error:
            last_error = error
            part_size = _regular_file_size(part)
            if part_size is not None and part_size > remote.size:
                break
            if attempt == retries:
                break
            time.sleep(min(30.0, 0.5 * (2 ** attempt)))
    raise RuntimeError(f"failed to download {remote.name}; resumable file: {part}") from last_error


def download_all(
        remotes: list[RemoteObject], raw_dir: Path, workers: int,
        timeout: float, retries: int) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(download_one, remote, raw_dir, timeout, retries): remote
            for remote in remotes
        }
        completed = 0
        downloaded = 0
        for future in concurrent.futures.as_completed(futures):
            remote = futures[future]
            status = future.result()
            completed += 1
            downloaded += int(status == "downloaded")
            print(
                f"files ready {completed}/{len(remotes)} "
                f"(downloaded={downloaded}, reused={completed - downloaded}): {remote.name}",
                file=sys.stderr,
            )


def collect_file_stats(path: Path, expected_size: int) -> FileStats:
    size = path.stat().st_size
    if size != expected_size:
        raise RuntimeError(f"local/remote size mismatch: {path}={size}, remote={expected_size}")
    digest = hashlib.sha256()
    rows = 0
    last_byte: int | None = None
    with path.open("rb") as source:
        while chunk := source.read(CHUNK_BYTES):
            digest.update(chunk)
            rows += chunk.count(b"\n")
            last_byte = chunk[-1]
    if size and last_byte != ord("\n"):
        rows += 1
    if rows <= 0:
        raise RuntimeError(f"local object has no JSONEachRow records: {path}")
    return FileStats(path.name, rows, size, digest.hexdigest())


def collect_all_stats(
        remotes: list[RemoteObject], raw_dir: Path, workers: int) -> list[FileStats]:
    stats_by_name: dict[str, FileStats] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                collect_file_stats, raw_dir / remote.name, remote.size): remote.name
            for remote in remotes
        }
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            stats_by_name[name] = future.result()
            completed += 1
            if completed % 100 == 0 or completed == len(remotes):
                print(f"hashed {completed}/{len(remotes)}", file=sys.stderr)
    return [stats_by_name[remote.name] for remote in remotes]


def assign_batches(stats: list[FileStats]) -> dict[str, int]:
    assignments = full_variant_p2_batch_assignments([
        (item.name, item.rows, item.size) for item in stats
    ])
    counts = [0] * FULL_VARIANT_P2_BATCHES
    rows = [0] * FULL_VARIANT_P2_BATCHES
    sizes = [0] * FULL_VARIANT_P2_BATCHES
    for item in stats:
        batch = assignments[item.name]
        counts[batch] += 1
        rows[batch] += item.rows
        sizes[batch] += item.size
    print(f"batch files: {counts}", file=sys.stderr)
    print(f"batch rows:  {rows}", file=sys.stderr)
    print(f"batch bytes: {sizes}", file=sys.stderr)
    return assignments


def publish_manifest(
        manifest: Path, stats: list[FileStats], assignments: dict[str, int]) -> None:
    manifest.parent.mkdir(parents=True, exist_ok=True)
    temporary = manifest.parent / f".{manifest.name}.tmp.{os.getpid()}"
    try:
        with temporary.open("x", encoding="utf-8", newline="") as destination:
            writer = csv.writer(destination, delimiter="\t", lineterminator="\n")
            writer.writerow(["batch", "file", "rows", "bytes", "sha256"])
            for item in stats:
                writer.writerow([
                    assignments[item.name], item.name, item.rows, item.size, item.sha256,
                ])
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, manifest)
        _fsync_directory(manifest.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-dir", type=Path, required=True,
        help="directory for the 2070 JSON files and resumable .part files")
    parser.add_argument(
        "--manifest", type=Path,
        help="output TSV (default: RAW_DIR/../manifests/full.validated.tsv)")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--retries", type=int, default=5)
    args = parser.parse_args()
    if not 1 <= args.workers <= 64:
        parser.error("--workers must be in [1, 64]")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if args.retries < 0:
        parser.error("--retries must be non-negative")
    args.raw_dir = args.raw_dir.resolve()
    if args.manifest is None:
        args.manifest = args.raw_dir.parent / "manifests" / "full.validated.tsv"
    args.manifest = args.manifest.resolve()
    if args.manifest == args.raw_dir or args.raw_dir in args.manifest.parents:
        parser.error("--manifest must be outside --raw-dir")
    return args


def main() -> int:
    args = parse_args()
    names = full_variant_p2_expected_files()
    args.raw_dir.mkdir(parents=True, exist_ok=True)
    lock_path = args.raw_dir / ".prepare_full_corpus.lock"
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"another corpus preparer holds {lock_path}") from error

        print(f"listing public corpus: {args.base_url}", file=sys.stderr)
        remotes = list_remote_objects(
            args.base_url, names, args.timeout_seconds, args.retries)
        verify_heads(remotes, args.workers, args.timeout_seconds, args.retries)
        download_all(
            remotes, args.raw_dir, args.workers, args.timeout_seconds, args.retries)
        stats = collect_all_stats(remotes, args.raw_dir, args.workers)

        if len(stats) != FULL_VARIANT_P2_EXPECTED_OBJECTS:
            raise RuntimeError(
                f"expected {FULL_VARIANT_P2_EXPECTED_OBJECTS} local objects, got {len(stats)}")
        total_rows = sum(item.rows for item in stats)
        if total_rows != FULL_VARIANT_P2_EXPECTED_ROWS:
            raise RuntimeError(
                f"golden row-count mismatch: expected {FULL_VARIANT_P2_EXPECTED_ROWS}, "
                f"got {total_rows}; "
                "formal manifest was not published")
        assignments = assign_batches(stats)
        publish_manifest(args.manifest, stats, assignments)
        print(
            f"published {args.manifest}: objects={len(stats)}, rows={total_rows}, "
            f"bytes={sum(item.size for item in stats)}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("interrupted; .part files are retained for resume", file=sys.stderr)
        raise SystemExit(130)
    except Exception as error:  # noqa: BLE001 - CLI boundary must keep manifest unpublished.
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
