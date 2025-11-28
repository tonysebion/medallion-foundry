"""Tests for concurrent silver_extract runs and consolidation behavior."""

from __future__ import annotations

import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _find_bronze_partition() -> Path:
    bronze_root = REPO_ROOT / "sampledata" / "bronze_samples"
    for p in bronze_root.rglob("dt=*"):
        if p.is_dir() and list(p.glob("*.csv")):
            return p
    raise RuntimeError("No Bronze partition found in sampledata")


def _run_extract_chunk(bronze_part: Path, silver_tmp: Path, chunk_tag: str, use_lock: bool):
    cmd = [
        sys.executable,
        str(REPO_ROOT / "silver_extract.py"),
        "--bronze-path",
        str(bronze_part),
        "--silver-base",
        str(silver_tmp),
        "--write-parquet",
        "--artifact-writer",
        "transactional",
        "--chunk-tag",
        chunk_tag,
    ]
    if use_lock:
        cmd.append("--use-locks")
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    # Return tuple for better diagnostics
    return (proc.returncode, proc.stdout, proc.stderr)


@pytest.mark.integration
def test_concurrent_writes_and_consolidation(tmp_path: Path) -> None:
    bronze_part = _find_bronze_partition()
    silver_tmp = tmp_path / "silver_tmp"
    silver_tmp.mkdir(parents=True)

    tags = [f"parallel-{uuid.uuid4().hex[:6]}" for _ in range(3)]
    failures: List[tuple] = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(_run_extract_chunk, bronze_part, silver_tmp, t, False) for t in tags]
        for fut in as_completed(futures):
            rc, out, err = fut.result()
            failures.append((rc, out, err))
    nonzeros = [t for t in failures if t[0] != 0]
    if nonzeros:
        for rc, out, err in nonzeros:
            print("Subprocess failed: RC=", rc)
            print("STDOUT:\n", out)
            print("STDERR:\n", err)
    assert all(rc == 0 for rc, *_ in failures), f"At least one subprocess failed: {[(f[0]) for f in failures]}"

    # Consolidate the results and check metadata exists
    subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "silver_consolidate.py"), "--silver-base", str(silver_tmp)], check=True, cwd=REPO_ROOT)
    metadata = list(silver_tmp.rglob("_metadata.json"))
    checksums = list(silver_tmp.rglob("_checksums.json"))
    assert metadata, "No _metadata.json after consolidation"
    assert checksums, "No _checksums.json after consolidation"


@pytest.mark.integration
def test_concurrent_writes_with_locks(tmp_path: Path) -> None:
    bronze_part = _find_bronze_partition()
    silver_tmp = tmp_path / "silver_tmp_locks"
    silver_tmp.mkdir(parents=True)

    tags = [f"lock-{uuid.uuid4().hex[:6]}" for _ in range(3)]
    failures: List[tuple] = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(_run_extract_chunk, bronze_part, silver_tmp, t, True) for t in tags]
        for fut in as_completed(futures):
            rc, out, err = fut.result()
            failures.append((rc, out, err))
    nonzeros = [t for t in failures if t[0] != 0]
    if nonzeros:
        for rc, out, err in nonzeros:
            print("Subprocess failed: RC=", rc)
            print("STDOUT:\n", out)
            print("STDERR:\n", err)
    assert all(rc == 0 for rc, *_ in failures), f"Lock-protected subprocesses had failures: {[(f[0]) for f in failures]}"

    # Consolidate results
    subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "silver_consolidate.py"), "--silver-base", str(silver_tmp)], check=True, cwd=REPO_ROOT)
    assert list(silver_tmp.rglob("_metadata.json")), "No metadata after consolidation"
    assert list(silver_tmp.rglob("_checksums.json")), "No checksums after consolidation"
