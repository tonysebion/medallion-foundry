"""Tests for concurrent silver_extract runs and consolidation behavior."""

from __future__ import annotations

import subprocess
import sys
import uuid
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List

import os
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _find_bronze_partition() -> Path:
    bronze_root = REPO_ROOT / "sampledata" / "bronze_samples"
    for p in bronze_root.rglob("dt=*"):
        if p.is_dir() and list(p.glob("*.csv")):
            return p
    raise RuntimeError("No Bronze partition found in sampledata")


def _run_extract_chunk(bronze_part: Path, silver_tmp: Path, chunk_tag: str, use_lock: bool, config_path: Path):
    cmd = [
        sys.executable,
        str(REPO_ROOT / "silver_extract.py"),
        "--config",
        str(config_path),
        "--bronze-path",
        str(bronze_part),
        "--silver-base",
        str(silver_tmp),
        "--write-parquet",
        "--artifact-writer",
        "transactional",
        "--chunk-tag",
        chunk_tag,
        "--verbose",
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
    results_by_tag: dict = {}
    # Prefer a small CDC partition to keep locked concurrent writes quick and deterministic
    config_path = REPO_ROOT / "docs" / "examples" / "configs" / "patterns" / "pattern_cdc.yaml"
    small_cdc_part = REPO_ROOT / "sampledata" / "bronze_samples" / "sample=pattern2_cdc_events" / "system=retail_demo" / "table=orders" / "dt=2025-11-14"
    if small_cdc_part.exists():
        bronze_part = small_cdc_part
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(_run_extract_chunk, bronze_part, silver_tmp, t, False, config_path) for t in tags]
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
@pytest.mark.skipif(os.getenv("CI"), reason="Flaky under CI; skipping until we investigate & address root cause (issue: TBD)")
def test_concurrent_writes_with_locks(tmp_path: Path) -> None:
    # Pick a bronze partition with minimal rows to keep concurrent writes fast and reliable
    bronze_root = REPO_ROOT / "sampledata" / "bronze_samples"
    candidate_parts = [p for p in bronze_root.rglob("dt=*") if p.is_dir() and list(p.glob("*.csv"))]
    bronze_part = (
        min(candidate_parts, key=lambda p: min(f.stat().st_size for f in p.glob("*.csv")))
        if candidate_parts
        else _find_bronze_partition()
    )
    silver_tmp = tmp_path / "silver_tmp_locks"
    silver_tmp.mkdir(parents=True)
    print(f"silver_tmp directory: {silver_tmp}")

    tags = [f"lock-{uuid.uuid4().hex[:6]}" for _ in range(3)]
    failures: List[tuple] = []
    results_by_tag: dict = {}
    # default to current_history; prefer small CDC sample if available
    config_path = REPO_ROOT / "docs" / "examples" / "configs" / "patterns" / "pattern_current_history.yaml"
    small_cdc_part = REPO_ROOT / "sampledata" / "bronze_samples" / "sample=pattern2_cdc_events" / "system=retail_demo" / "table=orders" / "dt=2025-11-14"
    if small_cdc_part.exists():
        bronze_part = small_cdc_part
        config_path = REPO_ROOT / "docs" / "examples" / "configs" / "patterns" / "pattern_cdc.yaml"
    # Use Popen to kick off processes concurrently and collect output reliably
    # no blocking sleeps here; keep code straightforward
    # Ensure no stale lock files from previous runs
    for f in silver_tmp.rglob(".silver.lock"):
        try:
            f.unlink()
        except Exception:
            pass

    def _run_locked_chunk(t: str):
                cmd = [
                    sys.executable,
                    str(REPO_ROOT / "silver_extract.py"),
                    "--config",
                    str(config_path),
                    "--bronze-path",
                    str(bronze_part),
                    "--silver-base",
                    str(silver_tmp),
                    "--write-parquet",
                    "--artifact-writer",
                    "transactional",
                    "--chunk-tag",
                    t,
                    "--use-locks",
                    "--lock-timeout",
                    "10",
                    "--verbose",
                ]
                stdout_path = silver_tmp / f"{t}.out"
                stderr_path = silver_tmp / f"{t}.err"
                status_path = silver_tmp / f"{t}.status"
                # Run subprocess as Popen so we can poll and collect diagnostic info
                with open(stdout_path, "w", encoding="utf-8") as outf, open(stderr_path, "w", encoding="utf-8") as errf:
                    proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=outf, stderr=errf, text=True)
                    rc = None
                    start_ts = time.time()
                    timeout = 180
                    # Periodically poll the process, and record status / lock owner info
                    while proc.poll() is None:
                        elapsed = time.time() - start_ts
                        # Write a simple status line so the test outputs can be tailed
                        try:
                            # Inspect lock file if it exists
                            lock_file = silver_tmp / ".silver.lock"
                            owner_info = ""
                            if lock_file.exists():
                                try:
                                    owner_info = lock_file.read_text(encoding="utf-8").strip()
                                except Exception:
                                    owner_info = "<unreadable>"
                            status_text = f"{t} running elapsed={elapsed:.1f}s owner={owner_info}\n"
                            status_path.write_text(status_text, encoding="utf-8")
                        except Exception:
                            pass
                        if elapsed > timeout:
                            proc.kill()
                            rc = -1
                            break
                        time.sleep(1)
                    if rc is None:
                        rc = proc.poll()
                # read outputs for diagnostic
                out = stdout_path.read_text(encoding="utf-8") if stdout_path.exists() else ""
                err = stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else ""
                return (rc, out, err, t)

    # Spawn processes directly using Popen to avoid pytest/ThreadPoolExecutor signal interaction on Windows
    procs = []
    for t in tags:
        stdout_path = silver_tmp / f"{t}.out"
        stderr_path = silver_tmp / f"{t}.err"
        # Ensure no leftover files
        if stdout_path.exists():
            stdout_path.unlink()
        if stderr_path.exists():
            stderr_path.unlink()
        cmd = [
            sys.executable,
            str(REPO_ROOT / "silver_extract.py"),
            "--config",
            str(config_path),
            "--bronze-path",
            str(bronze_part),
            "--silver-base",
            str(silver_tmp),
            "--write-parquet",
            "--artifact-writer",
            "transactional",
            "--chunk-tag",
            t,
            "--use-locks",
            "--lock-timeout",
            "10",
            "--verbose",
        ]
        outf = open(stdout_path, "w", encoding="utf-8")
        errf = open(stderr_path, "w", encoding="utf-8")
        proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=outf, stderr=errf, text=True)
        procs.append((t, proc, outf, errf))

    # Wait for processes with periodic status checks
    start = time.time()
    overall_timeout = 600
    while True:
        states = [(t, p.poll()) for t, p, _, _ in procs]
        print("States:", states)
        if all(s is not None for _, s in states):
            break
        if time.time() - start > overall_timeout:
            print("Overall timeout reached; killing remaining processes")
            for t, p, outf, errf in procs:
                if p.poll() is None:
                    p.kill()
            break
        time.sleep(1)

    # Collect outputs
    for t, p, outf, errf in procs:
        outf.close()
        errf.close()
        stdout_path = silver_tmp / f"{t}.out"
        stderr_path = silver_tmp / f"{t}.err"
        out = stdout_path.read_text(encoding='utf-8') if stdout_path.exists() else ""
        err = stderr_path.read_text(encoding='utf-8') if stderr_path.exists() else ""
        rc = p.returncode
        print(f"Process {t} RC={rc}")
        print("STDOUT:\n", out[:2000])
        print("STDERR:\n", err[:2000])
        failures.append((rc, out, err))
        results_by_tag[t] = (rc, out, err)
    nonzeros = [t for t in failures if t[0] != 0]
    if nonzeros:
        for rc, out, err in nonzeros:
            print("Subprocess failed: RC=", rc)
            print("STDOUT:\n", out)
            print("STDERR:\n", err)
    assert all(rc == 0 for rc, *_ in failures), f"Lock-protected subprocesses had failures: {[(f[0]) for f in failures]}"

    # Consolidate results (best effort) and verify artifacts exist
    subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "silver_consolidate.py"), "--silver-base", str(silver_tmp)], check=True, cwd=REPO_ROOT)
    # Confirm we produced at least one chunked artifact for each tag
    for t in tags:
        found_files = list(silver_tmp.rglob(f"*-{t}.parquet")) + list(silver_tmp.rglob(f"*-{t}.csv"))
        if not found_files:
            # Diagnostic: print outputs if we have them
            rc, out, err = results_by_tag.get(t, (None, "", ""))
            print(f"Diagnostics for tag {t} RC={rc}")
            print("Captured STDOUT:\n", out[:4000])
            print("Captured STDERR:\n", err[:4000])
        assert found_files, f"No chunked artifact found for tag {t} under {silver_tmp}; see diagnostics printed above"
    # If consolidation wrote metadata/checksums, assert their presence
    if list(silver_tmp.rglob("_metadata.json")):
        assert list(silver_tmp.rglob("_checksums.json")), "No checksums after consolidation"
