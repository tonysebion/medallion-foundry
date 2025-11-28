"""Integration test for chunked silver writes and consolidation."""

from __future__ import annotations

import json
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_BRONZE = REPO_ROOT / "sampledata" / "bronze_samples"
SAMPLE_SILVER_TMP = None


def _find_bronze_partition() -> Path:
    for p in SAMPLE_BRONZE.rglob("dt=*"):
        if p.is_dir() and list(p.glob("*.csv")):
            return p
    raise RuntimeError("No Bronze partition found in sampledata")


@pytest.mark.integration
def test_chunked_promotion_and_consolidation(tmp_path: Path) -> None:
    bronze_part = _find_bronze_partition()
    silver_tmp = tmp_path / "silver_tmp"
    silver_tmp.mkdir(parents=True)

    # Run two silver_extract subprocesses for same bronze partition with different chunk tags
    chunk1 = f"test-{uuid.uuid4().hex[:6]}"
    chunk2 = f"test-{uuid.uuid4().hex[:6]}"

    config_path = REPO_ROOT / "docs" / "examples" / "configs" / "patterns" / "pattern_current_history.yaml"
    cmd_base = [sys.executable, str(REPO_ROOT / "silver_extract.py"), "--config", str(config_path), "--bronze-path", str(bronze_part), "--silver-base", str(silver_tmp), "--write-parquet", "--write-csv", "--artifact-writer", "transactional"]
    p1 = subprocess.run([*cmd_base, "--chunk-tag", chunk1], cwd=REPO_ROOT, capture_output=True, text=True)
    if p1.returncode != 0:
        print("P1 STDOUT:\n", p1.stdout)
        print("P1 STDERR:\n", p1.stderr)
        raise RuntimeError("silver_extract failed for chunk 1")
    p2 = subprocess.run([*cmd_base, "--chunk-tag", chunk2], cwd=REPO_ROOT, capture_output=True, text=True)
    if p2.returncode != 0:
        print("P2 STDOUT:\n", p2.stdout)
        print("P2 STDERR:\n", p2.stderr)
        raise RuntimeError("silver_extract failed for chunk 2")

    # check chunk metadata exist
    chunk_meta_files = list(silver_tmp.rglob("_metadata_chunk_*.json"))
    assert chunk_meta_files, "Expected at least one chunk metadata file"

    # Consolidate
    subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "silver_consolidate.py"), "--silver-base", str(silver_tmp)], check=True, cwd=REPO_ROOT)

    # After consolidation, expect final _metadata.json and _checksums.json in some partition
    metadata_files = list(silver_tmp.rglob("_metadata.json"))
    checksum_files = list(silver_tmp.rglob("_checksums.json"))
    assert metadata_files, "Expected _metadata.json after consolidation"
    assert checksum_files, "Expected _checksums.json after consolidation"
