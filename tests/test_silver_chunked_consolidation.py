"""Integration test for chunked silver writes and consolidation."""

from __future__ import annotations

import subprocess
import sys
import uuid
from pathlib import Path
import yaml

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

    config_path = (
        REPO_ROOT
        / "docs"
        / "examples"
        / "configs"
        / "patterns"
        / "pattern_current_history.yaml"
    )
    cmd_base = [
        sys.executable,
        str(REPO_ROOT / "silver_extract.py"),
        "--config",
        str(config_path),
        "--bronze-path",
        str(bronze_part),
        "--silver-base",
        str(silver_tmp),
        "--write-parquet",
        "--write-csv",
        "--artifact-writer",
        "transactional",
    ]
    p1 = subprocess.run(
        [*cmd_base, "--chunk-tag", chunk1],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if p1.returncode != 0:
        print("P1 STDOUT:\n", p1.stdout)
        print("P1 STDERR:\n", p1.stderr)
        raise RuntimeError("silver_extract failed for chunk 1")
    p2 = subprocess.run(
        [*cmd_base, "--chunk-tag", chunk2],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if p2.returncode != 0:
        print("P2 STDOUT:\n", p2.stdout)
        print("P2 STDERR:\n", p2.stderr)
        raise RuntimeError("silver_extract failed for chunk 2")

    # check chunk metadata exist
    chunk_meta_files = list(silver_tmp.rglob("_metadata_chunk_*.json"))
    assert chunk_meta_files, "Expected at least one chunk metadata file"

    # Consolidate
    subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "silver_consolidate.py"),
            "--silver-base",
            str(silver_tmp),
        ],
        check=True,
        cwd=REPO_ROOT,
    )

    # After consolidation, expect final _metadata.json and _checksums.json in some partition
    metadata_files = list(silver_tmp.rglob("_metadata.json"))
    checksum_files = list(silver_tmp.rglob("_checksums.json"))
    assert metadata_files, "Expected _metadata.json after consolidation"
    assert checksum_files, "Expected _checksums.json after consolidation"


def test_consolidate_prune_chunks(tmp_path: Path) -> None:
    bronze_part = _find_bronze_partition()
    silver_tmp = tmp_path / "silver_tmp"
    silver_tmp.mkdir(parents=True)

    chunk1 = f"test-{uuid.uuid4().hex[:6]}"
    chunk2 = f"test-{uuid.uuid4().hex[:6]}"
    config_path = (
        REPO_ROOT
        / "docs"
        / "examples"
        / "configs"
        / "patterns"
        / "pattern_current_history.yaml"
    )
    cmd_base = [
        sys.executable,
        str(REPO_ROOT / "silver_extract.py"),
        "--config",
        str(config_path),
        "--bronze-path",
        str(bronze_part),
        "--silver-base",
        str(silver_tmp),
        "--write-parquet",
        "--write-csv",
        "--artifact-writer",
        "transactional",
    ]
    p1 = subprocess.run(
        [*cmd_base, "--chunk-tag", chunk1],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if p1.returncode != 0:
        print("P1 STDOUT:\n", p1.stdout)
        print("P1 STDERR:\n", p1.stderr)
        raise RuntimeError("silver_extract failed for chunk 1")
    p2 = subprocess.run(
        [*cmd_base, "--chunk-tag", chunk2],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if p2.returncode != 0:
        print("P2 STDOUT:\n", p2.stdout)
        print("P2 STDERR:\n", p2.stderr)
        raise RuntimeError("silver_extract failed for chunk 2")

    # Consolidate with prune option
    subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "silver_consolidate.py"),
            "--silver-base",
            str(silver_tmp),
            "--prune-chunks",
        ],
        check=True,
        cwd=REPO_ROOT,
    )

    # No chunk metadata should remain
    assert not list(
        silver_tmp.rglob("*_metadata_chunk_*.json")
    ), "Expected no chunk metadata files after pruning"
    # We can't easily assert no chunk artifact remain due to naming; ensure _metadata_chunk files removed is good enough


def test_intent_chunk_metadata_written(tmp_path: Path) -> None:
    bronze_part = _find_bronze_partition()
    silver_tmp = tmp_path / "silver_tmp"
    silver_tmp.mkdir(parents=True, exist_ok=True)

    # Build a simple intent config using 'datasets' style that points at the found Bronze partition
    config = {
        "datasets": [
            {
                "name": "orders_intent",
                "system": "retail_demo",
                "entity": "orders",
                "bronze": {
                    "enabled": True,
                    "source_type": "file",
                    "path_pattern": str(bronze_part),
                    "partition_column": "load_date",
                    "options": {"format": "csv", "load_pattern": "full"},
                },
                "silver": {
                    "enabled": True,
                    "entity_kind": "event",
                    "input_mode": "append_log",
                    "natural_keys": ["order_id"],
                    "event_ts_column": "updated_at",
                    "attributes": ["status"],
                    "partition_by": ["run_date"],
                    "write_parquet": True,
                    "write_csv": False,
                    "schema_mode": "allow_new_columns",
                },
            }
        ]
    }
    cfg_file = tmp_path / "intent_config.yaml"
    cfg_file.write_text(yaml.safe_dump(config), encoding="utf-8")

    chunk_tag = f"intent-test-{uuid.uuid4().hex[:6]}"
    cmd_base = [
        sys.executable,
        str(REPO_ROOT / "silver_extract.py"),
        "--config",
        str(cfg_file),
        "--bronze-path",
        str(bronze_part),
        "--silver-base",
        str(silver_tmp),
        "--write-parquet",
        "--artifact-writer",
        "transactional",
    ]
    p = subprocess.run(
        [*cmd_base, "--chunk-tag", chunk_tag],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        msg = (
            f"silver_extract failed for intent config: returncode={p.returncode}\n"
            f"STDOUT:\n{p.stdout}\n"
            f"STDERR:\n{p.stderr}"
        )
        raise RuntimeError(msg)

    # Assert chunk metadata exists
    chunk_meta_files = list(silver_tmp.rglob("_metadata_chunk_*.json"))
    assert chunk_meta_files, "Expected at least one intent chunk metadata file"
