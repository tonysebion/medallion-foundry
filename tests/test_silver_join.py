"""Tests for the Silver join helpers."""

import json
from pathlib import Path

import pandas as pd
import pandas.testing as pdt

from silver_join import JoinProgressTracker, build_input_audit, perform_join


def test_perform_join_streaming_with_progress(tmp_path: Path) -> None:
    left = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "status": ["new", "open", "pending"],
            "category": ["alpha", "beta", "gamma"],
        }
    )
    right = pd.DataFrame(
        {
            "id": [2, 3, 4],
            "status": ["open", "pending", "closed"],
            "amount": [100, 200, 300],
        }
    )
    output_cfg = {"join_type": "outer"}
    tracker = JoinProgressTracker(tmp_path / "progress")

    join_pairs = [("id", "id")]
    chunk_size = 2
    joined, stats = perform_join(left, right, join_pairs, chunk_size, output_cfg, tracker)

    expected = pd.merge(
        left,
        right,
        how="outer",
        on="id",
        suffixes=("_x", "_y"),
    )
    joined = joined[expected.columns]
    pdt.assert_frame_equal(
        joined.sort_values("id").reset_index(drop=True),
        expected.sort_values("id").reset_index(drop=True),
    )
    assert stats.chunk_count == 2
    assert stats.right_only_rows == 1
    summary = tracker.summary()
    assert summary["chunks_processed"] == 2
    assert (tmp_path / "progress" / "progress.json").exists()


def test_build_input_audit_reads_bronze_metadata(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    bronze_metadata = {"record_count": 5, "load_pattern": "full"}
    checksum_manifest = {"files": [{"path": "chunk-0001.csv", "hash": "abc123"}]}
    (bronze_dir / "_metadata.json").write_text(json.dumps(bronze_metadata), encoding="utf-8")
    (bronze_dir / "_checksums.json").write_text(json.dumps(checksum_manifest), encoding="utf-8")
    meta = {"silver_path": "silver", "bronze_path": str(bronze_dir), "silver_model": "scd_type_2"}

    audit = build_input_audit(meta)

    assert audit["bronze_metadata"]["record_count"] == 5
    checksum_info = audit["bronze_checksum_manifest"]
    assert checksum_info["file_count"] == 1
    assert checksum_info["path"].endswith("_checksums.json")
