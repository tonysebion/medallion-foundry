"""Tests for the Silver join helpers."""

import json
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from silver_join import JoinProgressTracker, apply_projection, build_input_audit, perform_join


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


def test_projection_limits_columns(tmp_path: Path) -> None:
    left = pd.DataFrame({"key": [1], "a": ["x"], "b": [10]})
    right = pd.DataFrame({"key": [1], "c": ["y"], "d": [20]})
    output_cfg = {
        "join_type": "inner",
        "select_columns": [
            {"key": "key"},
            {"source": "c", "alias": "currency"},
        ],
        "chunk_size": 1,
    }
    join_pairs = [("key", "key")]
    tracker = JoinProgressTracker(tmp_path / "progress")

    joined, stats = perform_join(left, right, join_pairs, 1, output_cfg, tracker)
    projected = apply_projection(joined, output_cfg)
    assert list(projected.columns) == ["key", "currency"]
    assert stats.chunk_count == 1


def test_projection_missing_fields_raises() -> None:
    df = pd.DataFrame({"key": [1], "value": [100]})
    output_cfg = {"select_columns": ["key", "missing_field"]}
    with pytest.raises(ValueError, match="Projection references missing columns"):
        apply_projection(df, output_cfg)


def test_projection_dict_with_alias(tmp_path: Path) -> None:
    left = pd.DataFrame({"key": [1], "value": [100]})
    right = pd.DataFrame({"key": [1], "other": ["x"]})
    output_cfg = {
        "join_type": "inner",
        "projection": [{"source": "value", "alias": "metric"}, {"other": "othervalue"}],
        "chunk_size": 1,
    }
    join_pairs = [("key", "key")]
    tracker = JoinProgressTracker(tmp_path / "progress")

    joined, stats = perform_join(left, right, join_pairs, 1, output_cfg, tracker)
    projected = apply_projection(joined, output_cfg)
    assert list(projected.columns) == ["metric", "othervalue"]
    assert stats.chunk_count == 1
