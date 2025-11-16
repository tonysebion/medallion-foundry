"""Tests for the Silver join helpers."""

import json
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from silver_join import (
    JoinProgressTracker,
    QualityGuardError,
    apply_projection,
    build_input_audit,
    perform_join,
    run_quality_guards,
)


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
    joined, stats, column_origin, join_metrics = perform_join(left, right, join_pairs, chunk_size, output_cfg, tracker)

    expected = pd.merge(
        left,
        right,
        how="outer",
        on="id",
        suffixes=("", "_right"),
    )
    joined = joined[expected.columns]
    pdt.assert_frame_equal(
        joined.sort_values("id").reset_index(drop=True),
        expected.sort_values("id").reset_index(drop=True),
    )
    assert stats.chunk_count == 2
    assert stats.right_only_rows == 1
    assert len(join_metrics) == 2
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

    joined, stats, column_origin, join_metrics = perform_join(left, right, join_pairs, 1, output_cfg, tracker)
    projected, lineage = apply_projection(joined, output_cfg, column_origin)
    assert list(projected.columns) == ["key", "currency"]
    assert stats.chunk_count == 1
    assert lineage[1]["column"] == "currency"
    assert lineage[1]["alias"] == "currency"
    assert lineage[1]["source"] == "right"


def test_projection_missing_fields_raises() -> None:
    df = pd.DataFrame({"key": [1], "value": [100]})
    output_cfg = {"select_columns": ["key", "missing_field"]}
    with pytest.raises(ValueError, match="Projection references missing columns"):
        apply_projection(df, output_cfg, {})


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

    joined, stats, column_origin, join_metrics = perform_join(left, right, join_pairs, 1, output_cfg, tracker)
    projected, lineage = apply_projection(joined, output_cfg, column_origin)
    assert list(projected.columns) == ["metric", "othervalue"]
    assert stats.chunk_count == 1
    assert lineage[0]["source"] == "left"
    assert lineage[1]["source"] == "right"


def test_datetime_alignment_preserves_timezone(tmp_path: Path) -> None:
    left = pd.DataFrame(
        {
            "key": [1],
            "event_time": [pd.to_datetime("2024-01-01T00:00:00").tz_localize("UTC")],
        }
    )
    right = pd.DataFrame(
        {
            "key": [1],
            "event_time": [pd.to_datetime("2024-01-01T00:00:00").tz_localize("US/Eastern")],
        }
    )
    output_cfg = {"join_type": "inner", "chunk_size": 1}
    join_pairs = [("key", "key")]
    tracker = JoinProgressTracker(tmp_path / "progress")

    joined, stats, column_origin, join_metrics = perform_join(left, right, join_pairs, 1, output_cfg, tracker)
    projected, lineage = apply_projection(joined, output_cfg, column_origin)
    assert projected["event_time"].dt.tz == left["event_time"].dt.tz
    event_entry = next(entry for entry in lineage if entry["column"] == "event_time")
    right_entry = next(entry for entry in lineage if entry["column"] == "event_time_right")
    assert event_entry["source"] == "left"
    assert right_entry["source"] == "right"


def test_run_quality_guards_success() -> None:
    df = pd.DataFrame({"key": [1, 2], "value": [None, 10]})
    cfg = {
        "row_count": {"min": 1, "max": 5},
        "null_ratio": [{"column": "value", "max_ratio": 0.6}],
        "unique_keys": [{"columns": ["key"]}],
    }
    results = run_quality_guards(df, cfg)
    names = {result["name"] for result in results}
    assert "row_count_range" in names
    assert any(result["status"] == "pass" for result in results)


def test_run_quality_guards_failure() -> None:
    df = pd.DataFrame({"key": [1, 1], "value": [1, None]})
    cfg = {
        "row_count": {"min": 1, "max": 5},
        "null_ratio": [{"column": "value", "max_ratio": 0.1}],
        "unique_keys": [{"columns": ["key"]}],
    }
    with pytest.raises(QualityGuardError) as exc:
        run_quality_guards(df, cfg)
    assert any(result["status"] == "fail" for result in exc.value.results)
