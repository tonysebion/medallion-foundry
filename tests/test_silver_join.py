"""Tests for the Silver join helpers."""

import json
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from core.patterns import LoadPattern
from core.run_options import RunOptions
from core.silver.models import SilverModel

from silver_join import (
    JoinProgressTracker,
    QualityGuardError,
    _order_inputs_by_reference,
    apply_projection,
    build_input_audit,
    perform_join,
    run_quality_guards,
    write_output,
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
    meta = {
        "silver_path": "silver",
        "bronze_path": str(bronze_dir),
        "silver_model": "scd_type_2",
        "reference_mode": {"role": "reference", "cadence_days": 7},
    }

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


def test_order_inputs_by_reference() -> None:
    base_df = pd.DataFrame({"key": [1]})
    reference_entry = (
        {"path": "ref"},
        base_df,
        {"reference_mode": {"role": "reference"}, "run_date": "2025-11-14"},
        "ref_path",
    )
    delta_entry = (
        {"path": "delta"},
        base_df,
        {"reference_mode": {"role": "delta"}, "run_date": "2025-11-14"},
        "delta_path",
    )
    auto_entry = ({"path": "auto"}, base_df, {"reference_mode": None, "run_date": "2025-11-13"}, "auto_path")
    ordered = _order_inputs_by_reference([reference_entry, delta_entry, auto_entry])
    assert ordered[0][3] == "auto_path"
    assert ordered[1][3] == "delta_path"
    assert ordered[-1][3] == "ref_path"


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


def test_reference_metadata_in_join_output(tmp_path: Path) -> None:
    left_dir = tmp_path / "left"
    right_dir = tmp_path / "right"
    left_dir.mkdir()
    right_dir.mkdir()

    left_df = pd.DataFrame({"key": [1], "value": ["ref"]})
    right_df = pd.DataFrame({"key": [1], "value": ["delta"]})
    left_df.to_csv(left_dir / "part.csv", index=False)
    right_df.to_csv(right_dir / "part.csv", index=False)

    left_meta = {
        "silver_path": str(left_dir),
        "record_count": 1,
        "chunk_count": 1,
        "reference_mode": {"role": "reference", "cadence_days": 7},
    }
    right_meta = {
        "silver_path": str(right_dir),
        "record_count": 1,
        "chunk_count": 1,
        "reference_mode": {"role": "delta"},
    }
    (left_dir / "_metadata.json").write_text(json.dumps(left_meta), encoding="utf-8")
    (right_dir / "_metadata.json").write_text(json.dumps(right_meta), encoding="utf-8")

    tracker = JoinProgressTracker(tmp_path / "progress")
    join_pairs = [("key", "key")]
    output_cfg = {"formats": ["parquet"], "join_type": "inner", "checkpoint_dir": str(tmp_path / ".join_progress")}
    joined, stats, column_origin, join_metrics = perform_join(
        left_df,
        right_df,
        join_pairs,
        chunk_size=1,
        output_cfg=output_cfg,
        progress_tracker=tracker,
        spill_dir=None,
    )
    joined, column_lineage = apply_projection(joined, output_cfg, column_origin)
    guard_results = run_quality_guards(joined, {})
    run_opts = RunOptions(
        load_pattern=LoadPattern.FULL,
        require_checksum=False,
        write_parquet=True,
        write_csv=False,
        parquet_compression="snappy",
        primary_keys=["key"],
        order_column=None,
        partition_columns=[],
        artifact_names=RunOptions.default_artifacts(),
    )
    metadata_out = tmp_path / "out"
    source_audits = [
        build_input_audit(left_meta),
        build_input_audit(right_meta),
    ]
    source_paths = [str(left_dir), str(right_dir)]
    progress_summary = tracker.summary()
    write_output(
        joined,
        metadata_out,
        SilverModel.PERIODIC_SNAPSHOT,
        run_opts,
        [left_meta, right_meta],
        output_cfg,
        source_paths,
        source_audits,
        progress_summary,
        stats,
        join_pairs,
        chunk_size=1,
        column_lineage=column_lineage,
        quality_guards=guard_results,
        join_metrics=join_metrics,
    )
    metadata_json = json.loads((metadata_out / "_metadata.json").read_text(encoding="utf-8"))
    inputs = metadata_json.get("inputs", [])
    assert any(input_meta.get("reference_mode") for input_meta in inputs)


def test_delta_preferred_same_day_reference() -> None:
    base_meta = {
        "silver_path": "slot",
        "silver_model": "scd_type_1",
        "record_count": 2,
        "chunk_count": 1,
        "run_date": "2025-11-14",
    }
    ref_meta = dict(base_meta)
    ref_meta["reference_mode"] = {"role": "reference", "reference_run_date": "2025-11-14"}
    delta_meta = dict(base_meta)
    delta_meta["reference_mode"] = {"role": "delta", "reference_run_date": "2025-11-14"}
    left = pd.DataFrame({"key": [1], "value": ["a"]})
    right = pd.DataFrame({"key": [1], "value": ["b"]})
    entries = [
        ({"path": "left"}, left, ref_meta, "left"),
        ({"path": "right"}, right, delta_meta, "right"),
    ]
    ordered = _order_inputs_by_reference(entries)
    assert ordered[0][3] == "right"
    assert ordered[1][3] == "left"
