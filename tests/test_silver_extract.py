"""Tests for the silver extraction helpers."""

from pathlib import Path

import pandas as pd
import pytest

from silver_extract import (
    apply_schema_settings,
    build_current_view,
    derive_relative_partition,
    discover_load_pattern,
    handle_error_rows,
    normalize_dataframe,
    partition_dataframe,
)
from core.patterns import LoadPattern


def test_discover_load_pattern_reads_metadata(tmp_path):
    metadata = tmp_path / "_metadata.json"
    metadata.write_text('{"load_pattern": "cdc"}', encoding="utf-8")

    pattern = discover_load_pattern(tmp_path)
    assert pattern == LoadPattern.CDC


def test_build_current_view_with_primary_keys():
    df = pd.DataFrame(
        [
            {"id": 1, "value": "old", "updated_at": "2024-01-01"},
            {"id": 1, "value": "new", "updated_at": "2024-01-02"},
            {"id": 2, "value": "only", "updated_at": "2024-01-01"},
        ]
    )

    current = build_current_view(df, primary_keys=["id"], order_column="updated_at")
    assert len(current) == 2
    assert current.loc[current["id"] == 1, "value"].item() == "new"


def test_derive_relative_partition_detects_system_folder(tmp_path):
    partition = tmp_path / "output" / "system=demo" / "table=sample" / "pattern=full" / "dt=2025-01-01"
    partition.mkdir(parents=True)

    relative = derive_relative_partition(partition)
    assert relative.as_posix().startswith("system=demo/table=sample")


def test_apply_schema_settings_renames_and_orders():
    df = pd.DataFrame(
        [
            {"id": 1, "B": 2, "A": 3},
        ]
    )
    schema_cfg = {"rename_map": {"B": "beta"}, "column_order": ["beta", "id"]}
    result = apply_schema_settings(df, schema_cfg)
    assert list(result.columns)[:2] == ["beta", "id"]
    assert "A" in result.columns


def test_normalize_dataframe_trim_and_empty():
    df = pd.DataFrame({"name": [" Alice ", ""], "value": [1, 2]})
    cfg = {"trim_strings": True, "empty_strings_as_null": True}
    result = normalize_dataframe(df, cfg)
    assert result.loc[0, "name"] == "Alice"
    assert result.loc[1, "name"] is None


def test_handle_error_rows_with_threshold(tmp_path):
    df = pd.DataFrame(
        [
            {"id": 1, "value": "ok"},
            {"id": None, "value": "bad"},
        ]
    )
    cfg = {"enabled": True, "max_bad_records": 1, "max_bad_percent": 50.0}
    cleaned = handle_error_rows(df, ["id"], cfg, "full_snapshot", tmp_path)
    assert len(cleaned) == 1
    error_file = tmp_path / "_errors" / "full_snapshot.csv"
    assert error_file.exists()


def test_handle_error_rows_failure(tmp_path):
    df = pd.DataFrame(
        [
            {"id": None, "value": "bad"},
        ]
    )
    cfg = {"enabled": True, "max_bad_records": 0, "max_bad_percent": 0.0}
    with pytest.raises(ValueError):
        handle_error_rows(df, ["id"], cfg, "cdc_changes", tmp_path)


def test_partition_dataframe_groups():
    df = pd.DataFrame(
        [
            {"status": "new", "id": 1},
            {"status": "done", "id": 2},
            {"status": "new", "id": 3},
        ]
    )
    partitions = partition_dataframe(df, ["status"])
    labels = {tuple(parts) for parts, _ in partitions}
    assert labels == {("status=new",), ("status=done",)}
    for parts, subset in partitions:
        if parts == ["status=new"]:
            assert len(subset) == 2
