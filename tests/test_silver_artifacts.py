from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.silver.artifacts import (
    apply_schema_settings,
    build_current_view,
    handle_error_rows,
    normalize_dataframe,
    partition_dataframe,
)


def test_build_current_view_uses_primary_keys() -> None:
    df = pd.DataFrame(
        {
            "id": [1, 1, 2],
            "seq": [1, 2, 1],
            "value": ["a", "b", "c"],
        }
    )
    current = build_current_view(df, ["id"], order_column="seq")
    assert len(current) == 2
    assert current[current["id"] == 1]["value"].iloc[0] == "b"


def test_apply_schema_settings_reorders_and_renames() -> None:
    df = pd.DataFrame({"old": [1], "stay": [2]})
    schema_cfg = {"rename_map": {"old": "new"}, "column_order": ["new", "stay"]}
    result = apply_schema_settings(df, schema_cfg)
    assert list(result.columns) == ["new", "stay"]
    assert result["new"].iloc[0] == 1


def test_normalize_dataframe_trims_and_nulls() -> None:
    df = pd.DataFrame({"name": ["  alice ", ""], "value": [1, 2]})
    cfg = {"trim_strings": True, "empty_strings_as_null": True}
    normalized = normalize_dataframe(df, cfg)
    assert normalized["name"].iloc[0] == "alice"
    assert pd.isna(normalized["name"].iloc[1])


def test_partition_dataframe_handles_missing_column() -> None:
    df = pd.DataFrame({"region": ["east", "west"], "value": [1, 2]})
    partitions = partition_dataframe(df, ["region", "missing"])
    assert len(partitions) == 2
    assert any(path == ["region=east"] for path, _ in partitions)


def test_handle_error_rows_writes_invalid_records(tmp_path: Path) -> None:
    df = pd.DataFrame({"pk": [1, None], "value": [10, 20]})
    cfg = {"enabled": True, "max_bad_records": 5}
    output_dir = tmp_path / "silver"
    result = handle_error_rows(df, ["pk"], cfg, "dataset", output_dir)
    assert len(result) == 1
    assert result["pk"].iloc[0] == 1.0
    error_file = output_dir / "_errors" / "dataset.csv"
    assert error_file.exists()
