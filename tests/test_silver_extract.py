"""Tests for the silver extraction helpers."""

from pathlib import Path

import pandas as pd

from silver_extract import (
    build_current_view,
    derive_relative_partition,
    discover_load_pattern,
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
