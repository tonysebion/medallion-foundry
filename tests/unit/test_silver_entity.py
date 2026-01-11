import ibis
import pandas as pd

from pipelines.lib import silver as silver_module
from pipelines.lib.silver import EntityKind, SilverEntity


def _make_entity(**overrides):
    config = {
        "unique_columns": ["id"],
        "last_updated_column": "ts",
        "source_path": "bronze/{run_date}.parquet",
        "target_path": "silver/",
    }
    config.update(overrides)
    return SilverEntity(**config)


def test_select_columns_prioritizes_attributes():
    entity = _make_entity(attributes=["extra"])
    df = pd.DataFrame(
        {
            "id": [1],
            "ts": [1],
            "extra": ["value"],
            "secret": ["hide"],
        }
    )

    table = ibis.memtable(df)
    selected = entity._select_columns(table)

    assert set(selected.columns) == {"id", "ts", "extra"}


def test_select_columns_excludes_columns():
    entity = _make_entity(exclude_columns=["secret"])
    df = pd.DataFrame(
        {
            "id": [1],
            "ts": [1],
            "extra": ["value"],
            "secret": ["hide"],
        }
    )

    table = ibis.memtable(df)
    selected = entity._select_columns(table)

    assert "secret" not in selected.columns
    assert {"id", "ts", "extra"} <= set(selected.columns)


def test_select_columns_renames_with_column_mapping():
    """Test column_mapping renames columns from Bronze to Silver."""
    entity = _make_entity(
        column_mapping={
            "id": "order_id",
            "extra": "extra_data",
        }
    )
    df = pd.DataFrame(
        {
            "id": [1],
            "ts": [1],
            "extra": ["value"],
        }
    )

    table = ibis.memtable(df)
    selected = entity._select_columns(table)

    # Columns should be renamed
    assert "order_id" in selected.columns
    assert "extra_data" in selected.columns
    # Original names should be gone
    assert "id" not in selected.columns
    assert "extra" not in selected.columns
    # ts was not renamed
    assert "ts" in selected.columns


def test_select_columns_mapping_with_attributes():
    """Test column_mapping works with attributes."""
    entity = _make_entity(
        attributes=["extra"],
        column_mapping={"extra": "renamed_extra"},
    )
    df = pd.DataFrame(
        {
            "id": [1],
            "ts": [1],
            "extra": ["value"],
            "secret": ["hide"],
        }
    )

    table = ibis.memtable(df)
    selected = entity._select_columns(table)

    # extra should be included (via attributes) and renamed
    assert "renamed_extra" in selected.columns
    assert "extra" not in selected.columns
    # secret should be excluded (not in attributes)
    assert "secret" not in selected.columns


def test_select_columns_mapping_ignores_missing_columns():
    """Test column_mapping ignores columns that don't exist."""
    entity = _make_entity(
        column_mapping={
            "id": "order_id",
            "nonexistent": "should_be_ignored",
        }
    )
    df = pd.DataFrame(
        {
            "id": [1],
            "ts": [1],
        }
    )

    table = ibis.memtable(df)
    selected = entity._select_columns(table)

    # id should be renamed
    assert "order_id" in selected.columns
    # nonexistent mapping should be ignored silently
    assert "should_be_ignored" not in selected.columns


def test_curate_event_deduplicates_records():
    entity = _make_entity(entity_kind=EntityKind.EVENT)
    df = pd.DataFrame(
        [
            {"id": 1, "ts": 1},
            {"id": 1, "ts": 1},
            {"id": 2, "ts": 2},
        ]
    )

    table = ibis.memtable(df)
    curated = entity._curate_event(table)
    result = curated.execute()

    assert len(result) == 2
    assert sorted(result["id"].tolist()) == [1, 2]


def test_check_source_reports_missing(monkeypatch):
    entity = _make_entity()
    monkeypatch.setattr(silver_module, "storage_path_exists", lambda path: False)

    issues = entity._check_source("2025-01-15")

    assert issues
    assert "2025-01-15" in issues[0]


def test_check_source_no_issues_when_present(monkeypatch):
    entity = _make_entity()
    monkeypatch.setattr(silver_module, "storage_path_exists", lambda path: True)

    issues = entity._check_source("2025-01-15")

    assert issues == []
