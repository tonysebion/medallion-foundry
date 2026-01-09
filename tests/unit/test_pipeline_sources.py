"""Unit tests for the new pipelines source abstractions."""

import json
from pathlib import Path

import pandas as pd
import pytest

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
from pipelines.lib.state import get_watermark


def _create_csv_input(tmp_path: Path, run_date: str, rows: list[dict[str, object]]) -> str:
    """Create a CSV file for a given run date and return a templated path."""
    template = tmp_path / "input_{run_date}.csv"
    target_path = template.as_posix().format(run_date=run_date)
    pd.DataFrame(rows).to_csv(target_path, index=False)
    return template.as_posix()


def test_bronze_source_writes_metadata_and_checksums(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """BronzeSource should land CSV data with metadata."""
    monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))
    run_date = "2025-01-15"
    source_path_template = _create_csv_input(
        tmp_path,
        run_date,
        [
            {"id": 1, "name": "Widget", "price": 12.3},
            {"id": 2, "name": "Gadget", "price": 45.6},
            {"id": 3, "name": "Thingamajig", "price": 7.8},
        ],
    )

    source = BronzeSource(
        system="tests",
        entity="items",
        source_type=SourceType.FILE_CSV,
        source_path=source_path_template,
        target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
        load_pattern=LoadPattern.FULL_SNAPSHOT,
        options={"csv_options": {"header": True, "sep": ","}},
    )

    result = source.run(run_date)
    assert result["row_count"] == 3

    target_dir = tmp_path / "bronze" / "system=tests" / "entity=items" / f"dt={run_date}"
    assert (target_dir / "items.parquet").exists()
    assert (target_dir / "_metadata.json").exists()
    assert (target_dir / "_checksums.json").exists()

    metadata = json.loads((target_dir / "_metadata.json").read_text())
    assert metadata["row_count"] == 3
    assert metadata["system"] == "tests"
    assert metadata["entity"] == "items"


def test_bronze_source_incremental_appends_updates_watermark(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Incremental Bronze runs should persist the watermark."""
    state_dir = tmp_path / "state"
    monkeypatch.setenv("PIPELINE_STATE_DIR", str(state_dir))

    run_date = "2025-01-16"
    source_path_template = _create_csv_input(
        tmp_path,
        run_date,
        [
            {"id": 1, "event_ts": "2025-01-16T00:00:00"},
            {"id": 2, "event_ts": "2025-01-16T01:00:00"},
        ],
    )

    source = BronzeSource(
        system="late",
        entity="events",
        source_type=SourceType.FILE_CSV,
        source_path=source_path_template,
        target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
        load_pattern=LoadPattern.INCREMENTAL_APPEND,
        watermark_column="event_ts",
        options={"csv_options": {"header": True, "sep": ","}},
    )

    result = source.run(run_date)
    assert result["row_count"] == 2
    assert "new_watermark" in result

    stored = get_watermark("late", "events")
    assert stored == result["new_watermark"]


def test_silver_entity_curates_state_entity(tmp_path: Path):
    """SilverEntity should deduplicate and write artifacts for state loads."""
    bronze_dir = tmp_path / "bronze" / "system=tests" / "entity=items" / "dt=2025-01-15"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"id": 1, "value": 100, "updated_at": "2025-01-15T00:00:00"},
            {"id": 1, "value": 150, "updated_at": "2025-01-15T01:00:00"},
            {"id": 2, "value": 200, "updated_at": "2025-01-15T00:30:00"},
        ]
    ).to_parquet(bronze_dir / "data.parquet", index=False)

    source_template = tmp_path / "bronze" / "system=tests" / "entity=items" / "dt={run_date}" / "*.parquet"
    silver_target = tmp_path / "silver" / "domain=tests" / "subject=items"

    silver = SilverEntity(
        source_path=source_template.as_posix(),
        target_path=silver_target.as_posix(),
        domain="tests",
        subject="items",
        natural_keys=["id"],
        change_timestamp="updated_at",
        entity_kind=EntityKind.STATE,
        history_mode=HistoryMode.CURRENT_ONLY,
    )

    result = silver.run("2025-01-15")
    assert result["row_count"] == 2
    assert (silver_target / "items.parquet").exists()
    assert (silver_target / "_metadata.json").exists()

    output_df = pd.read_parquet(silver_target / "items.parquet")
    assert len(output_df) == 2
    assert output_df[output_df["id"] == 1]["value"].iloc[0] == 150

    metadata = json.loads((silver_target / "_metadata.json").read_text())
    assert metadata["entity_kind"] == EntityKind.STATE.value
    assert metadata["history_mode"] == HistoryMode.CURRENT_ONLY.value
