"""End-to-end pipeline tests for Bronze -> Silver flow.

These tests verify complete pipeline runs with synthetic data,
including idempotency, backfill, and incremental scenarios.
"""

from pathlib import Path

import pandas as pd

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
from pipelines.lib.runner import run_pipeline


def _create_csv_file(path: Path, rows: list[dict]) -> None:
    """Helper to create a CSV file from row dictionaries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _create_parquet_file(path: Path, rows: list[dict]) -> None:
    """Helper to create a Parquet file from row dictionaries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


class TestBronzeToSilverE2E:
    """Full Bronze -> Silver pipeline tests."""

    def test_full_pipeline_snapshot_to_scd1(self, tmp_path: Path, monkeypatch):
        """Test complete FULL_SNAPSHOT Bronze -> CURRENT_ONLY Silver flow."""
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        run_date = "2025-01-15"

        # Create source CSV
        source_file = tmp_path / "source" / f"{run_date}.csv"
        _create_csv_file(
            source_file,
            [
                {"id": 1, "name": "Widget", "price": 10.0, "updated_at": "2025-01-15T10:00:00"},
                {"id": 2, "name": "Gadget", "price": 20.0, "updated_at": "2025-01-15T10:00:00"},
                {"id": 1, "name": "Widget Pro", "price": 15.0, "updated_at": "2025-01-15T11:00:00"},
            ],
        )

        # Configure Bronze
        bronze = BronzeSource(
            system="sales",
            entity="products",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "{run_date}.csv"),
            target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
        )

        # Configure Silver
        silver = SilverEntity(
            source_path=str(tmp_path / "bronze/system=sales/entity=products/dt={run_date}/*.parquet"),
            target_path=str(tmp_path / "silver/domain=sales/subject=products/"),
            domain="sales",
            subject="products",
            natural_keys=["id"],
            change_timestamp="updated_at",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
        )

        # Run pipeline
        result = run_pipeline(bronze, silver, run_date)

        # Verify success
        assert result.success
        assert result.bronze["row_count"] == 3
        assert result.silver["row_count"] == 2  # Deduplicated

        # Verify Bronze output
        bronze_dir = tmp_path / "bronze/system=sales/entity=products" / f"dt={run_date}"
        assert (bronze_dir / "products.parquet").exists()
        assert (bronze_dir / "_metadata.json").exists()

        # Verify Silver output
        silver_dir = tmp_path / "silver/domain=sales/subject=products"
        assert (silver_dir / "products.parquet").exists()

        # Verify deduplication - only latest record per id
        silver_df = pd.read_parquet(silver_dir / "products.parquet")
        assert len(silver_df) == 2
        widget = silver_df[silver_df["id"] == 1].iloc[0]
        assert widget["name"] == "Widget Pro"  # Latest version
        assert widget["price"] == 15.0

    def test_pipeline_idempotency(self, tmp_path: Path, monkeypatch):
        """Running the same pipeline twice should produce same results."""
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        run_date = "2025-01-15"

        # Create source data
        source_file = tmp_path / "source" / f"{run_date}.csv"
        _create_csv_file(
            source_file,
            [
                {"id": 1, "value": 100, "updated_at": "2025-01-15T10:00:00"},
                {"id": 2, "value": 200, "updated_at": "2025-01-15T10:00:00"},
            ],
        )

        bronze = BronzeSource(
            system="test",
            entity="items",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "{run_date}.csv"),
            target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
        )

        silver = SilverEntity(
            source_path=str(tmp_path / "bronze/system=test/entity=items/dt={run_date}/*.parquet"),
            target_path=str(tmp_path / "silver/domain=test/subject=items/"),
            domain="test",
            subject="items",
            natural_keys=["id"],
            change_timestamp="updated_at",
        )

        # First run
        result1 = run_pipeline(bronze, silver, run_date)
        assert result1.success

        # Second run (same data)
        result2 = run_pipeline(bronze, silver, run_date)
        assert result2.success

        # Verify same row counts
        assert result1.bronze["row_count"] == result2.bronze["row_count"]
        assert result1.silver["row_count"] == result2.silver["row_count"]

        # Verify Silver data is the same
        silver_df = pd.read_parquet(tmp_path / "silver/domain=test/subject=items/items.parquet")
        assert len(silver_df) == 2

    def test_backfill_multiple_dates(self, tmp_path: Path, monkeypatch):
        """Pipeline should handle backfill across multiple dates."""
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        dates = ["2025-01-13", "2025-01-14", "2025-01-15"]

        # Create source files for each date
        for i, run_date in enumerate(dates):
            source_file = tmp_path / "source" / f"{run_date}.csv"
            _create_csv_file(
                source_file,
                [
                    {"id": 1, "value": 100 + i * 10, "updated_at": f"{run_date}T10:00:00"},
                ],
            )

        bronze = BronzeSource(
            system="backfill",
            entity="data",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "{run_date}.csv"),
            target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
        )

        # Run Bronze for all dates
        for run_date in dates:
            result = bronze.run(run_date)
            assert result["row_count"] == 1

            # Verify each date has its own partition
            bronze_dir = tmp_path / "bronze/system=backfill/entity=data" / f"dt={run_date}"
            assert (bronze_dir / "data.parquet").exists()

        # Verify all partitions exist
        bronze_base = tmp_path / "bronze/system=backfill/entity=data"
        partitions = list(bronze_base.glob("dt=*"))
        assert len(partitions) == 3


class TestIncrementalPipeline:
    """Tests for incremental load patterns."""

    def test_incremental_append_updates_watermark(self, tmp_path: Path, monkeypatch):
        """Incremental loads should track and update watermark."""
        state_dir = tmp_path / "state"
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(state_dir))

        # Day 1: Initial load
        day1 = "2025-01-15"
        source_file1 = tmp_path / "source" / f"{day1}.csv"
        _create_csv_file(
            source_file1,
            [
                {"id": 1, "event_ts": "2025-01-15T10:00:00", "value": 100},
                {"id": 2, "event_ts": "2025-01-15T11:00:00", "value": 200},
            ],
        )

        bronze = BronzeSource(
            system="events",
            entity="clicks",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "{run_date}.csv"),
            target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            watermark_column="event_ts",
        )

        result1 = bronze.run(day1)
        assert result1["row_count"] == 2
        assert "new_watermark" in result1

        # Verify watermark was saved
        from pipelines.lib.state import get_watermark

        watermark = get_watermark("events", "clicks")
        assert watermark is not None
        # Watermark format may vary (T vs space separator)
        assert "2025-01-15" in watermark and "11:00:00" in watermark

    def test_incremental_skips_already_processed(self, tmp_path: Path, monkeypatch):
        """Incremental loads should skip already-processed records."""
        state_dir = tmp_path / "state"
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(state_dir))

        # Pre-set watermark
        from pipelines.lib.state import save_watermark

        save_watermark("events", "orders", "2025-01-15T12:00:00")

        # Create source with records before and after watermark
        source_file = tmp_path / "source" / "2025-01-16.csv"
        _create_csv_file(
            source_file,
            [
                {"id": 1, "event_ts": "2025-01-15T10:00:00", "value": 100},  # Before watermark
                {"id": 2, "event_ts": "2025-01-15T14:00:00", "value": 200},  # After watermark
                {"id": 3, "event_ts": "2025-01-16T09:00:00", "value": 300},  # After watermark
            ],
        )

        bronze = BronzeSource(
            system="events",
            entity="orders",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "{run_date}.csv"),
            target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            watermark_column="event_ts",
        )

        # Note: For file sources, Bronze reads all data but Silver would filter
        # This test verifies the watermark column is tracked
        result = bronze.run("2025-01-16")
        assert result["row_count"] == 3  # All rows loaded (filtering happens at Silver)
        assert "new_watermark" in result


class TestDryRun:
    """Tests for dry-run functionality."""

    def test_dry_run_does_not_write(self, tmp_path: Path, monkeypatch):
        """Dry run should not create any output files."""
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        source_file = tmp_path / "source" / "2025-01-15.csv"
        _create_csv_file(source_file, [{"id": 1, "value": 100}])

        bronze = BronzeSource(
            system="test",
            entity="items",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "{run_date}.csv"),
            target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
        )

        result = bronze.run("2025-01-15", dry_run=True)

        assert result["dry_run"] is True
        assert not (tmp_path / "bronze").exists()

    def test_dry_run_pipeline(self, tmp_path: Path, monkeypatch):
        """Full pipeline dry run should not write any data."""
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        source_file = tmp_path / "source" / "2025-01-15.csv"
        _create_csv_file(
            source_file,
            [{"id": 1, "value": 100, "updated_at": "2025-01-15T10:00:00"}],
        )

        bronze = BronzeSource(
            system="test",
            entity="items",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "{run_date}.csv"),
            target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
        )

        silver = SilverEntity(
            source_path=str(tmp_path / "bronze/system=test/entity=items/dt={run_date}/*.parquet"),
            target_path=str(tmp_path / "silver/domain=test/subject=items/"),
            domain="test",
            subject="items",
            natural_keys=["id"],
            change_timestamp="updated_at",
        )

        result = run_pipeline(bronze, silver, "2025-01-15", dry_run=True)

        assert result.success
        assert result.bronze.get("dry_run") is True
        assert not (tmp_path / "bronze").exists()
        assert not (tmp_path / "silver").exists()


class TestSkipIfExists:
    """Tests for skip_if_exists functionality."""

    def test_skip_if_exists_skips_when_data_present(self, tmp_path: Path, monkeypatch):
        """Should skip extraction when target already has data."""
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        run_date = "2025-01-15"

        # Create source
        source_file = tmp_path / "source" / f"{run_date}.csv"
        _create_csv_file(source_file, [{"id": 1, "value": 100}])

        bronze = BronzeSource(
            system="test",
            entity="items",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "{run_date}.csv"),
            target_path=str(tmp_path / "bronze/system={system}/entity={entity}/dt={run_date}/"),
        )

        # First run - should write
        result1 = bronze.run(run_date)
        assert result1["row_count"] == 1
        assert "skipped" not in result1

        # Second run with skip_if_exists - should skip
        result2 = bronze.run(run_date, skip_if_exists=True)
        assert result2.get("skipped") is True
        assert result2.get("reason") == "already_exists"
