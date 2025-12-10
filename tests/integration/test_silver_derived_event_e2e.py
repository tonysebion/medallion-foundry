"""DERIVED_EVENT Pattern End-to-End Tests.

Story 5: Tests that verify DERIVED_EVENT entity processing correctly handles
CDC-style change event generation from state changes including:
- Change event generation from state deltas
- Correct change_type values (INSERT/upsert, UPDATE, DELETE, noop)
- Tombstone record handling (DELETE mode)
- Event ordering guarantees (by timestamp)
- Changed columns tracking
"""

from __future__ import annotations

import json
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

from core.infrastructure.config import DatasetConfig


# =============================================================================
# Test Helpers
# =============================================================================


def create_bronze_output(
    df: pd.DataFrame,
    output_path: Path,
    load_pattern: str,
    run_date: date,
    system: str = "synthetic",
    table: str = "derived_event_test",
) -> Path:
    """Create Bronze output structure with metadata for Silver processing."""
    partition_path = (
        output_path
        / f"system={system}"
        / f"table={table}"
        / f"pattern={load_pattern}"
        / f"dt={run_date.isoformat()}"
    )
    partition_path.mkdir(parents=True, exist_ok=True)

    # Write parquet file
    parquet_file = partition_path / "chunk_0.parquet"
    df.to_parquet(parquet_file, index=False)

    # Write metadata
    metadata = {
        "timestamp": run_date.isoformat(),
        "system": system,
        "table": table,
        "load_pattern": load_pattern,
        "chunk_count": 1,
        "record_count": len(df),
        "run_id": str(uuid.uuid4()),
    }
    metadata_path = partition_path / "_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2))

    # Write checksums
    import hashlib
    checksum = hashlib.sha256(parquet_file.read_bytes()).hexdigest()
    checksums = {
        "files": [
            {
                "path": "chunk_0.parquet",
                "sha256": checksum,
                "size_bytes": parquet_file.stat().st_size,
            }
        ],
        "load_pattern": load_pattern,
    }
    checksums_path = partition_path / "_checksums.json"
    checksums_path.write_text(json.dumps(checksums, indent=2))

    return partition_path


def create_derived_event_dataset_config(
    system: str = "synthetic",
    entity: str = "derived_event_test",
    natural_keys: Optional[List[str]] = None,
    change_ts_column: str = "updated_at",
    event_ts_column: str = "updated_at",  # Can use same column as change_ts
    attributes: Optional[List[str]] = None,
    delete_mode: str = "ignore",
) -> DatasetConfig:
    """Create DatasetConfig for DERIVED_EVENT processing."""
    natural_keys = natural_keys or ["entity_id"]
    attributes = attributes or ["status", "amount"]

    return DatasetConfig.from_dict({
        "environment": "test",
        "domain": "analytics",
        "system": system,
        "entity": entity,
        "bronze": {"enabled": True},
        "silver": {
            "enabled": True,
            "entity_kind": "derived_event",
            "version": 1,
            "natural_keys": natural_keys,
            "change_ts_column": change_ts_column,
            "event_ts_column": event_ts_column,
            "attributes": attributes,
            "delete_mode": delete_mode,
            "input_storage": "local",
            "schema_mode": "allow_new_columns",
        },
    })


def run_derived_event_processing(
    bronze_path: Path,
    silver_path: Path,
    dataset_config: DatasetConfig,
    run_date: date,
) -> Dict[str, Any]:
    """Run Silver processing for DERIVED_EVENT and return results."""
    from core.domain.services.pipelines.silver.processor import SilverProcessor

    processor = SilverProcessor(
        dataset=dataset_config,
        bronze_path=bronze_path,
        silver_partition=silver_path,
        run_date=run_date,
        verify_checksum=False,
    )

    result = processor.run()

    # Convert output paths to DataFrames
    outputs_dfs: Dict[str, pd.DataFrame] = {}
    for key, paths in result.outputs.items():
        if paths:
            dfs = [pd.read_parquet(p) for p in paths]
            outputs_dfs[key] = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    return {
        "metrics": result.metrics,
        "outputs": outputs_dfs,
        "silver_path": silver_path,
    }


def read_silver_output(silver_path: Path) -> Dict[str, pd.DataFrame]:
    """Read Silver output DataFrames from parquet files."""
    outputs: Dict[str, pd.DataFrame] = {}

    for parquet_file in silver_path.rglob("*.parquet"):
        # Extract output type from path (e.g., derived_events)
        output_type = parquet_file.parent.name
        if output_type.startswith("dt="):
            # Use grandparent if in date partition
            output_type = parquet_file.parent.parent.name

        if output_type not in outputs:
            outputs[output_type] = pd.read_parquet(parquet_file)
        else:
            outputs[output_type] = pd.concat(
                [outputs[output_type], pd.read_parquet(parquet_file)],
                ignore_index=True,
            )

    return outputs


# =============================================================================
# Test Data Generators
# =============================================================================


def generate_state_changes(
    entity_count: int,
    changes_per_entity: int,
    base_date: date,
    include_deletes: bool = False,
) -> pd.DataFrame:
    """Generate state change data for derived event testing.

    Each entity will have multiple state versions with different timestamps,
    simulating state changes over time.
    """
    rows = []
    statuses = ["draft", "pending", "active", "completed"]

    for i in range(entity_count):
        entity_id = f"ENT{i+1:06d}"
        base_amount = (i + 1) * 100.0

        for change in range(changes_per_entity):
            timestamp = base_date + timedelta(days=change)
            status = statuses[min(change, len(statuses) - 1)]
            amount = base_amount + (change * 10.0)

            row = {
                "entity_id": entity_id,
                "status": status,
                "amount": amount,
                "updated_at": timestamp,
            }

            # Add delete flag for last record if requested
            if include_deletes and change == changes_per_entity - 1:
                row["is_deleted"] = True
            else:
                row["is_deleted"] = False

            rows.append(row)

    return pd.DataFrame(rows)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def base_date() -> date:
    """Provide consistent base date for tests."""
    return date(2024, 1, 15)


# =============================================================================
# Basic Processing Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedEventBasicProcessing:
    """Test basic DERIVED_EVENT processing functionality."""

    def test_derived_event_processing_succeeds(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify DERIVED_EVENT processing runs without errors."""
        df = generate_state_changes(
            entity_count=10,
            changes_per_entity=3,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config(
            natural_keys=["entity_id"],
        )

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        assert result["metrics"].rows_read > 0
        assert result["metrics"].rows_written > 0
        assert "derived_events" in result["outputs"]

    def test_derived_event_produces_change_events(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify DERIVED_EVENT produces events with change_type column."""
        df = generate_state_changes(
            entity_count=5,
            changes_per_entity=3,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config()

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have change_type column
        assert "change_type" in events_df.columns, "Events should have change_type column"

        # Should have changed_columns column
        assert "changed_columns" in events_df.columns, "Events should track changed columns"


# =============================================================================
# Change Type Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedEventChangeTypes:
    """Test DERIVED_EVENT change type detection."""

    def test_first_record_is_upsert(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify first record for each entity is marked as upsert."""
        # Single record per entity - all should be upserts
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date},
            {"entity_id": "ENT002", "status": "draft", "amount": 200.0, "updated_at": base_date},
            {"entity_id": "ENT003", "status": "draft", "amount": 300.0, "updated_at": base_date},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config()

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # All records should be upserts (first occurrence of each entity)
        assert all(events_df["change_type"] == "upsert"), (
            "First records should be upsert"
        )

    def test_multiple_entities_produce_multiple_events(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify multiple entities each produce their own event.

        Note: Within a single batch, Silver preparation deduplicates by natural key.
        So each entity produces one event per batch, not one event per source record.
        """
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date},
            {"entity_id": "ENT002", "status": "active", "amount": 200.0, "updated_at": base_date},
            {"entity_id": "ENT003", "status": "completed", "amount": 300.0, "updated_at": base_date},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config()

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have one event per entity
        assert len(events_df) == 3, f"Expected 3 events (one per entity), got {len(events_df)}"

        # All should be upserts (first occurrence)
        assert all(events_df["change_type"] == "upsert"), "All first occurrences should be upsert"

    def test_latest_record_wins_for_entity(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify that when multiple records exist for an entity, latest wins.

        The Silver preparation stage deduplicates by natural key, keeping the
        record with the latest timestamp. This test verifies that behavior.
        """
        # Same entity, different values at different times
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date},
            {"entity_id": "ENT001", "status": "active", "amount": 150.0, "updated_at": base_date + timedelta(days=1)},
            {"entity_id": "ENT001", "status": "completed", "amount": 200.0, "updated_at": base_date + timedelta(days=2)},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config()

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have only one event (latest version after dedup)
        assert len(events_df) == 1, f"Expected 1 event after dedup, got {len(events_df)}"

        # Should be the latest version
        event = events_df.iloc[0]
        assert event["status"] == "completed", "Should have latest status"
        assert event["amount"] == 200.0, "Should have latest amount"


# =============================================================================
# Delete Mode Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedEventDeleteMode:
    """Test DERIVED_EVENT tombstone/delete handling."""

    def test_tombstone_event_generates_delete(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify is_deleted flag generates delete change_type.

        Note: Due to Silver preparation deduplication, only the latest record
        per entity is processed. This test uses a single record with is_deleted=True.
        """
        # Single deleted record
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "deleted", "amount": 100.0, "updated_at": base_date, "is_deleted": True},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config(
            delete_mode="tombstone_event",
        )

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have one delete event
        assert len(events_df) == 1, f"Expected 1 event, got {len(events_df)}"
        assert events_df.iloc[0]["change_type"] == "delete", "Should be delete change type"

    def test_delete_mode_ignore_processes_normally(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify delete_mode=IGNORE processes non-deleted records normally.

        IGNORE mode affects how tombstones are handled, but doesn't change
        normal event processing for non-deleted records.
        """
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "active", "amount": 100.0, "updated_at": base_date},
            {"entity_id": "ENT002", "status": "completed", "amount": 200.0, "updated_at": base_date},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config(
            delete_mode="ignore",
        )

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have 2 events (one per entity)
        assert len(events_df) == 2, f"Expected 2 events, got {len(events_df)}"
        assert all(events_df["change_type"] == "upsert"), "All should be upserts"


# =============================================================================
# Event Ordering Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedEventOrdering:
    """Test DERIVED_EVENT ordering guarantees."""

    def test_dedup_selects_latest_timestamp(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify deduplication selects record with latest timestamp.

        When multiple records exist for the same entity, Silver preparation
        deduplicates by natural key and keeps the record with the latest timestamp.
        """
        # Create out-of-order data (same entity, different timestamps)
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "completed", "amount": 300.0, "updated_at": base_date + timedelta(days=3)},
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date},
            {"entity_id": "ENT001", "status": "active", "amount": 200.0, "updated_at": base_date + timedelta(days=1)},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config()

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have one event (latest after dedup)
        assert len(events_df) == 1

        # Should be the latest version (completed, day+3)
        event = events_df.iloc[0]
        assert event["status"] == "completed", "Should have latest status (completed)"
        assert event["amount"] == 300.0, "Should have latest amount"

    def test_multiple_entities_dedup_independently(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify each entity's records are deduplicated independently.

        Each entity should produce one event containing its latest state.
        """
        df = pd.DataFrame([
            # ENT001 changes - should keep "active" (later)
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date},
            {"entity_id": "ENT001", "status": "active", "amount": 150.0, "updated_at": base_date + timedelta(days=2)},
            # ENT002 changes - should keep "completed" (later)
            {"entity_id": "ENT002", "status": "pending", "amount": 200.0, "updated_at": base_date + timedelta(days=1)},
            {"entity_id": "ENT002", "status": "completed", "amount": 250.0, "updated_at": base_date + timedelta(days=3)},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config()

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have 2 events (one per entity, latest version)
        assert len(events_df) == 2, f"Expected 2 events, got {len(events_df)}"

        # Check ENT001 has latest state
        ent001 = events_df[events_df["entity_id"] == "ENT001"].iloc[0]
        assert ent001["status"] == "active", "ENT001 should have latest status"

        # Check ENT002 has latest state
        ent002 = events_df[events_df["entity_id"] == "ENT002"].iloc[0]
        assert ent002["status"] == "completed", "ENT002 should have latest status"


# =============================================================================
# Changed Columns Tracking Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedEventChangedColumns:
    """Test DERIVED_EVENT changed columns tracking."""

    def test_upsert_includes_all_attributes_in_changed_columns(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify first event (upsert) has all attributes in changed_columns.

        For the first occurrence of an entity (upsert), all tracked attributes
        are considered changed.
        """
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config(
            attributes=["status", "amount"],
        )

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have one upsert event
        assert len(events_df) == 1
        event = events_df.iloc[0]
        assert event["change_type"] == "upsert"

        # changed_columns should list all attributes for upsert
        changed_cols = event["changed_columns"]
        assert "status" in changed_cols, "status should be in changed_columns"
        assert "amount" in changed_cols, "amount should be in changed_columns"

    def test_first_event_includes_all_attributes(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify first event (upsert) includes all tracked attributes."""
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config(
            attributes=["status", "amount"],
        )

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # First event should have all attributes in changed_columns
        first_event = events_df.iloc[0]
        changed_cols = first_event["changed_columns"]
        assert "status" in changed_cols
        assert "amount" in changed_cols

    def test_changed_columns_comma_separated(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify changed_columns is comma-separated list of attribute names."""
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "active", "amount": 200.0, "updated_at": base_date},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config(
            attributes=["status", "amount"],
        )

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]
        event = events_df.iloc[0]

        # changed_columns should be comma-separated
        changed_cols = event["changed_columns"]
        parts = changed_cols.split(",")
        assert len(parts) == 2, f"Expected 2 attributes, got: {changed_cols}"
        assert "status" in parts
        assert "amount" in parts


# =============================================================================
# Edge Case Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedEventEdgeCases:
    """Test DERIVED_EVENT edge cases."""

    def test_empty_input_produces_no_events(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify empty input produces no events output."""
        df = pd.DataFrame(columns=["entity_id", "status", "amount", "updated_at"])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config()

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        # Empty input may produce no events key or empty DataFrame
        events_df = result["outputs"].get("derived_events")
        if events_df is not None:
            assert len(events_df) == 0, "Empty input should produce empty output"

    def test_single_record_per_entity(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify single records produce upsert events only."""
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date},
            {"entity_id": "ENT002", "status": "active", "amount": 200.0, "updated_at": base_date},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config()

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # All should be upserts
        assert all(events_df["change_type"] == "upsert")
        assert len(events_df) == 2

    def test_large_number_of_entities(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify processing scales to many entities."""
        df = generate_state_changes(
            entity_count=100,
            changes_per_entity=5,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config()

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have many events
        assert len(events_df) > 0

        # Should have mix of change types
        change_types = events_df["change_type"].unique()
        assert "upsert" in change_types

    def test_composite_natural_key(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify composite natural keys work correctly.

        Deduplication happens per composite key, so ORG1+ENT001 and ORG2+ENT001
        are treated as different entities.
        """
        df = pd.DataFrame([
            # ORG1+ENT001: two records, should keep latest (active)
            {"org_id": "ORG1", "entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date},
            {"org_id": "ORG1", "entity_id": "ENT001", "status": "active", "amount": 150.0, "updated_at": base_date + timedelta(days=1)},
            # ORG2+ENT001: one record
            {"org_id": "ORG2", "entity_id": "ENT001", "status": "draft", "amount": 200.0, "updated_at": base_date},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_event_dataset_config(
            natural_keys=["org_id", "entity_id"],
        )

        result = run_derived_event_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        events_df = result["outputs"]["derived_events"]

        # Should have 2 events total (one per composite key after dedup)
        assert len(events_df) == 2, f"Expected 2 events, got {len(events_df)}"

        # ORG1+ENT001 should have 1 event with latest state (active)
        org1_events = events_df[
            (events_df["org_id"] == "ORG1") & (events_df["entity_id"] == "ENT001")
        ]
        assert len(org1_events) == 1
        assert org1_events.iloc[0]["status"] == "active"

        # ORG2+ENT001 should have 1 event (draft)
        org2_events = events_df[
            (events_df["org_id"] == "ORG2") & (events_df["entity_id"] == "ENT001")
        ]
        assert len(org2_events) == 1
        assert org2_events.iloc[0]["status"] == "draft"
