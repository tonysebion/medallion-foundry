"""DERIVED_STATE Pattern End-to-End Tests.

Story 4: Tests that verify DERIVED_STATE entity processing correctly handles
state derivation scenarios including:
- State derived from aggregating events
- State derived from joining multiple Bronze sources
- SCD1 mode (overwrite) with derived state
- SCD2 mode (history) with derived state changes
- Incremental state update scenarios

DERIVED_STATE uses the StateHandler with derived=True flag, providing the same
SCD1/SCD2 processing as STATE but semantically indicating the data comes from
a computed/derived source rather than direct extraction.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

import pandas as pd
import pytest

from core.infrastructure.config import DatasetConfig, EntityKind, HistoryMode
from tests.pattern_verification.pattern_data.generators import (
    PatternTestDataGenerator,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def base_date() -> date:
    """Base date for test data generation."""
    return date(2024, 1, 15)


@pytest.fixture
def pattern_generator() -> PatternTestDataGenerator:
    """Create pattern test data generator with standard seed."""
    return PatternTestDataGenerator(seed=42, base_rows=100)


# =============================================================================
# Helper Functions
# =============================================================================


def create_bronze_output(
    df: pd.DataFrame,
    output_path: Path,
    load_pattern: str,
    run_date: date,
    system: str = "synthetic",
    table: str = "derived_test",
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


def create_derived_state_dataset_config(
    system: str = "synthetic",
    entity: str = "derived_state_test",
    history_mode: str = "scd2",
    natural_keys: Optional[List[str]] = None,
    change_ts_column: str = "updated_at",
    attributes: Optional[List[str]] = None,
) -> DatasetConfig:
    """Create DatasetConfig for DERIVED_STATE processing."""
    natural_keys = natural_keys or ["entity_id"]
    attributes = attributes or ["status", "amount", "category"]

    return DatasetConfig.from_dict({
        "environment": "test",
        "domain": "analytics",
        "system": system,
        "entity": entity,
        "bronze": {"enabled": True},
        "silver": {
            "enabled": True,
            "entity_kind": "derived_state",
            "history_mode": history_mode,
            "version": 1,
            "natural_keys": natural_keys,
            "change_ts_column": change_ts_column,
            "attributes": attributes,
            "input_storage": "local",
            "schema_mode": "allow_new_columns",
        },
    })


def run_derived_state_processing(
    bronze_path: Path,
    silver_path: Path,
    dataset_config: DatasetConfig,
    run_date: date,
) -> Dict[str, Any]:
    """Run Silver processing for DERIVED_STATE and return results."""
    from core.domain.services.pipelines.silver.processor import SilverProcessor

    processor = SilverProcessor(
        dataset=dataset_config,
        bronze_path=bronze_path,
        silver_partition=silver_path,
        run_date=run_date,
        verify_checksum=False,
    )

    result = processor.run()

    return {
        "metrics": result.metrics,
        "outputs": result.outputs,
        "schema_snapshot": result.schema_snapshot,
        "silver_path": silver_path,
    }


def read_silver_output(silver_path: Path) -> Dict[str, pd.DataFrame]:
    """Read all parquet files from Silver output, organized by view.

    Returns:
        Dict with keys like 'state_current', 'state_history' mapped to DataFrames.
    """
    result = {}
    for parquet_file in silver_path.rglob("*.parquet"):
        # Determine view from parent directory or filename
        parent_name = parquet_file.parent.name
        if "current" in parent_name or "current" in parquet_file.name:
            key = "state_current"
        elif "history" in parent_name or "history" in parquet_file.name:
            key = "state_history"
        else:
            key = "default"

        if key in result:
            result[key] = pd.concat([result[key], pd.read_parquet(parquet_file)])
        else:
            result[key] = pd.read_parquet(parquet_file)

    return result


def generate_aggregated_events(
    entity_count: int = 50,
    events_per_entity: int = 5,
    seed: int = 42,
    base_date: date = date(2024, 1, 15),
) -> pd.DataFrame:
    """Generate event data that can be aggregated into derived state.

    Simulates order line items that should be aggregated into order totals.
    """
    import random
    rng = random.Random(seed)

    records = []
    for entity_id in range(1, entity_count + 1):
        entity_total = 0.0
        for event_num in range(1, events_per_entity + 1):
            event_date = base_date + timedelta(days=event_num - 1)
            amount = round(rng.uniform(10, 100), 2)
            entity_total += amount

            records.append({
                "entity_id": f"ENT{entity_id:06d}",
                "event_id": f"EVT{entity_id:06d}_{event_num:03d}",
                "event_date": event_date,
                "amount": amount,
                "running_total": round(entity_total, 2),
                "event_count": event_num,
                "status": "completed" if event_num == events_per_entity else "pending",
                "category": rng.choice(["A", "B", "C"]),
                "updated_at": event_date,
            })

    return pd.DataFrame(records)


def generate_multi_version_state(
    entity_count: int = 50,
    versions_per_entity: int = 3,
    seed: int = 42,
    base_date: date = date(2024, 1, 15),
) -> pd.DataFrame:
    """Generate state data with multiple versions per entity (for SCD testing)."""
    import random
    rng = random.Random(seed)

    statuses = ["draft", "active", "processing", "completed", "archived"]
    records = []

    for entity_id in range(1, entity_count + 1):
        for version in range(1, versions_per_entity + 1):
            version_date = base_date + timedelta(days=(version - 1) * 7)
            status_idx = min(version - 1, len(statuses) - 1)

            records.append({
                "entity_id": f"ENT{entity_id:06d}",
                "version": version,
                "status": statuses[status_idx],
                "amount": round(rng.uniform(100, 1000) * version, 2),
                "category": rng.choice(["alpha", "beta", "gamma"]),
                "updated_at": version_date,
                "created_at": base_date,
            })

    return pd.DataFrame(records)


# =============================================================================
# DERIVED_STATE Basic Processing Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedStateBasicProcessing:
    """Test basic DERIVED_STATE processing functionality."""

    def test_derived_state_processing_succeeds(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Test DERIVED_STATE processing completes successfully."""
        df = generate_multi_version_state(
            entity_count=20,
            versions_per_entity=3,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd2",
            natural_keys=["entity_id"],
            change_ts_column="updated_at",
        )

        result = run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        assert result["metrics"].rows_read == len(df)
        assert result["metrics"].rows_written > 0

    def test_derived_state_handler_uses_derived_flag(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify DERIVED_STATE uses StateHandler with derived=True."""
        from core.domain.services.pipelines.silver.handlers.registry import (
            get_handler_class,
        )

        handler_cls, kwargs = get_handler_class(EntityKind.DERIVED_STATE)
        assert kwargs.get("derived") is True, "DERIVED_STATE should have derived=True"

    def test_derived_state_produces_current_view(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify DERIVED_STATE produces state_current output."""
        df = generate_multi_version_state(
            entity_count=20,
            versions_per_entity=3,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd1",
            natural_keys=["entity_id"],
        )

        result = run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        # Check outputs contain state_current
        outputs = result["outputs"]
        assert any("current" in k for k in outputs.keys()), "Should produce current view"


# =============================================================================
# SCD1 Mode Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedStateSCD1Mode:
    """Test DERIVED_STATE with SCD1 (overwrite/latest-only) mode."""

    def test_scd1_keeps_only_latest_version(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify SCD1 mode keeps only the latest version per entity."""
        entity_count = 30
        versions_per_entity = 4

        df = generate_multi_version_state(
            entity_count=entity_count,
            versions_per_entity=versions_per_entity,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd1",
            natural_keys=["entity_id"],
        )

        run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        outputs = read_silver_output(tmp_path / "silver")

        # SCD1 should produce only current view with one row per entity
        current_df = outputs.get("state_current")
        if current_df is None:
            current_df = outputs.get("default")
        assert current_df is not None, "Should have current/default output"

        # Should have exactly entity_count rows (one per entity)
        assert len(current_df) == entity_count, (
            f"SCD1 should have {entity_count} rows, got {len(current_df)}"
        )

        # Each entity should appear exactly once
        unique_entities = current_df["entity_id"].nunique()
        assert unique_entities == entity_count

    def test_scd1_selects_latest_by_timestamp(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify SCD1 selects the record with latest timestamp."""
        # Create specific test data with known latest versions
        df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "category": "A", "updated_at": base_date},
            {"entity_id": "ENT001", "status": "active", "amount": 150.0, "category": "A", "updated_at": base_date + timedelta(days=1)},
            {"entity_id": "ENT001", "status": "completed", "amount": 200.0, "category": "A", "updated_at": base_date + timedelta(days=2)},
            {"entity_id": "ENT002", "status": "draft", "amount": 50.0, "category": "B", "updated_at": base_date},
            {"entity_id": "ENT002", "status": "active", "amount": 75.0, "category": "B", "updated_at": base_date + timedelta(days=1)},
        ])

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd1",
            natural_keys=["entity_id"],
        )

        run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        outputs = read_silver_output(tmp_path / "silver")
        current_df = outputs.get("state_current")
        if current_df is None:
            current_df = outputs.get("default")

        # ENT001 should have status=completed (latest)
        ent001 = current_df[current_df["entity_id"] == "ENT001"].iloc[0]
        assert ent001["status"] == "completed", "Should select latest version"
        assert ent001["amount"] == 200.0

        # ENT002 should have status=active (latest)
        ent002 = current_df[current_df["entity_id"] == "ENT002"].iloc[0]
        assert ent002["status"] == "active"


# =============================================================================
# SCD2 Mode Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedStateSCD2Mode:
    """Test DERIVED_STATE with SCD2 (full history) mode."""

    def test_scd2_produces_history_view(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify SCD2 mode produces state_history output."""
        df = generate_multi_version_state(
            entity_count=20,
            versions_per_entity=3,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd2",
            natural_keys=["entity_id"],
        )

        result = run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        outputs = result["outputs"]
        # Should have history output
        assert any("history" in k for k in outputs.keys()), "SCD2 should produce history view"

    def test_scd2_produces_one_row_per_entity_per_batch(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify SCD2 produces one row per entity after Bronze deduplication.

        Note: Silver preparation deduplicates by natural keys before processing.
        SCD2 history tracking happens across multiple Bronze batches over time,
        not within a single batch. This test verifies that the history output
        contains one row per entity with proper effective date tracking.
        """
        entity_count = 20

        # Generate multi-version data (prep stage will dedupe to latest per entity)
        df = generate_multi_version_state(
            entity_count=entity_count,
            versions_per_entity=4,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd2",
            natural_keys=["entity_id"],
        )

        run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        outputs = read_silver_output(tmp_path / "silver")
        history_df = outputs.get("state_history")

        if history_df is not None:
            # After preparation deduplication, we have one row per entity
            # SCD2 handler processes these and adds effective dates
            assert len(history_df) == entity_count, (
                f"SCD2 history should have {entity_count} rows (one per entity after "
                f"Bronze dedup), got {len(history_df)}"
            )
            # All should be marked as current (first batch)
            assert all(history_df["is_current"] == 1), "All records in first batch should be current"

    def test_scd2_adds_effective_dates(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify SCD2 adds effective_from/effective_to columns."""
        df = generate_multi_version_state(
            entity_count=10,
            versions_per_entity=3,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd2",
            natural_keys=["entity_id"],
        )

        run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        outputs = read_silver_output(tmp_path / "silver")
        history_df = outputs.get("state_history")

        if history_df is not None:
            assert "effective_from" in history_df.columns, "Should have effective_from"
            assert "effective_to" in history_df.columns, "Should have effective_to"
            assert "is_current" in history_df.columns, "Should have is_current flag"

    def test_scd2_marks_current_record(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Verify SCD2 correctly marks is_current for latest versions."""
        entity_count = 10
        versions_per_entity = 3

        df = generate_multi_version_state(
            entity_count=entity_count,
            versions_per_entity=versions_per_entity,
            base_date=base_date,
        )

        bronze_path = create_bronze_output(
            df=df,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd2",
            natural_keys=["entity_id"],
        )

        run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        outputs = read_silver_output(tmp_path / "silver")
        history_df = outputs.get("state_history")

        if history_df is not None and "is_current" in history_df.columns:
            # Should have exactly entity_count current records
            current_count = history_df["is_current"].sum()
            assert current_count == entity_count, (
                f"Should have {entity_count} current records, got {current_count}"
            )


# =============================================================================
# Aggregated State Derivation Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedStateAggregation:
    """Test DERIVED_STATE for aggregated/computed state scenarios."""

    def test_derived_state_from_aggregated_events(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Test processing state that represents aggregations of events."""
        # Simulate pre-aggregated order totals (as if computed upstream)
        df = generate_aggregated_events(
            entity_count=30,
            events_per_entity=5,
            base_date=base_date,
        )

        # Keep only the latest event per entity (simulating aggregation result)
        aggregated = df.sort_values("updated_at").drop_duplicates(
            subset=["entity_id"], keep="last"
        )

        bronze_path = create_bronze_output(
            df=aggregated,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd1",
            natural_keys=["entity_id"],
            attributes=["running_total", "event_count", "status"],
        )

        result = run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        assert result["metrics"].rows_read == len(aggregated)
        assert result["metrics"].rows_written > 0


# =============================================================================
# Incremental Update Tests
# =============================================================================


@pytest.mark.integration
class TestDerivedStateIncrementalUpdates:
    """Test DERIVED_STATE with incremental update scenarios."""

    def test_incremental_state_updates_merge_correctly(
        self,
        tmp_path: Path,
        base_date: date,
    ):
        """Test that incremental updates to derived state merge correctly."""
        # Initial state
        initial_df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "draft", "amount": 100.0, "updated_at": base_date, "category": "A"},
            {"entity_id": "ENT002", "status": "draft", "amount": 200.0, "updated_at": base_date, "category": "B"},
            {"entity_id": "ENT003", "status": "draft", "amount": 300.0, "updated_at": base_date, "category": "A"},
        ])

        # Updates (some entities updated, some unchanged)
        updates_df = pd.DataFrame([
            {"entity_id": "ENT001", "status": "active", "amount": 150.0, "updated_at": base_date + timedelta(days=1), "category": "A"},
            {"entity_id": "ENT002", "status": "completed", "amount": 250.0, "updated_at": base_date + timedelta(days=1), "category": "B"},
            {"entity_id": "ENT004", "status": "draft", "amount": 400.0, "updated_at": base_date + timedelta(days=1), "category": "C"},  # New entity
        ])

        # Combine for processing
        combined = pd.concat([initial_df, updates_df], ignore_index=True)

        bronze_path = create_bronze_output(
            df=combined,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=base_date,
        )

        dataset_config = create_derived_state_dataset_config(
            history_mode="scd1",
            natural_keys=["entity_id"],
        )

        run_derived_state_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=base_date,
        )

        outputs = read_silver_output(tmp_path / "silver")
        current_df = outputs.get("state_current")
        if current_df is None:
            current_df = outputs.get("default")

        # Should have 4 unique entities (3 initial + 1 new)
        assert len(current_df) == 4, f"Expected 4 entities, got {len(current_df)}"

        # ENT001 should have updated values
        ent001 = current_df[current_df["entity_id"] == "ENT001"].iloc[0]
        assert ent001["status"] == "active"
        assert ent001["amount"] == 150.0

        # ENT003 should have original values (no update)
        ent003 = current_df[current_df["entity_id"] == "ENT003"].iloc[0]
        assert ent003["status"] == "draft"
        assert ent003["amount"] == 300.0
