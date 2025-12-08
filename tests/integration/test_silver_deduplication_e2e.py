"""Silver Deduplication End-to-End Tests (Story 2.2a).

Tests that verify Silver layer deduplication works correctly using
DuplicateInjector to inject various duplicate types:
- Exact duplicates (identical rows)
- Near duplicates (same key, different mutable fields)
- Out-of-order duplicates (late arrival of older records)

Silver uses "last-in-wins" deduplication strategy:
df.sort_values(natural_keys + timestamp).drop_duplicates(keep="last")
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

from core.infrastructure.config import DatasetConfig
from tests.synthetic_data import (
    ClaimsGenerator,
    OrdersGenerator,
    DuplicateInjector,
    DuplicateConfig,
)


# =============================================================================
# Helper Functions (adapted from test_silver_patterns_e2e.py)
# =============================================================================


def create_bronze_output(
    df: pd.DataFrame,
    output_path: Path,
    load_pattern: str,
    run_date: date,
    system: str = "synthetic",
    table: str = "dedup_test",
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
    checksum = hashlib.sha256(parquet_file.read_bytes()).hexdigest()
    checksums = {
        "files": [
            {
                "path": "chunk_0.parquet",
                "sha256": checksum,
                "size": parquet_file.stat().st_size,
            }
        ]
    }
    checksums_path = partition_path / "_checksums.json"
    checksums_path.write_text(json.dumps(checksums, indent=2))

    return partition_path


def create_silver_dataset_config(
    system: str = "synthetic",
    entity: str = "dedup_test",
    entity_kind: str = "event",
    natural_keys: Optional[List[str]] = None,
    order_column: str = "updated_at",
    event_ts_column: str = "created_at",
    change_ts_column: Optional[str] = None,
    history_mode: str = "latest_only",
) -> DatasetConfig:
    """Create DatasetConfig for Silver processing."""
    natural_keys = natural_keys or ["record_id"]

    silver_config = {
        "enabled": True,
        "entity_kind": entity_kind,
        "version": 1,
        "natural_keys": natural_keys,
        "order_column": order_column,
        "event_ts_column": event_ts_column,
        "input_storage": "local",
        "schema_mode": "allow_new_columns",
    }

    if entity_kind in ("state", "derived_state"):
        silver_config["change_ts_column"] = change_ts_column or event_ts_column
        silver_config["history_mode"] = history_mode  # Use LATEST_ONLY for dedup tests

    return DatasetConfig.from_dict({
        "environment": "test",
        "domain": "healthcare",
        "system": system,
        "entity": entity,
        "bronze": {"enabled": True},
        "silver": silver_config,
    })


def run_silver_processing(
    bronze_path: Path,
    silver_path: Path,
    dataset_config: DatasetConfig,
    run_date: date,
) -> Dict[str, Any]:
    """Run Silver processing and return results."""
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


def read_silver_parquet(silver_path: Path) -> pd.DataFrame:
    """Read all parquet files from Silver output."""
    parquet_files = list(silver_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {silver_path}")

    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def t0_date() -> date:
    """Standard T0 (initial load) date."""
    return date(2024, 1, 15)


@pytest.fixture
def orders_generator() -> OrdersGenerator:
    """OrdersGenerator for EVENT-like data."""
    return OrdersGenerator(seed=42, row_count=50)


@pytest.fixture
def claims_generator() -> ClaimsGenerator:
    """ClaimsGenerator for STATE-like data."""
    return ClaimsGenerator(seed=42, row_count=50)


@pytest.fixture
def duplicate_injector() -> DuplicateInjector:
    """DuplicateInjector with default config."""
    return DuplicateInjector(seed=42)


# =============================================================================
# Exact Duplicate Removal Tests
# =============================================================================


class TestExactDuplicateRemoval:
    """Test that exact duplicates are removed during Silver processing."""

    def test_exact_duplicates_removed_for_event_entity(
        self,
        orders_generator: OrdersGenerator,
        duplicate_injector: DuplicateInjector,
        tmp_path: Path,
        t0_date: date,
    ):
        """Exact duplicates should be removed for EVENT entities."""
        # Generate base data
        original_df = orders_generator.generate_t0(t0_date)
        original_count = len(original_df)

        # Inject exact duplicates (10 copies)
        df_with_dupes = duplicate_injector.inject_exact_duplicates(
            original_df, count=10
        )

        # Verify duplicates were injected
        assert len(df_with_dupes) == original_count + 10

        # Create Bronze output
        bronze_path = create_bronze_output(
            df=df_with_dupes,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Run Silver processing
        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["order_id"],
            event_ts_column="order_ts",
            order_column="updated_at",
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Read Silver output
        silver_df = read_silver_parquet(tmp_path / "silver")

        # Verify deduplication: should have original count (unique order_ids)
        assert len(silver_df) == original_count, (
            f"Expected {original_count} rows after deduplication, got {len(silver_df)}"
        )
        assert silver_df["order_id"].is_unique, "order_id should be unique after dedup"

    def test_exact_duplicates_removed_for_state_entity(
        self,
        claims_generator: ClaimsGenerator,
        duplicate_injector: DuplicateInjector,
        tmp_path: Path,
        t0_date: date,
    ):
        """Exact duplicates should be removed for STATE entities."""
        # Generate base data
        original_df = claims_generator.generate_t0(t0_date)
        original_count = len(original_df)

        # Inject exact duplicates
        df_with_dupes = duplicate_injector.inject_exact_duplicates(
            original_df, count=15
        )

        assert len(df_with_dupes) == original_count + 15

        # Create Bronze output
        bronze_path = create_bronze_output(
            df=df_with_dupes,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Run Silver processing with STATE entity kind
        dataset_config = create_silver_dataset_config(
            entity_kind="state",
            natural_keys=["claim_id"],
            event_ts_column="created_at",
            change_ts_column="updated_at",
            order_column="updated_at",
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Read Silver output
        silver_df = read_silver_parquet(tmp_path / "silver")

        # Verify deduplication
        assert len(silver_df) == original_count, (
            f"Expected {original_count} rows after deduplication, got {len(silver_df)}"
        )
        assert silver_df["claim_id"].is_unique, "claim_id should be unique after dedup"


# =============================================================================
# Near Duplicate Tests (Last Timestamp Wins)
# =============================================================================


class TestNearDuplicateLastTimestampWins:
    """Test that near duplicates resolve to the record with the latest timestamp."""

    def test_state_entity_near_duplicates_latest_wins(
        self,
        claims_generator: ClaimsGenerator,
        duplicate_injector: DuplicateInjector,
        tmp_path: Path,
        t0_date: date,
    ):
        """For STATE entities, same key with different timestamps keeps latest only."""
        # Generate base data
        original_df = claims_generator.generate_t0(t0_date)
        original_count = len(original_df)

        # Inject near duplicates with incremented timestamps
        # STATE entities deduplicate by natural_keys + change_ts_column
        df_with_dupes = duplicate_injector.inject_near_duplicates(
            original_df,
            key_columns=["claim_id"],
            mutable_columns=["status", "paid_amount"],
            timestamp_column="updated_at",
            timestamp_offset_seconds=3600,  # 1 hour later
            count=10,
        )

        # Verify duplicates were injected
        stats = duplicate_injector.get_duplicate_stats(df_with_dupes, ["claim_id"])
        assert stats["duplicate_rows"] > 0, "Should have duplicates by claim_id"

        # Create Bronze output
        bronze_path = create_bronze_output(
            df=df_with_dupes,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Run Silver processing with LATEST_ONLY
        dataset_config = create_silver_dataset_config(
            entity_kind="state",
            natural_keys=["claim_id"],
            event_ts_column="created_at",
            change_ts_column="updated_at",
            order_column="updated_at",
            history_mode="latest_only",
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Read Silver output
        silver_df = read_silver_parquet(tmp_path / "silver")

        # With LATEST_ONLY, should have original count (deduplicated by natural key)
        assert silver_df["claim_id"].is_unique, "claim_id should be unique after dedup"
        assert len(silver_df) == original_count, (
            f"Expected {original_count} unique rows, got {len(silver_df)}"
        )

    def test_near_duplicates_latest_timestamp_value_wins(
        self,
        claims_generator: ClaimsGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """The record with the latest timestamp value should be kept."""
        # Generate base data with known values
        original_df = claims_generator.generate_t0(t0_date)
        original_count = len(original_df)

        # Manually create a near duplicate with a later timestamp
        target_claim_id = original_df.iloc[0]["claim_id"]
        original_timestamp = original_df.iloc[0]["updated_at"]

        # Create a duplicate with later timestamp (2 hours later)
        duplicate_row = original_df.iloc[0].to_dict()
        later_timestamp = original_timestamp + timedelta(hours=2)
        duplicate_row["updated_at"] = later_timestamp

        df_with_dupe = pd.concat([original_df, pd.DataFrame([duplicate_row])], ignore_index=True)

        # Create Bronze output
        bronze_path = create_bronze_output(
            df=df_with_dupe,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Run Silver processing with LATEST_ONLY to get deduplication
        dataset_config = create_silver_dataset_config(
            entity_kind="state",
            natural_keys=["claim_id"],
            event_ts_column="created_at",
            change_ts_column="updated_at",
            order_column="updated_at",
            history_mode="latest_only",  # Force deduplication
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Read Silver output
        silver_df = read_silver_parquet(tmp_path / "silver")

        # With LATEST_ONLY, should have original count (deduplicated)
        assert len(silver_df) == original_count, (
            f"Expected {original_count} rows after dedup, got {len(silver_df)}"
        )

        # Find the target claim
        result_row = silver_df[silver_df["claim_id"] == target_claim_id]
        assert len(result_row) == 1, f"Expected 1 row for {target_claim_id}, got {len(result_row)}"

        # Verify the later timestamp won - the updated_at should match the later timestamp
        result_timestamp = result_row.iloc[0]["updated_at"]
        # Convert to comparable format (both should be pandas Timestamps)
        assert pd.Timestamp(result_timestamp) == pd.Timestamp(later_timestamp), (
            f"Expected timestamp {later_timestamp}, got {result_timestamp}"
        )


# =============================================================================
# Out-of-Order Duplicate Tests
# =============================================================================


class TestOutOfOrderDuplicates:
    """Test that out-of-order duplicates are handled correctly."""

    def test_out_of_order_duplicates_resolved_by_timestamp(
        self,
        orders_generator: OrdersGenerator,
        duplicate_injector: DuplicateInjector,
        tmp_path: Path,
        t0_date: date,
    ):
        """Out-of-order arrivals should deduplicate by timestamp, not arrival order."""
        # Generate chronologically ordered data
        original_df = orders_generator.generate_t0(t0_date)
        original_count = len(original_df)

        # Inject out-of-order duplicates (older records appearing late in the batch)
        df_with_dupes = duplicate_injector.inject_out_of_order_duplicates(
            original_df,
            key_columns=["order_id"],
            timestamp_column="updated_at",
            count=8,
        )

        # Verify duplicates were injected
        stats = duplicate_injector.get_duplicate_stats(df_with_dupes, ["order_id"])
        assert stats["duplicate_rows"] > 0, "Should have injected duplicates"

        # Create Bronze output
        bronze_path = create_bronze_output(
            df=df_with_dupes,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Run Silver processing
        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["order_id"],
            event_ts_column="order_ts",
            order_column="updated_at",
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Read Silver output
        silver_df = read_silver_parquet(tmp_path / "silver")

        # Verify deduplication worked
        assert silver_df["order_id"].is_unique, "order_id should be unique"
        assert len(silver_df) == original_count, (
            f"Expected {original_count} unique rows, got {len(silver_df)}"
        )


# =============================================================================
# Combined Duplicate Types Tests
# =============================================================================


class TestAllDuplicateTypesCombined:
    """Test that all duplicate types together are handled correctly."""

    def test_all_duplicate_types_deduplicated(
        self,
        orders_generator: OrdersGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Combined exact, near, and out-of-order duplicates should all be removed."""
        # Generate base data
        original_df = orders_generator.generate_t0(t0_date)
        original_count = len(original_df)

        # Configure injector with known rates
        config = DuplicateConfig(
            exact_duplicate_rate=0.1,  # 10% exact duplicates
            near_duplicate_rate=0.08,  # 8% near duplicates
            out_of_order_rate=0.05,  # 5% out-of-order duplicates
            seed=42,
        )
        injector = DuplicateInjector(seed=42, config=config)

        # Inject all duplicate types
        df_with_dupes = injector.inject_all_duplicate_types(
            original_df,
            key_columns=["order_id"],
            timestamp_column="updated_at",
            mutable_columns=["status", "total_amount"],
        )

        # Verify duplicates were injected
        stats_before = injector.get_duplicate_stats(df_with_dupes, ["order_id"])
        assert stats_before["duplicate_rows"] > 0, "Should have duplicates before processing"

        # Create Bronze output
        bronze_path = create_bronze_output(
            df=df_with_dupes,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Run Silver processing
        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["order_id"],
            event_ts_column="order_ts",
            order_column="updated_at",
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Read Silver output
        silver_df = read_silver_parquet(tmp_path / "silver")

        # Verify all duplicates removed
        stats_after = injector.get_duplicate_stats(silver_df, ["order_id"])
        assert stats_after["duplicate_rows"] == 0, (
            f"Expected 0 duplicates after Silver, got {stats_after['duplicate_rows']}"
        )
        assert stats_after["unique_keys"] == original_count, (
            f"Expected {original_count} unique keys, got {stats_after['unique_keys']}"
        )


# =============================================================================
# Duplicate Stats Validation Tests
# =============================================================================


class TestDuplicateStatsValidation:
    """Test that DuplicateInjector stats accurately reflect duplicate counts."""

    def test_duplicate_stats_match_actual_counts(
        self,
        claims_generator: ClaimsGenerator,
        duplicate_injector: DuplicateInjector,
        tmp_path: Path,
        t0_date: date,
    ):
        """DuplicateInjector stats should match actual duplicate counts."""
        # Generate base data
        original_df = claims_generator.generate_t0(t0_date)
        original_count = len(original_df)

        # Inject known number of exact duplicates
        exact_count = 12
        df_with_dupes = duplicate_injector.inject_exact_duplicates(
            original_df, count=exact_count
        )

        # Verify stats are accurate
        stats = duplicate_injector.get_duplicate_stats(df_with_dupes, ["claim_id"])

        assert stats["total_rows"] == original_count + exact_count, (
            f"Expected {original_count + exact_count} total rows"
        )
        # At least some duplicates should be detected
        # (exact count depends on which rows were duplicated)
        assert stats["duplicate_rows"] > 0, "Should detect duplicates"
        assert stats["keys_with_duplicates"] > 0, "Should have keys with duplicates"

        # Create Bronze and process through Silver
        bronze_path = create_bronze_output(
            df=df_with_dupes,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Use LATEST_ONLY history mode to get actual deduplication
        dataset_config = create_silver_dataset_config(
            entity_kind="state",
            natural_keys=["claim_id"],
            event_ts_column="created_at",
            change_ts_column="updated_at",
            history_mode="latest_only",  # Force deduplication
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        silver_df = read_silver_parquet(tmp_path / "silver")

        # After Silver, should have exactly original_count unique records
        stats_after = duplicate_injector.get_duplicate_stats(silver_df, ["claim_id"])
        assert stats_after["unique_keys"] == original_count, (
            f"Expected {original_count} unique keys, got {stats_after['unique_keys']}"
        )
        assert stats_after["duplicate_rows"] == 0, (
            f"Expected 0 duplicates, got {stats_after['duplicate_rows']}"
        )


# =============================================================================
# Idempotency Tests
# =============================================================================


class TestDeduplicationIdempotency:
    """Test that deduplication is idempotent (same result on reprocessing)."""

    def test_reprocessing_with_duplicates_is_idempotent(
        self,
        orders_generator: OrdersGenerator,
        duplicate_injector: DuplicateInjector,
        tmp_path: Path,
        t0_date: date,
    ):
        """Processing same data with duplicates twice should yield same result."""
        # Generate data with duplicates
        original_df = orders_generator.generate_t0(t0_date)
        df_with_dupes = duplicate_injector.inject_exact_duplicates(original_df, count=10)

        # Process first time
        bronze_path_1 = create_bronze_output(
            df=df_with_dupes,
            output_path=tmp_path / "bronze_1",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["order_id"],
            event_ts_column="order_ts",
            order_column="updated_at",
        )

        run_silver_processing(
            bronze_path=bronze_path_1,
            silver_path=tmp_path / "silver_1",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        silver_df_1 = read_silver_parquet(tmp_path / "silver_1")

        # Process second time (same data)
        bronze_path_2 = create_bronze_output(
            df=df_with_dupes,
            output_path=tmp_path / "bronze_2",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        run_silver_processing(
            bronze_path=bronze_path_2,
            silver_path=tmp_path / "silver_2",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        silver_df_2 = read_silver_parquet(tmp_path / "silver_2")

        # Results should be identical (same row count, same unique keys)
        assert len(silver_df_1) == len(silver_df_2), "Row counts should match"

        # Same order_ids should be present
        ids_1 = set(silver_df_1["order_id"])
        ids_2 = set(silver_df_2["order_id"])
        assert ids_1 == ids_2, "Same order_ids should be present in both runs"
