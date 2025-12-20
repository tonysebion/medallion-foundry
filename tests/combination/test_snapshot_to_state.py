"""Tests for FULL_SNAPSHOT -> STATE pattern combinations.

Tests both SCD1 (CURRENT_ONLY) and SCD2 (FULL_HISTORY) transformations
with actual data inspection.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_latest, build_history, dedupe_exact
from tests.data_validation.assertions import SCD1Assertions, SCD2Assertions
from tests.combination.helpers import simulate_snapshot_to_scd1, simulate_snapshot_to_scd2
from tests.combination.conftest import create_memtable


class TestSnapshotToSCD1:
    """Test FULL_SNAPSHOT -> STATE (CURRENT_ONLY/SCD1) pattern."""

    def test_single_batch_unique_keys(self, con):
        """Single snapshot batch produces unique keys in SCD1."""
        data = [
            {"id": 1, "name": "Alice", "status": "active", "ts": datetime(2025, 1, 10)},
            {"id": 2, "name": "Bob", "status": "pending", "ts": datetime(2025, 1, 10)},
            {"id": 3, "name": "Charlie", "status": "active", "ts": datetime(2025, 1, 10)},
        ]
        batch = create_memtable(data)

        result = simulate_snapshot_to_scd1([batch], ["id"], "ts")
        result_df = result.execute()

        # Validate SCD1 properties
        unique_result = SCD1Assertions.assert_unique_keys(result_df, ["id"])
        assert unique_result.passed, unique_result.message

        no_scd2_result = SCD1Assertions.assert_no_scd2_columns(result_df)
        assert no_scd2_result.passed, no_scd2_result.message

        # Verify row count
        assert len(result_df) == 3

    def test_multiple_batches_keeps_latest(self, con):
        """Multiple snapshot batches keep only latest from final batch."""
        batch_t0 = create_memtable([
            {"id": 1, "name": "Alice", "status": "active", "ts": datetime(2025, 1, 10)},
            {"id": 2, "name": "Bob", "status": "active", "ts": datetime(2025, 1, 10)},
        ])

        batch_t1 = create_memtable([
            {"id": 1, "name": "Alice Updated", "status": "inactive", "ts": datetime(2025, 1, 15)},
            {"id": 2, "name": "Bob", "status": "active", "ts": datetime(2025, 1, 10)},  # Unchanged
            {"id": 3, "name": "Charlie", "status": "active", "ts": datetime(2025, 1, 15)},  # New
        ])

        result = simulate_snapshot_to_scd1([batch_t0, batch_t1], ["id"], "ts")
        result_df = result.execute()

        # Should have 3 records (latest batch state)
        assert len(result_df) == 3

        # Verify values
        values_result = SCD1Assertions.assert_latest_values(
            result_df,
            keys=["id"],
            order_by="ts",
            expected_values={
                1: {"name": "Alice Updated", "status": "inactive"},
                2: {"name": "Bob", "status": "active"},
                3: {"name": "Charlie", "status": "active"},
            }
        )
        assert values_result.passed, values_result.message

    def test_duplicate_in_snapshot_deduplicated(self, con):
        """Duplicates within a snapshot batch are deduplicated."""
        batch = create_memtable([
            {"id": 1, "name": "Alice V1", "ts": datetime(2025, 1, 10)},
            {"id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 15)},  # Later version
            {"id": 2, "name": "Bob", "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_snapshot_to_scd1([batch], ["id"], "ts")
        result_df = result.execute()

        # Should keep latest version
        assert len(result_df) == 2
        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["name"] == "Alice V2"


class TestSnapshotToSCD2:
    """Test FULL_SNAPSHOT -> STATE (FULL_HISTORY/SCD2) pattern."""

    def test_single_batch_creates_history(self, con):
        """Single batch creates SCD2 history structure."""
        data = [
            {"id": 1, "name": "Alice", "status": "active", "ts": datetime(2025, 1, 10)},
            {"id": 2, "name": "Bob", "status": "pending", "ts": datetime(2025, 1, 10)},
        ]
        batch = create_memtable(data)

        result = simulate_snapshot_to_scd2([batch], ["id"], "ts")
        result_df = result.execute()

        # Validate SCD2 columns present
        has_cols = SCD2Assertions.assert_has_scd2_columns(result_df)
        assert has_cols.passed, has_cols.message

        # All records should be current (single batch)
        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed, one_current.message

    def test_multiple_batches_preserves_history(self, con):
        """Multiple batches preserve full version history."""
        batch_t0 = create_memtable([
            {"id": 1, "name": "Alice", "status": "active", "ts": datetime(2025, 1, 10)},
        ])

        batch_t1 = create_memtable([
            {"id": 1, "name": "Alice Updated", "status": "inactive", "ts": datetime(2025, 1, 15)},
        ])

        batch_t2 = create_memtable([
            {"id": 1, "name": "Alice Final", "status": "closed", "ts": datetime(2025, 1, 20)},
        ])

        result = simulate_snapshot_to_scd2([batch_t0, batch_t1, batch_t2], ["id"], "ts")
        result_df = result.execute()

        # Should have 3 versions for id=1
        history_result = SCD2Assertions.assert_history_preserved(
            result_df,
            keys=["id"],
            expected_counts={1: 3}
        )
        assert history_result.passed, history_result.message

        # Only latest should be current
        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed, one_current.message

        # Current should have NULL effective_to
        null_to = SCD2Assertions.assert_current_has_null_effective_to(result_df)
        assert null_to.passed, null_to.message

    def test_effective_dates_contiguous(self, con):
        """Effective dates form contiguous ranges."""
        batch_t0 = create_memtable([
            {"id": 1, "name": "V1", "ts": datetime(2025, 1, 10)},
        ])
        batch_t1 = create_memtable([
            {"id": 1, "name": "V2", "ts": datetime(2025, 1, 15)},
        ])
        batch_t2 = create_memtable([
            {"id": 1, "name": "V3", "ts": datetime(2025, 1, 20)},
        ])

        result = simulate_snapshot_to_scd2([batch_t0, batch_t1, batch_t2], ["id"], "ts")
        result_df = result.execute()

        contiguous = SCD2Assertions.assert_effective_dates_contiguous(result_df, ["id"])
        assert contiguous.passed, contiguous.message

    def test_effective_from_equals_timestamp(self, con):
        """effective_from equals the source timestamp."""
        data = [
            {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10, 12, 30, 45)},
        ]
        batch = create_memtable(data)

        result = simulate_snapshot_to_scd2([batch], ["id"], "ts")
        result_df = result.execute()

        equals_ts = SCD2Assertions.assert_effective_from_equals_ts(result_df, "ts")
        assert equals_ts.passed, equals_ts.message


class TestSnapshotCompositeKeys:
    """Test SNAPSHOT patterns with composite keys."""

    def test_scd1_composite_key(self, con):
        """SCD1 with composite natural key."""
        batch = create_memtable([
            {"region": "US", "product": "A", "price": 100, "ts": datetime(2025, 1, 10)},
            {"region": "US", "product": "B", "price": 200, "ts": datetime(2025, 1, 10)},
            {"region": "EU", "product": "A", "price": 90, "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_snapshot_to_scd1([batch], ["region", "product"], "ts")
        result_df = result.execute()

        unique = SCD1Assertions.assert_unique_keys(result_df, ["region", "product"])
        assert unique.passed, unique.message
        assert len(result_df) == 3

    def test_scd2_composite_key(self, con):
        """SCD2 with composite natural key."""
        batch_t0 = create_memtable([
            {"region": "US", "product": "A", "price": 100, "ts": datetime(2025, 1, 10)},
        ])
        batch_t1 = create_memtable([
            {"region": "US", "product": "A", "price": 110, "ts": datetime(2025, 1, 15)},
        ])

        result = simulate_snapshot_to_scd2([batch_t0, batch_t1], ["region", "product"], "ts")
        result_df = result.execute()

        # Should have 2 versions for US+A
        assert len(result_df) == 2

        unique_current = SCD2Assertions.assert_current_view_unique(
            result_df, ["region", "product"]
        )
        assert unique_current.passed, unique_current.message
