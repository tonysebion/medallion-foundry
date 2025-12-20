"""Tests for INCREMENTAL_APPEND -> STATE pattern combinations.

Tests both SCD1 (CURRENT_ONLY) and SCD2 (FULL_HISTORY) transformations
with actual data inspection.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_latest, build_history
from tests.data_validation.assertions import SCD1Assertions, SCD2Assertions
from tests.combination.helpers import simulate_incremental_to_scd1, simulate_incremental_to_scd2
from tests.combination.conftest import create_memtable


class TestIncrementalToSCD1:
    """Test INCREMENTAL_APPEND -> STATE (CURRENT_ONLY/SCD1) pattern."""

    def test_single_batch_produces_unique_keys(self, con):
        """Single incremental batch produces unique keys."""
        batch = create_memtable([
            {"id": 1, "name": "Alice", "status": "active", "ts": datetime(2025, 1, 10)},
            {"id": 2, "name": "Bob", "status": "pending", "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_incremental_to_scd1([batch], ["id"], "ts")
        result_df = result.execute()

        unique = SCD1Assertions.assert_unique_keys(result_df, ["id"])
        assert unique.passed, unique.message
        assert len(result_df) == 2

    def test_multiple_batches_keeps_latest(self, con):
        """Incremental batches keep latest version per key."""
        batch_t0 = create_memtable([
            {"id": 1, "name": "Alice", "status": "active", "ts": datetime(2025, 1, 10)},
            {"id": 2, "name": "Bob", "status": "active", "ts": datetime(2025, 1, 10)},
        ])

        batch_t1 = create_memtable([
            {"id": 1, "name": "Alice Updated", "status": "inactive", "ts": datetime(2025, 1, 15)},
            {"id": 3, "name": "Charlie", "status": "active", "ts": datetime(2025, 1, 15)},
        ])

        result = simulate_incremental_to_scd1([batch_t0, batch_t1], ["id"], "ts")
        result_df = result.execute()

        # Should have 3 unique keys
        assert len(result_df) == 3

        # Verify latest values
        values = SCD1Assertions.assert_latest_values(
            result_df,
            keys=["id"],
            order_by="ts",
            expected_values={
                1: {"name": "Alice Updated", "status": "inactive"},
                2: {"name": "Bob", "status": "active"},
                3: {"name": "Charlie", "status": "active"},
            }
        )
        assert values.passed, values.message

    def test_out_of_order_updates_handled(self, con):
        """Out-of-order incremental updates deduplicated correctly."""
        # Batch 1 has earlier and later timestamps
        batch = create_memtable([
            {"id": 1, "name": "V2", "ts": datetime(2025, 1, 15)},  # Later
            {"id": 1, "name": "V1", "ts": datetime(2025, 1, 10)},  # Earlier
            {"id": 1, "name": "V3", "ts": datetime(2025, 1, 20)},  # Latest
        ])

        result = simulate_incremental_to_scd1([batch], ["id"], "ts")
        result_df = result.execute()

        # Should keep V3 (latest timestamp)
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "V3"

    def test_no_scd2_columns_in_output(self, con):
        """SCD1 output should not have SCD2 columns."""
        batch = create_memtable([
            {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_incremental_to_scd1([batch], ["id"], "ts")
        result_df = result.execute()

        no_scd2 = SCD1Assertions.assert_no_scd2_columns(result_df)
        assert no_scd2.passed, no_scd2.message


class TestIncrementalToSCD2:
    """Test INCREMENTAL_APPEND -> STATE (FULL_HISTORY/SCD2) pattern."""

    def test_single_batch_creates_history(self, con):
        """Single incremental batch creates SCD2 structure."""
        batch = create_memtable([
            {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_incremental_to_scd2([batch], ["id"], "ts")
        result_df = result.execute()

        has_cols = SCD2Assertions.assert_has_scd2_columns(result_df)
        assert has_cols.passed, has_cols.message

        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed, one_current.message

    def test_multiple_batches_preserves_all_versions(self, con):
        """Incremental batches preserve all versions in history."""
        batch_t0 = create_memtable([
            {"id": 1, "name": "V1", "ts": datetime(2025, 1, 10)},
        ])

        batch_t1 = create_memtable([
            {"id": 1, "name": "V2", "ts": datetime(2025, 1, 15)},
        ])

        batch_t2 = create_memtable([
            {"id": 1, "name": "V3", "ts": datetime(2025, 1, 20)},
        ])

        result = simulate_incremental_to_scd2([batch_t0, batch_t1, batch_t2], ["id"], "ts")
        result_df = result.execute()

        # All 3 versions preserved
        history = SCD2Assertions.assert_history_preserved(
            result_df,
            keys=["id"],
            expected_counts={1: 3}
        )
        assert history.passed, history.message

    def test_current_view_after_multiple_updates(self, con):
        """Current view has unique keys after updates."""
        batch_t0 = create_memtable([
            {"id": 1, "name": "V1", "ts": datetime(2025, 1, 10)},
            {"id": 2, "name": "V1", "ts": datetime(2025, 1, 10)},
        ])

        batch_t1 = create_memtable([
            {"id": 1, "name": "V2", "ts": datetime(2025, 1, 15)},
            {"id": 2, "name": "V2", "ts": datetime(2025, 1, 15)},
        ])

        result = simulate_incremental_to_scd2([batch_t0, batch_t1], ["id"], "ts")
        result_df = result.execute()

        current_unique = SCD2Assertions.assert_current_view_unique(result_df, ["id"])
        assert current_unique.passed, current_unique.message

        # Current records have NULL effective_to
        null_to = SCD2Assertions.assert_current_has_null_effective_to(result_df)
        assert null_to.passed, null_to.message

    def test_effective_dates_form_chain(self, con):
        """Effective dates form contiguous chain for each key."""
        batches = [
            create_memtable([{"id": 1, "name": f"V{i}", "ts": datetime(2025, 1, i * 5)}])
            for i in range(1, 5)
        ]

        result = simulate_incremental_to_scd2(batches, ["id"], "ts")
        result_df = result.execute()

        contiguous = SCD2Assertions.assert_effective_dates_contiguous(result_df, ["id"])
        assert contiguous.passed, contiguous.message


class TestIncrementalNewAndExisting:
    """Test incremental patterns with mix of new and existing keys."""

    def test_scd1_new_keys_added(self, con):
        """New keys in incremental batches are added to SCD1."""
        batch_t0 = create_memtable([
            {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
        ])

        batch_t1 = create_memtable([
            {"id": 2, "name": "Bob", "ts": datetime(2025, 1, 15)},  # New key
        ])

        batch_t2 = create_memtable([
            {"id": 3, "name": "Charlie", "ts": datetime(2025, 1, 20)},  # New key
        ])

        result = simulate_incremental_to_scd1([batch_t0, batch_t1, batch_t2], ["id"], "ts")
        result_df = result.execute()

        assert len(result_df) == 3
        assert set(result_df["id"]) == {1, 2, 3}

    def test_scd2_new_keys_get_history(self, con):
        """New keys in incremental batches get proper history."""
        batch_t0 = create_memtable([
            {"id": 1, "name": "Alice V1", "ts": datetime(2025, 1, 10)},
        ])

        batch_t1 = create_memtable([
            {"id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 15)},  # Update
            {"id": 2, "name": "Bob V1", "ts": datetime(2025, 1, 15)},    # New key
        ])

        result = simulate_incremental_to_scd2([batch_t0, batch_t1], ["id"], "ts")
        result_df = result.execute()

        # id=1 should have 2 versions, id=2 should have 1
        history = SCD2Assertions.assert_history_preserved(
            result_df,
            keys=["id"],
            expected_counts={1: 2, 2: 1}
        )
        assert history.passed, history.message

        # Both should have exactly one current
        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed, one_current.message
