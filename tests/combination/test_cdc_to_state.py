"""Tests for CDC -> STATE pattern combinations.

Tests CDC (Change Data Capture) transformations to both
SCD1 (CURRENT_ONLY) and SCD2 (FULL_HISTORY).
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_latest, build_history
from tests.data_validation.assertions import SCD1Assertions, SCD2Assertions
from tests.combination.helpers import simulate_cdc_to_scd1, simulate_cdc_to_scd2
from tests.combination.conftest import create_memtable


class TestCDCToSCD1:
    """Test CDC -> STATE (CURRENT_ONLY/SCD1) pattern."""

    def test_insert_only_cdc(self, con):
        """CDC with only inserts produces unique keys."""
        cdc = create_memtable([
            {"op": "I", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
            {"op": "I", "id": 2, "name": "Bob", "ts": datetime(2025, 1, 10)},
            {"op": "I", "id": 3, "name": "Charlie", "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_cdc_to_scd1(cdc, ["id"], "ts")
        result_df = result.execute()

        unique = SCD1Assertions.assert_unique_keys(result_df, ["id"])
        assert unique.passed, unique.message
        assert len(result_df) == 3

    def test_insert_and_update_keeps_latest(self, con):
        """CDC with inserts and updates keeps latest version."""
        cdc = create_memtable([
            {"op": "I", "id": 1, "name": "Alice", "status": "active", "ts": datetime(2025, 1, 10)},
            {"op": "U", "id": 1, "name": "Alice Updated", "status": "inactive", "ts": datetime(2025, 1, 15)},
            {"op": "I", "id": 2, "name": "Bob", "status": "active", "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_cdc_to_scd1(cdc, ["id"], "ts")
        result_df = result.execute()

        # Should have 2 unique keys
        assert len(result_df) == 2

        # id=1 should have latest values
        values = SCD1Assertions.assert_latest_values(
            result_df,
            keys=["id"],
            order_by="ts",
            expected_values={
                1: {"name": "Alice Updated", "status": "inactive"},
                2: {"name": "Bob", "status": "active"},
            }
        )
        assert values.passed, values.message

    def test_deletes_excluded_from_scd1(self, con):
        """Deleted records not in SCD1 output."""
        cdc = create_memtable([
            {"op": "I", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
            {"op": "I", "id": 2, "name": "Bob", "ts": datetime(2025, 1, 10)},
            {"op": "D", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 15)},  # Delete
        ])

        result = simulate_cdc_to_scd1(cdc, ["id"], "ts")
        result_df = result.execute()

        # Only id=2 should remain (id=1 deleted)
        assert len(result_df) == 1
        assert result_df.iloc[0]["id"] == 2

    def test_multiple_updates_same_key(self, con):
        """Multiple updates to same key keeps only latest."""
        cdc = create_memtable([
            {"op": "I", "id": 1, "name": "V1", "ts": datetime(2025, 1, 10)},
            {"op": "U", "id": 1, "name": "V2", "ts": datetime(2025, 1, 11)},
            {"op": "U", "id": 1, "name": "V3", "ts": datetime(2025, 1, 12)},
            {"op": "U", "id": 1, "name": "V4", "ts": datetime(2025, 1, 13)},
        ])

        result = simulate_cdc_to_scd1(cdc, ["id"], "ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "V4"


class TestCDCToSCD2:
    """Test CDC -> STATE (FULL_HISTORY/SCD2) pattern."""

    def test_insert_only_creates_history(self, con):
        """CDC inserts create SCD2 history structure."""
        cdc = create_memtable([
            {"op": "I", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_cdc_to_scd2(cdc, ["id"], "ts")
        result_df = result.execute()

        has_cols = SCD2Assertions.assert_has_scd2_columns(result_df)
        assert has_cols.passed, has_cols.message

        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed, one_current.message

    def test_all_cdc_operations_in_history(self, con):
        """All CDC operations (I/U/D) preserved in SCD2 history."""
        cdc = create_memtable([
            {"op": "I", "id": 1, "name": "V1", "ts": datetime(2025, 1, 10)},
            {"op": "U", "id": 1, "name": "V2", "ts": datetime(2025, 1, 15)},
            {"op": "U", "id": 1, "name": "V3", "ts": datetime(2025, 1, 20)},
        ])

        result = simulate_cdc_to_scd2(cdc, ["id"], "ts")
        result_df = result.execute()

        # All 3 operations preserved
        history = SCD2Assertions.assert_history_preserved(
            result_df,
            keys=["id"],
            expected_counts={1: 3}
        )
        assert history.passed, history.message

    def test_effective_dates_follow_cdc_order(self, con):
        """Effective dates follow CDC operation order."""
        cdc = create_memtable([
            {"op": "I", "id": 1, "name": "Insert", "ts": datetime(2025, 1, 10)},
            {"op": "U", "id": 1, "name": "Update", "ts": datetime(2025, 1, 15)},
        ])

        result = simulate_cdc_to_scd2(cdc, ["id"], "ts")
        result_df = result.execute()

        # Check effective_from equals ts
        equals_ts = SCD2Assertions.assert_effective_from_equals_ts(result_df, "ts")
        assert equals_ts.passed, equals_ts.message

        # Check contiguous dates
        contiguous = SCD2Assertions.assert_effective_dates_contiguous(result_df, ["id"])
        assert contiguous.passed, contiguous.message

    def test_delete_in_history(self, con):
        """Delete operations are part of history."""
        cdc = create_memtable([
            {"op": "I", "id": 1, "name": "Created", "ts": datetime(2025, 1, 10)},
            {"op": "D", "id": 1, "name": "Created", "ts": datetime(2025, 1, 15)},  # Delete
        ])

        result = simulate_cdc_to_scd2(cdc, ["id"], "ts")
        result_df = result.execute()

        # Both operations in history
        assert len(result_df) == 2

        # Delete should be the current record
        current = result_df[result_df["is_current"] == 1]
        assert current.iloc[0]["op"] == "D"


class TestCDCComplexScenarios:
    """Test complex CDC scenarios."""

    def test_multiple_keys_mixed_operations(self, con):
        """Multiple keys with different operation patterns."""
        cdc = create_memtable([
            # Key 1: Insert -> Update
            {"op": "I", "id": 1, "name": "K1-V1", "ts": datetime(2025, 1, 10)},
            {"op": "U", "id": 1, "name": "K1-V2", "ts": datetime(2025, 1, 15)},
            # Key 2: Insert only
            {"op": "I", "id": 2, "name": "K2-V1", "ts": datetime(2025, 1, 10)},
            # Key 3: Insert -> Delete
            {"op": "I", "id": 3, "name": "K3-V1", "ts": datetime(2025, 1, 10)},
            {"op": "D", "id": 3, "name": "K3-V1", "ts": datetime(2025, 1, 15)},
        ])

        # SCD1: Key 3 should be excluded
        scd1_result = simulate_cdc_to_scd1(cdc, ["id"], "ts")
        scd1_df = scd1_result.execute()

        assert len(scd1_df) == 2  # Keys 1 and 2 only
        assert set(scd1_df["id"]) == {1, 2}

        # SCD2: All operations preserved
        scd2_result = simulate_cdc_to_scd2(cdc, ["id"], "ts")
        scd2_df = scd2_result.execute()

        history = SCD2Assertions.assert_history_preserved(
            scd2_df,
            keys=["id"],
            expected_counts={1: 2, 2: 1, 3: 2}
        )
        assert history.passed, history.message

    def test_out_of_order_cdc_events(self, con):
        """Out-of-order CDC events handled correctly."""
        cdc = create_memtable([
            {"op": "U", "id": 1, "name": "V2", "ts": datetime(2025, 1, 15)},  # Later event first
            {"op": "I", "id": 1, "name": "V1", "ts": datetime(2025, 1, 10)},  # Earlier event second
        ])

        # SCD1 should keep V2 (latest)
        scd1_result = simulate_cdc_to_scd1(cdc, ["id"], "ts")
        scd1_df = scd1_result.execute()

        assert len(scd1_df) == 1
        assert scd1_df.iloc[0]["name"] == "V2"

        # SCD2 should order properly
        scd2_result = simulate_cdc_to_scd2(cdc, ["id"], "ts")
        scd2_df = scd2_result.execute().sort_values("ts")

        assert scd2_df.iloc[0]["name"] == "V1"  # First by ts
        assert scd2_df.iloc[1]["name"] == "V2"  # Second by ts
