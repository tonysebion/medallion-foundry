"""Tests for late-arriving data handling.

Tests scenarios where data arrives out of chronological order.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_latest, build_history, filter_incremental
from tests.data_validation.assertions import SCD2Assertions


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestLateDataInSCD1:
    """Test late-arriving data in SCD1 scenarios."""

    def test_late_data_ignored_in_scd1(self, con):
        """Late-arriving data with older timestamp is ignored."""
        # Scenario: We have current state, then receive older data
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Current", "ts": datetime(2025, 1, 20)},
                {
                    "id": 1,
                    "name": "Old (late arrival)",
                    "ts": datetime(2025, 1, 10),
                },  # Late
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Current"  # Latest by ts wins

    def test_late_data_newer_than_current_wins(self, con):
        """Late data with newer timestamp overwrites current."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Old State", "ts": datetime(2025, 1, 10)},
                {
                    "id": 1,
                    "name": "Late But Newer",
                    "ts": datetime(2025, 1, 15),
                },  # Late but newer
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Late But Newer"


class TestLateDataInSCD2:
    """Test late-arriving data in SCD2 scenarios."""

    def test_late_data_creates_history_gap(self, con):
        """Late data in SCD2 inserts into history correctly."""
        # We have V1 and V3, then V2 arrives late
        df = pd.DataFrame(
            [
                {"id": 1, "version": "V1", "ts": datetime(2025, 1, 10)},
                {"id": 1, "version": "V3", "ts": datetime(2025, 1, 20)},
                {"id": 1, "version": "V2", "ts": datetime(2025, 1, 15)},  # Late arrival
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute().sort_values("ts")

        # All 3 versions in history
        assert len(result_df) == 3

        # Verify order
        assert result_df.iloc[0]["version"] == "V1"
        assert result_df.iloc[1]["version"] == "V2"
        assert result_df.iloc[2]["version"] == "V3"

        # V3 should be current
        current = result_df[result_df["is_current"] == 1]
        assert current.iloc[0]["version"] == "V3"

    def test_late_data_fixes_history_chain(self, con):
        """Late data properly links effective dates."""
        df = pd.DataFrame(
            [
                {"id": 1, "version": "V1", "ts": datetime(2025, 1, 10)},
                {"id": 1, "version": "V3", "ts": datetime(2025, 1, 20)},
                {"id": 1, "version": "V2", "ts": datetime(2025, 1, 15)},  # Late arrival
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute().sort_values("ts")

        # Check effective dates chain correctly
        contiguous = SCD2Assertions.assert_effective_dates_contiguous(result_df, ["id"])
        assert contiguous.passed, contiguous.message

    def test_multiple_late_arrivals(self, con):
        """Multiple late-arriving records handled correctly."""
        df = pd.DataFrame(
            [
                {"id": 1, "version": "V5", "ts": datetime(2025, 1, 25)},  # Current
                {
                    "id": 1,
                    "version": "V1",
                    "ts": datetime(2025, 1, 5),
                },  # Very old, late
                {"id": 1, "version": "V3", "ts": datetime(2025, 1, 15)},  # Late
                {"id": 1, "version": "V2", "ts": datetime(2025, 1, 10)},  # Late
                {"id": 1, "version": "V4", "ts": datetime(2025, 1, 20)},  # Late
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute().sort_values("ts")

        # All 5 versions in order
        assert len(result_df) == 5
        expected_versions = ["V1", "V2", "V3", "V4", "V5"]
        actual_versions = result_df["version"].tolist()
        assert actual_versions == expected_versions


class TestLateDataWithIncremental:
    """Test late data in incremental loading patterns."""

    def test_filter_incremental_excludes_late(self, con):
        """Incremental filter excludes late data by design."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "After watermark", "ts": datetime(2025, 1, 20)},
                {
                    "id": 2,
                    "name": "Before watermark (late)",
                    "ts": datetime(2025, 1, 5),
                },
                {"id": 3, "name": "At watermark", "ts": datetime(2025, 1, 10)},
            ]
        )

        t = ibis.memtable(df)
        watermark = "2025-01-10"

        result = filter_incremental(t, "ts", watermark)
        result_df = result.execute()

        # Only records after watermark
        assert len(result_df) == 1
        assert result_df.iloc[0]["id"] == 1

    def test_late_data_in_new_batch(self, con):
        """Late data arriving in new batch is filtered."""
        # Batch 1: Normal data
        pd.DataFrame(
            [
                {"id": 1, "name": "Normal", "ts": datetime(2025, 1, 15)},
            ]
        )

        # Batch 2: Contains late data
        batch2 = pd.DataFrame(
            [
                {"id": 2, "name": "New", "ts": datetime(2025, 1, 20)},
                {
                    "id": 3,
                    "name": "Late (before last watermark)",
                    "ts": datetime(2025, 1, 10),
                },
            ]
        )

        # Process batch 2 with watermark from batch 1
        t2 = ibis.memtable(batch2)
        result = filter_incremental(t2, "ts", "2025-01-15")
        result_df = result.execute()

        # Only new data after watermark
        assert len(result_df) == 1
        assert result_df.iloc[0]["id"] == 2


class TestLateDataMultipleKeys:
    """Test late data across multiple entity keys."""

    def test_late_data_different_keys(self, con):
        """Late data for different keys handled independently."""
        df = pd.DataFrame(
            [
                # Key 1: Normal order
                {"id": 1, "version": "1-V1", "ts": datetime(2025, 1, 10)},
                {"id": 1, "version": "1-V2", "ts": datetime(2025, 1, 15)},
                # Key 2: Late arrival
                {"id": 2, "version": "2-V2", "ts": datetime(2025, 1, 15)},
                {"id": 2, "version": "2-V1", "ts": datetime(2025, 1, 10)},  # Late
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # Each key has 2 versions
        id1_df = result_df[result_df["id"] == 1].sort_values("ts")
        assert len(id1_df) == 2
        assert id1_df.iloc[0]["version"] == "1-V1"

        id2_df = result_df[result_df["id"] == 2].sort_values("ts")
        assert len(id2_df) == 2
        assert id2_df.iloc[0]["version"] == "2-V1"

    def test_late_data_some_keys_only(self, con):
        """Late data affects only specific keys."""
        df = pd.DataFrame(
            [
                # Key 1: In order
                {"id": 1, "version": "V1", "ts": datetime(2025, 1, 10)},
                {"id": 1, "version": "V2", "ts": datetime(2025, 1, 15)},
                # Key 2: Has late data
                {"id": 2, "version": "V2", "ts": datetime(2025, 1, 15)},
                {"id": 2, "version": "V1-LATE", "ts": datetime(2025, 1, 5)},  # Late!
                # Key 3: Single version
                {"id": 3, "version": "V1", "ts": datetime(2025, 1, 12)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        # All 3 keys present
        assert len(result_df) == 3

        # Each key has latest version
        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["version"] == "V2"

        id2 = result_df[result_df["id"] == 2].iloc[0]
        assert id2["version"] == "V2"  # Late V1 ignored
