"""Tests for INCREMENTAL_APPEND -> EVENT pattern.

Events are immutable, append-only records. This pattern tests
that events are properly accumulated with only exact duplicates removed.
"""

from __future__ import annotations

from datetime import datetime


from tests.data_validation.assertions import EventAssertions
from tests.combination.helpers import simulate_incremental_to_event
from tests.combination.conftest import create_memtable


class TestIncrementalToEvent:
    """Test INCREMENTAL_APPEND -> EVENT pattern."""

    def test_single_batch_preserved(self, con):
        """Single batch events preserved as-is."""
        batch = create_memtable([
            {"event_id": "E001", "user_id": "U1", "type": "click", "ts": datetime(2025, 1, 10)},
            {"event_id": "E002", "user_id": "U1", "type": "view", "ts": datetime(2025, 1, 10)},
            {"event_id": "E003", "user_id": "U2", "type": "click", "ts": datetime(2025, 1, 11)},
        ])

        result = simulate_incremental_to_event([batch])
        result_df = result.execute()

        assert len(result_df) == 3

        no_scd2 = EventAssertions.assert_no_scd2_columns(result_df)
        assert no_scd2.passed, no_scd2.message

    def test_multiple_batches_accumulated(self, con):
        """Multiple batches accumulate all events."""
        batch_t0 = create_memtable([
            {"event_id": "E001", "user_id": "U1", "ts": datetime(2025, 1, 10)},
            {"event_id": "E002", "user_id": "U1", "ts": datetime(2025, 1, 10)},
        ])

        batch_t1 = create_memtable([
            {"event_id": "E003", "user_id": "U2", "ts": datetime(2025, 1, 11)},
            {"event_id": "E004", "user_id": "U2", "ts": datetime(2025, 1, 11)},
        ])

        result = simulate_incremental_to_event([batch_t0, batch_t1])
        result_df = result.execute()

        assert len(result_df) == 4

    def test_exact_duplicates_removed(self, con):
        """Exact duplicate events are removed."""
        batch = create_memtable([
            {"event_id": "E001", "user_id": "U1", "data": "A", "ts": datetime(2025, 1, 10)},
            {"event_id": "E001", "user_id": "U1", "data": "A", "ts": datetime(2025, 1, 10)},  # Exact dup
            {"event_id": "E002", "user_id": "U1", "data": "B", "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_incremental_to_event([batch])
        result_df = result.execute()

        assert len(result_df) == 2  # One duplicate removed

        no_dups = EventAssertions.assert_no_exact_duplicates(result_df)
        assert no_dups.passed, no_dups.message

    def test_near_duplicates_preserved(self, con):
        """Near duplicates (same event_id, different data) are preserved."""
        batch = create_memtable([
            {"event_id": "E001", "user_id": "U1", "data": "A", "ts": datetime(2025, 1, 10)},
            {"event_id": "E001", "user_id": "U1", "data": "B", "ts": datetime(2025, 1, 10)},  # Different data
        ])

        result = simulate_incremental_to_event([batch])
        result_df = result.execute()

        # Both preserved (not exact duplicates)
        assert len(result_df) == 2

    def test_no_scd2_columns(self, con):
        """Event output has no SCD2 columns."""
        batch = create_memtable([
            {"event_id": "E001", "ts": datetime(2025, 1, 10)},
        ])

        result = simulate_incremental_to_event([batch])
        result_df = result.execute()

        no_scd2 = EventAssertions.assert_no_scd2_columns(result_df)
        assert no_scd2.passed, no_scd2.message


class TestEventImmutability:
    """Test that events are treated as immutable between batches."""

    def test_events_not_modified(self, con):
        """Existing events remain unchanged when new events added."""
        batch_t0 = create_memtable([
            {"event_id": "E001", "data": "original"},
            {"event_id": "E002", "data": "original"},
        ])

        batch_t1 = create_memtable([
            {"event_id": "E003", "data": "new"},  # New event
        ])

        result_t0 = simulate_incremental_to_event([batch_t0])
        result_t0_df = result_t0.execute()

        result_t1 = simulate_incremental_to_event([batch_t0, batch_t1])
        result_t1_df = result_t1.execute()

        # Original events preserved
        immutable = EventAssertions.assert_records_immutable(
            result_t0_df, result_t1_df, "event_id"
        )
        assert immutable.passed, immutable.message

    def test_append_only_behavior(self, con):
        """Events are append-only between batches."""
        batch_t0 = create_memtable([
            {"event_id": "E001"},
            {"event_id": "E002"},
        ])

        batch_t1 = create_memtable([
            {"event_id": "E003"},  # New
        ])

        result_t0 = simulate_incremental_to_event([batch_t0])
        result_t0_df = result_t0.execute()

        result_t1 = simulate_incremental_to_event([batch_t0, batch_t1])
        result_t1_df = result_t1.execute()

        append_only = EventAssertions.assert_append_only(
            result_t0_df, result_t1_df, "event_id"
        )
        assert append_only.passed, append_only.message


class TestEventWithDuplicateAcrossBatches:
    """Test event handling when duplicates span multiple batches."""

    def test_duplicate_across_batches_removed(self, con):
        """Same exact event in multiple batches is deduplicated."""
        batch_t0 = create_memtable([
            {"event_id": "E001", "data": "same", "ts": datetime(2025, 1, 10)},
        ])

        batch_t1 = create_memtable([
            {"event_id": "E001", "data": "same", "ts": datetime(2025, 1, 10)},  # Exact duplicate
            {"event_id": "E002", "data": "new", "ts": datetime(2025, 1, 11)},
        ])

        result = simulate_incremental_to_event([batch_t0, batch_t1])
        result_df = result.execute()

        # Should have 2 unique events
        assert len(result_df) == 2

        no_dups = EventAssertions.assert_no_exact_duplicates(result_df)
        assert no_dups.passed, no_dups.message
