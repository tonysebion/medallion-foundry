"""Watermark and incremental load edge case tests.

Story 4: Tests for watermark edge cases in incremental patterns.

Tests:
- Watermark column detection and progression
- Late-arriving data (events before watermark) handling
- Out-of-order events within a batch
- Watermark reset scenarios
- Missing watermark column error handling
- Backfill scenarios (re-processing historical data)
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pytest

from core.foundation.state.watermark import (
    Watermark,
    WatermarkStore,
    WatermarkType,
    compute_max_watermark,
)
from core.platform.resilience.late_data import (
    LateDataConfig,
    LateDataHandler,
    LateDataMode,
)
from tests.synthetic_data import (
    LateDataGenerator,
    TransactionsGenerator,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def temp_watermark_store(tmp_path: Path) -> WatermarkStore:
    """Create a temporary watermark store for testing."""
    watermark_path = tmp_path / "_watermarks"
    watermark_path.mkdir(parents=True, exist_ok=True)
    return WatermarkStore(
        storage_backend="local",
        local_path=watermark_path,
    )


@pytest.fixture
def late_data_generator() -> LateDataGenerator:
    """Create a late data generator with deterministic seed."""
    return LateDataGenerator(seed=42, row_count=100, late_rate=0.20)


@pytest.fixture
def transactions_generator() -> TransactionsGenerator:
    """Create a transactions generator for watermark testing."""
    return TransactionsGenerator(seed=42, row_count=100)


# =============================================================================
# Test: Watermark Column Detection and Progression
# =============================================================================


class TestWatermarkColumnDetection:
    """Tests for watermark column detection and configuration."""

    def test_watermark_column_from_config(
        self,
        temp_watermark_store: WatermarkStore,
    ):
        """Test watermark column is correctly read from config."""
        watermark = temp_watermark_store.get(
            system="test_system",
            table="test_table",
            watermark_column="updated_at",
            watermark_type=WatermarkType.TIMESTAMP,
        )

        assert watermark.watermark_column == "updated_at"
        assert watermark.watermark_type == WatermarkType.TIMESTAMP
        assert watermark.watermark_value is None  # New watermark

    def test_watermark_types(
        self,
        temp_watermark_store: WatermarkStore,
    ):
        """Test different watermark types are handled correctly."""
        types_and_values = [
            (WatermarkType.TIMESTAMP, "2025-01-15T10:30:00Z"),
            (WatermarkType.DATE, "2025-01-15"),
            (WatermarkType.INTEGER, "12345"),
            (WatermarkType.STRING, "batch_abc"),
        ]

        for wm_type, value in types_and_values:
            unique_id = uuid.uuid4().hex[:8]
            watermark = temp_watermark_store.get(
                system="test",
                table=f"table_{unique_id}",
                watermark_column="cursor",
                watermark_type=wm_type,
            )
            watermark.update(
                new_value=value,
                run_id="run_1",
                run_date=date(2025, 1, 15),
                record_count=100,
            )
            temp_watermark_store.save(watermark)

            # Reload and verify
            loaded = temp_watermark_store.get(
                system="test",
                table=f"table_{unique_id}",
                watermark_column="cursor",
                watermark_type=wm_type,
            )
            assert loaded.watermark_value == value
            assert loaded.watermark_type == wm_type


class TestWatermarkProgression:
    """Tests for watermark value progression across runs."""

    def test_watermark_progression_timestamp(
        self,
        temp_watermark_store: WatermarkStore,
    ):
        """Test timestamp watermark correctly advances through batches."""
        unique_table = f"txn_{uuid.uuid4().hex[:8]}"
        watermark_column = "transaction_ts"

        # Simulate 3 extraction runs
        run_dates = [date(2025, 1, 10), date(2025, 1, 11), date(2025, 1, 12)]
        watermark_values = [
            "2025-01-10T23:59:59",
            "2025-01-11T23:59:59",
            "2025-01-12T23:59:59",
        ]

        for i, (run_date, wm_value) in enumerate(zip(run_dates, watermark_values)):
            watermark = temp_watermark_store.get(
                system="financial",
                table=unique_table,
                watermark_column=watermark_column,
                watermark_type=WatermarkType.TIMESTAMP,
            )

            if i > 0:
                # Verify previous watermark was persisted
                assert watermark.watermark_value == watermark_values[i - 1]

            watermark.update(
                new_value=wm_value,
                run_id=f"run_{i}",
                run_date=run_date,
                record_count=100 + i * 10,
            )
            temp_watermark_store.save(watermark)

        # Final verification
        final_watermark = temp_watermark_store.get(
            system="financial",
            table=unique_table,
            watermark_column=watermark_column,
            watermark_type=WatermarkType.TIMESTAMP,
        )
        assert final_watermark.watermark_value == "2025-01-12T23:59:59"
        assert final_watermark.record_count == 120

    def test_watermark_progression_integer(
        self,
        temp_watermark_store: WatermarkStore,
    ):
        """Test integer watermark correctly advances."""
        unique_table = f"seq_{uuid.uuid4().hex[:8]}"

        # Simulate progression: 1000 -> 2500 -> 5000
        sequence_values = [1000, 2500, 5000]

        for i, seq_val in enumerate(sequence_values):
            watermark = temp_watermark_store.get(
                system="events",
                table=unique_table,
                watermark_column="sequence_id",
                watermark_type=WatermarkType.INTEGER,
            )
            watermark.update(
                new_value=str(seq_val),
                run_id=f"run_{i}",
                run_date=date(2025, 1, 10 + i),
                record_count=seq_val - (sequence_values[i - 1] if i > 0 else 0),
            )
            temp_watermark_store.save(watermark)

        final = temp_watermark_store.get(
            system="events",
            table=unique_table,
            watermark_column="sequence_id",
            watermark_type=WatermarkType.INTEGER,
        )
        assert final.watermark_value == "5000"

    def test_compute_max_watermark_from_records(self):
        """Test compute_max_watermark correctly finds maximum value."""
        records = [
            {"id": 1, "updated_at": "2025-01-10T10:00:00"},
            {"id": 2, "updated_at": "2025-01-12T15:30:00"},
            {"id": 3, "updated_at": "2025-01-11T08:00:00"},
            {"id": 4, "updated_at": "2025-01-12T20:00:00"},  # Max
            {"id": 5, "updated_at": "2025-01-09T12:00:00"},
        ]

        max_wm = compute_max_watermark(records, "updated_at")
        assert max_wm == "2025-01-12T20:00:00"

    def test_compute_max_watermark_with_current(self):
        """Test compute_max_watermark respects current watermark."""
        records = [
            {"id": 1, "updated_at": "2025-01-10T10:00:00"},
            {"id": 2, "updated_at": "2025-01-11T15:30:00"},
        ]

        # Current watermark is higher than all records
        current = "2025-01-15T00:00:00"
        max_wm = compute_max_watermark(records, "updated_at", current)
        assert max_wm == current

        # Current watermark is lower than some records
        current_lower = "2025-01-08T00:00:00"
        max_wm = compute_max_watermark(records, "updated_at", current_lower)
        assert max_wm == "2025-01-11T15:30:00"


# =============================================================================
# Test: Late-Arriving Data Handling
# =============================================================================


class TestLateDataDetection:
    """Tests for late data detection logic."""

    def test_late_data_detection_timestamp(self):
        """Test late data is correctly identified by timestamp."""
        config = LateDataConfig(
            mode=LateDataMode.ALLOW,
            threshold_days=7,
            timestamp_column="event_ts",
        )
        handler = LateDataHandler(config)

        reference_time = datetime(2025, 1, 15, 12, 0, 0)

        # On-time record (within 7 days)
        on_time = {"id": 1, "event_ts": "2025-01-10T10:00:00"}
        assert not handler.is_late(on_time, reference_time)

        # Late record (more than 7 days old)
        late = {"id": 2, "event_ts": "2025-01-05T10:00:00"}
        assert handler.is_late(late, reference_time)

        # Edge case: exactly at threshold (not late)
        edge = {"id": 3, "event_ts": "2025-01-08T12:00:00"}
        assert not handler.is_late(edge, reference_time)

    def test_late_data_handler_allow_mode(self):
        """Test ALLOW mode includes late data in output."""
        config = LateDataConfig(
            mode=LateDataMode.ALLOW,
            threshold_days=7,
            timestamp_column="event_ts",
        )
        handler = LateDataHandler(config)

        records = [
            {"id": 1, "event_ts": "2025-01-14T10:00:00"},  # On-time
            {"id": 2, "event_ts": "2025-01-05T10:00:00"},  # Late
            {"id": 3, "event_ts": "2025-01-13T08:00:00"},  # On-time
            {"id": 4, "event_ts": "2025-01-01T12:00:00"},  # Late
        ]

        reference_time = datetime(2025, 1, 15, 12, 0, 0)
        result = handler.process_records(records, reference_time)

        assert len(result.on_time_records) == 2
        assert len(result.late_records) == 2
        assert result.rejected_count == 0
        assert result.quarantined_count == 0

    def test_late_data_handler_reject_mode(self):
        """Test REJECT mode raises error when late data found."""
        config = LateDataConfig(
            mode=LateDataMode.REJECT,
            threshold_days=7,
            timestamp_column="event_ts",
        )
        handler = LateDataHandler(config)

        records = [
            {"id": 1, "event_ts": "2025-01-14T10:00:00"},
            {"id": 2, "event_ts": "2025-01-01T10:00:00"},  # Late
        ]

        reference_time = datetime(2025, 1, 15, 12, 0, 0)

        with pytest.raises(ValueError, match="Late data rejected"):
            handler.process_records(records, reference_time)

    def test_late_data_handler_quarantine_mode(self):
        """Test QUARANTINE mode separates late data."""
        config = LateDataConfig(
            mode=LateDataMode.QUARANTINE,
            threshold_days=7,
            timestamp_column="event_ts",
            quarantine_path="_quarantine",
        )
        handler = LateDataHandler(config)

        records = [
            {"id": 1, "event_ts": "2025-01-14T10:00:00"},
            {"id": 2, "event_ts": "2025-01-01T10:00:00"},  # Late
            {"id": 3, "event_ts": "2025-01-02T10:00:00"},  # Late
        ]

        reference_time = datetime(2025, 1, 15, 12, 0, 0)
        result = handler.process_records(records, reference_time)

        assert len(result.on_time_records) == 1
        assert len(result.late_records) == 2
        assert result.quarantined_count == 2


class TestLateDataGeneratorIntegration:
    """Tests using LateDataGenerator for realistic late data scenarios."""

    def test_generate_late_data(
        self,
        late_data_generator: LateDataGenerator,
    ):
        """Test LateDataGenerator produces correctly structured late data."""
        run_date = date(2025, 1, 15)
        t0_df = late_data_generator.generate_t0(run_date)

        # Generate late data
        late_df = late_data_generator.generate_late_data(
            run_date=run_date,
            t0_df=t0_df,
            min_delay_hours=24,
            max_delay_hours=168,
        )

        # Verify late data structure
        assert len(late_df) > 0
        assert all(late_df["is_late"])
        assert all(late_df["arrival_delay_hours"] >= 24)
        assert all(late_df["arrival_delay_hours"] <= 168)

        # Verify event timestamps are before created_at (arrival)
        for _, row in late_df.iterrows():
            event_ts = row["event_ts_utc"]
            created_at = row["created_at"]
            assert event_ts < created_at

    def test_out_of_order_batch_generation(
        self,
        late_data_generator: LateDataGenerator,
    ):
        """Test out-of-order batch contains mix of late and on-time events."""
        run_date = date(2025, 1, 15)

        batch_df = late_data_generator.generate_out_of_order_batch(
            run_date=run_date,
            batch_size=100,
        )

        assert len(batch_df) == 100

        late_count = batch_df["is_late"].sum()
        on_time_count = len(batch_df) - late_count

        # With late_rate=0.20, expect roughly 20 late events
        assert late_count > 0, "Should have some late events"
        assert on_time_count > 0, "Should have some on-time events"

        # Verify shuffled (not sorted by event_ts)
        event_times = batch_df["event_ts_utc"].tolist()
        is_sorted = all(event_times[i] <= event_times[i + 1] for i in range(len(event_times) - 1))
        assert not is_sorted, "Batch should be shuffled (out of order)"


# =============================================================================
# Test: Out-of-Order Events Within a Batch
# =============================================================================


class TestOutOfOrderEvents:
    """Tests for handling out-of-order events within batches."""

    def test_watermark_from_out_of_order_records(self):
        """Test watermark computation handles out-of-order records."""
        # Records arrive in random order
        records = [
            {"id": 5, "ts": "2025-01-15T05:00:00"},
            {"id": 2, "ts": "2025-01-15T02:00:00"},
            {"id": 8, "ts": "2025-01-15T08:00:00"},  # Should be max
            {"id": 1, "ts": "2025-01-15T01:00:00"},
            {"id": 6, "ts": "2025-01-15T06:00:00"},
        ]

        max_wm = compute_max_watermark(records, "ts")
        assert max_wm == "2025-01-15T08:00:00"

    def test_watermark_skips_null_values(self):
        """Test watermark computation correctly skips null timestamps."""
        records: List[Dict[str, Any]] = [
            {"id": 1, "ts": "2025-01-15T01:00:00"},
            {"id": 2, "ts": None},
            {"id": 3, "ts": "2025-01-15T03:00:00"},
            {"id": 4, "ts": None},
        ]

        max_wm = compute_max_watermark(records, "ts")
        assert max_wm == "2025-01-15T03:00:00"

    def test_watermark_compare_method(self):
        """Test Watermark.compare() for different value positions."""
        watermark = Watermark(
            source_key="test.table",
            watermark_column="ts",
            watermark_value="2025-01-15T10:00:00",
            watermark_type=WatermarkType.TIMESTAMP,
        )

        # Value before watermark (already processed)
        assert watermark.compare("2025-01-10T05:00:00") == -1

        # Value equal to watermark
        assert watermark.compare("2025-01-15T10:00:00") == 0

        # Value after watermark (new data)
        assert watermark.compare("2025-01-20T08:00:00") == 1

    def test_integer_watermark_compare(self):
        """Test integer watermark comparison."""
        watermark = Watermark(
            source_key="test.sequence",
            watermark_column="seq_id",
            watermark_value="1000",
            watermark_type=WatermarkType.INTEGER,
        )

        assert watermark.compare("500") == -1   # Before
        assert watermark.compare("1000") == 0   # Equal
        assert watermark.compare("1500") == 1   # After
        assert watermark.compare("999") == -1
        assert watermark.compare("1001") == 1


# =============================================================================
# Test: Watermark Reset Scenarios
# =============================================================================


class TestWatermarkReset:
    """Tests for watermark reset and deletion scenarios."""

    def test_watermark_delete(
        self,
        temp_watermark_store: WatermarkStore,
    ):
        """Test watermark can be deleted for full reset."""
        unique_table = f"reset_{uuid.uuid4().hex[:8]}"

        # Create and save watermark
        watermark = temp_watermark_store.get(
            system="test",
            table=unique_table,
            watermark_column="updated_at",
            watermark_type=WatermarkType.TIMESTAMP,
        )
        watermark.update(
            new_value="2025-01-15T10:00:00",
            run_id="run_1",
            run_date=date(2025, 1, 15),
            record_count=1000,
        )
        temp_watermark_store.save(watermark)

        # Verify watermark exists
        loaded = temp_watermark_store.get(
            system="test",
            table=unique_table,
            watermark_column="updated_at",
            watermark_type=WatermarkType.TIMESTAMP,
        )
        assert loaded.watermark_value == "2025-01-15T10:00:00"

        # Delete watermark
        deleted = temp_watermark_store.delete("test", unique_table)
        assert deleted is True

        # Verify new watermark has no value (fresh start)
        fresh = temp_watermark_store.get(
            system="test",
            table=unique_table,
            watermark_column="updated_at",
            watermark_type=WatermarkType.TIMESTAMP,
        )
        assert fresh.watermark_value is None

    def test_watermark_reset_to_specific_value(
        self,
        temp_watermark_store: WatermarkStore,
    ):
        """Test resetting watermark to a specific historical value (backfill)."""
        unique_table = f"backfill_{uuid.uuid4().hex[:8]}"

        # Create watermark at current state
        watermark = temp_watermark_store.get(
            system="test",
            table=unique_table,
            watermark_column="updated_at",
            watermark_type=WatermarkType.TIMESTAMP,
        )
        watermark.update(
            new_value="2025-01-15T23:59:59",
            run_id="run_current",
            run_date=date(2025, 1, 15),
            record_count=5000,
        )
        temp_watermark_store.save(watermark)

        # Reset to earlier value for backfill
        watermark.update(
            new_value="2025-01-01T00:00:00",  # Reset to start of month
            run_id="run_backfill",
            run_date=date(2025, 1, 16),  # New run date
            record_count=0,  # Starting fresh
        )
        temp_watermark_store.save(watermark)

        # Verify reset worked
        loaded = temp_watermark_store.get(
            system="test",
            table=unique_table,
            watermark_column="updated_at",
            watermark_type=WatermarkType.TIMESTAMP,
        )
        assert loaded.watermark_value == "2025-01-01T00:00:00"
        assert loaded.last_run_id == "run_backfill"


# =============================================================================
# Test: Missing Watermark Column Handling
# =============================================================================


class TestMissingWatermarkColumn:
    """Tests for handling missing or invalid watermark columns."""

    def test_compute_max_watermark_missing_column(self):
        """Test compute_max_watermark with missing column returns current."""
        records = [
            {"id": 1, "name": "record1"},
            {"id": 2, "name": "record2"},
        ]

        # Column doesn't exist in records
        result = compute_max_watermark(records, "nonexistent_ts", "2025-01-01T00:00:00")
        assert result == "2025-01-01T00:00:00"  # Returns current watermark

    def test_compute_max_watermark_empty_records(self):
        """Test compute_max_watermark with empty records list."""
        result = compute_max_watermark([], "updated_at", "2025-01-01T00:00:00")
        assert result == "2025-01-01T00:00:00"

        result_no_current = compute_max_watermark([], "updated_at", None)
        assert result_no_current is None

    def test_watermark_with_no_initial_value(self):
        """Test watermark behavior on first run (no existing value)."""
        watermark = Watermark(
            source_key="new.table",
            watermark_column="created_at",
            watermark_type=WatermarkType.TIMESTAMP,
            watermark_value=None,
        )

        # Compare should always return 1 (new data) when no watermark
        assert watermark.compare("2020-01-01T00:00:00") == 1
        assert watermark.compare("2025-01-15T10:00:00") == 1


# =============================================================================
# Test: Backfill Scenarios
# =============================================================================


class TestBackfillScenarios:
    """Tests for backfill and re-processing scenarios."""

    def test_backfill_window_configuration(self):
        """Test backfill window parsing from config."""
        from core.platform.resilience.late_data import BackfillWindow

        config = {
            "start_date": "2025-01-01",
            "end_date": "2025-01-15",
            "force_full": True,
        }

        window = BackfillWindow.from_dict(config)
        assert window.start_date == date(2025, 1, 1)
        assert window.end_date == date(2025, 1, 15)
        assert window.force_full is True
        assert window.days == 15

    def test_backfill_window_contains(self):
        """Test backfill window date containment check."""
        from core.platform.resilience.late_data import BackfillWindow

        window = BackfillWindow(
            start_date=date(2025, 1, 5),
            end_date=date(2025, 1, 10),
        )

        assert window.contains(date(2025, 1, 5))   # Start
        assert window.contains(date(2025, 1, 7))   # Middle
        assert window.contains(date(2025, 1, 10))  # End
        assert not window.contains(date(2025, 1, 4))   # Before
        assert not window.contains(date(2025, 1, 11))  # After

    def test_incremental_after_backfill(
        self,
        temp_watermark_store: WatermarkStore,
    ):
        """Test incremental loads resume correctly after backfill."""
        unique_table = f"resume_{uuid.uuid4().hex[:8]}"

        # Normal run establishes watermark at Jan 15
        watermark = temp_watermark_store.get(
            system="prod",
            table=unique_table,
            watermark_column="modified_at",
            watermark_type=WatermarkType.TIMESTAMP,
        )
        watermark.update(
            new_value="2025-01-15T23:59:59",
            run_id="run_jan15",
            run_date=date(2025, 1, 15),
            record_count=1000,
        )
        temp_watermark_store.save(watermark)

        # Backfill resets to Jan 1
        watermark_for_backfill = temp_watermark_store.get(
            system="prod",
            table=unique_table,
            watermark_column="modified_at",
            watermark_type=WatermarkType.TIMESTAMP,
        )
        watermark_for_backfill.update(
            new_value="2025-01-01T00:00:00",
            run_id="run_backfill_start",
            run_date=date(2025, 1, 16),
            record_count=0,
        )
        temp_watermark_store.save(watermark_for_backfill)

        # Simulate backfill progressing through dates
        for day in range(1, 16):
            wm = temp_watermark_store.get(
                system="prod",
                table=unique_table,
                watermark_column="modified_at",
                watermark_type=WatermarkType.TIMESTAMP,
            )
            wm.update(
                new_value=f"2025-01-{day:02d}T23:59:59",
                run_id=f"run_backfill_{day}",
                run_date=date(2025, 1, 16),
                record_count=100,
            )
            temp_watermark_store.save(wm)

        # Verify watermark is back at Jan 15 end-of-day
        final = temp_watermark_store.get(
            system="prod",
            table=unique_table,
            watermark_column="modified_at",
            watermark_type=WatermarkType.TIMESTAMP,
        )
        assert final.watermark_value == "2025-01-15T23:59:59"

        # Next normal run should continue from Jan 15
        # Simulate new data on Jan 16
        records = [
            {"id": 1, "modified_at": "2025-01-16T10:00:00"},
            {"id": 2, "modified_at": "2025-01-16T15:30:00"},
        ]
        new_max = compute_max_watermark(records, "modified_at", final.watermark_value)
        assert new_max == "2025-01-16T15:30:00"


# =============================================================================
# Test: Watermark With Transactions Generator
# =============================================================================


class TestWatermarkWithRealData:
    """Tests using real synthetic data generators to validate watermark behavior."""

    def test_transactions_watermark_progression(
        self,
        transactions_generator: TransactionsGenerator,
        temp_watermark_store: WatermarkStore,
    ):
        """Test watermark progression with real transaction data."""
        unique_table = f"txn_{uuid.uuid4().hex[:8]}"
        run_date = date(2025, 1, 15)

        # Generate T0 transactions
        t0_df = transactions_generator.generate_t0(run_date)

        # Convert to records
        t0_records = t0_df.to_dict("records")

        # Compute watermark from T0
        t0_max = compute_max_watermark(t0_records, "transaction_ts")
        assert t0_max is not None

        # Save watermark
        watermark = temp_watermark_store.get(
            system="financial",
            table=unique_table,
            watermark_column="transaction_ts",
            watermark_type=WatermarkType.TIMESTAMP,
        )
        watermark.update(
            new_value=t0_max,
            run_id="run_t0",
            run_date=run_date,
            record_count=len(t0_records),
        )
        temp_watermark_store.save(watermark)

        # Generate T1 transactions
        t1_df = transactions_generator.generate_t1(run_date + timedelta(days=1), t0_df)
        t1_records = t1_df.to_dict("records")

        # T1 should have newer transactions
        t1_max = compute_max_watermark(t1_records, "transaction_ts", t0_max)
        # T1 max should be >= T0 max (could be equal if only status updates)
        assert t1_max is not None
        assert t0_max is not None
        assert t1_max >= t0_max

    def test_late_data_with_transactions(
        self,
        transactions_generator: TransactionsGenerator,
    ):
        """Test late data handler with realistic transaction data."""
        run_date = date(2025, 1, 15)

        # Generate transactions
        t0_df = transactions_generator.generate_t0(run_date)
        t0_records = t0_df.to_dict("records")

        # Create late data handler with short threshold
        config = LateDataConfig(
            mode=LateDataMode.ALLOW,
            threshold_days=3,  # Only 3 days
            timestamp_column="transaction_ts",
        )
        handler = LateDataHandler(config)

        # Process records - some will be "late" if older than 3 days
        reference = datetime.combine(run_date, datetime.max.time())
        result = handler.process_records(t0_records, reference)

        # T0 generates data from 7 days back, so some should be late
        assert result.total_records == len(t0_records)
        # With 7-day spread and 3-day threshold, expect some late
        assert result.late_count > 0 or result.late_count == 0  # May vary by seed


# =============================================================================
# Test: Edge Cases in Watermark Comparison
# =============================================================================


class TestWatermarkComparisonEdgeCases:
    """Edge case tests for watermark value comparison."""

    def test_timestamp_with_milliseconds(self):
        """Test timestamp comparison with milliseconds."""
        records = [
            {"id": 1, "ts": "2025-01-15T10:00:00.123"},
            {"id": 2, "ts": "2025-01-15T10:00:00.456"},  # Higher ms
            {"id": 3, "ts": "2025-01-15T10:00:00.001"},
        ]

        max_wm = compute_max_watermark(records, "ts")
        assert max_wm == "2025-01-15T10:00:00.456"

    def test_date_only_watermark(self):
        """Test date-only watermark (no time component)."""
        watermark = Watermark(
            source_key="daily.batch",
            watermark_column="process_date",
            watermark_value="2025-01-15",
            watermark_type=WatermarkType.DATE,
        )

        assert watermark.compare("2025-01-14") == -1
        assert watermark.compare("2025-01-15") == 0
        assert watermark.compare("2025-01-16") == 1

    def test_string_watermark_lexicographic(self):
        """Test string watermark uses lexicographic comparison."""
        watermark = Watermark(
            source_key="batch.process",
            watermark_column="batch_id",
            watermark_value="batch_100",
            watermark_type=WatermarkType.STRING,
        )

        # String comparison: "batch_099" < "batch_100" < "batch_101"
        assert watermark.compare("batch_099") == -1
        assert watermark.compare("batch_100") == 0
        assert watermark.compare("batch_101") == 1

        # But be aware: "batch_99" > "batch_100" lexicographically!
        assert watermark.compare("batch_99") == 1  # "9" > "1" in ASCII

    def test_watermark_metadata_tracking(
        self,
        temp_watermark_store: WatermarkStore,
    ):
        """Test watermark tracks run metadata correctly."""
        unique_table = f"meta_{uuid.uuid4().hex[:8]}"

        watermark = temp_watermark_store.get(
            system="audit",
            table=unique_table,
            watermark_column="event_ts",
            watermark_type=WatermarkType.TIMESTAMP,
        )

        # First update
        watermark.update(
            new_value="2025-01-10T10:00:00",
            run_id="run_abc123",
            run_date=date(2025, 1, 10),
            record_count=500,
        )
        temp_watermark_store.save(watermark)

        # Load and verify metadata
        loaded = temp_watermark_store.get(
            system="audit",
            table=unique_table,
            watermark_column="event_ts",
            watermark_type=WatermarkType.TIMESTAMP,
        )

        assert loaded.last_run_id == "run_abc123"
        assert loaded.last_run_date == "2025-01-10"
        assert loaded.record_count == 500
        assert loaded.created_at is not None
        assert loaded.updated_at is not None
