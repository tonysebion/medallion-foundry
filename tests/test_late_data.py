"""Tests for late data handling.

These tests verify the late data handling patterns used across the framework:
- LateDataMode: Enum with RichEnumMixin pattern
- LateDataConfig: Configuration with from_dict/to_dict
- LateDataHandler: Late record detection and processing
- BackfillWindow: Backfill window configuration
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from core.infrastructure.resilience.late_data import (
    BackfillWindow,
    LateDataConfig,
    LateDataHandler,
    LateDataMode,
    LateDataResult,
    build_late_data_handler,
    parse_backfill_window,
)


class TestLateDataMode:
    """Tests for LateDataMode enum."""

    def test_choices(self) -> None:
        """choices() should return all valid enum values."""
        choices = LateDataMode.choices()
        assert choices == ["allow", "reject", "quarantine"]

    def test_normalize_string(self) -> None:
        """normalize() should convert string to enum member."""
        assert LateDataMode.normalize("allow") == LateDataMode.ALLOW
        assert LateDataMode.normalize("reject") == LateDataMode.REJECT
        assert LateDataMode.normalize("quarantine") == LateDataMode.QUARANTINE

    def test_normalize_case_insensitive(self) -> None:
        """normalize() should be case-insensitive."""
        assert LateDataMode.normalize("ALLOW") == LateDataMode.ALLOW
        assert LateDataMode.normalize("Reject") == LateDataMode.REJECT
        assert LateDataMode.normalize("QUARANTINE") == LateDataMode.QUARANTINE

    def test_normalize_with_whitespace(self) -> None:
        """normalize() should handle whitespace."""
        assert LateDataMode.normalize("  allow  ") == LateDataMode.ALLOW
        assert LateDataMode.normalize("\treject\n") == LateDataMode.REJECT

    def test_normalize_none_returns_default(self) -> None:
        """normalize(None) should return default (ALLOW)."""
        assert LateDataMode.normalize(None) == LateDataMode.ALLOW

    def test_normalize_enum_member(self) -> None:
        """normalize() should pass through enum members."""
        assert LateDataMode.normalize(LateDataMode.ALLOW) == LateDataMode.ALLOW
        assert LateDataMode.normalize(LateDataMode.REJECT) == LateDataMode.REJECT

    def test_normalize_invalid_raises(self) -> None:
        """normalize() should raise ValueError for invalid values."""
        with pytest.raises(ValueError, match="Invalid LateDataMode"):
            LateDataMode.normalize("invalid")

    def test_describe(self) -> None:
        """describe() should return human-readable descriptions."""
        assert "Accept" in LateDataMode.ALLOW.describe()
        assert "Reject" in LateDataMode.REJECT.describe()
        assert "quarantine" in LateDataMode.QUARANTINE.describe().lower()


class TestLateDataConfig:
    """Tests for LateDataConfig dataclass."""

    def test_defaults(self) -> None:
        """Default config should have sensible defaults."""
        config = LateDataConfig()
        assert config.mode == LateDataMode.ALLOW
        assert config.threshold_days == 7
        assert config.quarantine_path == "_quarantine"
        assert config.timestamp_column is None

    def test_custom_values(self) -> None:
        """Custom values should be respected."""
        config = LateDataConfig(
            mode=LateDataMode.REJECT,
            threshold_days=14,
            quarantine_path="late_data",
            timestamp_column="event_time",
        )
        assert config.mode == LateDataMode.REJECT
        assert config.threshold_days == 14
        assert config.quarantine_path == "late_data"
        assert config.timestamp_column == "event_time"

    def test_from_dict_empty(self) -> None:
        """from_dict(None) should return default config."""
        config = LateDataConfig.from_dict(None)
        assert config.mode == LateDataMode.ALLOW
        assert config.threshold_days == 7

    def test_from_dict_empty_dict(self) -> None:
        """from_dict({}) should return default config."""
        config = LateDataConfig.from_dict({})
        assert config.mode == LateDataMode.ALLOW
        assert config.threshold_days == 7

    def test_from_dict_full(self) -> None:
        """from_dict should restore all fields."""
        data = {
            "mode": "quarantine",
            "threshold_days": 30,
            "quarantine_path": "quarantine_zone",
            "timestamp_column": "updated_at",
        }
        config = LateDataConfig.from_dict(data)
        assert config.mode == LateDataMode.QUARANTINE
        assert config.threshold_days == 30
        assert config.quarantine_path == "quarantine_zone"
        assert config.timestamp_column == "updated_at"

    def test_from_dict_invalid_mode(self) -> None:
        """from_dict should fallback to ALLOW for invalid mode."""
        config = LateDataConfig.from_dict({"mode": "invalid_mode"})
        assert config.mode == LateDataMode.ALLOW

    def test_to_dict(self) -> None:
        """to_dict should serialize all fields."""
        config = LateDataConfig(
            mode=LateDataMode.QUARANTINE,
            threshold_days=14,
            quarantine_path="late",
            timestamp_column="ts",
        )
        data = config.to_dict()
        assert data["mode"] == "quarantine"
        assert data["threshold_days"] == 14
        assert data["quarantine_path"] == "late"
        assert data["timestamp_column"] == "ts"

    def test_to_dict_from_dict_roundtrip(self) -> None:
        """to_dict -> from_dict should preserve config settings."""
        original = LateDataConfig(
            mode=LateDataMode.REJECT,
            threshold_days=21,
            quarantine_path="rejected",
            timestamp_column="created_at",
        )
        restored = LateDataConfig.from_dict(original.to_dict())
        assert restored.mode == original.mode
        assert restored.threshold_days == original.threshold_days
        assert restored.quarantine_path == original.quarantine_path
        assert restored.timestamp_column == original.timestamp_column


class TestLateDataResult:
    """Tests for LateDataResult dataclass."""

    def test_empty_result(self) -> None:
        """Empty result should have zero counts."""
        result = LateDataResult()
        assert result.total_records == 0
        assert result.late_count == 0
        assert result.rejected_count == 0
        assert result.quarantined_count == 0

    def test_total_records(self) -> None:
        """total_records should count on-time and late records."""
        result = LateDataResult(
            on_time_records=[{"id": 1}, {"id": 2}],
            late_records=[{"id": 3}],
        )
        assert result.total_records == 3
        assert result.late_count == 1

    def test_to_dict(self) -> None:
        """to_dict should return metrics dictionary."""
        result = LateDataResult(
            on_time_records=[{"id": 1}],
            late_records=[{"id": 2}],
            rejected_count=3,
            quarantined_count=1,
        )
        data = result.to_dict()
        assert data["on_time_count"] == 1
        assert data["late_count"] == 1
        assert data["rejected_count"] == 3
        assert data["quarantined_count"] == 1
        assert data["total_records"] == 2


class TestLateDataHandler:
    """Tests for LateDataHandler class."""

    def test_is_late_without_timestamp_column(self) -> None:
        """is_late should return False if no timestamp column configured."""
        config = LateDataConfig(timestamp_column=None)
        handler = LateDataHandler(config)
        record = {"id": 1, "ts": "2024-01-01"}
        assert handler.is_late(record, datetime.utcnow()) is False

    def test_is_late_with_missing_value(self) -> None:
        """is_late should return False if timestamp value is missing."""
        config = LateDataConfig(timestamp_column="ts")
        handler = LateDataHandler(config)
        record = {"id": 1}  # No 'ts' field
        assert handler.is_late(record, datetime.utcnow()) is False

    def test_is_late_datetime_format(self) -> None:
        """is_late should handle datetime strings."""
        config = LateDataConfig(timestamp_column="ts", threshold_days=7)
        handler = LateDataHandler(config)
        reference = datetime(2024, 1, 15, 12, 0, 0)

        # Record from 10 days ago (late)
        late_record = {"ts": "2024-01-05T00:00:00"}
        assert handler.is_late(late_record, reference) is True

        # Record from 3 days ago (on time)
        on_time_record = {"ts": "2024-01-12T00:00:00"}
        assert handler.is_late(on_time_record, reference) is False

    def test_is_late_date_format(self) -> None:
        """is_late should handle date strings."""
        config = LateDataConfig(timestamp_column="ts", threshold_days=7)
        handler = LateDataHandler(config)
        reference = datetime(2024, 1, 15, 12, 0, 0)

        late_record = {"ts": "2024-01-05"}
        assert handler.is_late(late_record, reference) is True

    def test_is_late_datetime_object(self) -> None:
        """is_late should handle datetime objects."""
        config = LateDataConfig(timestamp_column="ts", threshold_days=7)
        handler = LateDataHandler(config)
        reference = datetime(2024, 1, 15, 12, 0, 0)

        late_record = {"ts": datetime(2024, 1, 5, 0, 0, 0)}
        assert handler.is_late(late_record, reference) is True

    def test_is_late_date_object(self) -> None:
        """is_late should handle date objects."""
        config = LateDataConfig(timestamp_column="ts", threshold_days=7)
        handler = LateDataHandler(config)
        reference = datetime(2024, 1, 15, 12, 0, 0)

        late_record = {"ts": date(2024, 1, 5)}
        assert handler.is_late(late_record, reference) is True

    def test_process_records_allow_mode(self) -> None:
        """process_records in ALLOW mode should keep late records."""
        config = LateDataConfig(
            mode=LateDataMode.ALLOW,
            timestamp_column="ts",
            threshold_days=7,
        )
        handler = LateDataHandler(config)
        reference = datetime(2024, 1, 15, 12, 0, 0)

        records = [
            {"id": 1, "ts": "2024-01-14"},  # on time
            {"id": 2, "ts": "2024-01-05"},  # late
        ]
        result = handler.process_records(records, reference)

        assert len(result.on_time_records) == 1
        assert len(result.late_records) == 1
        assert result.late_records[0]["id"] == 2

    def test_process_records_reject_mode(self) -> None:
        """process_records in REJECT mode should raise on late data."""
        config = LateDataConfig(
            mode=LateDataMode.REJECT,
            timestamp_column="ts",
            threshold_days=7,
        )
        handler = LateDataHandler(config)
        reference = datetime(2024, 1, 15, 12, 0, 0)

        records = [
            {"id": 1, "ts": "2024-01-14"},  # on time
            {"id": 2, "ts": "2024-01-05"},  # late
        ]

        with pytest.raises(ValueError, match="Late data rejected"):
            handler.process_records(records, reference)

    def test_process_records_quarantine_mode(self) -> None:
        """process_records in QUARANTINE mode should separate late records."""
        config = LateDataConfig(
            mode=LateDataMode.QUARANTINE,
            timestamp_column="ts",
            threshold_days=7,
        )
        handler = LateDataHandler(config)
        reference = datetime(2024, 1, 15, 12, 0, 0)

        records = [
            {"id": 1, "ts": "2024-01-14"},  # on time
            {"id": 2, "ts": "2024-01-05"},  # late
        ]
        result = handler.process_records(records, reference)

        assert len(result.on_time_records) == 1
        assert len(result.late_records) == 1
        assert result.quarantined_count == 1

    def test_process_records_no_timestamp_column(self) -> None:
        """process_records without timestamp column should keep all records."""
        config = LateDataConfig(mode=LateDataMode.REJECT, timestamp_column=None)
        handler = LateDataHandler(config)

        records = [{"id": 1}, {"id": 2}]
        result = handler.process_records(records, datetime.utcnow())

        assert len(result.on_time_records) == 2
        assert len(result.late_records) == 0

    def test_get_quarantine_path(self) -> None:
        """get_quarantine_path should build correct path."""
        config = LateDataConfig(quarantine_path="_quarantine")
        handler = LateDataHandler(config)

        base = Path("/data/output")
        run_date = date(2024, 1, 15)
        path = handler.get_quarantine_path(base, run_date)

        assert path == Path("/data/output/_quarantine/load_date=2024-01-15")


class TestBackfillWindow:
    """Tests for BackfillWindow dataclass."""

    def test_days_property(self) -> None:
        """days property should count days in window."""
        window = BackfillWindow(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 10),
        )
        assert window.days == 10

    def test_contains(self) -> None:
        """contains() should check if date is in window."""
        window = BackfillWindow(
            start_date=date(2024, 1, 5),
            end_date=date(2024, 1, 10),
        )
        assert window.contains(date(2024, 1, 5)) is True
        assert window.contains(date(2024, 1, 7)) is True
        assert window.contains(date(2024, 1, 10)) is True
        assert window.contains(date(2024, 1, 4)) is False
        assert window.contains(date(2024, 1, 11)) is False

    def test_from_dict_string_dates(self) -> None:
        """from_dict should parse string dates."""
        data = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-10",
            "force_full": True,
        }
        window = BackfillWindow.from_dict(data)
        assert window.start_date == date(2024, 1, 1)
        assert window.end_date == date(2024, 1, 10)
        assert window.force_full is True

    def test_from_dict_date_objects(self) -> None:
        """from_dict should handle date objects."""
        data = {
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 1, 10),
        }
        window = BackfillWindow.from_dict(data)
        assert window.start_date == date(2024, 1, 1)
        assert window.end_date == date(2024, 1, 10)
        assert window.force_full is False

    def test_from_dict_missing_dates(self) -> None:
        """from_dict should raise if dates are missing."""
        with pytest.raises(ValueError, match="requires start_date and end_date"):
            BackfillWindow.from_dict({"start_date": "2024-01-01"})

    def test_to_dict(self) -> None:
        """to_dict should serialize all fields."""
        window = BackfillWindow(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 10),
            force_full=True,
        )
        data = window.to_dict()
        assert data["start_date"] == "2024-01-01"
        assert data["end_date"] == "2024-01-10"
        assert data["force_full"] is True

    def test_to_dict_from_dict_roundtrip(self) -> None:
        """to_dict -> from_dict should preserve window settings."""
        original = BackfillWindow(
            start_date=date(2024, 2, 15),
            end_date=date(2024, 2, 28),
            force_full=True,
        )
        restored = BackfillWindow.from_dict(original.to_dict())
        assert restored.start_date == original.start_date
        assert restored.end_date == original.end_date
        assert restored.force_full == original.force_full


class TestBuildLateDataHandler:
    """Tests for build_late_data_handler function."""

    def test_empty_config(self) -> None:
        """build_late_data_handler should handle empty config."""
        handler = build_late_data_handler({})
        assert handler.config.mode == LateDataMode.ALLOW
        assert handler.config.threshold_days == 7

    def test_full_config(self) -> None:
        """build_late_data_handler should extract config from nested structure."""
        cfg = {
            "source": {
                "run": {
                    "late_data": {
                        "mode": "quarantine",
                        "threshold_days": 14,
                        "timestamp_column": "event_time",
                    }
                }
            }
        }
        handler = build_late_data_handler(cfg)
        assert handler.config.mode == LateDataMode.QUARANTINE
        assert handler.config.threshold_days == 14
        assert handler.config.timestamp_column == "event_time"


class TestParseBackfillWindow:
    """Tests for parse_backfill_window function."""

    def test_no_backfill_config(self) -> None:
        """parse_backfill_window should return None without config."""
        assert parse_backfill_window({}) is None
        assert parse_backfill_window({"source": {}}) is None
        assert parse_backfill_window({"source": {"run": {}}}) is None

    def test_valid_backfill_config(self) -> None:
        """parse_backfill_window should parse valid config."""
        cfg = {
            "source": {
                "run": {
                    "backfill": {
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                        "force_full": True,
                    }
                }
            }
        }
        window = parse_backfill_window(cfg)
        assert window is not None
        assert window.start_date == date(2024, 1, 1)
        assert window.end_date == date(2024, 1, 31)
        assert window.force_full is True

    def test_invalid_backfill_config(self) -> None:
        """parse_backfill_window should return None for invalid config."""
        cfg = {
            "source": {
                "run": {
                    "backfill": {
                        "start_date": "2024-01-01",
                        # Missing end_date
                    }
                }
            }
        }
        assert parse_backfill_window(cfg) is None
