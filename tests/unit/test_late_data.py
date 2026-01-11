"""Tests for the pipelines late-data helpers."""

import ibis
import pandas as pd
import pytest

from pipelines.lib.state import (
    LateDataConfig,
    LateDataMode,
    LateDataResult,
    detect_late_data,
    filter_late_data,
    get_late_records,
)


@pytest.fixture
def sample_table():
    """Simple table with one late and two on-time records."""
    data = pd.DataFrame(
        [
            {"id": 1, "event_ts": "2025-01-10T10:00:00"},
            {"id": 2, "event_ts": "2025-01-16T00:00:00"},
            {"id": 3, "event_ts": "2025-01-15T08:00:00"},
        ]
    )
    return ibis.memtable(data)


class TestLateDataMode:
    """Ensure enums expose the new values."""

    def test_enum_values(self):
        assert LateDataMode.IGNORE.value == "ignore"
        assert LateDataMode.WARN.value == "warn"
        assert LateDataMode.REJECT.value == "reject"
        assert LateDataMode.QUARANTINE.value == "quarantine"


class TestLateDataResult:
    """Lightweight dataclass should report percentages."""

    def test_late_percentage_zero_total(self):
        result = LateDataResult(late_count=0, total_count=0)
        assert result.late_percentage == 0.0
        assert result.has_late_data is False
        assert str(result) == "No late data detected"

    def test_late_percentage_nonzero(self):
        result = LateDataResult(late_count=2, total_count=4)
        assert result.has_late_data is True
        assert result.late_percentage == 50.0
        assert "Late data: 2/4 rows" in str(result)


class TestDetectLateData:
    """Detection should count late rows and compute bounds."""

    def test_detects_late_records(self, sample_table):
        watermark = "2025-01-15T00:00:00"
        result = detect_late_data(sample_table, "event_ts", watermark)

        assert result.total_count == 3
        assert result.late_count == 1
        assert result.oldest_late == "2025-01-10T10:00:00"
        assert result.newest_late == "2025-01-10T10:00:00"

    def test_detects_no_late_records(self, sample_table):
        watermark = "2025-01-09T00:00:00"
        result = detect_late_data(sample_table, "event_ts", watermark)

        assert result.late_count == 0
        assert result.total_count == 3
        assert not result.has_late_data


class TestFilterLateData:
    """Filter respects the configured mode."""

    @pytest.fixture
    def watermark(self):
        return "2025-01-15T00:00:00"

    def test_ignore_mode_drops_late_data(self, sample_table, watermark):
        config = LateDataConfig(mode=LateDataMode.IGNORE, timestamp_column="event_ts")
        filtered = filter_late_data(sample_table, "event_ts", watermark, config)
        df = filtered.execute()
        assert len(df) == 2
        assert 1 not in df["id"].values

    def test_warn_mode_includes_late_data(self, sample_table, watermark):
        config = LateDataConfig(mode=LateDataMode.WARN, timestamp_column="event_ts")
        filtered = filter_late_data(sample_table, "event_ts", watermark, config)
        df = filtered.execute()
        assert len(df) == 3

    def test_reject_mode_raises(self, sample_table, watermark):
        config = LateDataConfig(mode=LateDataMode.REJECT, timestamp_column="event_ts")
        with pytest.raises(ValueError, match="Late data rejected"):
            filter_late_data(sample_table, "event_ts", watermark, config)

    def test_quarantine_mode_behaves_like_ignore(self, sample_table, watermark):
        config = LateDataConfig(
            mode=LateDataMode.QUARANTINE, timestamp_column="event_ts"
        )
        filtered = filter_late_data(sample_table, "event_ts", watermark, config)
        df = filtered.execute()
        assert len(df) == 2
        assert 1 not in df["id"].values


class TestGetLateRecords:
    """Helper should return only records before the watermark."""

    def test_returns_late_records(self, sample_table):
        watermark = "2025-01-15T00:00:00"
        late = get_late_records(sample_table, "event_ts", watermark)
        df = late.execute()
        assert len(df) == 1
        assert df["id"].iloc[0] == 1
