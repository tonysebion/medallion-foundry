"""Late data handling per spec Section 4.

Late data is data that arrives after its expected processing window. The framework
supports three handling modes:

- allow: Accept late data and write to appropriate partition (default)
- reject: Reject late data entirely (fail the job)
- quarantine: Write late data to a separate quarantine location

Late data detection is based on comparing the data timestamp against:
1. The partition window (for time-partitioned data)
2. The watermark value (for incremental loads)
3. A configurable lateness threshold (e.g., 7 days)

Config example:
```yaml
source:
  run:
    late_data:
      mode: quarantine
      threshold_days: 7
      quarantine_path: _quarantine
```
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.foundation.time_utils import utc_now
from core.foundation.primitives.base import RichEnumMixin
from core.platform.resilience.constants import (
    DEFAULT_LATE_DATA_THRESHOLD_DAYS,
    DEFAULT_QUARANTINE_PATH,
)

logger = logging.getLogger(__name__)


class LateDataMode(RichEnumMixin, str, Enum):
    """Late data handling modes."""

    ALLOW = "allow"
    REJECT = "reject"
    QUARANTINE = "quarantine"


# Class variables for RichEnumMixin (must be set outside class due to Enum metaclass)
LateDataMode._default = "ALLOW"
LateDataMode._descriptions = {
    "allow": "Accept late data and write to appropriate partition",
    "reject": "Reject late data entirely (fail the job)",
    "quarantine": "Write late data to a separate quarantine location",
}


@dataclass
class LateDataConfig:
    """Configuration for late data handling."""

    mode: LateDataMode = LateDataMode.ALLOW
    threshold_days: int = DEFAULT_LATE_DATA_THRESHOLD_DAYS
    quarantine_path: str = DEFAULT_QUARANTINE_PATH
    timestamp_column: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "LateDataConfig":
        """Create from dictionary."""
        if not data:
            return cls()

        mode_str = data.get("mode", "allow").lower()
        try:
            mode = LateDataMode(mode_str)
        except ValueError:
            logger.warning("Invalid late_data.mode '%s', using 'allow'", mode_str)
            mode = LateDataMode.ALLOW

        return cls(
            mode=mode,
            threshold_days=data.get("threshold_days", DEFAULT_LATE_DATA_THRESHOLD_DAYS),
            quarantine_path=data.get("quarantine_path", DEFAULT_QUARANTINE_PATH),
            timestamp_column=data.get("timestamp_column"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "mode": self.mode.value,
            "threshold_days": self.threshold_days,
            "quarantine_path": self.quarantine_path,
            "timestamp_column": self.timestamp_column,
        }


@dataclass
class LateDataResult:
    """Result of late data processing."""

    on_time_records: List[Dict[str, Any]] = field(default_factory=list)
    late_records: List[Dict[str, Any]] = field(default_factory=list)
    rejected_count: int = 0
    quarantined_count: int = 0

    @property
    def total_records(self) -> int:
        """Total number of records processed."""
        return len(self.on_time_records) + len(self.late_records)

    @property
    def late_count(self) -> int:
        """Number of late records."""
        return len(self.late_records)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to metrics dictionary."""
        return {
            "on_time_count": len(self.on_time_records),
            "late_count": self.late_count,
            "rejected_count": self.rejected_count,
            "quarantined_count": self.quarantined_count,
            "total_records": self.total_records,
        }


class LateDataHandler:
    """Handler for detecting and processing late data."""

    def __init__(self, config: LateDataConfig):
        """Initialize handler with config."""
        self.config = config
        self._threshold_delta = timedelta(days=config.threshold_days)

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        """Parse a timestamp value to datetime."""
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())

        if isinstance(value, str):
            # Try common formats
            formats = [
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(value.rstrip("Z"), fmt.rstrip("Z"))
                except ValueError:
                    continue

        # Try numeric timestamp
        try:
            ts = float(value)
            return datetime.fromtimestamp(ts)
        except (ValueError, TypeError, OSError):
            pass

        logger.warning("Could not parse timestamp: %s", value)
        return None

    def is_late(
        self,
        record: Dict[str, Any],
        reference_time: datetime,
        timestamp_column: Optional[str] = None,
    ) -> bool:
        """Check if a record is late based on its timestamp.

        Args:
            record: Record to check
            reference_time: Reference time for lateness calculation
            timestamp_column: Column containing timestamp (overrides config)

        Returns:
            True if record is late, False otherwise
        """
        column = timestamp_column or self.config.timestamp_column
        if not column:
            return False  # Can't determine lateness without timestamp

        value = record.get(column)
        record_time = self._parse_timestamp(value)

        if record_time is None:
            return False  # Can't determine lateness without valid timestamp

        threshold_time = reference_time - self._threshold_delta
        return record_time < threshold_time

    def process_records(
        self,
        records: List[Dict[str, Any]],
        reference_time: Optional[datetime] = None,
        timestamp_column: Optional[str] = None,
    ) -> LateDataResult:
        """Process records and separate late data.

        Args:
            records: Records to process
            reference_time: Reference time for lateness (default: now)
            timestamp_column: Column containing timestamp (overrides config)

        Returns:
            LateDataResult with separated records

        Raises:
            ValueError: If mode is 'reject' and late data is found
        """
        if reference_time is None:
            reference_time = utc_now()

        result = LateDataResult()
        column = timestamp_column or self.config.timestamp_column

        # If no timestamp column configured, all records are on-time
        if not column:
            result.on_time_records = list(records)
            return result

        for record in records:
            if self.is_late(record, reference_time, column):
                if self.config.mode == LateDataMode.REJECT:
                    result.rejected_count += 1
                elif self.config.mode == LateDataMode.QUARANTINE:
                    result.late_records.append(record)
                    result.quarantined_count += 1
                else:  # ALLOW
                    result.late_records.append(record)
            else:
                result.on_time_records.append(record)

        # Raise if rejecting and found late data
        if self.config.mode == LateDataMode.REJECT and result.rejected_count > 0:
            raise ValueError(
                f"Late data rejected: {result.rejected_count} records "
                f"exceeded threshold of {self.config.threshold_days} days"
            )

        if result.late_count > 0:
            logger.info(
                f"Late data detected: {result.late_count} records "
                f"(mode={self.config.mode.value})"
            )

        return result

    def get_quarantine_path(
        self,
        base_path: Path,
        run_date: date,
    ) -> Path:
        """Get the quarantine path for late data.

        Args:
            base_path: Base output directory
            run_date: Run date for partition

        Returns:
            Path to quarantine directory
        """
        return (
            base_path
            / self.config.quarantine_path
            / f"load_date={run_date.isoformat()}"
        )


@dataclass
class BackfillWindow:
    """Configuration for a backfill window."""

    start_date: date
    end_date: date
    force_full: bool = False  # Force full extraction instead of incremental

    @property
    def days(self) -> int:
        """Number of days in the window."""
        return (self.end_date - self.start_date).days + 1

    def contains(self, check_date: date) -> bool:
        """Check if a date is within the window."""
        return self.start_date <= check_date <= self.end_date

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BackfillWindow":
        """Create from dictionary."""
        start = data.get("start_date")
        end = data.get("end_date")

        if isinstance(start, str):
            start = date.fromisoformat(start)
        if isinstance(end, str):
            end = date.fromisoformat(end)

        if start is None or end is None:
            raise ValueError("BackfillWindow requires start_date and end_date")

        return cls(
            start_date=start,
            end_date=end,
            force_full=data.get("force_full", False),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "force_full": self.force_full,
        }


def build_late_data_handler(cfg: Dict[str, Any]) -> LateDataHandler:
    """Build a LateDataHandler from config.

    Args:
        cfg: Full pipeline config dictionary

    Returns:
        Configured LateDataHandler instance
    """
    source = cfg.get("source", {})
    run_cfg = source.get("run", {})
    late_data_cfg = run_cfg.get("late_data", {})

    config = LateDataConfig.from_dict(late_data_cfg)
    return LateDataHandler(config)


def parse_backfill_window(cfg: Dict[str, Any]) -> Optional[BackfillWindow]:
    """Parse backfill window from config.

    Args:
        cfg: Full pipeline config dictionary

    Returns:
        BackfillWindow if configured, None otherwise
    """
    source = cfg.get("source", {})
    run_cfg = source.get("run", {})
    backfill_cfg = run_cfg.get("backfill")

    if not backfill_cfg:
        return None

    try:
        return BackfillWindow.from_dict(backfill_cfg)
    except (ValueError, TypeError) as e:
        logger.warning("Invalid backfill config: %s", e)
        return None


__all__ = [
    "LateDataMode",
    "LateDataConfig",
    "LateDataResult",
    "LateDataHandler",
    "BackfillWindow",
    "build_late_data_handler",
    "parse_backfill_window",
]
