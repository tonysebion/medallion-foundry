"""Late data handling for incremental loads.

When doing incremental extraction, data can arrive after the watermark
has advanced. This module provides utilities to detect and handle
late-arriving data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import ibis

logger = logging.getLogger(__name__)

__all__ = [
    "LateDataConfig",
    "LateDataMode",
    "LateDataResult",
    "detect_late_data",
    "filter_late_data",
]


class LateDataMode(Enum):
    """How to handle late-arriving data."""

    IGNORE = "ignore"  # Skip late data silently
    WARN = "warn"  # Log warning but include late data
    REJECT = "reject"  # Raise error if late data detected
    QUARANTINE = "quarantine"  # Separate late data for manual review


@dataclass
class LateDataConfig:
    """Configuration for late data handling.

    Example:
        config = LateDataConfig(
            mode=LateDataMode.WARN,
            max_lateness=timedelta(days=7),
            timestamp_column="event_time",
        )
    """

    mode: LateDataMode = LateDataMode.WARN
    max_lateness: Optional[timedelta] = None  # How late is too late?
    timestamp_column: Optional[str] = None  # Column to check for lateness
    quarantine_path: Optional[str] = None  # Where to write quarantined data


@dataclass
class LateDataResult:
    """Result of late data detection."""

    late_count: int
    total_count: int
    oldest_late: Optional[datetime] = None
    newest_late: Optional[datetime] = None

    @property
    def has_late_data(self) -> bool:
        return self.late_count > 0

    @property
    def late_percentage(self) -> float:
        if self.total_count == 0:
            return 0.0
        return (self.late_count / self.total_count) * 100

    def __str__(self) -> str:
        if not self.has_late_data:
            return "No late data detected"
        return (
            f"Late data: {self.late_count}/{self.total_count} rows "
            f"({self.late_percentage:.1f}%)"
        )


def detect_late_data(
    t: "ibis.Table",
    timestamp_column: str,
    watermark: str,
    *,
    max_lateness: Optional[timedelta] = None,
) -> LateDataResult:
    """Detect late-arriving data in a table.

    Args:
        t: Input table
        timestamp_column: Column containing timestamps
        watermark: The current watermark (records before this are "late")
        max_lateness: Optional max allowed lateness (older = rejected)

    Returns:
        LateDataResult with counts and oldest/newest late records

    Example:
        result = detect_late_data(
            table,
            "event_time",
            "2025-01-15T00:00:00",
            max_lateness=timedelta(days=7),
        )
        if result.has_late_data:
            logger.warning(f"Found {result.late_count} late records")
    """

    total = t.count().execute()

    # Filter to records before watermark (these are "late")
    late_filter = t[timestamp_column] < watermark
    late_records = t.filter(late_filter)
    late_count = late_records.count().execute()

    if late_count == 0:
        return LateDataResult(late_count=0, total_count=total)

    # Get oldest and newest late records
    agg = late_records.aggregate(
        oldest=late_records[timestamp_column].min(),
        newest=late_records[timestamp_column].max(),
    ).execute()

    oldest = agg["oldest"].iloc[0] if not agg.empty else None
    newest = agg["newest"].iloc[0] if not agg.empty else None

    return LateDataResult(
        late_count=late_count,
        total_count=total,
        oldest_late=oldest,
        newest_late=newest,
    )


def filter_late_data(
    t: "ibis.Table",
    timestamp_column: str,
    watermark: str,
    config: LateDataConfig,
) -> "ibis.Table":
    """Filter late data according to configuration.

    Args:
        t: Input table
        timestamp_column: Column containing timestamps
        watermark: The current watermark
        config: Late data configuration

    Returns:
        Filtered table (may exclude late records based on mode)

    Raises:
        ValueError: If mode is REJECT and late data is detected

    Example:
        config = LateDataConfig(mode=LateDataMode.WARN)
        filtered = filter_late_data(table, "event_time", watermark, config)
    """
    result = detect_late_data(
        t,
        timestamp_column,
        watermark,
        max_lateness=config.max_lateness,
    )

    if not result.has_late_data:
        return t

    # Handle based on mode
    if config.mode == LateDataMode.IGNORE:
        # Silently remove late data
        return t.filter(t[timestamp_column] >= watermark)

    elif config.mode == LateDataMode.WARN:
        # Log but include all data
        logger.warning(
            "Late data detected: %d records before watermark %s",
            result.late_count,
            watermark,
        )
        return t

    elif config.mode == LateDataMode.REJECT:
        # Raise error
        raise ValueError(
            f"Late data rejected: {result.late_count} records "
            f"before watermark {watermark}"
        )

    elif config.mode == LateDataMode.QUARANTINE:
        # Log and exclude late data (caller should handle quarantine separately)
        logger.warning(
            "Quarantining %d late records (before watermark %s)",
            result.late_count,
            watermark,
        )
        return t.filter(t[timestamp_column] >= watermark)

    return t


def get_late_records(
    t: "ibis.Table",
    timestamp_column: str,
    watermark: str,
) -> "ibis.Table":
    """Get only the late-arriving records.

    Useful for quarantine or manual review workflows.

    Args:
        t: Input table
        timestamp_column: Column containing timestamps
        watermark: The current watermark

    Returns:
        Table containing only records before the watermark
    """
    return t.filter(t[timestamp_column] < watermark)
