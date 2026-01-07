"""State helpers for pipelines: watermarks and late data management."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

from pipelines.lib.env import utc_now_iso

if TYPE_CHECKING:
    import ibis

logger = logging.getLogger(__name__)

__all__ = [
    "LateDataConfig",
    "LateDataMode",
    "LateDataResult",
    "detect_late_data",
    "filter_late_data",
    "get_late_records",
    "delete_watermark",
    "get_watermark",
    "get_watermark_age",
    "list_watermarks",
    "clear_all_watermarks",
    "save_watermark",
    # Full refresh tracking
    "get_last_full_refresh",
    "save_full_refresh",
    "should_force_full_refresh",
]


class LateDataMode(Enum):
    """How to handle late-arriving rows."""

    IGNORE = "ignore"
    WARN = "warn"
    REJECT = "reject"
    QUARANTINE = "quarantine"


@dataclass
class LateDataConfig:
    """Configuration for late data handling."""

    mode: LateDataMode = LateDataMode.WARN
    max_lateness: Optional[timedelta] = None
    timestamp_column: Optional[str] = None
    quarantine_path: Optional[str] = None


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
    """Detect rows older than the watermark (or watermark - max_lateness).

    Args:
        t: Ibis table to check
        timestamp_column: Column containing timestamps
        watermark: Current watermark value (ISO format string or compatible)
        max_lateness: If provided, records are only considered "late" if they
                     are older than (watermark - max_lateness). This allows
                     a grace period for late-arriving data.

    Returns:
        LateDataResult with counts and timestamps of late records
    """
    total = t.count().execute()

    # Determine the cutoff timestamp
    # If max_lateness is specified, we allow records up to that much before the watermark
    if max_lateness is not None:
        try:
            # Parse watermark as datetime to apply max_lateness
            watermark_dt = datetime.fromisoformat(watermark.replace('Z', '+00:00'))
            cutoff_dt = watermark_dt - max_lateness
            cutoff = cutoff_dt.isoformat()
            logger.debug(
                "Using max_lateness=%s: cutoff=%s (watermark=%s)",
                max_lateness,
                cutoff,
                watermark,
            )
        except (ValueError, TypeError) as e:
            logger.warning(
                "Could not parse watermark '%s' for max_lateness calculation: %s. "
                "Using watermark directly.",
                watermark,
                e,
            )
            cutoff = watermark
    else:
        cutoff = watermark

    late_filter = t[timestamp_column] < cutoff
    late_records = t.filter(late_filter)
    late_count = late_records.count().execute()

    if late_count == 0:
        return LateDataResult(late_count=0, total_count=total)

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
    """Filter or reject late rows based on configuration.

    Args:
        t: Ibis table to filter
        timestamp_column: Column containing timestamps
        watermark: Current watermark value
        config: Late data handling configuration

    Returns:
        Filtered table (late records removed if mode is IGNORE or QUARANTINE)

    The max_lateness setting from config is now honored:
    - Records are only considered "late" if older than (watermark - max_lateness)
    - This provides a grace period for late-arriving data
    """
    result = detect_late_data(
        t,
        timestamp_column,
        watermark,
        max_lateness=config.max_lateness,
    )

    if not result.has_late_data:
        return t

    # Calculate the effective cutoff for filtering
    if config.max_lateness is not None:
        try:
            watermark_dt = datetime.fromisoformat(watermark.replace('Z', '+00:00'))
            cutoff_dt = watermark_dt - config.max_lateness
            cutoff = cutoff_dt.isoformat()
        except (ValueError, TypeError):
            cutoff = watermark
    else:
        cutoff = watermark

    if config.mode == LateDataMode.IGNORE:
        return t.filter(t[timestamp_column] >= cutoff)

    if config.mode == LateDataMode.WARN:
        logger.warning(
            "Late data detected: %d records before cutoff %s (watermark=%s, max_lateness=%s)",
            result.late_count,
            cutoff,
            watermark,
            config.max_lateness,
        )
        return t

    if config.mode == LateDataMode.REJECT:
        raise ValueError(
            f"Late data rejected: {result.late_count} records before cutoff {cutoff}"
        )

    # config.mode == LateDataMode.QUARANTINE (or unknown future mode)
    logger.warning(
        "Quarantining %d late records (before cutoff %s)",
        result.late_count,
        cutoff,
    )
    # Write late records to quarantine path if configured
    if config.quarantine_path:
        _write_quarantine_records(t, timestamp_column, cutoff, config.quarantine_path)
    return t.filter(t[timestamp_column] >= cutoff)


def _write_quarantine_records(
    t: "ibis.Table",
    timestamp_column: str,
    cutoff: str,
    quarantine_path: str,
) -> None:
    """Write late records to quarantine location.

    Args:
        t: Full table
        timestamp_column: Column containing timestamps
        cutoff: Cutoff timestamp - records before this are quarantined
        quarantine_path: Path to write quarantined records
    """
    try:
        late_records = t.filter(t[timestamp_column] < cutoff)
        late_count = late_records.count().execute()

        if late_count == 0:
            return

        # Create quarantine directory
        quarantine_dir = Path(quarantine_path)
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        # Write with timestamp to avoid overwriting
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = quarantine_dir / f"late_records_{timestamp}.parquet"

        late_records.to_parquet(str(output_file))
        logger.info(
            "Wrote %d late records to quarantine: %s",
            late_count,
            output_file,
        )
    except Exception as e:
        logger.error("Failed to write quarantine records: %s", e)


def get_late_records(
    t: "ibis.Table",
    timestamp_column: str,
    watermark: str,
) -> "ibis.Table":
    """Return only the late-arriving rows."""

    return t.filter(t[timestamp_column] < watermark)


DEFAULT_STATE_DIR = ".state"


def _get_state_dir() -> Path:
    state_dir = os.environ.get("PIPELINE_STATE_DIR", DEFAULT_STATE_DIR)
    return Path(state_dir)


def _get_watermark_path(system: str, entity: str) -> Path:
    state_dir = _get_state_dir()
    return state_dir / f"{system}_{entity}_watermark.json"


def get_watermark(system: str, entity: str) -> Optional[str]:
    """Return the last saved watermark value."""

    path = _get_watermark_path(system, entity)

    if not path.exists():
        logger.debug("No watermark found for %s.%s", system, entity)
        return None

    try:
        data = json.loads(path.read_text())
        value = data.get("last_value")
        logger.debug(
            "Found watermark for %s.%s: %s (updated %s)",
            system,
            entity,
            value,
            data.get("updated_at", "unknown"),
        )
        return str(value) if value is not None else None
    except (json.JSONDecodeError, KeyError) as exc:
        logger.warning(
            "Invalid watermark file for %s.%s: %s",
            system,
            entity,
            exc,
        )
        return None


def save_watermark(system: str, entity: str, value: str) -> None:
    """Persist a new watermark value."""

    state_dir = _get_state_dir()
    state_dir.mkdir(parents=True, exist_ok=True)

    path = _get_watermark_path(system, entity)
    data = {
        "system": system,
        "entity": entity,
        "last_value": value,
        "updated_at": utc_now_iso(),
    }

    path.write_text(json.dumps(data, indent=2))
    logger.info("Saved watermark for %s.%s: %s", system, entity, value)


def delete_watermark(system: str, entity: str) -> bool:
    """Remove an existing watermark file."""

    path = _get_watermark_path(system, entity)

    if path.exists():
        path.unlink()
        logger.info("Deleted watermark for %s.%s", system, entity)
        return True

    return False


def list_watermarks() -> Dict[str, Dict[str, Any]]:
    """List all stored watermark entries."""

    state_dir = _get_state_dir()

    if not state_dir.exists():
        return {}

    watermarks: Dict[str, Dict[str, Any]] = {}

    for path in state_dir.glob("*_watermark.json"):
        try:
            data = json.loads(path.read_text())
            system = data.get("system", "unknown")
            entity = data.get("entity", "unknown")
            key = f"{system}.{entity}"
            watermarks[key] = data
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Invalid watermark file %s: %s", path, exc)

    return watermarks


def clear_all_watermarks() -> int:
    """Delete every watermark file."""

    state_dir = _get_state_dir()

    if not state_dir.exists():
        return 0

    count = 0
    for path in state_dir.glob("*_watermark.json"):
        path.unlink()
        count += 1

    logger.info("Cleared %d watermarks", count)
    return count


def get_watermark_age(system: str, entity: str) -> Optional[float]:
    """Return the age of the watermark in hours."""

    path = _get_watermark_path(system, entity)

    if not path.exists():
        return None

    try:
        data = json.loads(path.read_text())
        updated_at = data.get("updated_at")
        if not updated_at:
            return None

        updated_dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - updated_dt

        return delta.total_seconds() / 3600
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "Could not calculate watermark age for %s.%s: %s",
            system,
            entity,
            exc,
        )
        return None


# =============================================================================
# Full Refresh Tracking
# =============================================================================


def _get_full_refresh_path(system: str, entity: str) -> Path:
    """Get the path for the full refresh state file."""
    state_dir = _get_state_dir()
    return state_dir / f"{system}_{entity}_full_refresh.json"


def get_last_full_refresh(system: str, entity: str) -> Optional[datetime]:
    """Get the timestamp of the last full refresh.

    Args:
        system: Source system name
        entity: Entity/table name

    Returns:
        Datetime of last full refresh, or None if never run
    """
    path = _get_full_refresh_path(system, entity)

    if not path.exists():
        return None

    try:
        data = json.loads(path.read_text())
        last_full = data.get("last_full_refresh")
        if last_full:
            return datetime.fromisoformat(last_full.replace("Z", "+00:00"))
        return None
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "Invalid full refresh file for %s.%s: %s",
            system,
            entity,
            exc,
        )
        return None


def save_full_refresh(system: str, entity: str) -> None:
    """Record that a full refresh was performed.

    Args:
        system: Source system name
        entity: Entity/table name
    """
    state_dir = _get_state_dir()
    state_dir.mkdir(parents=True, exist_ok=True)

    path = _get_full_refresh_path(system, entity)
    now = datetime.now(timezone.utc)

    # Load existing data if present to preserve run count
    run_count = 1
    if path.exists():
        try:
            existing = json.loads(path.read_text())
            run_count = existing.get("full_refresh_count", 0) + 1
        except (json.JSONDecodeError, KeyError):
            pass

    data = {
        "system": system,
        "entity": entity,
        "last_full_refresh": now.isoformat(),
        "full_refresh_count": run_count,
    }

    path.write_text(json.dumps(data, indent=2))
    logger.info(
        "Recorded full refresh for %s.%s at %s (count: %d)",
        system,
        entity,
        now.isoformat(),
        run_count,
    )


def should_force_full_refresh(
    system: str,
    entity: str,
    full_refresh_days: Optional[int],
) -> bool:
    """Check if a full refresh should be forced based on the interval.

    Args:
        system: Source system name
        entity: Entity/table name
        full_refresh_days: Number of days between full refreshes (None = never)

    Returns:
        True if a full refresh should be forced, False otherwise
    """
    if full_refresh_days is None:
        return False

    last_full = get_last_full_refresh(system, entity)

    if last_full is None:
        # Never had a full refresh - force one
        logger.info(
            "No previous full refresh for %s.%s - forcing full refresh",
            system,
            entity,
        )
        return True

    now = datetime.now(timezone.utc)
    days_since = (now - last_full).days

    if days_since >= full_refresh_days:
        logger.info(
            "Full refresh due for %s.%s: %d days since last full (threshold: %d)",
            system,
            entity,
            days_since,
            full_refresh_days,
        )
        return True

    logger.debug(
        "Full refresh not due for %s.%s: %d days since last full (threshold: %d)",
        system,
        entity,
        days_since,
        full_refresh_days,
    )
    return False
