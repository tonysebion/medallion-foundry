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
    """Detect rows older than the watermark."""

    total = t.count().execute()

    late_filter = t[timestamp_column] < watermark
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
    """Filter or reject late rows based on configuration."""

    result = detect_late_data(
        t,
        timestamp_column,
        watermark,
        max_lateness=config.max_lateness,
    )

    if not result.has_late_data:
        return t

    if config.mode == LateDataMode.IGNORE:
        return t.filter(t[timestamp_column] >= watermark)

    if config.mode == LateDataMode.WARN:
        logger.warning(
            "Late data detected: %d records before watermark %s",
            result.late_count,
            watermark,
        )
        return t

    if config.mode == LateDataMode.REJECT:
        raise ValueError(
            f"Late data rejected: {result.late_count} records before watermark {watermark}"
        )

    if config.mode == LateDataMode.QUARANTINE:
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
        return value
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
        "updated_at": datetime.now(timezone.utc).isoformat(),
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
