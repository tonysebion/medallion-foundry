"""Watermark persistence for incremental loads.

Watermarks track the last processed value for incremental loading,
allowing pipelines to resume from where they left off.

Watermarks are stored as JSON files in a state directory.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

__all__ = ["delete_watermark", "get_watermark", "save_watermark"]

# Default state directory - can be overridden via environment variable
DEFAULT_STATE_DIR = ".state"


def _get_state_dir() -> Path:
    """Get the state directory path."""
    state_dir = os.environ.get("PIPELINE_STATE_DIR", DEFAULT_STATE_DIR)
    return Path(state_dir)


def _get_watermark_path(system: str, entity: str) -> Path:
    """Get the path to a watermark file."""
    state_dir = _get_state_dir()
    return state_dir / f"{system}_{entity}_watermark.json"


def get_watermark(system: str, entity: str) -> Optional[str]:
    """Get the last watermark value for incremental loads.

    Args:
        system: Source system name
        entity: Entity/table name

    Returns:
        Last watermark value, or None if no watermark exists

    Example:
        >>> last_updated = get_watermark("claims_dw", "claims_header")
        >>> if last_updated:
        ...     print(f"Resuming from {last_updated}")
    """
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
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning("Invalid watermark file for %s.%s: %s", system, entity, e)
        return None


def save_watermark(system: str, entity: str, value: str) -> None:
    """Save watermark after successful extraction.

    Args:
        system: Source system name
        entity: Entity/table name
        value: New watermark value

    Example:
        >>> save_watermark("claims_dw", "claims_header", "2025-01-15T14:30:00")
    """
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
    """Delete a watermark to force full reload.

    Args:
        system: Source system name
        entity: Entity/table name

    Returns:
        True if watermark was deleted, False if it didn't exist

    Example:
        >>> delete_watermark("claims_dw", "claims_header")
        True
    """
    path = _get_watermark_path(system, entity)

    if path.exists():
        path.unlink()
        logger.info("Deleted watermark for %s.%s", system, entity)
        return True

    return False


def list_watermarks() -> Dict[str, Dict[str, Any]]:
    """List all stored watermarks.

    Returns:
        Dictionary mapping "system.entity" to watermark data

    Example:
        >>> for key, data in list_watermarks().items():
        ...     print(f"{key}: {data['last_value']}")
    """
    state_dir = _get_state_dir()

    if not state_dir.exists():
        return {}

    watermarks = {}

    for path in state_dir.glob("*_watermark.json"):
        try:
            data = json.loads(path.read_text())
            system = data.get("system", "unknown")
            entity = data.get("entity", "unknown")
            key = f"{system}.{entity}"
            watermarks[key] = data
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Invalid watermark file %s: %s", path, e)

    return watermarks


def clear_all_watermarks() -> int:
    """Clear all watermarks.

    Use with caution - this will cause all incremental loads
    to restart from the beginning.

    Returns:
        Number of watermarks deleted

    Example:
        >>> count = clear_all_watermarks()
        >>> print(f"Cleared {count} watermarks")
    """
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
    """Get the age of a watermark in hours.

    Useful for monitoring and alerting on stale data.

    Args:
        system: Source system name
        entity: Entity/table name

    Returns:
        Age in hours, or None if no watermark exists

    Example:
        >>> age = get_watermark_age("claims_dw", "claims_header")
        >>> if age and age > 24:
        ...     print("Warning: watermark is more than 24 hours old")
    """
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

        return delta.total_seconds() / 3600  # Convert to hours
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(
            "Could not calculate watermark age for %s.%s: %s", system, entity, e
        )
        return None
