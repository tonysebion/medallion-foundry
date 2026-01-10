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

from pipelines.lib.env import parse_iso_datetime, utc_now_iso

if TYPE_CHECKING:
    import ibis

logger = logging.getLogger(__name__)

__all__ = [
    "LateDataConfig",
    "LateDataMode",
    "LateDataResult",
    "WatermarkSource",
    "detect_late_data",
    "filter_late_data",
    "get_late_records",
    "delete_watermark",
    "get_watermark",
    "get_watermark_age",
    "get_watermark_from_destination",
    "get_watermark_with_source",
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


class WatermarkSource(Enum):
    """Where to read watermark state from."""

    DESTINATION = "destination"  # Read from Bronze _metadata.json (default)
    LOCAL = "local"  # Read from .state/ directory (legacy)
    AUTO = "auto"  # Try destination first, fall back to local


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
            watermark_dt = parse_iso_datetime(watermark)
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
            watermark_dt = parse_iso_datetime(watermark)
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


def _load_json_safe(path: Path, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Load JSON file, returning default if missing or invalid.

    Args:
        path: Path to JSON file
        default: Value to return if file doesn't exist or is invalid (default: {})

    Returns:
        Parsed JSON data or default value
    """
    if default is None:
        default = {}
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return default


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

    data = _load_json_safe(path)
    if not data:
        logger.warning("Invalid watermark file for %s.%s", system, entity)
        return None

    value = data.get("last_value")
    logger.debug(
        "Found watermark for %s.%s: %s (updated %s)",
        system,
        entity,
        value,
        data.get("updated_at", "unknown"),
    )
    return str(value) if value is not None else None


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
        data = _load_json_safe(path)
        if data:
            system = data.get("system", "unknown")
            entity = data.get("entity", "unknown")
            watermarks[f"{system}.{entity}"] = data
        else:
            logger.warning("Invalid watermark file %s", path)

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

    data = _load_json_safe(path)
    updated_at = data.get("updated_at")
    if not updated_at:
        return None

    try:
        updated_dt = parse_iso_datetime(updated_at)
        now = datetime.now(timezone.utc)
        return (now - updated_dt).total_seconds() / 3600
    except ValueError as exc:
        logger.warning("Could not parse updated_at for %s.%s: %s", system, entity, exc)
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

    data = _load_json_safe(path)
    last_full = data.get("last_full_refresh")
    if not last_full:
        return None

    try:
        return parse_iso_datetime(last_full)
    except ValueError as exc:
        logger.warning("Invalid full refresh timestamp for %s.%s: %s", system, entity, exc)
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

    # Load existing data to preserve run count
    existing = _load_json_safe(path)
    run_count = existing.get("full_refresh_count", 0) + 1

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


# =============================================================================
# Destination-Based Watermarks
# =============================================================================


def _find_latest_partition(
    base_path: str,
    partition_prefix: str = "dt=",
    storage_options: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Find the latest partition directory by lexicographic sort of dt= values.

    Uses get_storage() to work with any backend (local, S3, ADLS).

    Args:
        base_path: Base path to search for partitions
        partition_prefix: Prefix for partition directories (default: "dt=")
        storage_options: Storage credentials and configuration

    Returns:
        Full path to latest partition, or None if no partitions exist
    """
    from pipelines.lib.storage import get_storage, parse_uri

    try:
        scheme, _ = parse_uri(base_path)
        partition_values = []

        if scheme == "local":
            # For local storage, use Path directly to list directories
            base = Path(base_path)
            if not base.exists():
                logger.debug("Base path does not exist: %s", base_path)
                return None

            for item in base.iterdir():
                if item.is_dir() and item.name.startswith(partition_prefix):
                    value = item.name[len(partition_prefix) :]
                    partition_values.append(value)
        else:
            # For cloud storage (S3, ADLS), use storage backend
            # List files recursively and extract partition directories from paths
            storage = get_storage(base_path, **(storage_options or {}))
            items = storage.list_files(path="", recursive=True)

            # Extract unique partition directories from file paths
            seen_partitions = set()
            for item in items:
                # Path like "dt=2025-01-15/data.parquet" or "dt=2025-01-15/_metadata.json"
                parts = item.path.replace("\\", "/").split("/")
                for part in parts:
                    if part.startswith(partition_prefix) and part not in seen_partitions:
                        value = part[len(partition_prefix) :]
                        partition_values.append(value)
                        seen_partitions.add(part)

        if not partition_values:
            logger.debug("No partitions found at %s", base_path)
            return None

        # Sort and get latest (lexicographic sort works for ISO dates)
        latest_value = sorted(partition_values)[-1]
        latest_partition = f"{partition_prefix}{latest_value}"

        # Return full path
        if scheme == "local":
            full_path = str(Path(base_path) / latest_partition)
        else:
            storage = get_storage(base_path, **(storage_options or {}))
            full_path = storage.get_full_path(latest_partition)

        logger.debug("Found latest partition: %s", full_path)
        return full_path

    except Exception as e:
        logger.warning("Error finding latest partition at %s: %s", base_path, e)
        return None


def _scan_parquet_for_watermark(
    partition_path: str,
    watermark_column: str,
    storage_options: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Scan parquet files to find MAX(watermark_column).

    This is the fallback when _metadata.json is missing or corrupted.

    Args:
        partition_path: Path to the partition containing parquet files
        watermark_column: Column to aggregate
        storage_options: Storage credentials

    Returns:
        Maximum watermark value as string, or None
    """
    try:
        import ibis

        # Configure DuckDB for cloud storage if needed
        con = ibis.duckdb.connect()

        if partition_path.startswith("s3://"):
            from pipelines.lib.storage_config import _configure_duckdb_s3

            _configure_duckdb_s3(con, storage_options or {})

        # Find parquet files in the partition
        parquet_pattern = f"{partition_path.rstrip('/')}/*.parquet"

        # Read and aggregate
        t = con.read_parquet(parquet_pattern)
        result = t.select(t[watermark_column].max().name("max_watermark")).execute()

        if result.empty or result["max_watermark"].iloc[0] is None:
            return None

        max_val = result["max_watermark"].iloc[0]
        logger.debug("Scanned parquet for watermark: %s", max_val)
        return str(max_val)

    except Exception as e:
        logger.warning("Error scanning parquet for watermark: %s", e)
        return None


def get_watermark_from_destination(
    target_path: str,
    watermark_column: Optional[str] = None,
    storage_options: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Read watermark from the latest Bronze partition metadata.

    Works with any storage backend via get_storage().

    Fallback chain:
    1. Read _metadata.json from latest dt= partition
    2. If metadata missing, scan parquet for MAX(watermark_column)
    3. Return None if no data exists

    Args:
        target_path: Bronze target path (base path without dt= partition)
        watermark_column: Column name for fallback parquet scan
        storage_options: Storage credentials and configuration

    Returns:
        Watermark value from metadata or parquet scan, or None if not found
    """
    from pipelines.lib.storage import get_storage

    # Strip the dt={run_date} part from target_path to get base path
    # e.g., "s3://bucket/bronze/system=x/entity=y/dt={run_date}/" -> "s3://bucket/bronze/system=x/entity=y/"
    base_path = target_path
    if "/dt=" in base_path or "\\dt=" in base_path:
        # Find the last occurrence of /dt= or \dt= and truncate
        for sep in ["/dt=", "\\dt="]:
            if sep in base_path:
                base_path = base_path[: base_path.rfind(sep) + 1]
                break

    # Find the latest partition
    latest_partition = _find_latest_partition(
        base_path, partition_prefix="dt=", storage_options=storage_options
    )

    if latest_partition is None:
        logger.debug("No partitions found, returning None for watermark")
        return None

    # Try to read _metadata.json from the latest partition
    try:
        storage = get_storage(latest_partition, **(storage_options or {}))
        metadata_content = storage.read_text("_metadata.json")
        metadata = json.loads(metadata_content)

        # Extract watermark from metadata
        # It can be in extra.last_watermark or directly as last_watermark
        watermark = None
        if "extra" in metadata and "last_watermark" in metadata["extra"]:
            watermark = metadata["extra"]["last_watermark"]
        elif "last_watermark" in metadata:
            watermark = metadata["last_watermark"]

        if watermark is not None:
            logger.info(
                "Found watermark from destination metadata: %s (partition: %s)",
                watermark,
                latest_partition,
            )
            return str(watermark)

        logger.debug("No watermark in metadata, trying parquet scan fallback")

    except FileNotFoundError:
        logger.debug(
            "No _metadata.json found at %s, trying parquet scan fallback",
            latest_partition,
        )
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning(
            "Error parsing _metadata.json at %s: %s, trying parquet scan fallback",
            latest_partition,
            e,
        )
    except Exception as e:
        logger.warning(
            "Error reading metadata from %s: %s, trying parquet scan fallback",
            latest_partition,
            e,
        )

    # Fallback: scan parquet for MAX(watermark_column)
    if watermark_column:
        scanned_watermark = _scan_parquet_for_watermark(
            latest_partition, watermark_column, storage_options
        )
        if scanned_watermark:
            logger.info(
                "Found watermark from parquet scan: %s (partition: %s)",
                scanned_watermark,
                latest_partition,
            )
            return scanned_watermark

    logger.debug("No watermark found in destination")
    return None


def get_watermark_with_source(
    system: str,
    entity: str,
    source: WatermarkSource,
    target_path: Optional[str] = None,
    watermark_column: Optional[str] = None,
    storage_options: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Get watermark value using the specified source strategy.

    Main entry point that routes to appropriate watermark source.

    Args:
        system: Source system name
        entity: Entity name
        source: Where to read watermark from (DESTINATION, LOCAL, or AUTO)
        target_path: Bronze target path (required for DESTINATION/AUTO)
        watermark_column: Column name (for parquet fallback)
        storage_options: Storage credentials

    Returns:
        Watermark value or None
    """
    if source == WatermarkSource.LOCAL:
        return get_watermark(system, entity)

    if source == WatermarkSource.DESTINATION:
        if not target_path:
            logger.warning(
                "No target_path provided for DESTINATION watermark source, "
                "falling back to local"
            )
            return get_watermark(system, entity)

        return get_watermark_from_destination(
            target_path, watermark_column, storage_options
        )

    # AUTO: try destination first, then local
    if target_path:
        result = get_watermark_from_destination(
            target_path, watermark_column, storage_options
        )
        if result is not None:
            return result

        logger.info(
            "No watermark found in destination for %s.%s, falling back to local",
            system,
            entity,
        )

    return get_watermark(system, entity)
