"""Centralized watermark storage and retrieval per spec Section 4.

Watermarks track the progress of incremental data loads. Each source maintains
its own watermark, typically based on a timestamp or sequence column.

Supported storage backends:
- local: JSON files in .state/ directory (default)
- s3: JSON files in S3 bucket

Watermark file structure:
```json
{
  "source_key": "system.table",
  "watermark_column": "updated_at",
  "watermark_value": "2025-01-15T10:30:00Z",
  "watermark_type": "timestamp",
  "last_run_id": "uuid...",
  "last_run_date": "2025-01-15",
  "record_count": 1000,
  "created_at": "2025-01-10T08:00:00Z",
  "updated_at": "2025-01-15T10:30:00Z"
}
```
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from core.foundation.time_utils import utc_isoformat as _utc_isoformat

from core.foundation.primitives.base import RichEnumMixin

logger = logging.getLogger(__name__)



# Module-level constants for WatermarkType
_WATERMARK_TYPE_DESCRIPTIONS: Dict[str, str] = {
    "timestamp": "ISO timestamp watermark (e.g., 2025-01-15T10:30:00Z)",
    "date": "Date watermark (e.g., 2025-01-15)",
    "integer": "Integer sequence watermark (e.g., record ID)",
    "string": "String watermark for lexicographic comparison",
}


class WatermarkType(RichEnumMixin, str, Enum):
    """Type of watermark value for proper comparison."""

    TIMESTAMP = "timestamp"
    DATE = "date"
    INTEGER = "integer"
    STRING = "string"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "WatermarkType":
        """Normalize a watermark type value."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            return cls.TIMESTAMP

        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member

        raise ValueError(
            f"Invalid WatermarkType '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        return _WATERMARK_TYPE_DESCRIPTIONS.get(self.value, self.value)


@dataclass
class Watermark:
    """Watermark state for a source."""

    source_key: str
    watermark_column: str
    watermark_value: Optional[str] = None
    watermark_type: WatermarkType = WatermarkType.TIMESTAMP
    last_run_id: Optional[str] = None
    last_run_date: Optional[str] = None
    record_count: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = _utc_isoformat()

    def update(
        self,
        new_value: str,
        run_id: str,
        run_date: date,
        record_count: int = 0,
    ) -> "Watermark":
        """Update watermark with new value."""
        self.watermark_value = new_value
        self.last_run_id = run_id
        self.last_run_date = run_date.isoformat()
        self.record_count = record_count
        self.updated_at = _utc_isoformat()
        return self

    def compare(self, value: str) -> int:
        """Compare a value against the current watermark.

        Returns:
            -1 if value < watermark (already processed)
             0 if value == watermark
             1 if value > watermark (new data)
        """
        if self.watermark_value is None:
            return 1  # No watermark means all data is new

        if self.watermark_type == WatermarkType.INTEGER:
            wm_int = int(self.watermark_value)
            val_int = int(value)
            if val_int < wm_int:
                return -1
            elif val_int > wm_int:
                return 1
            return 0
        else:
            # String comparison works for timestamps, dates, and strings
            if value < self.watermark_value:
                return -1
            elif value > self.watermark_value:
                return 1
            return 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "source_key": self.source_key,
            "watermark_column": self.watermark_column,
            "watermark_value": self.watermark_value,
            "watermark_type": self.watermark_type.value,
            "last_run_id": self.last_run_id,
            "last_run_date": self.last_run_date,
            "record_count": self.record_count,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        if self.extra:
            result["extra"] = self.extra
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Watermark":
        """Create from dictionary."""
        watermark_type_str = data.get("watermark_type", "timestamp")
        try:
            watermark_type = WatermarkType(watermark_type_str)
        except ValueError:
            watermark_type = WatermarkType.TIMESTAMP

        return cls(
            source_key=data["source_key"],
            watermark_column=data["watermark_column"],
            watermark_value=data.get("watermark_value"),
            watermark_type=watermark_type,
            last_run_id=data.get("last_run_id"),
            last_run_date=data.get("last_run_date"),
            record_count=data.get("record_count", 0),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            extra=data.get("extra", {}),
        )


class WatermarkStore:
    """Centralized watermark storage.

    Supports local filesystem and S3 storage backends.
    """

    def __init__(
        self,
        storage_backend: str = "local",
        local_path: Optional[Path] = None,
        s3_bucket: Optional[str] = None,
        s3_prefix: str = "_watermarks",
    ):
        """Initialize watermark store.

        Args:
            storage_backend: Storage backend ("local" or "s3")
            local_path: Local directory for watermark files
            s3_bucket: S3 bucket for watermark files
            s3_prefix: S3 prefix for watermark files
        """
        self.storage_backend = storage_backend
        self.local_path = local_path or Path(".state")
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

        # Create local directory if needed
        if self.storage_backend == "local":
            self.local_path.mkdir(parents=True, exist_ok=True)

    def _get_source_key(self, system: str, table: str) -> str:
        """Generate source key from system and table."""
        return f"{system}.{table}"

    def _get_local_path(self, source_key: str) -> Path:
        """Get local file path for a watermark."""
        safe_key = source_key.replace(".", "_").replace("/", "_")
        return self.local_path / f"{safe_key}_watermark.json"

    def _get_s3_key(self, source_key: str) -> str:
        """Get S3 key for a watermark."""
        safe_key = source_key.replace(".", "_").replace("/", "_")
        return f"{self.s3_prefix}/{safe_key}_watermark.json"

    def get(
        self,
        system: str,
        table: str,
        watermark_column: str,
        watermark_type: WatermarkType = WatermarkType.TIMESTAMP,
    ) -> Watermark:
        """Get or create watermark for a source.

        Args:
            system: Source system name
            table: Table name
            watermark_column: Column used for watermark
            watermark_type: Type of watermark value

        Returns:
            Watermark object (existing or new)
        """
        source_key = self._get_source_key(system, table)

        try:
            if self.storage_backend == "s3":
                return self._load_s3(source_key, watermark_column, watermark_type)
            else:
                return self._load_local(source_key, watermark_column, watermark_type)
        except FileNotFoundError:
            logger.info("No existing watermark for %s, creating new", source_key)
            return Watermark(
                source_key=source_key,
                watermark_column=watermark_column,
                watermark_type=watermark_type,
            )
        except Exception as e:
            logger.warning("Error loading watermark for %s: %s, creating new", source_key, e)
            return Watermark(
                source_key=source_key,
                watermark_column=watermark_column,
                watermark_type=watermark_type,
            )

    def _load_local(
        self,
        source_key: str,
        watermark_column: str,
        watermark_type: WatermarkType,
    ) -> Watermark:
        """Load watermark from local file."""
        path = self._get_local_path(source_key)
        if not path.exists():
            raise FileNotFoundError(f"Watermark not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return Watermark.from_dict(data)

    def _load_s3(
        self,
        source_key: str,
        watermark_column: str,
        watermark_type: WatermarkType,
    ) -> Watermark:
        """Load watermark from S3."""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required for S3 watermark storage")

        s3 = boto3.client("s3")
        key = self._get_s3_key(source_key)

        try:
            response = s3.get_object(Bucket=self.s3_bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return Watermark.from_dict(data)
        except s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Watermark not found in S3: {key}")

    def save(self, watermark: Watermark) -> None:
        """Save watermark to storage."""
        if self.storage_backend == "s3":
            self._save_s3(watermark)
        else:
            self._save_local(watermark)

    def _save_local(self, watermark: Watermark) -> None:
        """Save watermark to local file."""
        path = self._get_local_path(watermark.source_key)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(watermark.to_dict(), f, indent=2)

        logger.info("Saved watermark to %s", path)

    def _save_s3(self, watermark: Watermark) -> None:
        """Save watermark to S3."""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required for S3 watermark storage")

        s3 = boto3.client("s3")
        key = self._get_s3_key(watermark.source_key)
        body = json.dumps(watermark.to_dict(), indent=2)

        s3.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=body.encode("utf-8"),
        )

        logger.info("Saved watermark to s3://%s/%s", self.s3_bucket, key)

    def delete(self, system: str, table: str) -> bool:
        """Delete watermark for a source.

        Returns:
            True if watermark was deleted, False if not found
        """
        source_key = self._get_source_key(system, table)

        try:
            if self.storage_backend == "s3":
                return self._delete_s3(source_key)
            else:
                return self._delete_local(source_key)
        except Exception as e:
            logger.warning("Error deleting watermark for %s: %s", source_key, e)
            return False

    def _delete_local(self, source_key: str) -> bool:
        """Delete watermark from local file."""
        path = self._get_local_path(source_key)
        if path.exists():
            path.unlink()
            logger.info("Deleted watermark at %s", path)
            return True
        return False

    def _delete_s3(self, source_key: str) -> bool:
        """Delete watermark from S3."""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required for S3 watermark storage")

        s3 = boto3.client("s3")
        key = self._get_s3_key(source_key)

        try:
            s3.delete_object(Bucket=self.s3_bucket, Key=key)
            logger.info("Deleted watermark from s3://%s/%s", self.s3_bucket, key)
            return True
        except Exception:
            return False

    def list_watermarks(self) -> list[Watermark]:
        """List all watermarks in storage."""
        if self.storage_backend == "s3":
            return self._list_s3()
        else:
            return self._list_local()

    def _list_local(self) -> list[Watermark]:
        """List all watermarks from local files."""
        watermarks = []
        for path in self.local_path.glob("*_watermark.json"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                watermarks.append(Watermark.from_dict(data))
            except Exception as e:
                logger.warning("Could not load watermark from %s: %s", path, e)
        return watermarks

    def _list_s3(self) -> list[Watermark]:
        """List all watermarks from S3."""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required for S3 watermark storage")

        s3 = boto3.client("s3")
        watermarks = []

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith("_watermark.json"):
                    try:
                        response = s3.get_object(Bucket=self.s3_bucket, Key=obj["Key"])
                        data = json.loads(response["Body"].read().decode("utf-8"))
                        watermarks.append(Watermark.from_dict(data))
                    except Exception as e:
                        logger.warning("Could not load watermark from %s: %s", obj['Key'], e)

        return watermarks


def build_watermark_store(cfg: Dict[str, Any]) -> WatermarkStore:
    """Build a WatermarkStore from config.

    Args:
        cfg: Full pipeline config dictionary

    Returns:
        Configured WatermarkStore instance
    """
    platform = cfg.get("platform", {})
    bronze = platform.get("bronze", {})
    storage_backend = bronze.get("storage_backend", "local")

    if storage_backend == "s3":
        return WatermarkStore(
            storage_backend="s3",
            s3_bucket=bronze.get("s3_bucket"),
            s3_prefix=bronze.get("s3_prefix", "") + "/_watermarks",
        )
    else:
        local_path = bronze.get("local_path")
        if local_path:
            return WatermarkStore(
                storage_backend="local",
                local_path=Path(local_path) / "_watermarks",
            )
        return WatermarkStore(storage_backend="local")


def compute_max_watermark(
    records: list[Dict[str, Any]],
    watermark_column: str,
    current_watermark: Optional[str] = None,
) -> Optional[str]:
    """Compute maximum watermark value from records.

    Args:
        records: List of records to scan
        watermark_column: Column to use for watermark
        current_watermark: Current watermark value (for comparison)

    Returns:
        Maximum watermark value found, or current_watermark if no higher value
    """
    if not records:
        return current_watermark

    max_value = current_watermark
    for record in records:
        value = record.get(watermark_column)
        if value is not None:
            value_str = str(value)
            if max_value is None or value_str > max_value:
                max_value = value_str

    return max_value
