"""Checkpoint state management for extraction resumption.

Checkpoints track the state of extraction runs to enable:
- Resumption from failed extractions
- Idempotent re-runs (detect already-completed partitions)
- Conflict detection (multiple concurrent runs)

Checkpoint file structure:
```json
{
  "checkpoint_id": "uuid...",
  "source_key": "system.table",
  "run_id": "uuid...",
  "run_date": "2025-01-15",
  "partition_path": "system=sys/table=tbl/dt=2025-01-15",
  "status": "completed",
  "watermark": {
    "column": "updated_at",
    "value": "2025-01-15T10:30:00Z",
    "type": "timestamp"
  },
  "stats": {
    "record_count": 1000,
    "chunk_count": 5,
    "artifact_count": 7
  },
  "started_at": "2025-01-15T10:00:00Z",
  "completed_at": "2025-01-15T10:30:00Z",
  "lock": {
    "holder_id": "uuid...",
    "acquired_at": "2025-01-15T10:00:00Z",
    "expires_at": "2025-01-15T10:15:00Z"
  }
}
```
"""

from __future__ import annotations

__all__ = [
    "CheckpointStatus",
    "Checkpoint",
    "CheckpointStore",
    "CheckpointLock",
    "CheckpointConflictError",
    "build_checkpoint_store",
]

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.foundation.time_utils import utc_isoformat as _utc_isoformat
from core.foundation.primitives.base import RichEnumMixin
from core.foundation.state.storage import StateStorageBackend, sanitize_path_for_filename

logger = logging.getLogger(__name__)


class CheckpointConflictError(Exception):
    """Raised when checkpoint lock conflict is detected."""

    def __init__(self, message: str, existing_lock: Optional["CheckpointLock"] = None):
        super().__init__(message)
        self.existing_lock = existing_lock


class CheckpointStatus(RichEnumMixin, str, Enum):
    """Status of a checkpoint."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


# RichEnumMixin class variables must be set AFTER class definition
CheckpointStatus._default = "PENDING"
CheckpointStatus._descriptions = {
    "pending": "Checkpoint created but extraction not started",
    "in_progress": "Extraction currently running",
    "completed": "Extraction completed successfully",
    "failed": "Extraction failed with error",
    "partial": "Extraction partially completed (some chunks written)",
}


@dataclass
class CheckpointLock:
    """Lock for concurrent run detection."""

    holder_id: str
    acquired_at: str
    expires_at: str

    def is_expired(self) -> bool:
        """Check if lock has expired."""
        try:
            expires = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
            now = datetime.now(expires.tzinfo)
            return now > expires
        except (ValueError, TypeError):
            return True

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary."""
        return {
            "holder_id": self.holder_id,
            "acquired_at": self.acquired_at,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CheckpointLock":
        """Create from dictionary."""
        return cls(
            holder_id=data["holder_id"],
            acquired_at=data["acquired_at"],
            expires_at=data["expires_at"],
        )

    @classmethod
    def create(cls, holder_id: str, ttl_minutes: int = 15) -> "CheckpointLock":
        """Create a new lock with TTL."""
        now = datetime.now(timezone.utc)
        expires = now + timedelta(minutes=ttl_minutes)
        acquired_iso = now.isoformat().replace("+00:00", "Z")
        expires_iso = expires.isoformat().replace("+00:00", "Z")
        return cls(
            holder_id=holder_id,
            acquired_at=acquired_iso,
            expires_at=expires_iso,
        )


@dataclass
class Checkpoint:
    """Checkpoint state for an extraction run."""

    checkpoint_id: str
    source_key: str
    run_id: str
    run_date: str
    partition_path: str
    status: CheckpointStatus = CheckpointStatus.PENDING
    watermark_column: Optional[str] = None
    watermark_value: Optional[str] = None
    watermark_type: str = "timestamp"
    record_count: int = 0
    chunk_count: int = 0
    artifact_count: int = 0
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    lock: Optional[CheckpointLock] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.checkpoint_id:
            self.checkpoint_id = str(uuid.uuid4())

    def start(self, lock_holder_id: Optional[str] = None) -> "Checkpoint":
        """Mark checkpoint as in progress."""
        self.status = CheckpointStatus.IN_PROGRESS
        self.started_at = _utc_isoformat()
        if lock_holder_id:
            self.lock = CheckpointLock.create(lock_holder_id)
        return self

    def complete(
        self,
        record_count: int,
        chunk_count: int,
        artifact_count: int,
        watermark_value: Optional[str] = None,
    ) -> "Checkpoint":
        """Mark checkpoint as completed."""
        self.status = CheckpointStatus.COMPLETED
        self.completed_at = _utc_isoformat()
        self.record_count = record_count
        self.chunk_count = chunk_count
        self.artifact_count = artifact_count
        if watermark_value:
            self.watermark_value = watermark_value
        self.lock = None  # Release lock
        return self

    def fail(self, error_message: str, partial: bool = False) -> "Checkpoint":
        """Mark checkpoint as failed."""
        self.status = CheckpointStatus.PARTIAL if partial else CheckpointStatus.FAILED
        self.completed_at = _utc_isoformat()
        self.error_message = error_message
        self.lock = None  # Release lock
        return self

    def is_completed(self) -> bool:
        """Check if checkpoint is in completed state."""
        return self.status == CheckpointStatus.COMPLETED

    def is_in_progress(self) -> bool:
        """Check if extraction is currently running."""
        if self.status != CheckpointStatus.IN_PROGRESS:
            return False
        # Check lock expiration
        if self.lock and not self.lock.is_expired():
            return True
        return False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "checkpoint_id": self.checkpoint_id,
            "source_key": self.source_key,
            "run_id": self.run_id,
            "run_date": self.run_date,
            "partition_path": self.partition_path,
            "status": self.status.value,
            "watermark": {
                "column": self.watermark_column,
                "value": self.watermark_value,
                "type": self.watermark_type,
            },
            "stats": {
                "record_count": self.record_count,
                "chunk_count": self.chunk_count,
                "artifact_count": self.artifact_count,
            },
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }
        if self.error_message:
            result["error_message"] = self.error_message
        if self.lock:
            result["lock"] = self.lock.to_dict()
        if self.extra:
            result["extra"] = self.extra
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Checkpoint":
        """Create from dictionary."""
        status_str = data.get("status", "pending")
        try:
            status = CheckpointStatus(status_str)
        except ValueError:
            status = CheckpointStatus.PENDING

        watermark = data.get("watermark", {})
        stats = data.get("stats", {})

        lock = None
        if "lock" in data and data["lock"]:
            lock = CheckpointLock.from_dict(data["lock"])

        return cls(
            checkpoint_id=data.get("checkpoint_id", ""),
            source_key=data["source_key"],
            run_id=data["run_id"],
            run_date=data["run_date"],
            partition_path=data["partition_path"],
            status=status,
            watermark_column=watermark.get("column"),
            watermark_value=watermark.get("value"),
            watermark_type=watermark.get("type", "timestamp"),
            record_count=stats.get("record_count", 0),
            chunk_count=stats.get("chunk_count", 0),
            artifact_count=stats.get("artifact_count", 0),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            error_message=data.get("error_message"),
            lock=lock,
            extra=data.get("extra", {}),
        )


class CheckpointStore(StateStorageBackend):
    """Centralized checkpoint storage.

    Supports local filesystem and S3 storage backends. Inherits common
    storage operations from StateStorageBackend.

    Checkpoints are stored per partition path to track extraction status
    and enable resumption.
    """

    def __init__(
        self,
        storage_backend: str = "local",
        local_path: Optional[Path] = None,
        s3_bucket: Optional[str] = None,
        s3_prefix: str = "_checkpoints",
        azure_container: Optional[str] = None,
        azure_prefix: str = "_checkpoints",
    ):
        """Initialize checkpoint store.

        Args:
            storage_backend: Storage backend ("local", "s3", or "azure")
            local_path: Local directory for checkpoint files
            s3_bucket: S3 bucket for checkpoint files
            s3_prefix: S3 prefix for checkpoint files
            azure_container: Azure container for checkpoint files
            azure_prefix: Azure prefix for checkpoint files
        """
        self.storage_backend = storage_backend
        self.local_path = local_path or Path(".state/checkpoints")
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.azure_container = azure_container
        self.azure_prefix = azure_prefix

        # Create local directory if needed
        if self.storage_backend == "local":
            self.local_path.mkdir(parents=True, exist_ok=True)

    def _get_local_path(self, partition_path: str) -> Path:
        """Get local file path for a checkpoint."""
        safe_path = sanitize_path_for_filename(partition_path)
        return self.local_path / f"{safe_path}_checkpoint.json"

    def _get_s3_key(self, partition_path: str) -> str:
        """Get S3 key for a checkpoint."""
        safe_path = sanitize_path_for_filename(partition_path)
        return f"{self.s3_prefix}/{safe_path}_checkpoint.json"

    def _get_azure_path(self, partition_path: str) -> str:
        """Get Azure path for a checkpoint."""
        safe_path = sanitize_path_for_filename(partition_path)
        return f"{self.azure_prefix}/{safe_path}_checkpoint.json"

    def get(self, partition_path: str) -> Optional[Checkpoint]:
        """Get checkpoint for a partition path.

        Args:
            partition_path: Hive-style partition path (e.g., "system=sys/table=tbl/dt=2025-01-15")

        Returns:
            Checkpoint object if found, None otherwise
        """
        try:
            data = self._dispatch_storage_operation(
                local_op=lambda: self._load_json_local(
                    self._get_local_path(partition_path)
                ),
                s3_op=lambda: self._load_json_s3(
                    self.s3_bucket or "", self._get_s3_key(partition_path)
                ),
            )
            return Checkpoint.from_dict(data)
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.warning(
                "Error loading checkpoint for %s: %s", partition_path, e
            )
            return None

    def save(self, checkpoint: Checkpoint) -> None:
        """Save checkpoint to storage."""
        data = checkpoint.to_dict()
        self._dispatch_storage_operation(
            local_op=lambda: self._save_json_local(
                self._get_local_path(checkpoint.partition_path), data
            ),
            s3_op=lambda: self._save_json_s3(
                self.s3_bucket or "",
                self._get_s3_key(checkpoint.partition_path),
                data,
            ),
        )

    def delete(self, partition_path: str) -> bool:
        """Delete checkpoint for a partition.

        Returns:
            True if checkpoint was deleted, False if not found
        """
        try:
            return self._dispatch_storage_operation(
                local_op=lambda: self._delete_local(
                    self._get_local_path(partition_path)
                ),
                s3_op=lambda: self._delete_s3(
                    self.s3_bucket or "", self._get_s3_key(partition_path)
                ),
            )
        except Exception as e:
            logger.warning("Error deleting checkpoint for %s: %s", partition_path, e)
            return False

    def acquire_lock(
        self,
        partition_path: str,
        source_key: str,
        run_id: str,
        run_date: str,
        watermark_column: Optional[str] = None,
        watermark_type: str = "timestamp",
        ttl_minutes: int = 15,
    ) -> Checkpoint:
        """Acquire lock for a partition and create/update checkpoint.

        Args:
            partition_path: Hive-style partition path
            source_key: Source identifier (system.table)
            run_id: Current run ID
            run_date: Current run date
            watermark_column: Column used for watermark tracking
            watermark_type: Type of watermark value
            ttl_minutes: Lock TTL in minutes

        Returns:
            Checkpoint with lock acquired

        Raises:
            CheckpointConflictError: If lock is held by another process
        """
        existing = self.get(partition_path)

        if existing:
            # Check if already completed
            if existing.is_completed():
                raise CheckpointConflictError(
                    f"Partition {partition_path} already has completed checkpoint "
                    f"from run {existing.run_id}",
                    existing.lock,
                )

            # Check if lock is held by another process
            if existing.is_in_progress():
                raise CheckpointConflictError(
                    f"Partition {partition_path} has active lock held by "
                    f"{existing.lock.holder_id if existing.lock else 'unknown'}",
                    existing.lock,
                )

            # Existing checkpoint with expired lock - take over
            logger.info(
                "Taking over checkpoint for %s (previous lock expired)",
                partition_path,
            )
            checkpoint = existing
            checkpoint.run_id = run_id
            checkpoint.start(lock_holder_id=run_id)
        else:
            # Create new checkpoint
            checkpoint = Checkpoint(
                checkpoint_id=str(uuid.uuid4()),
                source_key=source_key,
                run_id=run_id,
                run_date=run_date,
                partition_path=partition_path,
                watermark_column=watermark_column,
                watermark_type=watermark_type,
            )
            checkpoint.start(lock_holder_id=run_id)

        self.save(checkpoint)
        return checkpoint

    def release_lock(
        self,
        partition_path: str,
        run_id: str,
        success: bool = True,
        record_count: int = 0,
        chunk_count: int = 0,
        artifact_count: int = 0,
        watermark_value: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> Optional[Checkpoint]:
        """Release lock and update checkpoint status.

        Args:
            partition_path: Hive-style partition path
            run_id: Current run ID (must match lock holder)
            success: Whether extraction succeeded
            record_count: Number of records extracted
            chunk_count: Number of chunks written
            artifact_count: Number of artifacts created
            watermark_value: Final watermark value
            error_message: Error message if failed

        Returns:
            Updated checkpoint, or None if checkpoint not found
        """
        checkpoint = self.get(partition_path)
        if not checkpoint:
            logger.warning("No checkpoint found for %s", partition_path)
            return None

        # Verify lock holder
        if checkpoint.lock and checkpoint.lock.holder_id != run_id:
            logger.warning(
                "Lock holder mismatch for %s: expected %s, got %s",
                partition_path,
                checkpoint.lock.holder_id if checkpoint.lock else "none",
                run_id,
            )

        if success:
            checkpoint.complete(
                record_count=record_count,
                chunk_count=chunk_count,
                artifact_count=artifact_count,
                watermark_value=watermark_value,
            )
        else:
            checkpoint.fail(
                error_message=error_message or "Unknown error",
                partial=chunk_count > 0,
            )

        self.save(checkpoint)
        return checkpoint

    def list_checkpoints(
        self, source_key: Optional[str] = None
    ) -> List[Checkpoint]:
        """List all checkpoints, optionally filtered by source.

        Args:
            source_key: Optional source filter (e.g., "system.table")

        Returns:
            List of checkpoints
        """
        checkpoints = self._dispatch_storage_operation(
            local_op=self._list_checkpoints_local,
            s3_op=self._list_checkpoints_s3,
        )

        if source_key:
            checkpoints = [c for c in checkpoints if c.source_key == source_key]

        return checkpoints

    def _list_checkpoints_local(self) -> List[Checkpoint]:
        """List all checkpoints from local files."""
        checkpoints = []
        for path in self._list_json_local(self.local_path, "*_checkpoint.json"):
            try:
                data = self._load_json_local(path)
                checkpoints.append(Checkpoint.from_dict(data))
            except Exception as e:
                logger.warning("Could not load checkpoint from %s: %s", path, e)
        return checkpoints

    def _list_checkpoints_s3(self) -> List[Checkpoint]:
        """List all checkpoints from S3."""
        checkpoints = []
        keys = self._list_json_s3(
            self.s3_bucket or "", self.s3_prefix, "_checkpoint.json"
        )
        for key in keys:
            data = self._load_json_s3_by_key(self.s3_bucket or "", key)
            if data:
                checkpoints.append(Checkpoint.from_dict(data))
        return checkpoints

    def cleanup_old_checkpoints(
        self, source_key: str, keep_count: int = 10
    ) -> int:
        """Clean up old checkpoints for a source, keeping the most recent.

        Args:
            source_key: Source identifier (system.table)
            keep_count: Number of checkpoints to keep

        Returns:
            Number of checkpoints deleted
        """
        checkpoints = self.list_checkpoints(source_key)

        # Sort by completion time (newest first)
        checkpoints.sort(
            key=lambda c: c.completed_at or c.started_at or "",
            reverse=True,
        )

        # Keep only completed checkpoints for cleanup consideration
        completed = [c for c in checkpoints if c.is_completed()]

        if len(completed) <= keep_count:
            return 0

        deleted = 0
        for checkpoint in completed[keep_count:]:
            if self.delete(checkpoint.partition_path):
                deleted += 1
                logger.info(
                    "Deleted old checkpoint for %s", checkpoint.partition_path
                )

        return deleted


def build_checkpoint_store(cfg: Dict[str, Any]) -> CheckpointStore:
    """Build a CheckpointStore from config.

    Args:
        cfg: Full pipeline config dictionary

    Returns:
        Configured CheckpointStore instance
    """
    platform = cfg.get("platform", {})
    bronze = platform.get("bronze", {})
    storage_backend = bronze.get("storage_backend", "local")

    if storage_backend == "s3":
        return CheckpointStore(
            storage_backend="s3",
            s3_bucket=bronze.get("s3_bucket"),
            s3_prefix=bronze.get("s3_prefix", "") + "/_checkpoints",
        )
    elif storage_backend == "azure":
        return CheckpointStore(
            storage_backend="azure",
            azure_container=bronze.get("azure_container"),
            azure_prefix=bronze.get("azure_prefix", "") + "/_checkpoints",
        )
    else:
        local_path = bronze.get("local_path")
        if local_path:
            return CheckpointStore(
                storage_backend="local",
                local_path=Path(local_path) / "_checkpoints",
            )
        return CheckpointStore(storage_backend="local")
