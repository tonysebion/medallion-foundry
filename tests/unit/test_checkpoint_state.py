"""Unit tests for checkpoint state management.

Tests cover:
- Checkpoint creation and serialization
- Checkpoint locking and conflict detection
- Checkpoint storage backends (local, S3)
- Watermark integration
"""

import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.foundation.state.checkpoint import (
    Checkpoint,
    CheckpointLock,
    CheckpointStatus,
    CheckpointStore,
    CheckpointConflictError,
    build_checkpoint_store,
)


class TestCheckpointLock:
    """Tests for CheckpointLock class."""

    def test_lock_creation(self):
        """Test creating a new lock."""
        lock = CheckpointLock.create("run-123", ttl_minutes=15)

        assert lock.holder_id == "run-123"
        assert lock.acquired_at is not None
        assert lock.expires_at is not None

    def test_lock_not_expired(self):
        """Test lock is not expired when within TTL."""
        lock = CheckpointLock.create("run-123", ttl_minutes=15)
        assert not lock.is_expired()

    def test_lock_expired(self):
        """Test lock is expired when past TTL."""
        now = datetime.now(timezone.utc)
        expired = now - timedelta(minutes=30)
        lock = CheckpointLock(
            holder_id="run-123",
            acquired_at=expired.isoformat() + "Z",
            expires_at=(expired + timedelta(minutes=15)).isoformat() + "Z",
        )
        assert lock.is_expired()

    def test_lock_serialization(self):
        """Test lock to_dict and from_dict."""
        lock = CheckpointLock.create("run-123", ttl_minutes=15)
        data = lock.to_dict()

        assert "holder_id" in data
        assert "acquired_at" in data
        assert "expires_at" in data

        restored = CheckpointLock.from_dict(data)
        assert restored.holder_id == lock.holder_id
        assert restored.acquired_at == lock.acquired_at
        assert restored.expires_at == lock.expires_at


class TestCheckpoint:
    """Tests for Checkpoint class."""

    def test_checkpoint_creation(self):
        """Test creating a new checkpoint."""
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
        )

        assert checkpoint.checkpoint_id == "cp-123"
        assert checkpoint.source_key == "system.table"
        assert checkpoint.status == CheckpointStatus.PENDING

    def test_checkpoint_auto_id(self):
        """Test checkpoint generates ID if not provided."""
        checkpoint = Checkpoint(
            checkpoint_id="",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
        )
        assert checkpoint.checkpoint_id  # Should be auto-generated UUID

    def test_checkpoint_start(self):
        """Test starting a checkpoint with lock."""
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
        )
        checkpoint.start(lock_holder_id="run-456")

        assert checkpoint.status == CheckpointStatus.IN_PROGRESS
        assert checkpoint.started_at is not None
        assert checkpoint.lock is not None
        assert checkpoint.lock.holder_id == "run-456"

    def test_checkpoint_complete(self):
        """Test completing a checkpoint."""
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
        )
        checkpoint.start(lock_holder_id="run-456")
        checkpoint.complete(
            record_count=1000,
            chunk_count=5,
            artifact_count=7,
            watermark_value="2025-01-15T23:59:59Z",
        )

        assert checkpoint.status == CheckpointStatus.COMPLETED
        assert checkpoint.record_count == 1000
        assert checkpoint.chunk_count == 5
        assert checkpoint.artifact_count == 7
        assert checkpoint.watermark_value == "2025-01-15T23:59:59Z"
        assert checkpoint.lock is None  # Lock released

    def test_checkpoint_fail(self):
        """Test failing a checkpoint."""
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
        )
        checkpoint.start(lock_holder_id="run-456")
        checkpoint.fail("Connection timeout", partial=False)

        assert checkpoint.status == CheckpointStatus.FAILED
        assert checkpoint.error_message == "Connection timeout"
        assert checkpoint.lock is None

    def test_checkpoint_partial_fail(self):
        """Test partial failure with some chunks written."""
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
        )
        checkpoint.start(lock_holder_id="run-456")
        checkpoint.chunk_count = 3  # Some chunks written
        checkpoint.fail("Disk full", partial=True)

        assert checkpoint.status == CheckpointStatus.PARTIAL

    def test_checkpoint_serialization(self):
        """Test checkpoint to_dict and from_dict."""
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
            watermark_column="updated_at",
            watermark_type="timestamp",
        )
        checkpoint.start(lock_holder_id="run-456")

        data = checkpoint.to_dict()
        restored = Checkpoint.from_dict(data)

        assert restored.checkpoint_id == checkpoint.checkpoint_id
        assert restored.source_key == checkpoint.source_key
        assert restored.status == checkpoint.status
        assert restored.lock is not None
        assert checkpoint.lock is not None
        restored_lock = restored.lock
        original_lock = checkpoint.lock
        assert restored_lock.holder_id == original_lock.holder_id

    def test_is_completed(self):
        """Test is_completed helper."""
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="path",
        )
        assert not checkpoint.is_completed()

        checkpoint.status = CheckpointStatus.COMPLETED
        assert checkpoint.is_completed()

    def test_is_in_progress_with_active_lock(self):
        """Test is_in_progress with active lock."""
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="path",
        )
        checkpoint.start(lock_holder_id="run-456")

        assert checkpoint.is_in_progress()

    def test_is_in_progress_with_expired_lock(self):
        """Test is_in_progress returns False with expired lock."""
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="path",
        )
        checkpoint.status = CheckpointStatus.IN_PROGRESS
        # Create expired lock
        now = datetime.now(timezone.utc)
        expired = now - timedelta(minutes=30)
        checkpoint.lock = CheckpointLock(
            holder_id="run-456",
            acquired_at=expired.isoformat() + "Z",
            expires_at=(expired + timedelta(minutes=15)).isoformat() + "Z",
        )

        assert not checkpoint.is_in_progress()


class TestCheckpointStore:
    """Tests for CheckpointStore class."""

    def test_store_initialization_local(self, tmp_path):
        """Test local store initialization."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        assert store.storage_backend == "local"
        assert store.local_path.exists()

    def test_save_and_get_checkpoint(self, tmp_path):
        """Test saving and retrieving a checkpoint."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
        )

        store.save(checkpoint)
        loaded = store.get(checkpoint.partition_path)

        assert loaded is not None
        assert loaded.checkpoint_id == checkpoint.checkpoint_id
        assert loaded.source_key == checkpoint.source_key

    def test_get_nonexistent_checkpoint(self, tmp_path):
        """Test getting a nonexistent checkpoint returns None."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        result = store.get("nonexistent/path")
        assert result is None

    def test_delete_checkpoint(self, tmp_path):
        """Test deleting a checkpoint."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
        )
        store.save(checkpoint)

        deleted = store.delete(checkpoint.partition_path)
        assert deleted is True

        loaded = store.get(checkpoint.partition_path)
        assert loaded is None

    def test_acquire_lock_new_checkpoint(self, tmp_path):
        """Test acquiring lock creates new checkpoint."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        checkpoint = store.acquire_lock(
            partition_path="system=sys/table=tbl/dt=2025-01-15",
            source_key="system.table",
            run_id="run-123",
            run_date="2025-01-15",
        )

        assert checkpoint.status == CheckpointStatus.IN_PROGRESS
        assert checkpoint.lock is not None
        assert checkpoint.lock.holder_id == "run-123"

    def test_acquire_lock_conflict_completed(self, tmp_path):
        """Test acquiring lock fails if checkpoint already completed."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        # Create completed checkpoint
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
            status=CheckpointStatus.COMPLETED,
        )
        store.save(checkpoint)

        with pytest.raises(CheckpointConflictError) as exc_info:
            store.acquire_lock(
                partition_path="system=sys/table=tbl/dt=2025-01-15",
                source_key="system.table",
                run_id="run-789",
                run_date="2025-01-15",
            )
        assert "already has completed checkpoint" in str(exc_info.value)

    def test_acquire_lock_conflict_active(self, tmp_path):
        """Test acquiring lock fails if another process holds it."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        # Create checkpoint with active lock
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
        )
        checkpoint.start(lock_holder_id="run-456")
        store.save(checkpoint)

        with pytest.raises(CheckpointConflictError) as exc_info:
            store.acquire_lock(
                partition_path="system=sys/table=tbl/dt=2025-01-15",
                source_key="system.table",
                run_id="run-789",
                run_date="2025-01-15",
            )
        assert "has active lock" in str(exc_info.value)

    def test_acquire_lock_takeover_expired(self, tmp_path):
        """Test acquiring lock succeeds if existing lock expired."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        # Create checkpoint with expired lock
        checkpoint = Checkpoint(
            checkpoint_id="cp-123",
            source_key="system.table",
            run_id="run-456",
            run_date="2025-01-15",
            partition_path="system=sys/table=tbl/dt=2025-01-15",
            status=CheckpointStatus.IN_PROGRESS,
        )
        now = datetime.now(timezone.utc)
        expired = now - timedelta(minutes=30)
        checkpoint.lock = CheckpointLock(
            holder_id="run-456",
            acquired_at=expired.isoformat() + "Z",
            expires_at=(expired + timedelta(minutes=15)).isoformat() + "Z",
        )
        store.save(checkpoint)

        # Should succeed - expired lock
        new_checkpoint = store.acquire_lock(
            partition_path="system=sys/table=tbl/dt=2025-01-15",
            source_key="system.table",
            run_id="run-789",
            run_date="2025-01-15",
        )
        assert new_checkpoint.run_id == "run-789"
        assert new_checkpoint.lock is not None
        assert new_checkpoint.lock.holder_id == "run-789"

    def test_release_lock_success(self, tmp_path):
        """Test releasing lock with success."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        store.acquire_lock(
            partition_path="system=sys/table=tbl/dt=2025-01-15",
            source_key="system.table",
            run_id="run-123",
            run_date="2025-01-15",
        )

        released = store.release_lock(
            partition_path="system=sys/table=tbl/dt=2025-01-15",
            run_id="run-123",
            success=True,
            record_count=1000,
            chunk_count=5,
            artifact_count=7,
            watermark_value="2025-01-15T23:59:59Z",
        )

        assert released is not None
        assert released.status == CheckpointStatus.COMPLETED
        assert released.record_count == 1000
        assert released.watermark_value == "2025-01-15T23:59:59Z"

    def test_release_lock_failure(self, tmp_path):
        """Test releasing lock with failure."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        store.acquire_lock(
            partition_path="system=sys/table=tbl/dt=2025-01-15",
            source_key="system.table",
            run_id="run-123",
            run_date="2025-01-15",
        )

        released = store.release_lock(
            partition_path="system=sys/table=tbl/dt=2025-01-15",
            run_id="run-123",
            success=False,
            error_message="Connection timeout",
        )

        assert released is not None
        assert released.status == CheckpointStatus.FAILED
        assert released.error_message == "Connection timeout"

    def test_list_checkpoints(self, tmp_path):
        """Test listing all checkpoints."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        # Create multiple checkpoints
        for i in range(3):
            checkpoint = Checkpoint(
                checkpoint_id=f"cp-{i}",
                source_key="system.table",
                run_id=f"run-{i}",
                run_date="2025-01-15",
                partition_path=f"system=sys/table=tbl/dt=2025-01-{15+i}",
            )
            store.save(checkpoint)

        checkpoints = store.list_checkpoints()
        assert len(checkpoints) == 3

    def test_list_checkpoints_filtered(self, tmp_path):
        """Test listing checkpoints filtered by source."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        # Create checkpoints for different sources
        for source in ["system1.table1", "system1.table2", "system2.table1"]:
            checkpoint = Checkpoint(
                checkpoint_id=f"cp-{source}",
                source_key=source,
                run_id="run-1",
                run_date="2025-01-15",
                partition_path=f"path/{source}",
            )
            store.save(checkpoint)

        checkpoints = store.list_checkpoints(source_key="system1.table1")
        assert len(checkpoints) == 1
        assert checkpoints[0].source_key == "system1.table1"

    def test_cleanup_old_checkpoints(self, tmp_path):
        """Test cleaning up old checkpoints."""
        store = CheckpointStore(
            storage_backend="local",
            local_path=tmp_path / "checkpoints",
        )
        # Create multiple completed checkpoints
        for i in range(5):
            checkpoint = Checkpoint(
                checkpoint_id=f"cp-{i}",
                source_key="system.table",
                run_id=f"run-{i}",
                run_date="2025-01-15",
                partition_path=f"system=sys/table=tbl/dt=2025-01-{15+i}",
                status=CheckpointStatus.COMPLETED,
                completed_at=f"2025-01-{15+i}T10:00:00Z",
            )
            store.save(checkpoint)

        deleted = store.cleanup_old_checkpoints("system.table", keep_count=2)
        assert deleted == 3

        remaining = store.list_checkpoints()
        assert len(remaining) == 2


class TestBuildCheckpointStore:
    """Tests for build_checkpoint_store factory."""

    def test_build_local_store_default(self):
        """Test building local store with defaults."""
        cfg = {"platform": {"bronze": {"storage_backend": "local"}}}
        store = build_checkpoint_store(cfg)

        assert store.storage_backend == "local"
        assert store.local_path == Path(".state/checkpoints")

    def test_build_local_store_custom_path(self):
        """Test building local store with custom path."""
        cfg = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": "/custom/path",
                }
            }
        }
        store = build_checkpoint_store(cfg)

        assert store.storage_backend == "local"
        assert store.local_path == Path("/custom/path/_checkpoints")

    def test_build_s3_store(self):
        """Test building S3 store."""
        cfg = {
            "platform": {
                "bronze": {
                    "storage_backend": "s3",
                    "s3_bucket": "my-bucket",
                    "s3_prefix": "bronze",
                }
            }
        }
        store = build_checkpoint_store(cfg)

        assert store.storage_backend == "s3"
        assert store.s3_bucket == "my-bucket"
        assert store.s3_prefix == "bronze/_checkpoints"

    def test_build_azure_store(self):
        """Test building Azure store."""
        cfg = {
            "platform": {
                "bronze": {
                    "storage_backend": "azure",
                    "azure_container": "my-container",
                    "azure_prefix": "bronze",
                }
            }
        }
        store = build_checkpoint_store(cfg)

        assert store.storage_backend == "azure"
        assert store.azure_container == "my-container"
        assert store.azure_prefix == "bronze/_checkpoints"
