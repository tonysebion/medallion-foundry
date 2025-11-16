"""Tests for checkpoint metadata management."""

import json
from core.checkpoint import CheckpointManager


def test_checkpoint_manager_disabled(tmp_path):
    """Test that disabled checkpointing is a no-op."""
    manager = CheckpointManager(tmp_path, enabled=False)

    manager.save_checkpoint("test", 5, 100)
    checkpoint = manager.load_checkpoint("test")

    assert checkpoint is None
    assert not (manager.checkpoint_dir / "test.json").exists()


def test_save_and_load_checkpoint(tmp_path):
    """Test basic save and load."""
    manager = CheckpointManager(tmp_path, enabled=True)

    manager.save_checkpoint(
        artifact_name="full_snapshot",
        last_chunk=10,
        total_records=1000,
        metadata={"run_id": "test-123"},
    )

    checkpoint = manager.load_checkpoint("full_snapshot")
    assert checkpoint is not None
    assert checkpoint["artifact_name"] == "full_snapshot"
    assert checkpoint["last_chunk"] == 10
    assert checkpoint["total_records"] == 1000
    assert checkpoint["metadata"]["run_id"] == "test-123"
    assert "timestamp" in checkpoint


def test_checkpoint_path(tmp_path):
    """Test checkpoint file path construction."""
    manager = CheckpointManager(tmp_path)

    path = manager.get_checkpoint_path("current")
    assert path == tmp_path / "_checkpoints" / "current.json"


def test_load_nonexistent_checkpoint(tmp_path):
    """Test loading checkpoint that doesn't exist."""
    manager = CheckpointManager(tmp_path, enabled=True)

    checkpoint = manager.load_checkpoint("nonexistent")
    assert checkpoint is None


def test_clear_checkpoint(tmp_path):
    """Test clearing a specific checkpoint."""
    manager = CheckpointManager(tmp_path, enabled=True)

    manager.save_checkpoint("test", 5, 100)
    assert (manager.checkpoint_dir / "test.json").exists()

    manager.clear_checkpoint("test")
    assert not (manager.checkpoint_dir / "test.json").exists()


def test_clear_all_checkpoints(tmp_path):
    """Test clearing all checkpoints."""
    manager = CheckpointManager(tmp_path, enabled=True)

    manager.save_checkpoint("artifact1", 1, 10)
    manager.save_checkpoint("artifact2", 2, 20)
    manager.save_checkpoint("artifact3", 3, 30)

    assert len(list(manager.checkpoint_dir.glob("*.json"))) == 3

    manager.clear_all_checkpoints()
    assert len(list(manager.checkpoint_dir.glob("*.json"))) == 0


def test_should_skip_chunk(tmp_path):
    """Test chunk skip logic based on checkpoint."""
    manager = CheckpointManager(tmp_path, enabled=True)

    # No checkpoint = don't skip
    assert not manager.should_skip_chunk("test", 5)

    # Save checkpoint at chunk 10
    manager.save_checkpoint("test", 10, 1000)

    # Chunks <= 10 should be skipped
    assert manager.should_skip_chunk("test", 5)
    assert manager.should_skip_chunk("test", 10)

    # Chunks > 10 should not be skipped
    assert not manager.should_skip_chunk("test", 11)
    assert not manager.should_skip_chunk("test", 15)


def test_update_checkpoint(tmp_path):
    """Test updating an existing checkpoint."""
    manager = CheckpointManager(tmp_path, enabled=True)

    manager.save_checkpoint("test", 5, 100)
    first = manager.load_checkpoint("test")

    manager.save_checkpoint("test", 10, 200)
    second = manager.load_checkpoint("test")

    assert first["last_chunk"] == 5
    assert second["last_chunk"] == 10
    assert second["total_records"] == 200


def test_checkpoint_directory_creation(tmp_path):
    """Test that checkpoint directory is created automatically."""
    manager = CheckpointManager(tmp_path / "nested" / "path", enabled=True)

    manager.save_checkpoint("test", 1, 10)
    assert manager.checkpoint_dir.exists()
    assert manager.checkpoint_dir.is_dir()


def test_checkpoint_json_format(tmp_path):
    """Test checkpoint file is valid JSON."""
    manager = CheckpointManager(tmp_path, enabled=True)

    manager.save_checkpoint("test", 42, 1337, metadata={"key": "value"})

    checkpoint_file = manager.get_checkpoint_path("test")
    with open(checkpoint_file, "r") as f:
        data = json.load(f)

    assert data["artifact_name"] == "test"
    assert data["last_chunk"] == 42
    assert data["total_records"] == 1337
    assert data["metadata"]["key"] == "value"
