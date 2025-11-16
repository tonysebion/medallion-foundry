"""Tests for streaming resume behavior using checkpoints.

These tests verify that Silver streaming can resume from checkpoints after
interruption without re-processing chunks.
"""

from core.checkpoint import CheckpointManager
from core.run_options import RunOptions
from core.patterns import LoadPattern


def test_checkpoint_skip_logic(tmp_path):
    """Test that checkpoint manager correctly identifies chunks to skip."""
    cp = CheckpointManager(tmp_path, enabled=True)

    # No checkpoint yet; nothing should be skipped
    assert not cp.should_skip_chunk("stream", 1)
    assert not cp.should_skip_chunk("stream", 5)

    # Save checkpoint at chunk 3
    cp.save_checkpoint("stream", last_chunk=3, total_records=300)

    # Chunks 1-3 should be skipped, 4+ should not
    assert cp.should_skip_chunk("stream", 1)
    assert cp.should_skip_chunk("stream", 2)
    assert cp.should_skip_chunk("stream", 3)
    assert not cp.should_skip_chunk("stream", 4)
    assert not cp.should_skip_chunk("stream", 5)


def test_checkpoint_clear(tmp_path):
    """Test that clearing checkpoint removes skip behavior."""
    cp = CheckpointManager(tmp_path, enabled=True)

    cp.save_checkpoint("stream", last_chunk=5, total_records=500)
    assert cp.should_skip_chunk("stream", 3)

    # Clear and verify
    cp.clear_checkpoint("stream")
    assert not cp.should_skip_chunk("stream", 3)


def test_checkpoint_disabled(tmp_path):
    """Test that disabled checkpoint manager is a no-op."""
    cp = CheckpointManager(tmp_path, enabled=False)

    # Save should not write files
    cp.save_checkpoint("stream", last_chunk=10, total_records=1000)

    # No skip behavior
    assert not cp.should_skip_chunk("stream", 5)

    # Verify no checkpoint files created
    checkpoint_dir = tmp_path / "_checkpoints"
    if checkpoint_dir.exists():
        assert len(list(checkpoint_dir.glob("*.json"))) == 0


def test_checkpoint_metadata(tmp_path):
    """Test that checkpoint stores and retrieves metadata."""
    cp = CheckpointManager(tmp_path, enabled=True)

    metadata = {"source": "test_bronze", "user": "test"}
    cp.save_checkpoint("stream", last_chunk=7, total_records=700, metadata=metadata)

    loaded = cp.load_checkpoint("stream")
    assert loaded is not None
    assert loaded["last_chunk"] == 7
    assert loaded["total_records"] == 700
    assert loaded["metadata"]["source"] == "test_bronze"
    assert loaded["metadata"]["user"] == "test"
    assert "timestamp" in loaded


def test_run_options_resume_flag():
    """Test that RunOptions properly stores resume flag."""
    opts = RunOptions(
        load_pattern=LoadPattern.FULL,
        require_checksum=False,
        write_parquet=True,
        write_csv=False,
        parquet_compression="snappy",
        resume=True,
    )

    assert opts.resume is True

    opts_no_resume = RunOptions(
        load_pattern=LoadPattern.FULL,
        require_checksum=False,
        write_parquet=True,
        write_csv=False,
        parquet_compression="snappy",
    )

    assert opts_no_resume.resume is False
