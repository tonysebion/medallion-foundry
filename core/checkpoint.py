"""Checkpoint metadata for resumable streaming operations.

Tracks completion state per artifact/chunk to enable safe resume after failures.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manage checkpoint state for streaming operations."""

    def __init__(self, output_dir: Path, enabled: bool = True):
        """Initialize checkpoint manager.

        Args:
            output_dir: Base output directory for artifacts
            enabled: Whether checkpointing is enabled
        """
        self.output_dir = Path(output_dir)
        self.enabled = enabled
        self.checkpoint_dir = self.output_dir / "_checkpoints"

        if self.enabled:
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def get_checkpoint_path(self, artifact_name: str) -> Path:
        """Get checkpoint file path for an artifact.

        Args:
            artifact_name: Name of the artifact (e.g., 'full_snapshot', 'current')

        Returns:
            Path to checkpoint JSON file
        """
        return self.checkpoint_dir / f"{artifact_name}.json"

    def save_checkpoint(
        self,
        artifact_name: str,
        last_chunk: int,
        total_records: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Save checkpoint state for an artifact.

        Args:
            artifact_name: Name of the artifact
            last_chunk: Last successfully written chunk number
            total_records: Total records written so far
            metadata: Additional metadata to store
        """
        if not self.enabled:
            return

        checkpoint_data = {
            "artifact_name": artifact_name,
            "last_chunk": last_chunk,
            "total_records": total_records,
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata or {},
        }

        checkpoint_path = self.get_checkpoint_path(artifact_name)
        try:
            with open(checkpoint_path, "w") as f:
                json.dump(checkpoint_data, f, indent=2)
            logger.debug(f"Saved checkpoint for {artifact_name}: chunk {last_chunk}")
        except Exception as exc:
            logger.warning(f"Failed to save checkpoint for {artifact_name}: {exc}")

    def load_checkpoint(self, artifact_name: str) -> Optional[Dict[str, Any]]:
        """Load checkpoint state for an artifact.

        Args:
            artifact_name: Name of the artifact

        Returns:
            Checkpoint data dict or None if not found/disabled
        """
        if not self.enabled:
            return None

        checkpoint_path = self.get_checkpoint_path(artifact_name)
        if not checkpoint_path.exists():
            return None

        try:
            with open(checkpoint_path, "r") as f:
                data = json.load(f)
            logger.info(
                f"Loaded checkpoint for {artifact_name}: "
                f"last_chunk={data.get('last_chunk')}, "
                f"total_records={data.get('total_records')}"
            )
            return data
        except Exception as exc:
            logger.warning(f"Failed to load checkpoint for {artifact_name}: {exc}")
            return None

    def clear_checkpoint(self, artifact_name: str) -> None:
        """Clear checkpoint for an artifact (e.g., on successful completion).

        Args:
            artifact_name: Name of the artifact
        """
        if not self.enabled:
            return

        checkpoint_path = self.get_checkpoint_path(artifact_name)
        if checkpoint_path.exists():
            try:
                checkpoint_path.unlink()
                logger.debug(f"Cleared checkpoint for {artifact_name}")
            except Exception as exc:
                logger.warning(f"Failed to clear checkpoint for {artifact_name}: {exc}")

    def clear_all_checkpoints(self) -> None:
        """Clear all checkpoints in the directory."""
        if not self.enabled or not self.checkpoint_dir.exists():
            return

        try:
            for checkpoint_file in self.checkpoint_dir.glob("*.json"):
                checkpoint_file.unlink()
            logger.info("Cleared all checkpoints")
        except Exception as exc:
            logger.warning(f"Failed to clear checkpoints: {exc}")

    def should_skip_chunk(self, artifact_name: str, chunk_number: int) -> bool:
        """Check if a chunk should be skipped based on checkpoint.

        Args:
            artifact_name: Name of the artifact
            chunk_number: Chunk number to check

        Returns:
            True if chunk was already written (based on checkpoint)
        """
        if not self.enabled:
            return False

        checkpoint = self.load_checkpoint(artifact_name)
        if not checkpoint:
            return False

        last_chunk = checkpoint.get("last_chunk", 0)
        return chunk_number <= last_chunk
