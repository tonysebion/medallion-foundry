"""Quarantine handling for corrupted or invalid files.

This module provides utilities for quarantining files that fail integrity
checks, allowing the pipeline to continue while preserving evidence for
investigation.

Key functions:
- quarantine_corrupted_files: Move corrupted files to quarantine directory
"""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.primitives.time_utils import utc_isoformat

logger = logging.getLogger(__name__)


@dataclass
class QuarantineConfig:
    """Configuration for file quarantine behavior.

    Attributes:
        enabled: Whether quarantine is enabled (if False, files are not moved)
        quarantine_dir: Name of quarantine subdirectory (default: "_quarantine")
        write_manifest: Whether to write a manifest of quarantined files
    """

    enabled: bool = True
    quarantine_dir: str = "_quarantine"
    write_manifest: bool = True

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "QuarantineConfig":
        """Create from dictionary."""
        if not data:
            return cls()
        return cls(
            enabled=data.get("enabled", True),
            quarantine_dir=data.get("quarantine_dir", "_quarantine"),
            write_manifest=data.get("write_manifest", True),
        )


@dataclass
class QuarantineResult:
    """Result of a quarantine operation.

    Attributes:
        quarantined_files: List of files that were moved to quarantine
        failed_files: List of files that could not be quarantined
        reason: Reason for quarantine (e.g., "checksum_mismatch")
        timestamp: ISO timestamp of quarantine operation
        quarantine_path: Path to quarantine directory
    """

    quarantined_files: List[Path] = field(default_factory=list)
    failed_files: List[str] = field(default_factory=list)
    reason: str = ""
    timestamp: str = ""
    quarantine_path: Optional[Path] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "quarantined_files": [str(p) for p in self.quarantined_files],
            "failed_files": self.failed_files,
            "reason": self.reason,
            "timestamp": self.timestamp,
            "quarantine_path": str(self.quarantine_path) if self.quarantine_path else None,
        }

    @property
    def count(self) -> int:
        """Number of files successfully quarantined."""
        return len(self.quarantined_files)

    def __str__(self) -> str:
        return (
            f"QuarantineResult(quarantined={self.count}, "
            f"failed={len(self.failed_files)}, reason='{self.reason}')"
        )


def quarantine_corrupted_files(
    source_dir: Path,
    files: List[str],
    reason: str = "checksum_mismatch",
    config: Optional[QuarantineConfig] = None,
) -> QuarantineResult:
    """Move corrupted files to a quarantine directory.

    Files are moved (not copied) to a subdirectory within source_dir,
    preserving them for investigation while removing them from the
    active data path.

    Args:
        source_dir: Directory containing files to quarantine
        files: List of file names (relative to source_dir) to quarantine
        reason: Reason for quarantine, recorded in manifest
        config: Optional quarantine configuration

    Returns:
        QuarantineResult with details of the operation

    Example:
        >>> result = quarantine_corrupted_files(
        ...     bronze_path,
        ...     ["chunk_0001.parquet", "chunk_0002.parquet"],
        ...     reason="checksum_mismatch",
        ... )
        >>> print(f"Quarantined {result.count} files")
    """
    config = config or QuarantineConfig()
    timestamp = utc_isoformat()

    result = QuarantineResult(
        reason=reason,
        timestamp=timestamp,
    )

    if not config.enabled:
        logger.info("Quarantine disabled, skipping %d files", len(files))
        result.failed_files = files
        return result

    if not files:
        logger.debug("No files to quarantine")
        return result

    # Create quarantine directory
    quarantine_path = source_dir / config.quarantine_dir
    try:
        quarantine_path.mkdir(parents=True, exist_ok=True)
        result.quarantine_path = quarantine_path
    except OSError as exc:
        logger.error("Failed to create quarantine directory %s: %s", quarantine_path, exc)
        result.failed_files = files
        return result

    # Move files to quarantine
    for filename in files:
        src = source_dir / filename
        dst = quarantine_path / filename

        if not src.exists():
            logger.warning("File to quarantine does not exist: %s", src)
            result.failed_files.append(filename)
            continue

        try:
            # Handle case where destination already exists
            if dst.exists():
                # Add timestamp suffix to avoid overwriting
                stem = dst.stem
                suffix = dst.suffix
                dst = quarantine_path / f"{stem}_{timestamp.replace(':', '-')}{suffix}"

            shutil.move(str(src), str(dst))
            result.quarantined_files.append(dst)
            logger.warning("Quarantined corrupted file: %s -> %s", src, dst)
        except OSError as exc:
            logger.error("Failed to quarantine %s: %s", src, exc)
            result.failed_files.append(filename)

    # Write quarantine manifest
    if config.write_manifest and result.quarantined_files:
        manifest = {
            "timestamp": timestamp,
            "reason": reason,
            "files": [
                {
                    "original_name": f.name,
                    "quarantined_path": str(f),
                }
                for f in result.quarantined_files
            ],
            "source_directory": str(source_dir),
        }

        manifest_path = quarantine_path / "_quarantine_manifest.json"
        try:
            # Append to existing manifest if present
            if manifest_path.exists():
                with manifest_path.open("r", encoding="utf-8") as f:
                    existing = json.load(f)
                if isinstance(existing, list):
                    existing.append(manifest)
                    manifest = existing
                else:
                    manifest = [existing, manifest]

            with manifest_path.open("w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
            logger.debug("Wrote quarantine manifest to %s", manifest_path)
        except OSError as exc:
            logger.warning("Failed to write quarantine manifest: %s", exc)

    if result.quarantined_files:
        logger.info(
            "Quarantined %d files to %s (reason: %s)",
            len(result.quarantined_files),
            quarantine_path,
            reason,
        )

    return result
