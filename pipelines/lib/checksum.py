"""Checksum utilities for data integrity verification.

Provides SHA256 hashing and checksum manifest generation for
pipeline outputs to enable data integrity verification.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "ChecksumManifest",
    "ChecksumVerificationResult",
    "ChecksumValidationError",
    "compute_file_sha256",
    "write_checksum_manifest",
    "verify_checksum_manifest",
    "validate_bronze_checksums",
]


class ChecksumValidationError(Exception):
    """Raised when checksum validation fails and validation_mode is 'strict'."""

    def __init__(
        self,
        message: str,
        result: Optional["ChecksumVerificationResult"] = None,
    ) -> None:
        super().__init__(message)
        self.result = result


@dataclass
class ChecksumManifest:
    """Checksum manifest for a set of data files."""

    timestamp: str
    files: List[Dict[str, Any]]
    entity_kind: Optional[str] = None
    history_mode: Optional[str] = None
    row_count: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChecksumManifest":
        """Create from dictionary."""
        return cls(
            timestamp=data.get("timestamp", ""),
            files=data.get("files", []),
            entity_kind=data.get("entity_kind"),
            history_mode=data.get("history_mode"),
            row_count=data.get("row_count", 0),
            extra=data.get("extra", {}),
        )

    @classmethod
    def from_file(cls, path: Path) -> "ChecksumManifest":
        """Load manifest from file."""
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


@dataclass
class ChecksumVerificationResult:
    """Result of checksum verification."""

    valid: bool
    verified_files: List[str] = field(default_factory=list)
    missing_files: List[str] = field(default_factory=list)
    mismatched_files: List[str] = field(default_factory=list)
    manifest: Optional[ChecksumManifest] = None
    verification_time_ms: float = 0.0

    def __str__(self) -> str:
        status = "VALID" if self.valid else "INVALID"
        parts = [f"ChecksumVerification({status}"]
        parts.append(f"verified={len(self.verified_files)}")
        if self.missing_files:
            parts.append(f"missing={len(self.missing_files)}")
        if self.mismatched_files:
            parts.append(f"mismatched={len(self.mismatched_files)}")
        parts.append(f"time={self.verification_time_ms:.1f}ms)")
        return ", ".join(parts)


def compute_file_sha256(path: Path) -> str:
    """Compute SHA256 hash of a file.

    Reads file in 1MB chunks to handle large files efficiently.

    Args:
        path: File path to hash

    Returns:
        Hexadecimal digest string
    """
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_checksum_manifest(
    out_dir: Path,
    files: List[Path],
    *,
    entity_kind: Optional[str] = None,
    history_mode: Optional[str] = None,
    row_count: int = 0,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Path:
    """Write a checksum manifest for data files.

    Creates a _checksums.json file containing timestamps, file hashes,
    and sizes for data integrity verification.

    Args:
        out_dir: Directory to write manifest to
        files: List of file paths to include in manifest
        entity_kind: Entity kind (state/event) for metadata
        history_mode: History mode (current_only/full_history)
        row_count: Total row count across files
        extra_metadata: Optional additional metadata to include

    Returns:
        Path to the created manifest file

    Example:
        >>> manifest_path = write_checksum_manifest(
        ...     Path("./silver/orders/"),
        ...     [Path("./silver/orders/data.parquet")],
        ...     entity_kind="state",
        ...     history_mode="current_only",
        ...     row_count=1000,
        ... )
    """
    now = datetime.now(timezone.utc).isoformat()

    file_entries = []
    for file_path in files:
        if not file_path.exists():
            logger.warning("File not found for checksum: %s", file_path)
            continue

        file_entries.append({
            "path": file_path.name,
            "size_bytes": file_path.stat().st_size,
            "sha256": compute_file_sha256(file_path),
        })

    manifest = ChecksumManifest(
        timestamp=now,
        files=file_entries,
        entity_kind=entity_kind,
        history_mode=history_mode,
        row_count=row_count,
        extra=extra_metadata or {},
    )

    manifest_path = out_dir / "_checksums.json"
    manifest_path.write_text(manifest.to_json(), encoding="utf-8")

    logger.info("Wrote checksum manifest to %s (%d files)", manifest_path, len(file_entries))
    return manifest_path


def verify_checksum_manifest(
    directory: Path,
    manifest_name: str = "_checksums.json",
) -> ChecksumVerificationResult:
    """Verify files against recorded checksums.

    Args:
        directory: Directory containing files and manifest
        manifest_name: Name of the manifest file

    Returns:
        ChecksumVerificationResult with verification details

    Example:
        >>> result = verify_checksum_manifest(Path("./silver/orders/"))
        >>> if not result.valid:
        ...     print(f"Corrupted files: {result.mismatched_files}")
    """
    start_time = time.perf_counter()

    manifest_path = directory / manifest_name
    if not manifest_path.exists():
        return ChecksumVerificationResult(
            valid=False,
            missing_files=[manifest_name],
            verification_time_ms=0.0,
        )

    try:
        manifest = ChecksumManifest.from_file(manifest_path)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to read checksum manifest: %s", exc)
        return ChecksumVerificationResult(
            valid=False,
            verification_time_ms=(time.perf_counter() - start_time) * 1000,
        )

    verified_files: List[str] = []
    missing_files: List[str] = []
    mismatched_files: List[str] = []

    for entry in manifest.files:
        rel_name = entry.get("path")
        if not rel_name:
            continue

        target = directory / rel_name
        if not target.exists():
            missing_files.append(rel_name)
            continue

        expected_size = entry.get("size_bytes")
        expected_hash = entry.get("sha256")

        actual_size = target.stat().st_size
        actual_hash = compute_file_sha256(target)

        if actual_size != expected_size or actual_hash != expected_hash:
            mismatched_files.append(rel_name)
            logger.debug(
                "Checksum mismatch for %s: expected %s, got %s",
                rel_name,
                expected_hash[:16] + "..." if expected_hash else "None",
                actual_hash[:16] + "...",
            )
        else:
            verified_files.append(rel_name)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    is_valid = not (missing_files or mismatched_files) and len(verified_files) > 0

    result = ChecksumVerificationResult(
        valid=is_valid,
        verified_files=verified_files,
        missing_files=missing_files,
        mismatched_files=mismatched_files,
        manifest=manifest,
        verification_time_ms=elapsed_ms,
    )

    if is_valid:
        logger.debug(
            "Checksum verification passed: %d files in %.1fms",
            len(verified_files),
            elapsed_ms,
        )
    else:
        logger.warning(
            "Checksum verification failed: %d missing, %d mismatched",
            len(missing_files),
            len(mismatched_files),
        )

    return result


def validate_bronze_checksums(
    bronze_path: Path,
    *,
    validation_mode: str = "warn",
) -> ChecksumVerificationResult:
    """Validate Bronze data checksums before Silver processing.

    This function should be called before processing Bronze data into Silver
    to ensure data integrity. It verifies that:
    1. The _checksums.json file exists
    2. All data files referenced in the manifest exist
    3. All checksums match the expected values

    Args:
        bronze_path: Path to Bronze data directory
        validation_mode: How to handle validation failures:
            - "skip": Don't verify checksums (return valid result)
            - "warn": Log warning but continue (default)
            - "strict": Raise ChecksumValidationError on failure

    Returns:
        ChecksumVerificationResult with validation details

    Raises:
        ChecksumValidationError: If validation_mode is "strict" and validation fails

    Example:
        >>> # Verify Bronze data before processing
        >>> result = validate_bronze_checksums(
        ...     Path("./bronze/claims/header/dt=2025-01-15/"),
        ...     validation_mode="strict",
        ... )
        >>> if result.valid:
        ...     # Proceed with Silver processing
        ...     pass
    """
    if validation_mode == "skip":
        logger.debug("Checksum validation skipped for %s", bronze_path)
        return ChecksumVerificationResult(valid=True)

    if not bronze_path.exists():
        if validation_mode == "strict":
            raise ChecksumValidationError(
                f"Bronze data directory does not exist: {bronze_path}"
            )
        logger.warning("Bronze data directory does not exist: %s", bronze_path)
        return ChecksumVerificationResult(
            valid=False,
            missing_files=[str(bronze_path)],
        )

    result = verify_checksum_manifest(bronze_path)

    if not result.valid:
        msg = f"Bronze checksum validation failed for {bronze_path}"
        if result.missing_files:
            msg += f"\n  Missing files: {', '.join(result.missing_files)}"
        if result.mismatched_files:
            msg += f"\n  Mismatched files: {', '.join(result.mismatched_files)}"

        if validation_mode == "strict":
            raise ChecksumValidationError(msg, result)
        else:
            logger.warning(msg)
    else:
        logger.info(
            "Bronze checksum validation passed for %s (%d files, %.1fms)",
            bronze_path,
            len(result.verified_files),
            result.verification_time_ms,
        )

    return result
