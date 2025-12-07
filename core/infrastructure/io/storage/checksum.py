"""File hashing and checksum manifest utilities.

This module provides file integrity verification through SHA256 hashing
and manifest file management. These are generic storage utilities used
by both Bronze and Silver layers.

Key functions:
- compute_file_sha256: Compute SHA256 hash of a file
- write_checksum_manifest: Write a manifest of file hashes
- verify_checksum_manifest: Verify files match their recorded hashes (raises on failure)
- verify_checksum_manifest_with_result: Verify files and return detailed result
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from core.foundation.time_utils import utc_isoformat

logger = logging.getLogger(__name__)


@dataclass
class ChecksumVerificationResult:
    """Result of checksum verification.

    Provides detailed information about verification outcome without raising
    exceptions, allowing callers to handle failures gracefully.

    Attributes:
        valid: True if all files passed verification
        verified_files: List of files that passed verification
        missing_files: List of files in manifest but not on disk
        mismatched_files: List of files with hash/size mismatch
        manifest: The parsed manifest dictionary
        verification_time_ms: Time taken for verification in milliseconds
    """

    valid: bool
    verified_files: List[str] = field(default_factory=list)
    missing_files: List[str] = field(default_factory=list)
    mismatched_files: List[str] = field(default_factory=list)
    manifest: Dict[str, Any] = field(default_factory=dict)
    verification_time_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "valid": self.valid,
            "verified_files": self.verified_files,
            "missing_files": self.missing_files,
            "mismatched_files": self.mismatched_files,
            "verification_time_ms": self.verification_time_ms,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChecksumVerificationResult":
        """Create from dictionary."""
        return cls(
            valid=data.get("valid", False),
            verified_files=data.get("verified_files", []),
            missing_files=data.get("missing_files", []),
            mismatched_files=data.get("mismatched_files", []),
            manifest=data.get("manifest", {}),
            verification_time_ms=data.get("verification_time_ms", 0.0),
        )

    def __str__(self) -> str:
        status = "VALID" if self.valid else "INVALID"
        parts = [f"ChecksumVerificationResult({status}"]
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
    files: list[Path],
    load_pattern: str,
    extra_metadata: dict[str, Any] | None = None,
) -> Path:
    """Write a checksum manifest containing hashes of produced files.

    Creates a JSON manifest file containing timestamps, file hashes,
    sizes, and optional metadata for data integrity verification.

    Args:
        out_dir: Directory to write manifest to
        files: List of file paths to include in manifest
        load_pattern: Load pattern identifier for the manifest
        extra_metadata: Optional additional metadata to include

    Returns:
        Path to the created manifest file
    """
    manifest: dict[str, Any] = {
        "timestamp": utc_isoformat(),
        "load_pattern": load_pattern,
        "files": [],
    }

    for file_path in files:
        if not file_path.exists():
            continue
        manifest["files"].append(
            {
                "path": file_path.name,
                "size_bytes": file_path.stat().st_size,
                "sha256": compute_file_sha256(file_path),
            }
        )

    if extra_metadata:
        manifest.update(extra_metadata)

    manifest_path = out_dir / "_checksums.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    logger.info("Wrote checksum manifest to %s", manifest_path)
    return manifest_path


def verify_checksum_manifest(
    directory: Path,
    manifest_name: str = "_checksums.json",
    expected_pattern: str | None = None,
) -> dict[str, Any]:
    """Validate that files in a directory match the recorded checksums.

    Args:
        directory: The directory containing files to verify
        manifest_name: Name of the manifest file (defaults to '_checksums.json')
        expected_pattern: Optional load pattern that must match the manifest

    Returns:
        Parsed manifest dictionary if verification succeeds

    Raises:
        FileNotFoundError: If the manifest is missing
        ValueError: If the manifest is malformed or verification fails
    """
    manifest_path = directory / manifest_name
    if not manifest_path.exists():
        raise FileNotFoundError(f"Checksum manifest not found at {manifest_path}")

    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest: dict[str, Any] = json.load(handle)

    if expected_pattern and manifest.get("load_pattern") != expected_pattern:
        raise ValueError(
            f"Manifest load_pattern {manifest.get('load_pattern')} "
            f"does not match expected {expected_pattern}"
        )

    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError(
            f"Checksum manifest at {manifest_path} does not list any files to validate"
        )

    missing_files: list[str] = []
    mismatched_files: list[str] = []

    for entry in files:
        rel_name = entry.get("path")
        if not rel_name:
            raise ValueError(f"Malformed entry in checksum manifest: {entry}")
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

    if missing_files or mismatched_files:
        issues = []
        if missing_files:
            issues.append(f"missing files: {missing_files}")
        if mismatched_files:
            issues.append(f"checksum mismatches: {mismatched_files}")
        raise ValueError(
            f"Checksum verification failed for {manifest_path}: {', '.join(issues)}"
        )

    return manifest


def verify_checksum_manifest_with_result(
    directory: Path,
    manifest_name: str = "_checksums.json",
    expected_pattern: str | None = None,
) -> ChecksumVerificationResult:
    """Validate files against recorded checksums, returning detailed result.

    Unlike verify_checksum_manifest(), this function does not raise exceptions
    on verification failures. Instead, it returns a ChecksumVerificationResult
    with details about what passed and failed.

    Args:
        directory: The directory containing files to verify
        manifest_name: Name of the manifest file (defaults to '_checksums.json')
        expected_pattern: Optional load pattern that must match the manifest

    Returns:
        ChecksumVerificationResult with verification details

    Example:
        >>> result = verify_checksum_manifest_with_result(bronze_path)
        >>> if not result.valid:
        ...     logger.error("Corrupted files: %s", result.mismatched_files)
        ...     quarantine_corrupted_files(bronze_path, result.mismatched_files)
    """
    start_time = time.perf_counter()

    manifest_path = directory / manifest_name
    if not manifest_path.exists():
        return ChecksumVerificationResult(
            valid=False,
            missing_files=[manifest_name],
            manifest={},
            verification_time_ms=0.0,
        )

    try:
        with manifest_path.open("r", encoding="utf-8") as handle:
            manifest: Dict[str, Any] = json.load(handle)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to read checksum manifest: %s", exc)
        return ChecksumVerificationResult(
            valid=False,
            manifest={},
            verification_time_ms=(time.perf_counter() - start_time) * 1000,
        )

    # Check load pattern if specified
    if expected_pattern and manifest.get("load_pattern") != expected_pattern:
        logger.warning(
            "Manifest load_pattern '%s' does not match expected '%s'",
            manifest.get("load_pattern"),
            expected_pattern,
        )
        return ChecksumVerificationResult(
            valid=False,
            manifest=manifest,
            verification_time_ms=(time.perf_counter() - start_time) * 1000,
        )

    files = manifest.get("files")
    if not isinstance(files, list):
        return ChecksumVerificationResult(
            valid=False,
            manifest=manifest,
            verification_time_ms=(time.perf_counter() - start_time) * 1000,
        )

    verified_files: List[str] = []
    missing_files: List[str] = []
    mismatched_files: List[str] = []

    for entry in files:
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
