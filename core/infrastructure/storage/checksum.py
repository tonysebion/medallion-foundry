"""File hashing and checksum manifest utilities.

This module provides file integrity verification through SHA256 hashing
and manifest file management. These are generic storage utilities used
by both Bronze and Silver layers.

Key functions:
- compute_file_sha256: Compute SHA256 hash of a file
- write_checksum_manifest: Write a manifest of file hashes
- verify_checksum_manifest: Verify files match their recorded hashes
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

from core.primitives.time_utils import utc_isoformat

logger = logging.getLogger(__name__)


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
