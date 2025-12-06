"""Manifest tracking for file_batch sources per spec Section 3.3.

Provides manifest-based file tracking to avoid reprocessing files.

Example config:
```yaml
source:
  type: "file_batch"
  protocol: "s3"
  path_pattern: "s3://partner/claims/*.csv"
  file_format: "csv"
  has_header: true
  delimiter: ","
  file_tracking:
    strategy: "manifest"
    manifest_path: "s3://mdf/system/manifests/claims.json"
```

Manifest file structure:
```json
{
  "last_updated": "2025-01-15T10:30:00Z",
  "processed_files": [
    {
      "path": "s3://partner/claims/batch_001.csv",
      "size_bytes": 1024000,
      "checksum": "sha256:abc123...",
      "processed_at": "2025-01-15T10:30:00Z",
      "run_id": "uuid..."
    }
  ],
  "pending_files": [
    {
      "path": "s3://partner/claims/batch_002.csv",
      "size_bytes": 512000,
      "discovered_at": "2025-01-15T11:00:00Z"
    }
  ]
}
```
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class FileEntry:
    """Entry for a single file in the manifest."""

    path: str
    size_bytes: int = 0
    checksum: Optional[str] = None
    discovered_at: Optional[str] = None
    processed_at: Optional[str] = None
    run_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "path": self.path,
            "size_bytes": self.size_bytes,
        }
        if self.checksum:
            result["checksum"] = self.checksum
        if self.discovered_at:
            result["discovered_at"] = self.discovered_at
        if self.processed_at:
            result["processed_at"] = self.processed_at
        if self.run_id:
            result["run_id"] = self.run_id
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileEntry":
        """Create from dictionary."""
        return cls(
            path=data["path"],
            size_bytes=data.get("size_bytes", 0),
            checksum=data.get("checksum"),
            discovered_at=data.get("discovered_at"),
            processed_at=data.get("processed_at"),
            run_id=data.get("run_id"),
        )


@dataclass
class FileManifest:
    """Manifest for tracking file processing state.

    Tracks which files have been processed and which are pending.
    """

    manifest_path: str
    last_updated: Optional[str] = None
    processed_files: List[FileEntry] = field(default_factory=list)
    pending_files: List[FileEntry] = field(default_factory=list)

    def __post_init__(self):
        self._processed_paths: set = set()
        self._rebuild_index()

    def _rebuild_index(self) -> None:
        """Rebuild the processed paths index."""
        self._processed_paths = {f.path for f in self.processed_files}

    def is_processed(self, path: str) -> bool:
        """Check if a file has already been processed."""
        return path in self._processed_paths

    def add_discovered_file(self, path: str, size_bytes: int = 0) -> bool:
        """Add a discovered file to pending if not already processed.

        Returns True if file was added, False if already processed.
        """
        if self.is_processed(path):
            return False

        # Check if already in pending
        for entry in self.pending_files:
            if entry.path == path:
                return False

        self.pending_files.append(FileEntry(
            path=path,
            size_bytes=size_bytes,
            discovered_at=datetime.utcnow().isoformat() + "Z",
        ))
        return True

    def mark_processed(
        self,
        path: str,
        run_id: str,
        checksum: Optional[str] = None,
    ) -> None:
        """Mark a file as processed and move from pending to processed."""
        # Find in pending
        entry = None
        for i, e in enumerate(self.pending_files):
            if e.path == path:
                entry = self.pending_files.pop(i)
                break

        if entry is None:
            # File wasn't in pending, create new entry
            entry = FileEntry(path=path)

        entry.processed_at = datetime.utcnow().isoformat() + "Z"
        entry.run_id = run_id
        if checksum:
            entry.checksum = checksum

        self.processed_files.append(entry)
        self._processed_paths.add(path)

    def get_pending_files(self) -> List[str]:
        """Get list of pending file paths."""
        return [f.path for f in self.pending_files]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "manifest_path": self.manifest_path,
            "processed_files": [f.to_dict() for f in self.processed_files],
            "pending_files": [f.to_dict() for f in self.pending_files],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileManifest":
        """Create from dictionary."""
        manifest = cls(
            manifest_path=data.get("manifest_path", ""),
            last_updated=data.get("last_updated"),
        )
        manifest.processed_files = [
            FileEntry.from_dict(f) for f in data.get("processed_files", [])
        ]
        manifest.pending_files = [
            FileEntry.from_dict(f) for f in data.get("pending_files", [])
        ]
        manifest._rebuild_index()
        return manifest


def _discover_local_files(path_pattern: str) -> List[Tuple[str, int]]:
    """Discover files from local filesystem."""
    from pathlib import Path as LocalPath
    import glob

    files: List[Tuple[str, int]] = []
    for path_str in glob.glob(path_pattern, recursive=True):
        path = LocalPath(path_str)
        if path.is_file() and not path.name.startswith("_"):
            files.append((str(path), path.stat().st_size))

    return files


class DiscoveryStrategy(ABC):
    """Abstract base for manifest file discovery strategies."""

    @abstractmethod
    def matches(self, path_pattern: str, protocol: str) -> bool:
        ...

    @abstractmethod
    def discover(self, path_pattern: str) -> List[Tuple[str, int]]:
        ...


class LocalDiscoveryStrategy(DiscoveryStrategy):
    """Discover files from the local filesystem."""

    def matches(self, path_pattern: str, protocol: str) -> bool:
        return protocol != "s3" and not path_pattern.startswith("s3://")

    def discover(self, path_pattern: str) -> List[Tuple[str, int]]:
        return _discover_local_files(path_pattern)


class S3DiscoveryStrategy(DiscoveryStrategy):
    """Discover files stored in S3."""

    def matches(self, path_pattern: str, protocol: str) -> bool:
        return protocol == "s3" or path_pattern.startswith("s3://")

    def discover(self, path_pattern: str) -> List[Tuple[str, int]]:
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required for S3 file discovery")

        clean_pattern = path_pattern.replace("s3://", "")
        parts = clean_pattern.split("/", 1)
        bucket = parts[0]
        prefix_pattern = parts[1] if len(parts) > 1 else ""

        prefix = ""
        for char in prefix_pattern:
            if char in "*?[":
                break
            prefix += char

        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")

        files: List[Tuple[str, int]] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                full_path = f"s3://{bucket}/{key}"
                if fnmatch.fnmatch(full_path, path_pattern):
                    if not key.split("/")[-1].startswith("_"):
                        files.append((full_path, obj["Size"]))
        return files


_DISCOVERY_STRATEGIES: List[DiscoveryStrategy] = [
    S3DiscoveryStrategy(),
    LocalDiscoveryStrategy(),
]


class ManifestTracker:
    """Tracker for managing file manifests.

    Supports local filesystem and S3 storage for manifests.
    """

    def __init__(self, manifest_path: str):
        """Initialize tracker with manifest path.

        Args:
            manifest_path: Path to manifest file (local or s3://)
        """
        self.manifest_path = manifest_path
        self._manifest: Optional[FileManifest] = None

    @property
    def is_s3(self) -> bool:
        """Check if manifest is stored in S3."""
        return self.manifest_path.startswith("s3://")

    def load(self) -> FileManifest:
        """Load manifest from storage."""
        if self._manifest is not None:
            return self._manifest

        try:
            if self.is_s3:
                self._manifest = self._load_s3()
            else:
                self._manifest = self._load_local()
        except FileNotFoundError:
            logger.info(f"Manifest not found at {self.manifest_path}, creating new")
            self._manifest = FileManifest(manifest_path=self.manifest_path)
        except Exception as e:
            logger.warning(f"Error loading manifest: {e}, creating new")
            self._manifest = FileManifest(manifest_path=self.manifest_path)

        return self._manifest

    def _load_local(self) -> FileManifest:
        """Load manifest from local filesystem."""
        path = Path(self.manifest_path)
        if not path.exists():
            raise FileNotFoundError(f"Manifest not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return FileManifest.from_dict(data)

    def _load_s3(self) -> FileManifest:
        """Load manifest from S3."""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required for S3 manifest storage")

        # Parse S3 path
        parts = self.manifest_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        s3 = boto3.client("s3")
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return FileManifest.from_dict(data)
        except s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Manifest not found in S3: {self.manifest_path}")

    def save(self) -> None:
        """Save manifest to storage."""
        if self._manifest is None:
            return

        if self.is_s3:
            self._save_s3()
        else:
            self._save_local()

    def _save_local(self) -> None:
        """Save manifest to local filesystem."""
        path = Path(self.manifest_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(self._manifest.to_dict(), f, indent=2)

        logger.info(f"Saved manifest to {path}")

    def _save_s3(self) -> None:
        """Save manifest to S3."""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required for S3 manifest storage")

        # Parse S3 path
        parts = self.manifest_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        s3 = boto3.client("s3")
        body = json.dumps(self._manifest.to_dict(), indent=2)
        s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))

        logger.info(f"Saved manifest to s3://{bucket}/{key}")

    def discover_files(
        self,
        path_pattern: str,
        protocol: str = "local",
    ) -> List[str]:
        """Discover new files matching pattern.

        Args:
            path_pattern: Glob pattern for files (e.g., "s3://bucket/path/*.csv")
            protocol: Storage protocol (local, s3)

        Returns:
            List of new file paths not yet in manifest
        """
        manifest = self.load()

        strategy = next(
            (s for s in _DISCOVERY_STRATEGIES if s.matches(path_pattern, protocol)),
            LocalDiscoveryStrategy(),
        )
        all_files = strategy.discover(path_pattern)

        # Filter out already processed files
        new_files = []
        for file_path, size in all_files:
            if manifest.add_discovered_file(file_path, size):
                new_files.append(file_path)

        logger.info(
            f"Discovered {len(all_files)} files, {len(new_files)} new"
        )
        return new_files

    def mark_processed(
        self,
        paths: List[str],
        run_id: str,
    ) -> None:
        """Mark files as processed."""
        manifest = self.load()
        for path in paths:
            manifest.mark_processed(path, run_id)
        self.save()


def compute_file_checksum(path: str, protocol: str = "local") -> str:
    """Compute SHA-256 checksum for a file.

    Args:
        path: File path (local or s3://)
        protocol: Storage protocol

    Returns:
        Checksum string prefixed with algorithm (e.g., "sha256:abc123...")
    """
    hasher = hashlib.sha256()

    if protocol == "s3" or path.startswith("s3://"):
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required for S3 checksum")

        parts = path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)

        for chunk in response["Body"].iter_chunks(chunk_size=1024 * 1024):
            hasher.update(chunk)
    else:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chunk)

    return f"sha256:{hasher.hexdigest()}"
