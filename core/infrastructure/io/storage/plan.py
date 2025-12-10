"""Storage plan helpers moved into the infrastructure layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from core.infrastructure.io.storage.base import StorageBackend


@dataclass
class StoragePlan:
    """Upload plan that pairs a backend with a prefix path."""

    enabled: bool
    backend: Optional[StorageBackend]
    relative_path: str

    def upload(self, file_path: Path) -> None:
        if not (self.enabled and self.backend):
            return
        remote_path = f"{self.relative_path}{file_path.name}"
        self.backend.upload_file(str(file_path), remote_path)

    def remote_path_for(self, file_path: Path) -> str:
        """Return the remote path that would be used for a given local file."""
        return f"{self.relative_path}{file_path.name}"

    def delete(self, file_path: Path) -> bool:
        """Delete the remote artifact that corresponds to the provided local file."""
        if not (self.enabled and self.backend):
            return False
        remote_path = self.remote_path_for(file_path)
        return self.backend.delete_file(remote_path)


@dataclass
class ChunkWriterConfig:
    """Configuration for writing Bronze data chunks."""

    out_dir: Path
    write_csv: bool
    write_parquet: bool
    parquet_compression: str
    storage_plan: StoragePlan
    chunk_prefix: str


__all__ = ["StoragePlan", "ChunkWriterConfig"]
