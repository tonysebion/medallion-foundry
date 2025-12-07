"""State management for incremental patterns in bronze-foundry.

This package contains state tracking for incremental extraction:
- storage: Base class for state storage backends (local, S3)
- watermark: Watermark tracking for incremental loads (timestamps, cursors)
- manifest: File manifest tracking for file_batch sources
"""

from .storage import StateStorageBackend
from .watermark import (
    Watermark,
    WatermarkStore,
    WatermarkType,
    build_watermark_store,
    compute_max_watermark,
)
from .manifest import (
    FileEntry,
    FileManifest,
    ManifestTracker,
)

__all__ = [
    # Storage base
    "StateStorageBackend",
    # Watermark
    "Watermark",
    "WatermarkStore",
    "WatermarkType",
    "build_watermark_store",
    "compute_max_watermark",
    # Manifest
    "FileEntry",
    "FileManifest",
    "ManifestTracker",
]
