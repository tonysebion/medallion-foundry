"""State management for incremental patterns in bronze-foundry.

This package contains state tracking for incremental extraction:
- watermark: Watermark tracking for incremental loads (timestamps, cursors)
- manifest: File manifest tracking for file_batch sources
"""

from .watermark import (
    Watermark,
    WatermarkStore,
    WatermarkType,
    load_watermark,
    save_watermark,
)
from .manifest import (
    FileEntry,
    FileManifest,
    ManifestTracker,
)

__all__ = [
    # Watermark
    "Watermark",
    "WatermarkStore",
    "WatermarkType",
    "load_watermark",
    "save_watermark",
    # Manifest
    "FileEntry",
    "FileManifest",
    "ManifestTracker",
]
