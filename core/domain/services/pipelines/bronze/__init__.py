"""Bronze extraction layer: I/O, chunking, and storage planning.

Primary modules:
- io: CSV/Parquet file writing utilities
- models: Storage and chunk configuration dataclasses
- base: Base utilities for bronze extraction
"""

from __future__ import annotations

from . import io
from . import base
from . import models

from core.infrastructure.io.storage.plan import ChunkWriterConfig, StoragePlan

__all__ = [
    "io",
    "base",
    "models",
    "StoragePlan",
    "ChunkWriterConfig",
]
