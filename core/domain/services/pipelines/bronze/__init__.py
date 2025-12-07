"""Bronze extraction layer: I/O, chunking, and storage planning.

Primary modules:
- io: CSV/Parquet file writing utilities
- models: Storage and chunk configuration dataclasses
- base: Base utilities for bronze extraction
"""

from . import io
from . import models
from . import base
from .models import StoragePlan, ChunkWriterConfig

__all__ = [
    "io",
    "models",
    "base",
    "StoragePlan",
    "ChunkWriterConfig",
]
