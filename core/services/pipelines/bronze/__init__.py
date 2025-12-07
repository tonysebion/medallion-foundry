"""Bronze extraction layer: I/O, chunking, and storage planning.

Primary modules:
- io: CSV/Parquet file writing utilities
- models: Storage and chunk configuration dataclasses
"""

from .models import StoragePlan, ChunkWriterConfig

__all__ = [
    "StoragePlan",
    "ChunkWriterConfig",
]
