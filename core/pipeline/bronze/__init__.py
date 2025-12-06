"""Bronze extraction layer: I/O, chunking, and storage planning.

Primary modules:
- io: CSV/Parquet file writing utilities
- plan: Storage and chunk configuration dataclasses
"""

from .plan import StoragePlan, ChunkWriterConfig

__all__ = [
    "StoragePlan",
    "ChunkWriterConfig",
]
