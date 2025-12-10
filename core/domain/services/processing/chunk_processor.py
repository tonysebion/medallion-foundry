"""Domain façade for chunk writer/processor (keeps L3 imports stable)."""

from __future__ import annotations

from core.infrastructure.runtime.chunking import ChunkProcessor, ChunkWriter

__all__ = [
    "ChunkWriter",
    "ChunkProcessor",
]
