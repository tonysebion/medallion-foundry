"""Compatibility shim for bronze pipeline models redirecting to core.services.processing.chunk_config."""
from core.domain.services.processing.chunk_config import (
    StoragePlan,
    ChunkWriterConfig,
    build_chunk_writer_config,
    compute_output_formats,
    resolve_load_pattern,
)

__all__ = [
    "StoragePlan",
    "ChunkWriterConfig",
    "build_chunk_writer_config",
    "compute_output_formats",
    "resolve_load_pattern",
]
