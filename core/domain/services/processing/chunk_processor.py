"""Chunk writing and processing for Bronze extraction."""

from __future__ import annotations

import logging
from typing import Any, Dict, List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.domain.services.pipelines.bronze.io import write_csv_chunk, write_parquet_chunk
from core.domain.services.processing.chunk_config import StoragePlan, ChunkWriterConfig

logger = logging.getLogger(__name__)


class ChunkWriter:
    """Write CSV/Parquet chunks and push to storage as needed."""

    def __init__(self, config: ChunkWriterConfig) -> None:
        self.config = config

    def write(self, chunk_index: int, chunk: List[Dict[str, Any]]) -> List[Path]:
        created_files: List[Path] = []
        suffix = f"{self.config.chunk_prefix}-part-{chunk_index:04d}"

        try:
            if self.config.write_csv:
                csv_path = self.config.out_dir / f"{suffix}.csv"
                write_csv_chunk(chunk, csv_path)
                created_files.append(csv_path)
                self.config.storage_plan.upload(csv_path)

            if self.config.write_parquet:
                parquet_path = self.config.out_dir / f"{suffix}.parquet"
                write_parquet_chunk(
                    chunk, parquet_path, compression=self.config.parquet_compression
                )
                created_files.append(parquet_path)
                self.config.storage_plan.upload(parquet_path)

            logger.debug("Completed processing chunk %d", chunk_index)
            return created_files
        except Exception as exc:
            logger.error("Failed to process chunk %d: %s", chunk_index, exc)
            raise


class ChunkProcessor:
    """Coordinate sequential/parallel chunk execution."""

    def __init__(self, writer: ChunkWriter, parallel_workers: int) -> None:
        self.writer = writer
        self.parallel_workers = max(1, parallel_workers)

    def process(self, chunks: List[List[Dict[str, Any]]]) -> List[Path]:
        if not chunks:
            return []

        created_files: List[Path] = []
        if self.parallel_workers > 1 and len(chunks) > 1:
            logger.info(
                "Processing %d chunks with %d workers", len(chunks), self.parallel_workers
            )
            with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                futures = {
                    executor.submit(self.writer.write, idx, chunk): idx
                    for idx, chunk in enumerate(chunks, start=1)
                }
                for future in as_completed(futures):
                    created_files.extend(future.result())
        else:
            for idx, chunk in enumerate(chunks, start=1):
                created_files.extend(self.writer.write(idx, chunk))

        return created_files


# Re-export dataclasses for backward compatibility
__all__ = [
    "StoragePlan",
    "ChunkWriterConfig",
    "ChunkWriter",
    "ChunkProcessor",
]
