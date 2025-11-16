from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.io import write_csv_chunk, write_parquet_chunk
from core.storage.backend import StorageBackend

logger = logging.getLogger(__name__)


@dataclass
class StoragePlan:
    enabled: bool
    backend: Optional[StorageBackend]
    relative_path: str

    def upload(self, file_path: Path) -> None:
        if not (self.enabled and self.backend):
            return
        remote_path = f"{self.relative_path}{file_path.name}"
        self.backend.upload_file(str(file_path), remote_path)


@dataclass
class ChunkWriterConfig:
    out_dir: Path
    write_csv: bool
    write_parquet: bool
    parquet_compression: str
    storage_plan: StoragePlan
    chunk_prefix: str


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

            logger.debug(f"Completed processing chunk {chunk_index}")
            return created_files
        except Exception as exc:
            logger.error(f"Failed to process chunk {chunk_index}: {exc}")
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
                f"Processing {len(chunks)} chunks with {self.parallel_workers} workers"
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
