from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from core.runner.chunks import (
    ChunkProcessor,
    ChunkWriter,
    ChunkWriterConfig,
    StoragePlan,
)


class FailingChunkWriter(ChunkWriter):
    def write(self, chunk_index: int, chunk: List[dict]):
        raise RuntimeError("boom")


def _build_config(tmp_path: Path) -> ChunkWriterConfig:
    storage_plan = StoragePlan(enabled=False, backend=None, relative_path="")
    return ChunkWriterConfig(
        out_dir=tmp_path,
        write_csv=True,
        write_parquet=False,
        parquet_compression="snappy",
        storage_plan=storage_plan,
        chunk_prefix="failing",
    )


def test_chunk_processor_raises_when_writer_fails(tmp_path: Path):
    writer = FailingChunkWriter(_build_config(tmp_path))
    processor = ChunkProcessor(writer, parallel_workers=2)
    with pytest.raises(RuntimeError, match="boom"):
        processor.process([[{"id": 1}], [{"id": 2}]])
