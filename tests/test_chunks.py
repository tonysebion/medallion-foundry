from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pytest

from core.runner.chunks import ChunkProcessor, ChunkWriter, ChunkWriterConfig, StoragePlan


class DummyBackend:
    def __init__(self) -> None:
        self.uploads: List[Tuple[Path, str]] = []

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        self.uploads.append((Path(local_path), remote_path))
        return True


def _make_chunk() -> List[dict]:
    return [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]


@pytest.mark.parametrize("include_parquet", [True, False])
def test_chunk_writer_creates_files_and_uploads(tmp_path: Path, include_parquet: bool) -> None:
    """ChunkWriter should materialize CSV/Parquet artifacts and upload each file."""
    chunk = _make_chunk()
    backend = DummyBackend()
    storage_plan = StoragePlan(enabled=True, backend=backend, relative_path="remote/")
    writer_config = ChunkWriterConfig(
        out_dir=tmp_path,
        write_csv=True,
        write_parquet=include_parquet,
        parquet_compression="snappy",
        storage_plan=storage_plan,
        chunk_prefix="pattern",
    )

    writer = ChunkWriter(writer_config)
    created = writer.write(1, chunk)

    assert created, "ChunkWriter should return the list of created files"
    assert all(path.exists() for path in created)

    csv_files = [path for path in created if path.suffix == ".csv"]
    assert csv_files, "CSV output is expected"

    if include_parquet:
        parquet_files = [path for path in created if path.suffix == ".parquet"]
        assert parquet_files, "Parquet output is expected when enabled"
    else:
        assert all(path.suffix != ".parquet" for path in created)

    assert len(backend.uploads) == len(created)
    for local_path, remote_path in backend.uploads:
        assert local_path.exists()
        assert remote_path.startswith("remote/")


def test_chunk_processor_parallel(tmp_path: Path) -> None:
    """ChunkProcessor executes multiple chunk writes even when parallel."""
    chunks = [_make_chunk(), _make_chunk()]
    storage_plan = StoragePlan(enabled=False, backend=None, relative_path="")
    writer_config = ChunkWriterConfig(
        out_dir=tmp_path,
        write_csv=True,
        write_parquet=False,
        parquet_compression="snappy",
        storage_plan=storage_plan,
        chunk_prefix="parallel",
    )

    writer = ChunkWriter(writer_config)
    processor = ChunkProcessor(writer, parallel_workers=2)
    created_files = processor.process(chunks)

    assert len(created_files) == len(chunks)
    expected_names = {f"parallel-part-{i:04d}.csv" for i in range(1, len(chunks) + 1)}
    assert {path.name for path in created_files} == expected_names
