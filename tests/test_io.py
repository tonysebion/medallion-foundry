"""Tests for IO functions (chunking, CSV, Parquet)."""

import csv
import pandas as pd
from typing import Any

import pytest

from core.bronze.io import (
    chunk_records,
    verify_checksum_manifest,
    write_checksum_manifest,
    write_csv_chunk,
    write_parquet_chunk,
)


class TestChunkRecords:
    """Test record chunking."""

    def test_chunk_with_zero_max(self):
        """Test that max_rows=0 returns all records in one chunk."""
        records: list[dict[str, Any]] = [{"id": i} for i in range(100)]
        chunks = chunk_records(records, 0)

        assert len(chunks) == 1
        assert len(chunks[0]) == 100

    def test_chunk_with_max_rows(self):
        """Test chunking with specific max_rows."""
        records: list[dict[str, Any]] = [{"id": i} for i in range(100)]
        chunks = chunk_records(records, 25)

        assert len(chunks) == 4
        assert all(len(chunk) == 25 for chunk in chunks)

    def test_chunk_uneven_division(self):
        """Test chunking when records don't divide evenly."""
        records: list[dict[str, Any]] = [{"id": i} for i in range(105)]
        chunks = chunk_records(records, 25)

        assert len(chunks) == 5
        assert len(chunks[-1]) == 5  # Last chunk has remainder

    def test_empty_records(self):
        """Test chunking empty list."""
        records: list[dict[str, Any]] = []
        chunks = chunk_records(records, 10)

        assert len(chunks) == 0


class TestWriteCsvChunk:
    """Test CSV writing."""

    def test_write_csv_basic(self, tmp_path):
        """Test writing basic CSV file."""
        records: list[dict[str, Any]] = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
        ]

        csv_path = tmp_path / "test.csv"
        write_csv_chunk(records, csv_path)

        assert csv_path.exists()

        # Verify content
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["value"] == "200"

    def test_write_empty_chunk(self, tmp_path):
        """Test writing empty chunk does nothing."""
        csv_path = tmp_path / "empty.csv"
        write_csv_chunk([], csv_path)

        # File shouldn't be created for empty chunk
        assert not csv_path.exists()


class TestWriteParquetChunk:
    """Test Parquet writing."""

    def test_write_parquet_basic(self, tmp_path):
        """Test writing basic Parquet file."""
        records = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
        ]

        parquet_path = tmp_path / "test.parquet"
        write_parquet_chunk(records, parquet_path, compression="snappy")

        assert parquet_path.exists()

        # Verify content
        df = pd.read_parquet(parquet_path)
        assert len(df) == 2
        assert df["name"].tolist() == ["Alice", "Bob"]

    def test_write_parquet_with_compression(self, tmp_path):
        """Test writing Parquet with different compression."""
        records: list[dict[str, Any]] = [
            {"id": i, "data": f"row_{i}"} for i in range(10)
        ]

        parquet_path = tmp_path / "compressed.parquet"
        write_parquet_chunk(records, parquet_path, compression="gzip")

        assert parquet_path.exists()
        df = pd.read_parquet(parquet_path)
        assert len(df) == 10

    def test_write_empty_parquet(self, tmp_path):
        """Test writing empty Parquet chunk."""
        parquet_path = tmp_path / "empty.parquet"
        write_parquet_chunk([], parquet_path)

        # File shouldn't be created for empty chunk
        assert not parquet_path.exists()


class TestChecksumManifest:
    def test_verify_checksum_manifest_success(self, tmp_path):
        data_file = tmp_path / "part-0001.csv"
        data_file.write_text("hello,world", encoding="utf-8")

        write_checksum_manifest(
            tmp_path, [data_file], "full", extra_metadata={"system": "demo"}
        )

        manifest = verify_checksum_manifest(tmp_path, expected_pattern="full")
        assert manifest["load_pattern"] == "full"
        assert len(manifest["files"]) == 1

    def test_verify_checksum_manifest_mismatch(self, tmp_path):
        data_file = tmp_path / "part-0001.csv"
        data_file.write_text("hello,world", encoding="utf-8")
        write_checksum_manifest(tmp_path, [data_file], "full")

        # Tamper with the file to force checksum mismatch
        data_file.write_text("tampered", encoding="utf-8")

        with pytest.raises(ValueError):
            verify_checksum_manifest(tmp_path, expected_pattern="full")
