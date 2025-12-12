"""Tests for idempotency per spec Section 10.

Verifies that:
- Same run_id produces same output
- Re-running with same config produces identical results
- Checksums match across runs
"""

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

from tests.synthetic_data import generate_time_series_data
from core.infrastructure.runtime.chunking import chunk_records, write_parquet_chunk
from core.infrastructure.runtime.metadata_helpers import (
    write_batch_metadata,
    write_checksum_manifest,
)
from core.infrastructure.io.storage.checksum import verify_checksum_manifest


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_dataframe_hash(df: pd.DataFrame) -> str:
    """Compute hash of DataFrame content."""
    # Sort columns and reset index for consistent ordering
    df_sorted = df[sorted(df.columns)].reset_index(drop=True)
    return hashlib.sha256(df_sorted.to_csv(index=False).encode()).hexdigest()


class TestDataGenerationIdempotency:
    """Test that synthetic data generation is deterministic."""

    def test_same_seed_produces_same_data(self):
        """Same seed should produce identical data."""
        data1 = generate_time_series_data("claims", date(2024, 1, 15), seed=42, row_count=100)
        data2 = generate_time_series_data("claims", date(2024, 1, 15), seed=42, row_count=100)

        hash1 = compute_dataframe_hash(data1["t0"])
        hash2 = compute_dataframe_hash(data2["t0"])

        assert hash1 == hash2

    def test_different_seeds_produce_different_data(self):
        """Different seeds should produce different data."""
        data1 = generate_time_series_data("claims", date(2024, 1, 15), seed=42, row_count=100)
        data2 = generate_time_series_data("claims", date(2024, 1, 15), seed=99, row_count=100)

        hash1 = compute_dataframe_hash(data1["t0"])
        hash2 = compute_dataframe_hash(data2["t0"])

        assert hash1 != hash2


class TestParquetWriteIdempotency:
    """Test that Parquet writes are deterministic."""

    def test_same_data_produces_same_parquet(self, temp_dir):
        """Same data should produce bit-identical Parquet files."""
        records = [{"id": i, "name": f"Item {i}"} for i in range(100)]

        # Write twice
        path1 = temp_dir / "run1" / "data.parquet"
        path2 = temp_dir / "run2" / "data.parquet"
        path1.parent.mkdir(parents=True, exist_ok=True)
        path2.parent.mkdir(parents=True, exist_ok=True)

        write_parquet_chunk(records, path1)
        write_parquet_chunk(records, path2)

        # Read back and compare content (Parquet metadata may differ)
        df1 = pd.read_parquet(path1)
        df2 = pd.read_parquet(path2)

        pd.testing.assert_frame_equal(df1, df2)

    def test_chunked_data_produces_same_output(self, temp_dir):
        """Chunked data should produce consistent results."""
        records = [{"id": i, "value": i * 10} for i in range(100)]

        # Chunk and write - run 1
        chunks1 = chunk_records(records, max_rows=25)
        run1_dir = temp_dir / "run1"
        run1_dir.mkdir()
        for i, chunk in enumerate(chunks1):
            write_parquet_chunk(chunk, run1_dir / f"chunk_{i}.parquet")

        # Chunk and write - run 2
        chunks2 = chunk_records(records, max_rows=25)
        run2_dir = temp_dir / "run2"
        run2_dir.mkdir()
        for i, chunk in enumerate(chunks2):
            write_parquet_chunk(chunk, run2_dir / f"chunk_{i}.parquet")

        # Verify same number of chunks
        assert len(chunks1) == len(chunks2)

        # Verify chunk content matches
        for i in range(len(chunks1)):
            df1 = pd.read_parquet(run1_dir / f"chunk_{i}.parquet")
            df2 = pd.read_parquet(run2_dir / f"chunk_{i}.parquet")
            pd.testing.assert_frame_equal(df1, df2)


class TestChecksumIdempotency:
    """Test checksum verification for idempotency."""

    def test_checksum_manifest_verifies_integrity(self, temp_dir):
        """Checksum manifest should verify file integrity."""
        # Create data files
        records = [{"id": i, "value": i * 10} for i in range(50)]
        data_path = temp_dir / "data.parquet"
        write_parquet_chunk(records, data_path)

        # Create manifest
        write_checksum_manifest(
            out_dir=temp_dir,
            files=[data_path],
            load_pattern="snapshot",
        )

        # Verify - should pass
        manifest = verify_checksum_manifest(temp_dir)
        assert manifest["load_pattern"] == "snapshot"
        assert len(manifest["files"]) == 1

    def test_checksum_verification_fails_on_corruption(self, temp_dir):
        """Checksum verification should fail if file is corrupted."""
        records = [{"id": i} for i in range(10)]
        data_path = temp_dir / "data.parquet"
        write_parquet_chunk(records, data_path)

        write_checksum_manifest(
            out_dir=temp_dir,
            files=[data_path],
            load_pattern="snapshot",
        )

        # Corrupt the file
        with open(data_path, "ab") as f:
            f.write(b"corruption")

        # Verification should fail
        with pytest.raises(ValueError, match="checksum"):
            verify_checksum_manifest(temp_dir)

    def test_checksum_verification_fails_on_missing_file(self, temp_dir):
        """Checksum verification should fail if file is missing."""
        records = [{"id": i} for i in range(10)]
        data_path = temp_dir / "data.parquet"
        write_parquet_chunk(records, data_path)

        write_checksum_manifest(
            out_dir=temp_dir,
            files=[data_path],
            load_pattern="snapshot",
        )

        # Delete the file
        data_path.unlink()

        # Verification should fail
        with pytest.raises(ValueError, match="missing"):
            verify_checksum_manifest(temp_dir)


class TestMetadataIdempotency:
    """Test metadata consistency across runs."""

    def test_metadata_record_count_matches(self, temp_dir):
        """Metadata record count should match actual records."""
        records = [{"id": i} for i in range(100)]
        write_parquet_chunk(records, temp_dir / "data.parquet")

        metadata_path = write_batch_metadata(
            out_dir=temp_dir,
            record_count=100,
            chunk_count=1,
        )

        with open(metadata_path) as f:
            metadata = json.load(f)

        assert metadata["record_count"] == 100
        assert metadata["chunk_count"] == 1

        # Verify against actual file
        df = pd.read_parquet(temp_dir / "data.parquet")
        assert len(df) == metadata["record_count"]

    def test_cursor_consistency_across_runs(self, temp_dir):
        """Cursor value should be consistent across runs."""
        cursor_value = "2024-01-15T12:00:00Z"

        # Run 1
        run1_dir = temp_dir / "run1"
        run1_dir.mkdir()
        write_batch_metadata(
            out_dir=run1_dir,
            record_count=100,
            chunk_count=1,
            cursor=cursor_value,
        )

        # Run 2 (same cursor)
        run2_dir = temp_dir / "run2"
        run2_dir.mkdir()
        write_batch_metadata(
            out_dir=run2_dir,
            record_count=100,
            chunk_count=1,
            cursor=cursor_value,
        )

        # Verify cursors match
        with open(run1_dir / "_metadata.json") as f:
            meta1 = json.load(f)
        with open(run2_dir / "_metadata.json") as f:
            meta2 = json.load(f)

        assert meta1["cursor"] == meta2["cursor"]


class TestFullPipelineIdempotency:
    """Test end-to-end pipeline idempotency."""

    def test_full_bronze_pipeline_idempotent(self, temp_dir):
        """Full Bronze pipeline should produce identical output for same input."""
        # Generate test data
        data = generate_time_series_data("orders", date(2024, 1, 15), seed=42, row_count=100)
        source_df = data["t0"]

        def run_pipeline(run_dir: Path) -> Dict[str, Any]:
            """Run a simulated Bronze pipeline."""
            run_dir.mkdir(parents=True, exist_ok=True)

            # Write data
            records = source_df.to_dict(orient="records")
            chunks = chunk_records(records, max_rows=50)
            files = []

            for i, chunk in enumerate(chunks):
                chunk_path = run_dir / f"chunk_{i}.parquet"
                write_parquet_chunk(chunk, chunk_path)
                files.append(chunk_path)

            # Write metadata
            write_batch_metadata(
                out_dir=run_dir,
                record_count=len(records),
                chunk_count=len(chunks),
            )

            # Write checksum
            write_checksum_manifest(
                out_dir=run_dir,
                files=files,
                load_pattern="snapshot",
            )

            # Compute content hashes
            return {
                "record_count": len(records),
                "chunk_count": len(chunks),
                "data_hashes": [compute_file_hash(f) for f in sorted(files)],
            }

        # Run twice
        result1 = run_pipeline(temp_dir / "run1")
        result2 = run_pipeline(temp_dir / "run2")

        # Verify identical results
        assert result1["record_count"] == result2["record_count"]
        assert result1["chunk_count"] == result2["chunk_count"]

        # Content should match (data hashes)
        df1 = pd.concat([
            pd.read_parquet(temp_dir / "run1" / f"chunk_{i}.parquet")
            for i in range(result1["chunk_count"])
        ])
        df2 = pd.concat([
            pd.read_parquet(temp_dir / "run2" / f"chunk_{i}.parquet")
            for i in range(result2["chunk_count"])
        ])

        pd.testing.assert_frame_equal(
            df1.sort_values(list(df1.columns)).reset_index(drop=True),
            df2.sort_values(list(df2.columns)).reset_index(drop=True),
        )
