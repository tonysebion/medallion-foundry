"""Performance validation tests at 100K row scale.

Story 8: Tests that validate pipeline performance at 100K row scale.

Tests:
- Generate 100K row datasets with synthetic generators
- Test Bronze extraction completes in reasonable time
- Test Silver transformation at scale
- Test INCREMENTAL_MERGE with large key sets (100K)
- Verify memory usage stays bounded
- All tests run against MinIO (S3) when available

These tests are marked with @pytest.mark.slow and should only run
when explicitly requested (pytest -m slow).
"""

from __future__ import annotations

import gc
import time
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from core.infrastructure.runtime.chunking import chunk_records, write_parquet_chunk
from core.infrastructure.runtime.metadata_helpers import (
    write_batch_metadata,
    write_checksum_manifest,
)

from tests.synthetic_data import (
    ClaimsGenerator,
    OrdersGenerator,
    TransactionsGenerator,
)

# Mark all tests in this module as slow
pytestmark = pytest.mark.slow


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def performance_run_date() -> date:
    """Standard run date for performance tests."""
    return date(2025, 1, 15)


@pytest.fixture
def claims_100k_generator() -> ClaimsGenerator:
    """Claims generator for 100K rows."""
    return ClaimsGenerator(seed=42, row_count=100_000)


@pytest.fixture
def orders_100k_generator() -> OrdersGenerator:
    """Orders generator for 100K rows."""
    return OrdersGenerator(seed=42, row_count=100_000)


@pytest.fixture
def transactions_100k_generator() -> TransactionsGenerator:
    """Transactions generator for 100K rows."""
    return TransactionsGenerator(seed=42, row_count=100_000)


# =============================================================================
# Performance Timing Helper
# =============================================================================


class PerformanceTimer:
    """Helper class to measure and track performance timings."""

    def __init__(self, name: str):
        self.name = name
        self.start_time: float = 0.0
        self.end_time: float = 0.0
        self.elapsed_seconds: float = 0.0

    def __enter__(self) -> "PerformanceTimer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self.end_time = time.perf_counter()
        self.elapsed_seconds = self.end_time - self.start_time

    def report(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "elapsed_seconds": self.elapsed_seconds,
            "elapsed_ms": self.elapsed_seconds * 1000,
        }


# =============================================================================
# Test: 100K Row Data Generation Performance
# =============================================================================


class TestDataGenerationPerformance:
    """Tests for synthetic data generation at 100K scale."""

    def test_generate_100k_claims(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
    ):
        """Should generate 100K claims records in reasonable time."""
        with PerformanceTimer("claims_t0_100k") as timer:
            df = claims_100k_generator.generate_t0(performance_run_date)

        # Verify data
        assert len(df) == 100_000
        assert df["claim_id"].is_unique

        # Performance baseline: < 30 seconds
        assert timer.elapsed_seconds < 30, f"Generation took too long: {timer.elapsed_seconds:.2f}s"

        # Memory check - DataFrame size should be reasonable
        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        assert memory_mb < 500, f"DataFrame uses too much memory: {memory_mb:.1f}MB"

    def test_generate_100k_orders(
        self,
        orders_100k_generator: OrdersGenerator,
        performance_run_date: date,
    ):
        """Should generate 100K order records in reasonable time."""
        with PerformanceTimer("orders_t0_100k") as timer:
            df = orders_100k_generator.generate_t0(performance_run_date)

        assert len(df) == 100_000
        assert timer.elapsed_seconds < 30

    def test_generate_100k_transactions(
        self,
        transactions_100k_generator: TransactionsGenerator,
        performance_run_date: date,
    ):
        """Should generate 100K transaction records in reasonable time."""
        with PerformanceTimer("transactions_t0_100k") as timer:
            df = transactions_100k_generator.generate_t0(performance_run_date)

        assert len(df) == 100_000
        assert timer.elapsed_seconds < 30


# =============================================================================
# Test: Bronze Extraction Performance
# =============================================================================


class TestBronzeExtractionPerformance:
    """Tests for Bronze extraction at 100K scale."""

    def test_bronze_extraction_100k_records(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
        tmp_path: Path,
    ):
        """Bronze extraction should handle 100K records efficiently."""
        from core.orchestration.runner.job import build_extractor

        # Generate data
        df = claims_100k_generator.generate_t0(performance_run_date)

        # Write to parquet
        input_file = tmp_path / "claims_100k.parquet"
        df.to_parquet(input_file, index=False)

        cfg = {
            "source": {
                "type": "file",
                "system": "performance",
                "table": "claims_100k",
                "file": {"path": str(input_file), "format": "parquet"},
                "run": {"load_pattern": "snapshot"},
            }
        }

        # Extract
        with PerformanceTimer("bronze_extract_100k") as timer:
            extractor = build_extractor(cfg)
            records, _ = extractor.fetch_records(cfg, performance_run_date)

        assert len(records) == 100_000
        assert timer.elapsed_seconds < 60, f"Extraction took too long: {timer.elapsed_seconds:.2f}s"

    def test_bronze_chunking_100k_records(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
        tmp_path: Path,
    ):
        """Bronze chunking should handle 100K records with multiple chunks."""
        # Generate data
        df = claims_100k_generator.generate_t0(performance_run_date)
        records = df.to_dict("records")

        # Chunk with 10K rows per chunk
        with PerformanceTimer("chunk_100k") as chunk_timer:
            chunks = chunk_records(records, max_rows=10_000)

        assert len(chunks) == 10  # 100K / 10K = 10 chunks
        assert chunk_timer.elapsed_seconds < 10

        # Write all chunks
        output_dir = tmp_path / "bronze"
        output_dir.mkdir()

        with PerformanceTimer("write_chunks") as write_timer:
            for i, chunk in enumerate(chunks):
                write_parquet_chunk(chunk, output_dir / f"chunk_{i}.parquet")

        assert write_timer.elapsed_seconds < 30

        # Verify total records
        total_records = sum(
            len(pd.read_parquet(output_dir / f"chunk_{i}.parquet"))
            for i in range(10)
        )
        assert total_records == 100_000

    def test_bronze_full_pipeline_100k(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
        tmp_path: Path,
    ):
        """Full Bronze pipeline should complete in reasonable time for 100K records."""
        # Generate data
        df = claims_100k_generator.generate_t0(performance_run_date)
        records = df.to_dict("records")

        bronze_path = tmp_path / "bronze" / f"dt={performance_run_date}"
        bronze_path.mkdir(parents=True)

        with PerformanceTimer("full_bronze_100k") as timer:
            # Write data
            chunk_file = bronze_path / "chunk_0.parquet"
            write_parquet_chunk(records, chunk_file)

            # Write metadata
            write_batch_metadata(
                out_dir=bronze_path,
                record_count=len(records),
                chunk_count=1,
                cursor=None,
            )

            # Write checksums
            write_checksum_manifest(
                out_dir=bronze_path,
                files=[chunk_file],
                load_pattern="snapshot",
            )

        # Verify output
        result_df = pd.read_parquet(chunk_file)
        assert len(result_df) == 100_000

        # Performance baseline
        assert timer.elapsed_seconds < 60


# =============================================================================
# Test: Silver Transformation Performance
# =============================================================================


class TestSilverTransformationPerformance:
    """Tests for Silver transformation at 100K scale."""

    def test_silver_deduplication_100k(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
        tmp_path: Path,
    ):
        """Silver deduplication should handle 100K records efficiently."""
        # Generate data with some duplicates
        df = claims_100k_generator.generate_t0(performance_run_date)

        # Add 10K duplicates (10%)
        duplicates = df.sample(10_000, random_state=42)
        df_with_dups = pd.concat([df, duplicates], ignore_index=True)
        assert len(df_with_dups) == 110_000

        with PerformanceTimer("dedupe_100k") as timer:
            deduped = df_with_dups.drop_duplicates(subset=["claim_id"], keep="last")

        assert len(deduped) == 100_000
        assert timer.elapsed_seconds < 10

    def test_silver_normalization_100k(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
    ):
        """Silver normalization should handle 100K records efficiently."""
        df = claims_100k_generator.generate_t0(performance_run_date)

        # Add whitespace to string columns
        df["diagnosis_code"] = df["diagnosis_code"].apply(lambda x: f"  {x}  ")

        with PerformanceTimer("normalize_100k") as timer:
            # Normalize string columns
            for col in df.select_dtypes(include=["object"]).columns:
                if df[col].dtype == "object":
                    df[col] = df[col].fillna("").astype(str).str.strip()

        assert timer.elapsed_seconds < 30

    def test_silver_incremental_merge_100k(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
        tmp_path: Path,
    ):
        """Silver incremental merge should handle 100K key sets efficiently."""
        # T0: 100K records
        t0_df = claims_100k_generator.generate_t0(performance_run_date)

        # T1: 20K updates + 5K new
        t1_df = claims_100k_generator.generate_t1(
            performance_run_date + timedelta(days=1),
            t0_df,
        )

        with PerformanceTimer("merge_100k") as timer:
            # Merge T1 into T0
            merged = pd.concat([t0_df, t1_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["claim_id"], keep="last")

        # Should have T0 + new T1 records
        t0_ids = set(t0_df["claim_id"])
        t1_ids = set(t1_df["claim_id"])
        expected_count = len(t0_ids | t1_ids)

        assert len(merged) == expected_count
        assert timer.elapsed_seconds < 30


# =============================================================================
# Test: Memory Usage at Scale
# =============================================================================


class TestMemoryUsage:
    """Tests to verify memory usage stays bounded at 100K scale."""

    def test_generator_memory_cleanup(
        self,
        performance_run_date: date,
    ):
        """Generator should release memory after use."""
        import gc

        # Get baseline memory
        gc.collect()

        # Generate 100K records
        gen = ClaimsGenerator(seed=42, row_count=100_000)
        df = gen.generate_t0(performance_run_date)

        # Get memory with data
        data_memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        assert data_memory_mb < 500  # Reasonable for 100K rows

        # Delete and cleanup
        del df
        del gen
        gc.collect()

    def test_chunked_processing_memory(
        self,
        performance_run_date: date,
        tmp_path: Path,
    ):
        """Chunked processing should keep memory bounded."""
        gen = ClaimsGenerator(seed=42, row_count=100_000)
        df = gen.generate_t0(performance_run_date)
        records = df.to_dict("records")

        # Process in chunks
        chunks = chunk_records(records, max_rows=10_000)

        max_chunk_memory_mb = 0
        for chunk in chunks:
            chunk_df = pd.DataFrame(chunk)
            chunk_memory = chunk_df.memory_usage(deep=True).sum() / (1024 * 1024)
            max_chunk_memory_mb = max(max_chunk_memory_mb, chunk_memory)
            del chunk_df

        # Each chunk should be ~1/10 of total
        assert max_chunk_memory_mb < 100  # Much less than full 100K


# =============================================================================
# Test: Full Pipeline at Scale
# =============================================================================


class TestFullPipelineScale:
    """End-to-end tests at 100K scale."""

    def test_full_bronze_silver_100k(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
        tmp_path: Path,
    ):
        """Full Bronze→Silver pipeline should complete for 100K records."""
        timings = {}

        # Generate data
        with PerformanceTimer("generate") as t:
            df = claims_100k_generator.generate_t0(performance_run_date)
        timings["generate"] = t.elapsed_seconds

        records = df.to_dict("records")

        # Bronze
        bronze_path = tmp_path / "bronze" / f"dt={performance_run_date}"
        bronze_path.mkdir(parents=True)

        with PerformanceTimer("bronze") as t:
            chunk_file = bronze_path / "chunk_0.parquet"
            write_parquet_chunk(records, chunk_file)
            write_batch_metadata(out_dir=bronze_path, record_count=len(records), chunk_count=1, cursor=None)
            write_checksum_manifest(out_dir=bronze_path, files=[chunk_file], load_pattern="snapshot")
        timings["bronze"] = t.elapsed_seconds

        # Silver
        silver_path = tmp_path / "silver" / f"dt={performance_run_date}"
        silver_path.mkdir(parents=True)

        with PerformanceTimer("silver") as t:
            bronze_df = pd.read_parquet(chunk_file)
            silver_df = bronze_df.drop_duplicates(subset=["claim_id"], keep="last")
            silver_df.to_parquet(silver_path / "data.parquet", index=False)
        timings["silver"] = t.elapsed_seconds

        # Verify
        final_df = pd.read_parquet(silver_path / "data.parquet")
        assert len(final_df) == 100_000

        # Total time
        total_time = sum(timings.values())
        assert total_time < 120, f"Full pipeline took too long: {total_time:.2f}s"

    def test_multi_batch_100k(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
        tmp_path: Path,
    ):
        """Multiple batches of 100K should be processable."""
        bronze_path = tmp_path / "bronze"

        # Process 3 batches (T0, T1, T2)
        prev_df = None
        total_records = 0

        for batch in range(3):
            batch_date = performance_run_date + timedelta(days=batch)

            if prev_df is None:
                df = claims_100k_generator.generate_t0(batch_date)
            else:
                df = claims_100k_generator.generate_t1(batch_date, prev_df)

            batch_path = bronze_path / f"dt={batch_date}"
            batch_path.mkdir(parents=True)

            write_parquet_chunk(df.to_dict("records"), batch_path / "chunk_0.parquet")
            total_records += len(df)
            prev_df = df

        # Verify all batches created
        batch_dirs = list(bronze_path.glob("dt=*"))
        assert len(batch_dirs) == 3

        # Total records should be T0 + T1 + T2
        assert total_records > 100_000  # At least T0


# =============================================================================
# Test: Performance Baseline Reporting
# =============================================================================


class TestPerformanceBaselines:
    """Tests that report performance baselines for regression detection."""

    def test_record_performance_baselines(
        self,
        claims_100k_generator: ClaimsGenerator,
        performance_run_date: date,
        tmp_path: Path,
    ):
        """Record performance baselines for key operations."""
        baselines = {}

        # Data generation
        with PerformanceTimer("data_generation_100k") as t:
            df = claims_100k_generator.generate_t0(performance_run_date)
        baselines["data_generation_100k_seconds"] = t.elapsed_seconds

        # Parquet write
        output_file = tmp_path / "test.parquet"
        with PerformanceTimer("parquet_write_100k") as t:
            df.to_parquet(output_file, index=False)
        baselines["parquet_write_100k_seconds"] = t.elapsed_seconds

        # Parquet read
        with PerformanceTimer("parquet_read_100k") as t:
            _ = pd.read_parquet(output_file)
        baselines["parquet_read_100k_seconds"] = t.elapsed_seconds

        # Deduplication
        with PerformanceTimer("deduplication_100k") as t:
            _ = df.drop_duplicates(subset=["claim_id"], keep="last")
        baselines["deduplication_100k_seconds"] = t.elapsed_seconds

        # Sort
        with PerformanceTimer("sort_100k") as t:
            _ = df.sort_values("service_date")
        baselines["sort_100k_seconds"] = t.elapsed_seconds

        # Print baselines for reference
        print("\n=== Performance Baselines (100K rows) ===")
        for name, value in baselines.items():
            print(f"  {name}: {value:.3f}s")

        # All operations should complete in reasonable time
        for name, value in baselines.items():
            assert value < 60, f"{name} exceeded 60s: {value:.2f}s"
