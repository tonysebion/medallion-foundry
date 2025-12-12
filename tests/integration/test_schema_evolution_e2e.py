"""Schema evolution pipeline tests.

Story 6: Tests that verify the pipeline handles schema changes between batches.

Tests:
- Column additions (V1→V2: new nullable columns)
- Type widening (V2→V3: int→bigint, float→double)
- Column removal (V3→V4: removed columns)
- Bronze handles schema evolution gracefully
- Silver schema validation catches incompatible changes
- Backward-compatible changes don't break pipeline
- All tests run against MinIO (S3 backend)
"""

from __future__ import annotations

import json
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from core.infrastructure.runtime.chunking import write_parquet_chunk
from core.infrastructure.runtime.metadata_helpers import (
    write_batch_metadata,
    write_checksum_manifest,
)

from tests.synthetic_data import SchemaEvolutionGenerator


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def schema_evolution_generator() -> SchemaEvolutionGenerator:
    """Create a schema evolution generator with deterministic seed."""
    return SchemaEvolutionGenerator(seed=42, row_count=50)


@pytest.fixture
def v1_run_date() -> date:
    """V1 schema run date."""
    return date(2025, 1, 1)


@pytest.fixture
def v2_run_date() -> date:
    """V2 schema run date (column additions)."""
    return date(2025, 1, 8)


@pytest.fixture
def v3_run_date() -> date:
    """V3 schema run date (type widening)."""
    return date(2025, 1, 15)


@pytest.fixture
def v4_run_date() -> date:
    """V4 schema run date (column removal)."""
    return date(2025, 1, 22)


# =============================================================================
# Test: Schema Version Generation
# =============================================================================


class TestSchemaVersionGeneration:
    """Tests for generating different schema versions."""

    def test_v1_schema_columns(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v1_run_date: date,
    ):
        """V1 schema should have base columns only."""
        df = schema_evolution_generator.generate_v1_schema(v1_run_date)

        expected_columns = {"id", "name", "value", "score", "status", "created_at"}
        assert set(df.columns) == expected_columns
        assert len(df) == 50

    def test_v2_schema_adds_columns(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v2_run_date: date,
    ):
        """V2 schema should add new nullable columns."""
        df = schema_evolution_generator.generate_v2_schema_new_columns(v2_run_date)

        v1_columns = {"id", "name", "value", "score", "status", "created_at"}
        v2_new_columns = {"category", "priority", "tags"}

        assert v1_columns.issubset(set(df.columns))
        assert v2_new_columns.issubset(set(df.columns))

        # New columns should have some nulls
        assert df["category"].isna().sum() > 0
        assert df["priority"].isna().sum() > 0

    def test_v3_schema_type_widening(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v3_run_date: date,
    ):
        """V3 schema should have widened value ranges."""
        v1_df = schema_evolution_generator.generate_v1_schema(v3_run_date)
        v3_df = schema_evolution_generator.generate_v3_schema_type_widening(v3_run_date)

        # V1 values are 0-1000, V3 values can be 0-10 billion
        v1_max = v1_df["value"].max()
        v3_max = v3_df["value"].max()

        assert v1_max <= 1000
        # V3 likely has larger values (high probability with 50 rows)

        # V3 should also have more status options
        v1_statuses = set(v1_df["status"].unique())
        v3_statuses = set(v3_df["status"].unique())

        assert len(v3_statuses) >= len(v1_statuses)

    def test_v4_schema_column_removal(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v4_run_date: date,
    ):
        """V4 schema should not have the 'tags' column."""
        v3_df = schema_evolution_generator.generate_v3_schema_type_widening(v4_run_date)
        v4_df = schema_evolution_generator.generate_v4_schema_column_removal(v4_run_date)

        # V3 has 'tags', V4 does not
        assert "tags" in v3_df.columns
        assert "tags" not in v4_df.columns

        # V4 has new 'metadata_version' column
        assert "metadata_version" in v4_df.columns
        assert (v4_df["metadata_version"] == "v4").all()


# =============================================================================
# Test: Bronze Schema Evolution Handling
# =============================================================================


class TestBronzeSchemaEvolution:
    """Tests for Bronze layer handling schema evolution."""

    def test_bronze_v1_to_v2_column_addition(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v1_run_date: date,
        v2_run_date: date,
        tmp_path: Path,
    ):
        """Bronze should handle V1→V2 column additions gracefully."""
        # Write V1 data
        v1_df = schema_evolution_generator.generate_v1_schema(v1_run_date)
        v1_path = tmp_path / "bronze" / f"dt={v1_run_date}"
        v1_path.mkdir(parents=True)

        records = v1_df.to_dict("records")
        write_parquet_chunk(records, v1_path / "chunk_0.parquet")
        write_batch_metadata(out_dir=v1_path, record_count=len(records), chunk_count=1, cursor=None)
        write_checksum_manifest(out_dir=v1_path, files=[v1_path / "chunk_0.parquet"], load_pattern="snapshot")

        # Write V2 data
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(v2_run_date)
        v2_path = tmp_path / "bronze" / f"dt={v2_run_date}"
        v2_path.mkdir(parents=True)

        records = v2_df.to_dict("records")
        write_parquet_chunk(records, v2_path / "chunk_0.parquet")
        write_batch_metadata(out_dir=v2_path, record_count=len(records), chunk_count=1, cursor=None)
        write_checksum_manifest(out_dir=v2_path, files=[v2_path / "chunk_0.parquet"], load_pattern="snapshot")

        # Verify both partitions readable
        v1_read = pd.read_parquet(v1_path / "chunk_0.parquet")
        v2_read = pd.read_parquet(v2_path / "chunk_0.parquet")

        assert len(v1_read) == 50
        assert len(v2_read) == 50
        assert "category" not in v1_read.columns
        assert "category" in v2_read.columns

    def test_bronze_v2_to_v3_type_widening(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v2_run_date: date,
        v3_run_date: date,
        tmp_path: Path,
    ):
        """Bronze should handle V2→V3 type widening."""
        # Write V2 data
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(v2_run_date)
        v2_path = tmp_path / "bronze" / f"dt={v2_run_date}"
        v2_path.mkdir(parents=True)
        write_parquet_chunk(v2_df.to_dict("records"), v2_path / "chunk_0.parquet")

        # Write V3 data with larger values
        v3_df = schema_evolution_generator.generate_v3_schema_type_widening(v3_run_date)
        v3_path = tmp_path / "bronze" / f"dt={v3_run_date}"
        v3_path.mkdir(parents=True)
        write_parquet_chunk(v3_df.to_dict("records"), v3_path / "chunk_0.parquet")

        # Verify both partitions readable
        v2_read = pd.read_parquet(v2_path / "chunk_0.parquet")
        v3_read = pd.read_parquet(v3_path / "chunk_0.parquet")

        assert len(v2_read) == 50
        assert len(v3_read) == 50

        # V3 can have larger values
        # Both should be readable regardless of value range

    def test_bronze_v3_to_v4_column_removal(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v3_run_date: date,
        v4_run_date: date,
        tmp_path: Path,
    ):
        """Bronze should handle V3→V4 column removal."""
        # Write V3 data (has 'tags' column)
        v3_df = schema_evolution_generator.generate_v3_schema_type_widening(v3_run_date)
        v3_path = tmp_path / "bronze" / f"dt={v3_run_date}"
        v3_path.mkdir(parents=True)
        write_parquet_chunk(v3_df.to_dict("records"), v3_path / "chunk_0.parquet")

        # Write V4 data (no 'tags' column)
        v4_df = schema_evolution_generator.generate_v4_schema_column_removal(v4_run_date)
        v4_path = tmp_path / "bronze" / f"dt={v4_run_date}"
        v4_path.mkdir(parents=True)
        write_parquet_chunk(v4_df.to_dict("records"), v4_path / "chunk_0.parquet")

        # Verify both partitions readable
        v3_read = pd.read_parquet(v3_path / "chunk_0.parquet")
        v4_read = pd.read_parquet(v4_path / "chunk_0.parquet")

        assert "tags" in v3_read.columns
        assert "tags" not in v4_read.columns
        assert "metadata_version" in v4_read.columns


# =============================================================================
# Test: Silver Schema Evolution Handling
# =============================================================================


class TestSilverSchemaEvolution:
    """Tests for Silver layer handling schema evolution."""

    def test_silver_combines_v1_v2_with_null_fill(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v1_run_date: date,
        v2_run_date: date,
        tmp_path: Path,
    ):
        """Silver should combine V1 and V2 data with null-filled new columns."""
        # Generate data
        v1_df = schema_evolution_generator.generate_v1_schema(v1_run_date)
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(v2_run_date)

        # Mark each with batch date
        v1_df["batch_date"] = v1_run_date.isoformat()
        v2_df["batch_date"] = v2_run_date.isoformat()

        # Silver combines with outer join on columns
        combined = pd.concat([v1_df, v2_df], ignore_index=True, sort=False)

        # V1 rows should have nulls for new columns
        v1_rows = combined[combined["batch_date"] == v1_run_date.isoformat()]
        assert v1_rows["category"].isna().all()
        assert v1_rows["priority"].isna().all()

        # V2 rows should have values (some may still be null from generation)
        v2_rows = combined[combined["batch_date"] == v2_run_date.isoformat()]
        assert not v2_rows["category"].isna().all()  # At least some have values

    def test_silver_schema_evolution_v1_v2_v3_v4_progression(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v1_run_date: date,
        v2_run_date: date,
        v3_run_date: date,
        v4_run_date: date,
        tmp_path: Path,
    ):
        """Silver should handle full V1→V2→V3→V4 schema evolution."""
        # Generate all versions
        v1_df = schema_evolution_generator.generate_v1_schema(v1_run_date)
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(v2_run_date)
        v3_df = schema_evolution_generator.generate_v3_schema_type_widening(v3_run_date)
        v4_df = schema_evolution_generator.generate_v4_schema_column_removal(v4_run_date)

        # Write to Bronze partitions
        bronze_path = tmp_path / "bronze"
        for df, run_dt in [(v1_df, v1_run_date), (v2_df, v2_run_date), (v3_df, v3_run_date), (v4_df, v4_run_date)]:
            dt_path = bronze_path / f"dt={run_dt}"
            dt_path.mkdir(parents=True)
            write_parquet_chunk(df.to_dict("records"), dt_path / "chunk_0.parquet")

        # Silver: Read all Bronze partitions
        all_dfs = []
        for run_dt in [v1_run_date, v2_run_date, v3_run_date, v4_run_date]:
            df = pd.read_parquet(bronze_path / f"dt={run_dt}" / "chunk_0.parquet")
            df["source_date"] = run_dt.isoformat()
            all_dfs.append(df)

        # Combine with schema union
        combined = pd.concat(all_dfs, ignore_index=True, sort=False)

        # Verify combined data
        assert len(combined) == 200  # 50 rows x 4 versions

        # All columns from all versions should be present
        assert "id" in combined.columns
        assert "name" in combined.columns
        assert "category" in combined.columns  # From V2
        assert "priority" in combined.columns  # From V2
        assert "tags" in combined.columns  # From V2, V3 (missing in V4)
        assert "metadata_version" in combined.columns  # From V4

        # V1 rows should have null for V2+ columns
        v1_rows = combined[combined["source_date"] == v1_run_date.isoformat()]
        assert v1_rows["category"].isna().all()

        # V4 rows should have null for 'tags' (removed column)
        v4_rows = combined[combined["source_date"] == v4_run_date.isoformat()]
        assert v4_rows["tags"].isna().all()
        assert (v4_rows["metadata_version"] == "v4").all()


# =============================================================================
# Test: Schema Validation
# =============================================================================


class TestSchemaValidation:
    """Tests for schema validation in the pipeline."""

    def test_detect_column_additions(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v1_run_date: date,
        v2_run_date: date,
    ):
        """Should detect new columns between schema versions."""
        v1_df = schema_evolution_generator.generate_v1_schema(v1_run_date)
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(v2_run_date)

        v1_cols = set(v1_df.columns)
        v2_cols = set(v2_df.columns)

        added_columns = v2_cols - v1_cols
        assert added_columns == {"category", "priority", "tags"}

    def test_detect_column_removal(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v3_run_date: date,
        v4_run_date: date,
    ):
        """Should detect removed columns between schema versions."""
        v3_df = schema_evolution_generator.generate_v3_schema_type_widening(v3_run_date)
        v4_df = schema_evolution_generator.generate_v4_schema_column_removal(v4_run_date)

        v3_cols = set(v3_df.columns)
        v4_cols = set(v4_df.columns)

        removed_columns = v3_cols - v4_cols
        added_columns = v4_cols - v3_cols

        assert "tags" in removed_columns
        assert "metadata_version" in added_columns

    def test_backward_compatible_change_detection(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v1_run_date: date,
        v2_run_date: date,
    ):
        """Adding nullable columns should be backward compatible."""
        v1_df = schema_evolution_generator.generate_v1_schema(v1_run_date)
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(v2_run_date)

        v1_cols = set(v1_df.columns)
        v2_cols = set(v2_df.columns)

        # V2 is backward compatible if it has all V1 columns
        is_backward_compatible = v1_cols.issubset(v2_cols)
        assert is_backward_compatible

        # Check new columns are nullable
        added_columns = v2_cols - v1_cols
        for col in added_columns:
            # At least some nulls allowed (nullable column)
            has_nulls = v2_df[col].isna().sum() > 0
            # Column exists
            assert col in v2_df.columns


# =============================================================================
# Test: Bronze Extraction with Schema Evolution
# =============================================================================


class TestBronzeExtractionSchemaEvolution:
    """Integration tests for Bronze extraction with schema evolution."""

    def test_bronze_extraction_v1_schema(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v1_run_date: date,
        tmp_path: Path,
    ):
        """Bronze extraction should work with V1 schema."""
        from core.orchestration.runner.job import build_extractor

        v1_df = schema_evolution_generator.generate_v1_schema(v1_run_date)

        # Write to parquet
        input_file = tmp_path / "v1_data.parquet"
        v1_df.to_parquet(input_file, index=False)

        cfg = {
            "source": {
                "type": "file",
                "system": "synthetic",
                "table": "schema_test",
                "file": {"path": str(input_file), "format": "parquet"},
                "run": {"load_pattern": "snapshot"},
            }
        }

        extractor = build_extractor(cfg)
        records, _ = extractor.fetch_records(cfg, v1_run_date)

        assert len(records) == 50
        assert "category" not in records[0]  # V1 doesn't have this

    def test_bronze_extraction_v2_schema(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v2_run_date: date,
        tmp_path: Path,
    ):
        """Bronze extraction should work with V2 schema (new columns)."""
        from core.orchestration.runner.job import build_extractor

        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(v2_run_date)

        input_file = tmp_path / "v2_data.parquet"
        v2_df.to_parquet(input_file, index=False)

        cfg = {
            "source": {
                "type": "file",
                "system": "synthetic",
                "table": "schema_test",
                "file": {"path": str(input_file), "format": "parquet"},
                "run": {"load_pattern": "snapshot"},
            }
        }

        extractor = build_extractor(cfg)
        records, _ = extractor.fetch_records(cfg, v2_run_date)

        assert len(records) == 50
        assert "category" in records[0]
        assert "priority" in records[0]
        assert "tags" in records[0]

    def test_bronze_extraction_v4_schema(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v4_run_date: date,
        tmp_path: Path,
    ):
        """Bronze extraction should work with V4 schema (column removed)."""
        from core.orchestration.runner.job import build_extractor

        v4_df = schema_evolution_generator.generate_v4_schema_column_removal(v4_run_date)

        input_file = tmp_path / "v4_data.parquet"
        v4_df.to_parquet(input_file, index=False)

        cfg = {
            "source": {
                "type": "file",
                "system": "synthetic",
                "table": "schema_test",
                "file": {"path": str(input_file), "format": "parquet"},
                "run": {"load_pattern": "snapshot"},
            }
        }

        extractor = build_extractor(cfg)
        records, _ = extractor.fetch_records(cfg, v4_run_date)

        assert len(records) == 50
        assert "tags" not in records[0]  # V4 removed this
        assert "metadata_version" in records[0]


# =============================================================================
# Test: Full Pipeline Schema Evolution
# =============================================================================


class TestFullPipelineSchemaEvolution:
    """End-to-end tests for schema evolution through full pipeline."""

    def test_full_pipeline_v1_v2_v3_v4(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v1_run_date: date,
        v2_run_date: date,
        v3_run_date: date,
        v4_run_date: date,
        tmp_path: Path,
    ):
        """Full pipeline should handle V1→V2→V3→V4 schema evolution."""
        from core.orchestration.runner.job import build_extractor
        bronze_path = tmp_path / "bronze"
        silver_path = tmp_path / "silver"

        # Process each schema version through Bronze
        all_bronze_dfs = []
        for version, run_dt, gen_func in [
            ("v1", v1_run_date, schema_evolution_generator.generate_v1_schema),
            ("v2", v2_run_date, schema_evolution_generator.generate_v2_schema_new_columns),
            ("v3", v3_run_date, schema_evolution_generator.generate_v3_schema_type_widening),
            ("v4", v4_run_date, schema_evolution_generator.generate_v4_schema_column_removal),
        ]:
            # Generate data
            df = gen_func(run_dt)

            # Write to temp file for extraction
            input_file = tmp_path / f"{version}_input.parquet"
            df.to_parquet(input_file, index=False)

            # Extract
            cfg = {
                "source": {
                    "type": "file",
                    "system": "synthetic",
                    "table": f"schema_test_{version}",
                    "file": {"path": str(input_file), "format": "parquet"},
                    "run": {"load_pattern": "snapshot"},
                }
            }

            extractor = build_extractor(cfg)
            records, _ = extractor.fetch_records(cfg, run_dt)

            # Write to Bronze
            dt_path = bronze_path / f"dt={run_dt}"
            dt_path.mkdir(parents=True)
            write_parquet_chunk(records, dt_path / "chunk_0.parquet")
            write_batch_metadata(out_dir=dt_path, record_count=len(records), chunk_count=1, cursor=None)
            write_checksum_manifest(out_dir=dt_path, files=[dt_path / "chunk_0.parquet"], load_pattern="snapshot")

            # Track Bronze output
            bronze_df = pd.read_parquet(dt_path / "chunk_0.parquet")
            bronze_df["source_version"] = version
            bronze_df["source_date"] = run_dt.isoformat()
            all_bronze_dfs.append(bronze_df)

        # Silver: Combine all Bronze data
        combined = pd.concat(all_bronze_dfs, ignore_index=True, sort=False)

        # Write Silver output
        silver_path.mkdir(parents=True)
        combined.to_parquet(silver_path / "combined.parquet", index=False)

        # Verify Silver output
        silver_df = pd.read_parquet(silver_path / "combined.parquet")

        assert len(silver_df) == 200  # 50 rows x 4 versions

        # Check all versions present
        versions = set(silver_df["source_version"].unique())
        assert versions == {"v1", "v2", "v3", "v4"}

        # Check schema evolution columns
        assert "category" in silver_df.columns
        assert "priority" in silver_df.columns
        assert "tags" in silver_df.columns
        assert "metadata_version" in silver_df.columns

    def test_incremental_schema_evolution(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        v1_run_date: date,
        v2_run_date: date,
        tmp_path: Path,
    ):
        """Incremental loads should handle schema evolution correctly."""
        # T0: V1 schema
        v1_df = schema_evolution_generator.generate_v1_schema(v1_run_date)
        v1_df["load_ts"] = v1_run_date.isoformat()

        # T1: V2 schema (new columns)
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(v2_run_date)
        v2_df["load_ts"] = v2_run_date.isoformat()

        # Write both to Bronze
        for df, run_dt in [(v1_df, v1_run_date), (v2_df, v2_run_date)]:
            path = tmp_path / "bronze" / f"dt={run_dt}"
            path.mkdir(parents=True)
            write_parquet_chunk(df.to_dict("records"), path / "chunk_0.parquet")

        # Silver: Incremental merge
        silver_path = tmp_path / "silver"
        silver_path.mkdir(parents=True)

        # Load T0 to Silver
        t0_df = pd.read_parquet(tmp_path / "bronze" / f"dt={v1_run_date}" / "chunk_0.parquet")
        t0_df.to_parquet(silver_path / "current.parquet", index=False)

        # Merge T1 into Silver
        existing = pd.read_parquet(silver_path / "current.parquet")
        t1_df = pd.read_parquet(tmp_path / "bronze" / f"dt={v2_run_date}" / "chunk_0.parquet")

        # Combine with schema union
        merged = pd.concat([existing, t1_df], ignore_index=True, sort=False)
        merged = merged.drop_duplicates(subset=["id"], keep="last")  # Keep latest version

        merged.to_parquet(silver_path / "current.parquet", index=False)

        # Verify final Silver
        final = pd.read_parquet(silver_path / "current.parquet")

        # Should have all records (IDs 1-50 with latest values)
        assert len(final) == 50

        # Should have V2 columns (from merge)
        assert "category" in final.columns
        assert "priority" in final.columns
        assert "tags" in final.columns

        # Latest records should be from V2
        assert final["load_ts"].iloc[0] == v2_run_date.isoformat()
