"""Tests for Bronze ingestion patterns per spec Section 3.

Tests all Bronze source types:
- db_table: Single table extraction
- db_query: Custom SQL query extraction
- db_multi: Multi-entity parallel extraction
- file_batch: File batch with manifest tracking
- api: REST API extraction

Tests all load patterns per spec Section 4:
- SNAPSHOT: Full replacement
- INCREMENTAL_APPEND: Append-only
- INCREMENTAL_MERGE: Upsert/merge
"""

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

from core.services.pipelines.bronze.io import (
    chunk_records,
    write_parquet_chunk,
    write_csv_chunk,
    write_batch_metadata,
    merge_parquet_records,
    merge_csv_records,
)
from core.primitives.foundations.patterns import LoadPattern
from core.infrastructure.config.validation import validate_config_dict
from core.primitives.state.manifest import ManifestTracker


class TestLoadPatterns:
    """Test load pattern configurations and validation."""

    def test_snapshot_pattern_normalization(self):
        """SNAPSHOT pattern should be recognized."""
        pattern = LoadPattern.normalize("snapshot")
        assert pattern == LoadPattern.SNAPSHOT

    def test_incremental_append_pattern_normalization(self):
        """INCREMENTAL_APPEND pattern should be recognized."""
        pattern = LoadPattern.normalize("incremental_append")
        assert pattern == LoadPattern.INCREMENTAL_APPEND

    def test_incremental_merge_pattern_normalization(self):
        """INCREMENTAL_MERGE pattern should be recognized."""
        pattern = LoadPattern.normalize("incremental_merge")
        assert pattern == LoadPattern.INCREMENTAL_MERGE

    def test_legacy_full_pattern_maps_to_snapshot(self):
        """Legacy 'full' pattern should map to SNAPSHOT."""
        pattern = LoadPattern.normalize("full")
        assert pattern == LoadPattern.SNAPSHOT

    def test_legacy_cdc_pattern_maps_to_incremental_append(self):
        """Legacy 'cdc' pattern should map to INCREMENTAL_APPEND."""
        pattern = LoadPattern.normalize("cdc")
        assert pattern == LoadPattern.INCREMENTAL_APPEND


class TestChunkRecords:
    """Test record chunking functionality."""

    def test_chunk_by_row_count(self):
        """Records should be chunked by row count."""
        records = [{"id": i} for i in range(100)]
        chunks = chunk_records(records, max_rows=25)
        assert len(chunks) == 4
        assert all(len(chunk) == 25 for chunk in chunks)

    def test_chunk_by_size(self):
        """Records should be chunked by size."""
        records = [{"id": i, "data": "x" * 1000} for i in range(100)]
        chunks = chunk_records(records, max_size_mb=0.05)
        assert len(chunks) > 1
        # Each chunk should be under size limit

    def test_no_chunking_returns_single_chunk(self):
        """Without limits, records should be single chunk."""
        records = [{"id": i} for i in range(100)]
        chunks = chunk_records(records, max_rows=0)
        assert len(chunks) == 1
        assert len(chunks[0]) == 100

    def test_empty_records_returns_empty_list(self):
        """Empty records should return empty list."""
        chunks = chunk_records([], max_rows=10)
        assert chunks == []


class TestWriteChunks:
    """Test chunk writing functionality."""

    def test_write_parquet_chunk(self, temp_dir):
        """Should write records as Parquet."""
        records = [{"id": i, "name": f"Item {i}"} for i in range(10)]
        out_path = temp_dir / "test.parquet"
        write_parquet_chunk(records, out_path)

        assert out_path.exists()
        df = pd.read_parquet(out_path)
        assert len(df) == 10
        assert list(df.columns) == ["id", "name"]

    def test_write_csv_chunk(self, temp_dir):
        """Should write records as CSV."""
        records = [{"id": i, "name": f"Item {i}"} for i in range(10)]
        out_path = temp_dir / "test.csv"
        write_csv_chunk(records, out_path)

        assert out_path.exists()
        df = pd.read_csv(out_path)
        assert len(df) == 10

    def test_write_empty_chunk_does_nothing(self, temp_dir):
        """Empty chunk should not create file."""
        out_path = temp_dir / "empty.parquet"
        write_parquet_chunk([], out_path)
        assert not out_path.exists()


class TestMergeRecords:
    """Test INCREMENTAL_MERGE pattern implementation."""

    def test_merge_parquet_creates_new_file_if_not_exists(self, temp_dir):
        """Merge should create new file if none exists."""
        out_path = temp_dir / "merged.parquet"
        records = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]

        count = merge_parquet_records(out_path, records, primary_keys=["id"])
        assert count == 2
        assert out_path.exists()

    def test_merge_parquet_updates_existing_records(self, temp_dir):
        """Merge should update records with matching keys."""
        out_path = temp_dir / "merged.parquet"

        # Create initial data
        initial = pd.DataFrame([
            {"id": 1, "name": "A", "value": 100},
            {"id": 2, "name": "B", "value": 200},
        ])
        initial.to_parquet(out_path, index=False)

        # Merge with updates
        new_records = [
            {"id": 2, "name": "B Updated", "value": 250},
            {"id": 3, "name": "C", "value": 300},
        ]

        count = merge_parquet_records(out_path, new_records, primary_keys=["id"])
        assert count == 3

        df = pd.read_parquet(out_path)
        assert len(df) == 3
        assert df[df["id"] == 2]["name"].values[0] == "B Updated"

    def test_merge_csv_updates_existing_records(self, temp_dir):
        """CSV merge should update records with matching keys."""
        out_path = temp_dir / "merged.csv"

        # Create initial data
        initial = pd.DataFrame([
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"},
        ])
        initial.to_csv(out_path, index=False)

        # Merge with updates
        new_records = [{"id": 2, "name": "B Updated"}]
        count = merge_csv_records(out_path, new_records, primary_keys=["id"])

        df = pd.read_csv(out_path)
        assert len(df) == 2
        assert df[df["id"] == 2]["name"].values[0] == "B Updated"

    def test_merge_with_composite_key(self, temp_dir):
        """Merge should work with composite primary keys."""
        out_path = temp_dir / "merged.parquet"

        initial = pd.DataFrame([
            {"region": "US", "id": 1, "value": 100},
            {"region": "EU", "id": 1, "value": 200},
        ])
        initial.to_parquet(out_path, index=False)

        new_records = [
            {"region": "US", "id": 1, "value": 150},  # Update
            {"region": "US", "id": 2, "value": 300},  # Insert
        ]

        count = merge_parquet_records(out_path, new_records, primary_keys=["region", "id"])
        assert count == 3

        df = pd.read_parquet(out_path)
        us_id1 = df[(df["region"] == "US") & (df["id"] == 1)]
        assert us_id1["value"].values[0] == 150


class TestBatchMetadata:
    """Test batch metadata writing."""

    def test_write_batch_metadata(self, temp_dir):
        """Should write metadata JSON with required fields."""
        metadata_path = write_batch_metadata(
            out_dir=temp_dir,
            record_count=100,
            chunk_count=4,
            cursor="2024-01-15T12:00:00Z",
        )

        assert metadata_path.exists()
        with open(metadata_path) as f:
            metadata = json.load(f)

        assert metadata["record_count"] == 100
        assert metadata["chunk_count"] == 4
        assert metadata["cursor"] == "2024-01-15T12:00:00Z"
        assert "timestamp" in metadata

    def test_metadata_includes_performance_metrics(self, temp_dir):
        """Metadata should include optional performance metrics."""
        metadata_path = write_batch_metadata(
            out_dir=temp_dir,
            record_count=100,
            chunk_count=4,
            performance_metrics={
                "duration_seconds": 5.5,
                "records_per_second": 18.2,
            },
        )

        with open(metadata_path) as f:
            metadata = json.load(f)

        assert metadata["performance"]["duration_seconds"] == 5.5


class TestConfigValidation:
    """Test configuration validation for Bronze patterns."""

    def test_validate_db_table_config(self, temp_dir):
        """db_table config should validate successfully."""
        config = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": str(temp_dir),
                }
            },
            "source": {
                "system": "test",
                "table": "users",
                "type": "db_table",
                "db": {
                    "conn_str_env": "TEST_DB",
                    "base_query": "SELECT * FROM users",
                },
                "run": {"load_pattern": "snapshot"},
            },
        }
        result = validate_config_dict(config)
        assert result["source"]["type"] == "db_table"

    def test_validate_db_query_config(self, temp_dir):
        """db_query config should validate successfully."""
        config = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": str(temp_dir),
                }
            },
            "source": {
                "system": "test",
                "table": "custom_query",
                "type": "db_query",
                "db": {
                    "conn_str_env": "TEST_DB",
                    "base_query": "SELECT a.*, b.name FROM a JOIN b ON a.id = b.id",
                },
                "run": {"load_pattern": "incremental_append"},
            },
        }
        result = validate_config_dict(config)
        assert result["source"]["type"] == "db_query"

    def test_validate_db_multi_config(self, temp_dir):
        """db_multi config should validate successfully."""
        config = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": str(temp_dir),
                }
            },
            "source": {
                "system": "test",
                "table": "multi_extract",
                "type": "db_multi",
                "db": {
                    "conn_str_env": "TEST_DB",
                    "base_query": "SELECT 1",  # Placeholder
                },
                "entities": [
                    {"name": "users", "query": "SELECT * FROM users"},
                    {"name": "orders", "query": "SELECT * FROM orders"},
                ],
                "run": {"load_pattern": "snapshot"},
            },
        }
        result = validate_config_dict(config)
        assert result["source"]["type"] == "db_multi"
        assert len(result["source"]["entities"]) == 2

    def test_validate_file_batch_config(self, temp_dir):
        """file_batch config should validate successfully."""
        config = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": str(temp_dir),
                }
            },
            "source": {
                "system": "test",
                "table": "batch_files",
                "type": "file_batch",
                "file": {
                    "path": str(temp_dir / "input"),
                    "format": "csv",
                },
                "run": {"load_pattern": "snapshot"},
            },
        }
        result = validate_config_dict(config)
        assert result["source"]["type"] == "file_batch"

    def test_validate_api_config(self, temp_dir):
        """api config should validate successfully."""
        config = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": str(temp_dir),
                }
            },
            "source": {
                "system": "test",
                "table": "api_data",
                "type": "api",
                "api": {
                    "base_url": "https://api.example.com",
                    "endpoint": "/data",
                },
                "run": {"load_pattern": "incremental_append"},
            },
        }
        result = validate_config_dict(config)
        assert result["source"]["type"] == "api"

    def test_validate_config_with_late_data(self, temp_dir):
        """Config with late_data section should validate."""
        config = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": str(temp_dir),
                }
            },
            "source": {
                "system": "test",
                "table": "events",
                "type": "file",
                "file": {"path": str(temp_dir)},
                "run": {
                    "load_pattern": "incremental_append",
                    "late_data": {
                        "mode": "quarantine",
                        "threshold_days": 7,
                        "timestamp_column": "event_ts",
                    },
                },
            },
        }
        result = validate_config_dict(config)
        late_data = result["source"]["run"]["late_data"]
        assert late_data["mode"] == "quarantine"

    def test_validate_config_with_backfill(self, temp_dir):
        """Config with backfill section should validate."""
        config = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": str(temp_dir),
                }
            },
            "source": {
                "system": "test",
                "table": "history",
                "type": "file",
                "file": {"path": str(temp_dir)},
                "run": {
                    "load_pattern": "snapshot",
                    "backfill": {
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                        "force_full": True,
                    },
                },
            },
        }
        result = validate_config_dict(config)
        backfill = result["source"]["run"]["backfill"]
        assert backfill["start_date"] == "2024-01-01"


class TestManifestTracking:
    """Test file_batch manifest tracking."""

    def test_manifest_tracker_records_files(self, temp_dir):
        """ManifestTracker should record processed files."""
        manifest_path = str(temp_dir / "manifest.json")
        tracker = ManifestTracker(manifest_path)

        # Load manifest and add discovered files
        manifest = tracker.load()
        manifest.add_discovered_file("/data/file1.csv", 1000)
        manifest.add_discovered_file("/data/file2.csv", 2000)

        # Mark as processed
        manifest.mark_processed("/data/file1.csv", "run_1", "abc123")
        manifest.mark_processed("/data/file2.csv", "run_1", "def456")
        tracker.save()

        # Reload and verify
        loaded = ManifestTracker(manifest_path)
        loaded_manifest = loaded.load()
        assert loaded_manifest.is_processed("/data/file1.csv")
        assert loaded_manifest.is_processed("/data/file2.csv")
        assert not loaded_manifest.is_processed("/data/file3.csv")

    def test_manifest_tracker_total_records(self, temp_dir):
        """ManifestTracker should track file counts."""
        manifest_path = str(temp_dir / "manifest.json")
        tracker = ManifestTracker(manifest_path)

        manifest = tracker.load()
        manifest.add_discovered_file("/data/file1.csv", 1000)
        manifest.add_discovered_file("/data/file2.csv", 2000)
        manifest.mark_processed("/data/file1.csv", "run_1")
        manifest.mark_processed("/data/file2.csv", "run_1")

        # Verify file counts via manifest
        assert len(manifest.processed_files) == 2
        assert len(manifest.pending_files) == 0
