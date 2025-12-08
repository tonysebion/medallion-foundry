"""Tests for orchestration runner job module."""

import pytest
from unittest.mock import Mock, patch
from datetime import date
from pathlib import Path
from typing import Any

from core.orchestration.runner.job import (
    build_extractor,
    ExtractJob,
    run_extract,
)
from core.infrastructure.runtime.context import RunContext
from core.infrastructure.runtime.metadata import Layer, RunStatus
from core.foundation.primitives.patterns import LoadPattern
from core.foundation.primitives.exceptions import ConfigValidationError
from core.infrastructure.io.extractors.base import EXTRACTOR_REGISTRY
from core.domain.services.processing.chunk_config import StoragePlan


class TestBuildExtractor:
    """Tests for build_extractor function."""

    def test_builds_api_extractor(self):
        """Test building API extractor."""
        cfg = {
            "source": {
                "type": "api",
                "system": "test",
                "table": "test_table",
                "api": {
                    "base_url": "https://api.example.com",
                    "endpoint": "/data",
                },
                "run": {},
            }
        }
        extractor = build_extractor(cfg)
        assert extractor is not None
        assert hasattr(extractor, "fetch_records")

    def test_builds_file_extractor(self):
        """Test building file extractor."""
        cfg = {
            "source": {
                "type": "file",
                "system": "test",
                "table": "test_table",
                "file": {"path": "/path/to/file.csv"},
                "run": {},
            }
        }
        extractor = build_extractor(cfg)
        assert extractor is not None

    def test_builds_db_extractor(self):
        """Test building database extractor."""
        cfg = {
            "source": {
                "type": "db",
                "system": "test",
                "table": "test_table",
                "db": {
                    "conn_str_env": "TEST_DB_CONN",
                    "base_query": "SELECT * FROM table",
                },
                "run": {},
            }
        }
        extractor = build_extractor(cfg)
        assert extractor is not None

    def test_raises_for_unknown_type(self):
        """Test that unknown type raises ConfigValidationError."""
        cfg = {
            "source": {
                "type": "unknown_type",
                "system": "test",
                "table": "test_table",
                "run": {},
            }
        }
        with pytest.raises(ConfigValidationError, match="Unknown source.type"):
            build_extractor(cfg)

    def test_custom_extractor_requires_module_and_class(self):
        """Test custom extractor requires module and class_name."""
        cfg = {
            "source": {
                "type": "custom",
                "system": "test",
                "table": "test_table",
                "custom_extractor": {},
                "run": {},
            }
        }
        with pytest.raises(ConfigValidationError, match="custom extractor requires both"):
            build_extractor(cfg)

    def test_custom_extractor_missing_class(self):
        """Test custom extractor missing class_name raises error."""
        cfg = {
            "source": {
                "type": "custom",
                "system": "test",
                "table": "test_table",
                "custom_extractor": {"module": "some.module"},
                "run": {},
            }
        }
        with pytest.raises(ConfigValidationError, match="custom extractor requires both"):
            build_extractor(cfg)

    def test_db_multi_extractor_with_workers(self):
        """Test db_multi extractor uses parallel_workers config."""
        cfg = {
            "source": {
                "type": "db_multi",
                "system": "test",
                "table": "test_table",
                "db": {
                    "conn_str_env": "TEST_DB_CONN",
                    "base_query": "SELECT * FROM table",
                },
                "run": {"parallel_workers": 8},
            }
        }
        extractor = build_extractor(cfg)
        assert extractor is not None


class TestExtractJob:
    """Tests for ExtractJob class."""

    def _make_context(self, records_to_return: list[Any] | None = None) -> RunContext:
        """Create a RunContext for testing."""
        return RunContext(
            cfg={
                "source": {
                    "system": "test",
                    "table": "test_table",
                    "type": "api",
                    "api": {"base_url": "https://api.example.com"},
                    "run": {
                        "load_pattern": "snapshot",
                        "max_rows_per_file": 0,
                        "checkpoint_enabled": False,
                    },
                },
                "platform": {
                    "bronze": {
                        "output_defaults": {
                            "allow_csv": True,
                            "allow_parquet": True,
                            "parquet_compression": "snappy",
                        },
                        "local_path": str(Path("/tmp/bronze/checkpoints")),
                    },
                    "s3_connection": {},
                },
            },
            run_date=date(2024, 1, 15),
            relative_path="system=test/table=test_table",
            local_output_dir=Path("/tmp/bronze"),
            bronze_path=Path("/tmp/bronze/system=test/table=test_table"),
            source_system="test",
            source_table="test_table",
            dataset_id="test.test_table",
            config_name="test.test_table",
            load_pattern=LoadPattern.SNAPSHOT,
            env_config=None,
            run_id="test-run-123",
        )

    def test_job_initialization(self):
        """Test ExtractJob initializes correctly."""
        ctx = self._make_context()
        job = ExtractJob(ctx)

        assert job.ctx == ctx
        assert job.run_date == date(2024, 1, 15)
        assert job.load_pattern == LoadPattern.SNAPSHOT
        assert job.created_files == []

    def test_source_cfg_property(self):
        """Test source_cfg property returns correct config."""
        ctx = self._make_context()
        job = ExtractJob(ctx)

        source_cfg = job.source_cfg
        assert source_cfg["system"] == "test"
        assert source_cfg["table"] == "test_table"

    @patch("core.orchestration.runner.job.build_extractor")
    @patch("core.orchestration.runner.job.emit_bronze_metadata")
    @patch("core.orchestration.runner.job.report_schema_snapshot")
    @patch("core.orchestration.runner.job.report_quality_snapshot")
    @patch("core.orchestration.runner.job.report_run_metadata")
    def test_run_with_empty_records(
        self,
        mock_report_run,
        mock_report_quality,
        mock_report_schema,
        mock_emit_metadata,
        mock_build_extractor,
    ):
        """Test run returns 0 when extractor returns empty records."""
        mock_extractor = Mock()
        mock_extractor.fetch_records.return_value = ([], None)
        mock_build_extractor.return_value = mock_extractor

        ctx = self._make_context()
        job = ExtractJob(ctx)
        result = job.run()

        assert result == 0
        mock_extractor.fetch_records.assert_called_once()

    @patch("core.orchestration.runner.job.build_extractor")
    @patch("core.orchestration.runner.job.emit_bronze_metadata")
    @patch("core.orchestration.runner.job.report_schema_snapshot")
    @patch("core.orchestration.runner.job.report_quality_snapshot")
    @patch("core.orchestration.runner.job.report_run_metadata")
    @patch("core.orchestration.runner.job.report_lineage")
    @patch("core.orchestration.runner.job.write_run_metadata")
    @patch("core.orchestration.runner.job.build_run_metadata")
    @patch("core.orchestration.runner.job.ChunkWriter")
    @patch("core.orchestration.runner.job.ChunkProcessor")
    def test_run_with_records(
        self,
        mock_chunk_processor_cls,
        mock_chunk_writer_cls,
        mock_build_run_metadata,
        mock_write_run_metadata,
        mock_report_lineage,
        mock_report_run,
        mock_report_quality,
        mock_report_schema,
        mock_emit_metadata,
        mock_build_extractor,
        tmp_path,
    ):
        """Test run processes records and returns 0."""
        mock_extractor = Mock()
        mock_extractor.fetch_records.return_value = (
            [{"id": 1, "name": "test"}],
            "cursor_123",
        )
        mock_build_extractor.return_value = mock_extractor

        mock_processor = Mock()
        mock_processor.process.return_value = [tmp_path / "chunk_001.parquet"]
        mock_chunk_processor_cls.return_value = mock_processor

        metadata_path = tmp_path / "metadata.json"
        manifest_path = tmp_path / "_checksums.json"
        mock_emit_metadata.return_value = (metadata_path, manifest_path)

        metadata_mock = Mock()
        metadata_mock.complete.return_value = metadata_mock
        mock_build_run_metadata.return_value = metadata_mock
        run_metadata_path = tmp_path / "run_metadata.json"
        mock_write_run_metadata.return_value = run_metadata_path

        ctx = RunContext(
            cfg={
                "source": {
                    "system": "test",
                    "table": "test_table",
                    "type": "api",
                    "api": {"base_url": "https://api.example.com"},
                    "run": {
                        "load_pattern": "snapshot",
                        "max_rows_per_file": 0,
                    },
                },
                "platform": {
                    "bronze": {
                        "output_defaults": {
                            "allow_csv": True,
                            "allow_parquet": True,
                            "parquet_compression": "snappy",
                        }
                    },
                    "s3_connection": {},
                },
            },
            run_date=date(2024, 1, 15),
            relative_path="system=test/table=test_table",
            local_output_dir=tmp_path,
            bronze_path=tmp_path / "system=test/table=test_table",
            source_system="test",
            source_table="test_table",
            dataset_id="test.test_table",
            config_name="test.test_table",
            load_pattern=LoadPattern.SNAPSHOT,
            env_config=None,
            run_id="test-run-123",
        )

        job = ExtractJob(ctx)
        result = job.run()

        assert result == 0
        mock_processor.process.assert_called_once()
        mock_build_run_metadata.assert_called_once()
        called_args = mock_build_run_metadata.call_args
        assert called_args.args[1] == Layer.BRONZE
        assert called_args.kwargs["run_id"] == ctx.run_id
        metadata_mock.complete.assert_called_once_with(
            row_count_in=1, row_count_out=1, status=RunStatus.SUCCESS
        )
        mock_write_run_metadata.assert_called_once_with(
            metadata_mock, ctx.local_output_dir
        )
        assert job.created_files[-1] == run_metadata_path
        mock_report_lineage.assert_called_once()
        lineage_args = mock_report_lineage.call_args[0]
        assert lineage_args[0] == "source:test.test_table"
        assert lineage_args[1] == "bronze:test.test_table"
        lineage_metadata = lineage_args[2]
        assert lineage_metadata["partition_path"] == "system=test/table=test_table"
        assert lineage_metadata["record_count"] == 1
        assert lineage_metadata["chunk_count"] == 1
        assert lineage_metadata["load_pattern"] == "snapshot"
        assert lineage_metadata["files"] == ["chunk_001.parquet"]
        assert lineage_metadata["metadata"] == metadata_path.name
        assert lineage_metadata["manifest"] == manifest_path.name
        assert lineage_metadata["cursor"] == "cursor_123"
        assert "reference_mode" not in lineage_metadata

    @patch("core.orchestration.runner.manifest_inspector.verify_checksum_manifest")
    def test_existing_manifest_aborts_run(self, mock_verify, tmp_path):
        """Bronze run should fail if a verified manifest already exists."""
        mock_verify.return_value = {"load_pattern": "snapshot"}
        ctx = self._make_context()
        ctx.local_output_dir = tmp_path
        ctx.bronze_path = tmp_path / "system=test/table=test_table"
        ctx.relative_path = "system=test/table=test_table"
        job = ExtractJob(ctx)
        job._out_dir.mkdir(parents=True, exist_ok=True)
        (job._out_dir / "_checksums.json").write_text("{}", encoding="utf-8")
        (job._out_dir / "_checksums.json").write_text("{}")

        with pytest.raises(RuntimeError, match="already contains a verified checksum manifest"):
            job._process_chunks([{"id": 1}])

    @patch("core.orchestration.runner.manifest_inspector.verify_checksum_manifest")
    @patch("core.orchestration.runner.job.ChunkProcessor")
    def test_partial_manifest_resets_and_proceeds(
        self,
        mock_chunk_processor_cls,
        mock_verify,
        tmp_path,
    ):
        """Partial Bronze runs should clear artifacts before continuing."""
        mock_verify.side_effect = ValueError("missing files")
        mock_processor = Mock()
        mock_processor.process.return_value = []
        mock_chunk_processor_cls.return_value = mock_processor

        ctx = self._make_context()
        ctx.local_output_dir = tmp_path
        ctx.bronze_path = tmp_path / "system=test/table=test_table"
        ctx.relative_path = "system=test/table=test_table"
        job = ExtractJob(ctx)
        job._out_dir.mkdir(parents=True, exist_ok=True)
        manifest = job._out_dir / "_checksums.json"
        manifest.write_text("{}")
        assert manifest.exists()

        chunk_count, chunk_files = job._process_chunks([{"id": 1}])

        assert chunk_count == 0
        assert chunk_files == []
        assert not manifest.exists()

    @patch("core.orchestration.runner.manifest_inspector.verify_checksum_manifest")
    @patch("core.orchestration.runner.job.ChunkProcessor")
    def test_schema_drift_detected_before_chunks(
        self,
        mock_chunk_processor_cls,
        mock_verify,
        tmp_path,
    ):
        """Runs with incompatible schema should abort before chunking."""
        mock_verify.return_value = {"schema": [{"name": "id", "dtype": "integer"}]}
        mock_processor = Mock()
        mock_processor.process.return_value = []
        mock_chunk_processor_cls.return_value = mock_processor

        ctx = self._make_context()
        job = ExtractJob(ctx)
        job._out_dir.mkdir(parents=True, exist_ok=True)
        (job._out_dir / "_checksums.json").write_text("{}", encoding="utf-8")
        job.schema_snapshot = [
            {"name": "id", "dtype": "integer"},
            {"name": "extra", "dtype": "string"},
        ]
        job.load_pattern = LoadPattern.SNAPSHOT

        with pytest.raises(RuntimeError, match="Schema drift detected"):
            job._inspect_existing_manifest()

    def test_process_chunks_requires_output_format(self):
        """Bronze chunking should fail if both CSV and Parquet are disabled."""
        ctx = self._make_context()
        ctx.cfg["source"]["run"].update({"write_csv": False, "write_parquet": False})
        ctx.cfg["platform"]["bronze"]["output_defaults"].update(
            {"allow_csv": False, "allow_parquet": False}
        )
        job = ExtractJob(ctx)

        with pytest.raises(ValueError, match="output format"):
            job._process_chunks([{"id": 1}])

    def test_cleanup_on_failure_removes_files(self, tmp_path):
        """Test cleanup removes created files on failure."""
        ctx = self._make_context()
        ctx = RunContext(
            cfg=ctx.cfg,
            run_date=ctx.run_date,
            relative_path=ctx.relative_path,
            local_output_dir=tmp_path,
            bronze_path=tmp_path / "output",
            source_system="test",
            source_table="test_table",
            dataset_id="test.test_table",
            config_name="test.test_table",
            load_pattern=LoadPattern.SNAPSHOT,
            env_config=None,
            run_id="test-run-123",
        )

        job = ExtractJob(ctx)

        # Create some temporary files
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test data")
        job.created_files.append(test_file)

        assert test_file.exists()
        job._cleanup_on_failure()
        assert not test_file.exists()

    def test_cleanup_on_failure_deletes_remote_artifacts(self, tmp_path):
        """Cleanup should delete remote files via the storage plan."""
        ctx = self._make_context()
        ctx.local_output_dir = tmp_path
        ctx.bronze_path = tmp_path / "output"
        ctx.relative_path = "system=test/table=test_table/"
        job = ExtractJob(ctx)

        chunk_file = job._out_dir / "chunk.json"
        chunk_file.parent.mkdir(parents=True, exist_ok=True)
        chunk_file.write_text("payload")
        job.created_files.append(chunk_file)

        backend = Mock()
        backend.delete_file.return_value = True
        job.storage_plan = StoragePlan(
            enabled=True,
            backend=backend,
            relative_path="system=test/table=test_table/",
        )

        job._cleanup_on_failure()

        backend.delete_file.assert_called_once_with("system=test/table=test_table/chunk.json")


class TestRunExtract:
    """Tests for run_extract function."""

    @patch("core.orchestration.runner.job.ExtractJob")
    def test_run_extract_creates_job_and_runs(self, mock_job_cls):
        """Test run_extract creates job and calls run."""
        mock_job = Mock()
        mock_job.run.return_value = 0
        mock_job_cls.return_value = mock_job

        ctx = RunContext(
            cfg={"source": {"system": "test", "table": "t"}},
            run_date=date(2024, 1, 15),
            relative_path="test/path",
            local_output_dir=Path("/tmp"),
            bronze_path=Path("/tmp/bronze"),
            source_system="test",
            source_table="t",
            dataset_id="test.t",
            config_name="test.t",
            load_pattern=LoadPattern.SNAPSHOT,
            env_config=None,
            run_id="run-123",
        )

        result = run_extract(ctx)

        assert result == 0
        mock_job_cls.assert_called_once_with(ctx)
        mock_job.run.assert_called_once()

    @patch("core.orchestration.runner.job.ExtractJob")
    def test_run_extract_propagates_exceptions(self, mock_job_cls):
        """Test run_extract propagates exceptions from job."""
        mock_job = Mock()
        mock_job.run.side_effect = RuntimeError("Test error")
        mock_job_cls.return_value = mock_job

        ctx = RunContext(
            cfg={"source": {"system": "test", "table": "t"}},
            run_date=date(2024, 1, 15),
            relative_path="test/path",
            local_output_dir=Path("/tmp"),
            bronze_path=Path("/tmp/bronze"),
            source_system="test",
            source_table="t",
            dataset_id="test.t",
            config_name="test.t",
            load_pattern=LoadPattern.SNAPSHOT,
            env_config=None,
            run_id="run-123",
        )

        with pytest.raises(RuntimeError, match="Test error"):
            run_extract(ctx)


class TestLoadExtractors:
    """Tests for _load_extractors function."""

    def test_load_extractors_populates_registry(self):
        """Test that _load_extractors populates the registry."""
        # This should already be called at module import time
        # Just verify registry has expected types
        assert "api" in EXTRACTOR_REGISTRY
        assert "db" in EXTRACTOR_REGISTRY
        assert "file" in EXTRACTOR_REGISTRY
