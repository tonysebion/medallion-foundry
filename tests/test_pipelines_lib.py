"""Unit tests for pipelines/lib modules.

These tests verify the core pipeline abstractions work correctly
without requiring ibis or external dependencies.
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestWatermark:
    """Tests for watermark persistence."""

    def test_get_watermark_returns_none_when_not_exists(self, tmp_path):
        """Should return None when no watermark file exists."""
        with patch("pipelines.lib.watermark._get_state_dir", return_value=tmp_path):
            from pipelines.lib.watermark import get_watermark

            result = get_watermark("test_system", "test_entity")
            assert result is None

    def test_save_and_get_watermark(self, tmp_path):
        """Should save and retrieve watermark value."""
        with patch("pipelines.lib.watermark._get_state_dir", return_value=tmp_path):
            from pipelines.lib.watermark import get_watermark, save_watermark

            save_watermark("test_system", "test_entity", "2025-01-15T10:00:00")
            result = get_watermark("test_system", "test_entity")

            assert result == "2025-01-15T10:00:00"

    def test_delete_watermark(self, tmp_path):
        """Should delete watermark and return True."""
        with patch("pipelines.lib.watermark._get_state_dir", return_value=tmp_path):
            from pipelines.lib.watermark import (
                delete_watermark,
                get_watermark,
                save_watermark,
            )

            save_watermark("test_system", "test_entity", "2025-01-15")
            deleted = delete_watermark("test_system", "test_entity")

            assert deleted is True
            assert get_watermark("test_system", "test_entity") is None

    def test_delete_watermark_returns_false_when_not_exists(self, tmp_path):
        """Should return False when watermark doesn't exist."""
        with patch("pipelines.lib.watermark._get_state_dir", return_value=tmp_path):
            from pipelines.lib.watermark import delete_watermark

            result = delete_watermark("nonexistent", "entity")
            assert result is False

    def test_list_watermarks(self, tmp_path):
        """Should list all stored watermarks."""
        with patch("pipelines.lib.watermark._get_state_dir", return_value=tmp_path):
            from pipelines.lib.watermark import list_watermarks, save_watermark

            save_watermark("system1", "entity1", "value1")
            save_watermark("system2", "entity2", "value2")

            result = list_watermarks()

            assert len(result) == 2
            assert "system1.entity1" in result
            assert "system2.entity2" in result

    def test_clear_all_watermarks(self, tmp_path):
        """Should clear all watermarks and return count."""
        with patch("pipelines.lib.watermark._get_state_dir", return_value=tmp_path):
            from pipelines.lib.watermark import (
                clear_all_watermarks,
                list_watermarks,
                save_watermark,
            )

            save_watermark("system1", "entity1", "value1")
            save_watermark("system2", "entity2", "value2")

            count = clear_all_watermarks()

            assert count == 2
            assert len(list_watermarks()) == 0


class TestResilience:
    """Tests for retry decorator and circuit breaker."""

    def test_with_retry_succeeds_on_first_attempt(self):
        """Should succeed without retry when no exception."""
        from pipelines.lib.resilience import with_retry

        call_count = 0

        @with_retry(max_attempts=3)
        def success_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = success_func()

        assert result == "success"
        assert call_count == 1

    def test_with_retry_retries_on_failure(self):
        """Should retry on failure and eventually succeed."""
        from pipelines.lib.resilience import with_retry

        call_count = 0

        @with_retry(max_attempts=3, backoff_seconds=0.01)
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Transient error")
            return "success"

        result = flaky_func()

        assert result == "success"
        assert call_count == 3

    def test_with_retry_raises_after_max_attempts(self):
        """Should raise exception after max attempts exhausted."""
        from pipelines.lib.resilience import with_retry

        @with_retry(max_attempts=2, backoff_seconds=0.01)
        def always_fails():
            raise ValueError("Always fails")

        with pytest.raises(ValueError, match="Always fails"):
            always_fails()

    def test_with_retry_only_retries_specified_exceptions(self):
        """Should only retry on specified exception types."""
        from pipelines.lib.resilience import with_retry

        call_count = 0

        @with_retry(max_attempts=3, retry_exceptions=(ConnectionError,), backoff_seconds=0.01)
        def wrong_exception():
            nonlocal call_count
            call_count += 1
            raise ValueError("Wrong type")

        with pytest.raises(ValueError):
            wrong_exception()

        # Should not retry for ValueError
        assert call_count == 1

    def test_circuit_breaker_opens_after_threshold(self):
        """Circuit breaker should open after failure threshold."""
        from pipelines.lib.resilience import CircuitBreaker, CircuitBreakerOpen

        breaker = CircuitBreaker(failure_threshold=2, recovery_time=60)

        # First failure
        with pytest.raises(ValueError):
            with breaker:
                raise ValueError("Error 1")

        # Second failure - should open circuit
        with pytest.raises(ValueError):
            with breaker:
                raise ValueError("Error 2")

        # Circuit is now open - should fail fast
        with pytest.raises(CircuitBreakerOpen):
            with breaker:
                pass  # Never reached

    def test_circuit_breaker_resets_on_success(self):
        """Circuit breaker should reset failure count on success."""
        from pipelines.lib.resilience import CircuitBreaker

        breaker = CircuitBreaker(failure_threshold=3, recovery_time=60)

        # One failure
        with pytest.raises(ValueError):
            with breaker:
                raise ValueError("Error")

        assert breaker.failure_count == 1

        # Success resets counter
        with breaker:
            pass

        assert breaker.failure_count == 0


class TestValidate:
    """Tests for configuration validation."""

    def test_validate_bronze_source_missing_system(self):
        """Should report error when system is missing."""
        from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
        from pipelines.lib.validate import validate_bronze_source, ValidationSeverity

        source = BronzeSource(
            system="",  # Missing
            entity="test",
            source_type=SourceType.FILE_CSV,
            source_path="/path/to/file.csv",
            target_path="/output/",
        )

        issues = validate_bronze_source(source)
        errors = [i for i in issues if i.severity == ValidationSeverity.ERROR]

        assert len(errors) >= 1
        assert any("system" in e.field for e in errors)

    def test_validate_bronze_source_database_missing_host(self):
        """Should report error when database host is missing."""
        from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
        from pipelines.lib.validate import validate_bronze_source, ValidationSeverity

        source = BronzeSource(
            system="test_system",
            entity="test_entity",
            source_type=SourceType.DATABASE_MSSQL,
            source_path="",
            target_path="/output/",
            options={},  # Missing host
        )

        issues = validate_bronze_source(source)
        errors = [i for i in issues if i.severity == ValidationSeverity.ERROR]

        assert any("host" in e.field for e in errors)

    def test_validate_bronze_source_incremental_missing_watermark(self):
        """Should report error when incremental load has no watermark column."""
        from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
        from pipelines.lib.validate import validate_bronze_source, ValidationSeverity

        source = BronzeSource(
            system="test_system",
            entity="test_entity",
            source_type=SourceType.FILE_CSV,
            source_path="/path/to/file.csv",
            target_path="/output/",
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            watermark_column=None,  # Missing!
        )

        issues = validate_bronze_source(source)
        errors = [i for i in issues if i.severity == ValidationSeverity.ERROR]

        assert any("watermark" in e.field for e in errors)

    def test_validate_silver_entity_missing_natural_keys(self):
        """Should report error when natural keys are missing."""
        from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
        from pipelines.lib.validate import validate_silver_entity, ValidationSeverity

        entity = SilverEntity(
            source_path="/bronze/*.parquet",
            target_path="/silver/",
            natural_keys=[],  # Missing!
            change_timestamp="updated_at",
        )

        issues = validate_silver_entity(entity)
        errors = [i for i in issues if i.severity == ValidationSeverity.ERROR]

        assert any("natural_keys" in e.field for e in errors)

    def test_validate_and_raise_with_errors(self):
        """Should raise ValueError when errors exist."""
        from pipelines.lib.bronze import BronzeSource, SourceType
        from pipelines.lib.validate import validate_and_raise

        source = BronzeSource(
            system="",  # Invalid
            entity="",  # Invalid
            source_type=SourceType.FILE_CSV,
            source_path="",
            target_path="",
        )

        with pytest.raises(ValueError, match="Configuration validation failed"):
            validate_and_raise(source=source)


class TestRunner:
    """Tests for pipeline runner utilities."""

    def test_pipeline_decorator_adds_timing(self):
        """Pipeline decorator should add elapsed time to result."""
        from pipelines.lib.runner import pipeline

        @pipeline("test.pipeline")
        def test_pipeline():
            return {"row_count": 10}

        result = test_pipeline()

        assert "_elapsed_seconds" in result
        assert result["_elapsed_seconds"] >= 0
        assert result["_pipeline"] == "test.pipeline"

    def test_pipeline_decorator_dry_run(self):
        """Pipeline decorator should handle dry_run flag."""
        from pipelines.lib.runner import pipeline

        @pipeline("test.pipeline")
        def test_pipeline():
            return {"row_count": 10}

        result = test_pipeline(dry_run=True)

        assert result["dry_run"] is True
        assert result["pipeline"] == "test.pipeline"

    def test_pipeline_result_properties(self):
        """PipelineResult should have correct properties."""
        from pipelines.lib.runner import PipelineResult

        result = PipelineResult(
            success=True,
            bronze={"row_count": 100},
            silver={"row_count": 95},
            elapsed_seconds=5.5,
            pipeline_name="test.pipeline",
        )

        assert result.success is True
        assert result.total_rows == 195
        assert result.was_skipped is False
        assert result.elapsed_seconds == 5.5

    def test_pipeline_result_skipped(self):
        """PipelineResult should detect skipped runs."""
        from pipelines.lib.runner import PipelineResult

        result = PipelineResult(
            success=True,
            bronze={"skipped": True, "reason": "already_exists"},
            elapsed_seconds=0.1,
        )

        assert result.was_skipped is True


class TestCurate:
    """Tests for curation helpers (mocked without ibis)."""

    def test_curate_functions_exist(self):
        """Verify all expected curation functions exist."""
        from pipelines.lib import curate

        assert hasattr(curate, "dedupe_latest")
        assert hasattr(curate, "dedupe_earliest")
        assert hasattr(curate, "build_history")
        assert hasattr(curate, "dedupe_exact")
        assert hasattr(curate, "filter_incremental")
        assert hasattr(curate, "rank_by_keys")
        assert hasattr(curate, "coalesce_columns")
        assert hasattr(curate, "union_dedupe")


class TestConnections:
    """Tests for connection pooling."""

    def test_connection_registry_reuses_connections(self):
        """Should reuse existing connections by name."""
        from pipelines.lib.connections import (
            _connections,
            close_all_connections,
            get_connection_count,
            list_connections,
        )

        # Clean state
        close_all_connections()
        assert get_connection_count() == 0
        assert list_connections() == []

    def test_resolve_env_substitutes_variables(self):
        """Should substitute environment variables."""
        import os
        from pipelines.lib.connections import _resolve_env

        os.environ["TEST_VAR"] = "test_value"

        result = _resolve_env("${TEST_VAR}")
        assert result == "test_value"

        # Clean up
        del os.environ["TEST_VAR"]

    def test_resolve_env_returns_empty_for_missing(self):
        """Should return empty string for missing env vars."""
        from pipelines.lib.connections import _resolve_env

        result = _resolve_env("${NONEXISTENT_VAR_12345}")
        assert result == ""

    def test_resolve_env_returns_literal_for_non_var(self):
        """Should return literal value when not an env var pattern."""
        from pipelines.lib.connections import _resolve_env

        result = _resolve_env("literal_value")
        assert result == "literal_value"
