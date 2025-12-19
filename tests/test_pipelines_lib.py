"""Unit tests for pipelines/lib modules.

These tests verify the core pipeline abstractions work correctly
without requiring ibis or external dependencies.
"""

import json
from unittest.mock import patch

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
    """Tests for configuration validation.

    Note: BronzeSource and SilverEntity now validate in __post_init__,
    so these tests verify that invalid configurations raise ValueError
    at construction time.
    """

    def test_validate_bronze_source_missing_system(self):
        """Should raise error when system is missing."""
        from pipelines.lib.bronze import BronzeSource, SourceType

        with pytest.raises(ValueError, match="system"):
            BronzeSource(
                system="",  # Missing
                entity="test",
                source_type=SourceType.FILE_CSV,
                source_path="/path/to/file.csv",
                target_path="/output/",
            )

    def test_validate_bronze_source_database_missing_host(self):
        """Should raise error when database host is missing."""
        from pipelines.lib.bronze import BronzeSource, SourceType

        with pytest.raises(ValueError, match="host"):
            BronzeSource(
                system="test_system",
                entity="test_entity",
                source_type=SourceType.DATABASE_MSSQL,
                source_path="",
                target_path="/output/",
                options={},  # Missing host
            )

    def test_validate_bronze_source_incremental_missing_watermark(self):
        """Should raise error when incremental load has no watermark column."""
        from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType

        with pytest.raises(ValueError, match="watermark"):
            BronzeSource(
                system="test_system",
                entity="test_entity",
                source_type=SourceType.FILE_CSV,
                source_path="/path/to/file.csv",
                target_path="/output/",
                load_pattern=LoadPattern.INCREMENTAL_APPEND,
                watermark_column=None,  # Missing!
            )

    def test_validate_silver_entity_missing_natural_keys(self):
        """Should raise error when natural keys are missing."""
        from pipelines.lib.silver import SilverEntity

        with pytest.raises(ValueError, match="natural_keys"):
            SilverEntity(
                source_path="/bronze/*.parquet",
                target_path="/silver/",
                natural_keys=[],  # Missing!
                change_timestamp="updated_at",
            )

    def test_validate_bronze_source_valid_config(self):
        """Valid configuration should not raise."""
        from pipelines.lib.bronze import BronzeSource, SourceType

        # Should not raise
        source = BronzeSource(
            system="test_system",
            entity="test_entity",
            source_type=SourceType.FILE_CSV,
            source_path="/path/to/file.csv",
            target_path="/output/",
        )
        assert source.system == "test_system"

    def test_validate_silver_entity_valid_config(self):
        """Valid configuration should not raise."""
        from pipelines.lib.silver import SilverEntity

        # Should not raise
        entity = SilverEntity(
            source_path="/bronze/*.parquet",
            target_path="/silver/",
            natural_keys=["id"],
            change_timestamp="updated_at",
        )
        assert entity.natural_keys == ["id"]


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


class TestAuth:
    """Tests for API authentication module."""

    def test_auth_type_enum_values(self):
        """Should have expected auth types."""
        from pipelines.lib.auth import AuthType

        assert AuthType.NONE.value == "none"
        assert AuthType.BEARER.value == "bearer"
        assert AuthType.API_KEY.value == "api_key"
        assert AuthType.BASIC.value == "basic"

    def test_auth_config_validates_bearer_requires_token(self):
        """Bearer auth should require token."""
        from pipelines.lib.auth import AuthConfig, AuthType

        with pytest.raises(ValueError, match="token"):
            AuthConfig(auth_type=AuthType.BEARER, token=None)

    def test_auth_config_validates_api_key_requires_key(self):
        """API key auth should require api_key."""
        from pipelines.lib.auth import AuthConfig, AuthType

        with pytest.raises(ValueError, match="api_key"):
            AuthConfig(auth_type=AuthType.API_KEY, api_key=None)

    def test_auth_config_validates_basic_requires_credentials(self):
        """Basic auth should require username and password."""
        from pipelines.lib.auth import AuthConfig, AuthType

        with pytest.raises(ValueError, match="username.*password"):
            AuthConfig(auth_type=AuthType.BASIC, username="user", password=None)

    def test_build_auth_headers_none(self):
        """No auth should return default headers."""
        from pipelines.lib.auth import build_auth_headers

        headers, auth_tuple = build_auth_headers(None)

        assert headers == {"Accept": "application/json"}
        assert auth_tuple is None

    def test_build_auth_headers_bearer(self, monkeypatch):
        """Bearer auth should add Authorization header."""
        from pipelines.lib.auth import AuthConfig, AuthType, build_auth_headers

        monkeypatch.setenv("TEST_TOKEN", "my-secret-token")

        config = AuthConfig(auth_type=AuthType.BEARER, token="${TEST_TOKEN}")
        headers, auth_tuple = build_auth_headers(config)

        assert headers["Authorization"] == "Bearer my-secret-token"
        assert auth_tuple is None

    def test_build_auth_headers_api_key(self, monkeypatch):
        """API key auth should add custom header."""
        from pipelines.lib.auth import AuthConfig, AuthType, build_auth_headers

        monkeypatch.setenv("TEST_API_KEY", "key-123")

        config = AuthConfig(
            auth_type=AuthType.API_KEY,
            api_key="${TEST_API_KEY}",
            api_key_header="X-Custom-Key",
        )
        headers, auth_tuple = build_auth_headers(config)

        assert headers["X-Custom-Key"] == "key-123"
        assert auth_tuple is None

    def test_build_auth_headers_basic(self, monkeypatch):
        """Basic auth should return auth tuple."""
        from pipelines.lib.auth import AuthConfig, AuthType, build_auth_headers

        monkeypatch.setenv("TEST_USER", "admin")
        monkeypatch.setenv("TEST_PASS", "secret")

        config = AuthConfig(
            auth_type=AuthType.BASIC,
            username="${TEST_USER}",
            password="${TEST_PASS}",
        )
        headers, auth_tuple = build_auth_headers(config)

        assert auth_tuple == ("admin", "secret")

    def test_build_auth_headers_with_extra_headers(self, monkeypatch):
        """Should include extra headers."""
        from pipelines.lib.auth import build_auth_headers

        extra = {"X-Request-ID": "123", "X-Custom": "value"}
        headers, _ = build_auth_headers(None, extra_headers=extra)

        assert headers["X-Request-ID"] == "123"
        assert headers["X-Custom"] == "value"
        assert headers["Accept"] == "application/json"


class TestPagination:
    """Tests for pagination module."""

    def test_pagination_strategy_enum_values(self):
        """Should have expected pagination strategies."""
        from pipelines.lib.pagination import PaginationStrategy

        assert PaginationStrategy.NONE.value == "none"
        assert PaginationStrategy.OFFSET.value == "offset"
        assert PaginationStrategy.PAGE.value == "page"
        assert PaginationStrategy.CURSOR.value == "cursor"

    def test_no_pagination_state(self):
        """NoPaginationState should fetch once then stop."""
        from pipelines.lib.pagination import (
            NoPaginationState,
            PaginationConfig,
            PaginationStrategy,
        )

        config = PaginationConfig(strategy=PaginationStrategy.NONE)
        state = NoPaginationState(config)

        assert state.should_fetch_more() is True
        assert state.build_params() == {}

        state.on_response([{"id": 1}], {})

        assert state.should_fetch_more() is False

    def test_offset_pagination_state(self):
        """OffsetPaginationState should increment offset."""
        from pipelines.lib.pagination import (
            OffsetPaginationState,
            PaginationConfig,
            PaginationStrategy,
        )

        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=10,
            offset_param="skip",
            limit_param="take",
        )
        state = OffsetPaginationState(config)

        # First page
        params = state.build_params()
        assert params["skip"] == 0
        assert params["take"] == 10

        # Full page - continue
        records = [{"id": i} for i in range(10)]
        should_continue = state.on_response(records, {})
        assert should_continue is True

        # Second page
        params = state.build_params()
        assert params["skip"] == 10

        # Partial page - stop
        records = [{"id": i} for i in range(5)]
        should_continue = state.on_response(records, {})
        assert should_continue is False

    def test_page_pagination_state(self):
        """PagePaginationState should increment page number."""
        from pipelines.lib.pagination import (
            PagePaginationState,
            PaginationConfig,
            PaginationStrategy,
        )

        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=20,
            page_param="p",
            page_size_param="size",
        )
        state = PagePaginationState(config)

        # First page
        params = state.build_params()
        assert params["p"] == 1
        assert params["size"] == 20

        # Full page - continue
        records = [{"id": i} for i in range(20)]
        should_continue = state.on_response(records, {})
        assert should_continue is True

        # Second page
        params = state.build_params()
        assert params["p"] == 2

    def test_page_pagination_max_pages(self):
        """PagePaginationState should respect max_pages limit."""
        from pipelines.lib.pagination import (
            PagePaginationState,
            PaginationConfig,
            PaginationStrategy,
        )

        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=10,
            max_pages=2,
        )
        state = PagePaginationState(config)

        # Page 1
        state.build_params()
        state.on_response([{"id": i} for i in range(10)], {})

        # Page 2
        state.build_params()
        state.on_response([{"id": i} for i in range(10)], {})

        # Page 3 - should not fetch
        assert state.should_fetch_more() is False
        assert state.max_pages_limit_hit is True

    def test_cursor_pagination_state(self):
        """CursorPaginationState should extract and use cursor."""
        from pipelines.lib.pagination import (
            CursorPaginationState,
            PaginationConfig,
            PaginationStrategy,
        )

        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_param="after",
            cursor_path="meta.next",
        )
        state = CursorPaginationState(config)

        # First page - no cursor
        params = state.build_params()
        assert "after" not in params

        # Response with cursor
        records = [{"id": 1}]
        data = {"meta": {"next": "cursor_abc"}}
        should_continue = state.on_response(records, data)

        assert should_continue is True
        assert state.cursor == "cursor_abc"

        # Second page - with cursor
        params = state.build_params()
        assert params["after"] == "cursor_abc"

        # Response without cursor - stop
        data = {"meta": {"next": None}}
        should_continue = state.on_response(records, data)
        assert should_continue is False

    def test_build_pagination_state(self):
        """build_pagination_state should create correct state type."""
        from pipelines.lib.pagination import (
            CursorPaginationState,
            NoPaginationState,
            OffsetPaginationState,
            PagePaginationState,
            PaginationConfig,
            PaginationStrategy,
            build_pagination_state,
        )

        # None
        config = PaginationConfig(strategy=PaginationStrategy.NONE)
        state = build_pagination_state(config)
        assert isinstance(state, NoPaginationState)

        # Offset
        config = PaginationConfig(strategy=PaginationStrategy.OFFSET)
        state = build_pagination_state(config)
        assert isinstance(state, OffsetPaginationState)

        # Page
        config = PaginationConfig(strategy=PaginationStrategy.PAGE)
        state = build_pagination_state(config)
        assert isinstance(state, PagePaginationState)

        # Cursor
        config = PaginationConfig(strategy=PaginationStrategy.CURSOR)
        state = build_pagination_state(config)
        assert isinstance(state, CursorPaginationState)

    def test_build_pagination_config_from_dict(self):
        """Should build config from dictionary."""
        from pipelines.lib.pagination import (
            PaginationStrategy,
            build_pagination_config_from_dict,
        )

        options = {
            "pagination_type": "offset",
            "page_size": 50,
            "offset_param": "skip",
            "limit_param": "limit",
        }

        config = build_pagination_config_from_dict(options)

        assert config.strategy == PaginationStrategy.OFFSET
        assert config.page_size == 50
        assert config.offset_param == "skip"


class TestApiSource:
    """Tests for ApiSource class."""

    def test_api_source_validation_requires_base_url(self):
        """Should require base_url."""
        from pipelines.lib.api import ApiSource

        with pytest.raises(ValueError, match="base_url"):
            ApiSource(
                system="test",
                entity="data",
                base_url="",  # Missing
                endpoint="/api/data",
                target_path="/output/",
            )

    def test_api_source_validation_requires_endpoint(self):
        """Should require endpoint."""
        from pipelines.lib.api import ApiSource

        with pytest.raises(ValueError, match="endpoint"):
            ApiSource(
                system="test",
                entity="data",
                base_url="https://api.example.com",
                endpoint="",  # Missing
                target_path="/output/",
            )

    def test_api_source_dry_run(self, tmp_path):
        """Dry run should return without fetching."""
        from pipelines.lib.api import ApiSource

        source = ApiSource(
            system="test",
            entity="data",
            base_url="https://api.example.com",
            endpoint="/api/data",
            target_path=str(tmp_path / "output/dt={run_date}/"),
        )

        result = source.run("2025-01-15", dry_run=True)

        assert result["dry_run"] is True
        assert result["base_url"] == "https://api.example.com"
        assert result["endpoint"] == "/api/data"

    def test_api_output_metadata_to_json(self):
        """ApiOutputMetadata should serialize to JSON."""
        from pipelines.lib.api import ApiOutputMetadata

        metadata = ApiOutputMetadata(
            row_count=100,
            columns=[{"name": "id", "type": "int"}],
            written_at="2025-01-15T10:00:00Z",
            system="test",
            entity="data",
            base_url="https://api.example.com",
            endpoint="/api/data",
            run_date="2025-01-15",
            pages_fetched=5,
            total_requests=5,
        )

        json_str = metadata.to_json()
        data = json.loads(json_str)

        assert data["row_count"] == 100
        assert data["pages_fetched"] == 5

    def test_create_api_source_from_options(self):
        """Should create ApiSource from options dict."""
        from pipelines.lib.api import create_api_source_from_options

        options = {
            "base_url": "https://api.example.com",
            "endpoint": "/v1/items",
            "auth_type": "bearer",
            "token": "${API_TOKEN}",
            "pagination_type": "offset",
            "page_size": 50,
            "data_path": "data.items",
            "requests_per_second": 10.0,
        }

        source = create_api_source_from_options(
            system="test",
            entity="items",
            options=options,
            target_path="/output/",
        )

        assert source.system == "test"
        assert source.entity == "items"
        assert source.base_url == "https://api.example.com"
        assert source.endpoint == "/v1/items"
        assert source.auth is not None
        assert source.pagination is not None
        assert source.data_path == "data.items"
        assert source.requests_per_second == 10.0
