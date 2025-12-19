"""Tests for ApiSource configuration and dry-run functionality.

These tests verify API source configuration validation, dry-run behavior,
and helper functions without making actual HTTP calls.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipelines.lib.api import ApiSource, ApiOutputMetadata, create_api_source_from_options
from pipelines.lib.auth import AuthConfig, AuthType
from pipelines.lib.pagination import PaginationConfig, PaginationStrategy


class TestApiSourceValidation:
    """Tests for ApiSource configuration validation."""

    def test_requires_base_url(self):
        """Should raise error when base_url is empty."""
        with pytest.raises(ValueError, match="base_url"):
            ApiSource(
                system="test",
                entity="data",
                base_url="",
                endpoint="/api/data",
                target_path="/output/",
            )

    def test_requires_endpoint(self):
        """Should raise error when endpoint is empty."""
        with pytest.raises(ValueError, match="endpoint"):
            ApiSource(
                system="test",
                entity="data",
                base_url="https://api.example.com",
                endpoint="",
                target_path="/output/",
            )

    def test_requires_target_path(self):
        """Should raise error when target_path is empty."""
        with pytest.raises(ValueError, match="target"):
            ApiSource(
                system="test",
                entity="data",
                base_url="https://api.example.com",
                endpoint="/api/data",
                target_path="",
            )

    def test_valid_config_passes(self, tmp_path):
        """Valid configuration should not raise."""
        source = ApiSource(
            system="test",
            entity="items",
            base_url="https://api.example.com",
            endpoint="/v1/items",
            target_path=str(tmp_path / "output/"),
        )

        assert source.system == "test"
        assert source.entity == "items"
        assert source.base_url == "https://api.example.com"


class TestApiSourceDryRun:
    """Tests for dry-run functionality."""

    def test_dry_run_returns_without_fetch(self, tmp_path):
        """Dry run should return metadata without making HTTP calls."""
        source = ApiSource(
            system="test",
            entity="items",
            base_url="https://api.example.com",
            endpoint="/v1/items",
            target_path=str(tmp_path / "output/dt={run_date}/"),
        )

        result = source.run("2025-01-15", dry_run=True)

        assert result["dry_run"] is True
        assert result["base_url"] == "https://api.example.com"
        assert result["endpoint"] == "/v1/items"
        assert not (tmp_path / "output").exists()

    def test_dry_run_with_auth(self, tmp_path, monkeypatch):
        """Dry run should not make HTTP calls even with auth configured."""
        monkeypatch.setenv("API_TOKEN", "secret")

        source = ApiSource(
            system="test",
            entity="secure",
            base_url="https://api.example.com",
            endpoint="/secure",
            target_path=str(tmp_path / "output/dt={run_date}/"),
            auth=AuthConfig(
                auth_type=AuthType.BEARER,
                token="${API_TOKEN}",
            ),
        )

        result = source.run("2025-01-15", dry_run=True)

        # Dry run returns basic info, not auth details
        assert result["dry_run"] is True
        assert result["base_url"] == "https://api.example.com"
        assert not (tmp_path / "output").exists()

    def test_dry_run_with_pagination(self, tmp_path):
        """Dry run should not make HTTP calls even with pagination configured."""
        source = ApiSource(
            system="test",
            entity="paginated",
            base_url="https://api.example.com",
            endpoint="/items",
            target_path=str(tmp_path / "output/dt={run_date}/"),
            pagination=PaginationConfig(
                strategy=PaginationStrategy.OFFSET,
                page_size=100,
            ),
        )

        result = source.run("2025-01-15", dry_run=True)

        # Dry run returns basic info
        assert result["dry_run"] is True
        assert result["target"] is not None


class TestApiSourceFromOptions:
    """Tests for creating ApiSource from options dict."""

    def test_creates_with_minimal_options(self):
        """Should create source with minimal options."""
        options = {
            "base_url": "https://api.example.com",
            "endpoint": "/v1/items",
        }

        source = create_api_source_from_options(
            system="test",
            entity="items",
            options=options,
            target_path="/output/",
        )

        assert source.system == "test"
        assert source.base_url == "https://api.example.com"
        assert source.endpoint == "/v1/items"

    def test_creates_with_bearer_auth(self, monkeypatch):
        """Should configure bearer auth from options."""
        monkeypatch.setenv("API_TOKEN", "test-token")

        options = {
            "base_url": "https://api.example.com",
            "endpoint": "/v1/items",
            "auth_type": "bearer",
            "token": "${API_TOKEN}",
        }

        source = create_api_source_from_options(
            system="test",
            entity="items",
            options=options,
            target_path="/output/",
        )

        assert source.auth is not None
        assert source.auth.auth_type == AuthType.BEARER

    def test_creates_with_api_key_auth(self, monkeypatch):
        """Should configure API key auth from options."""
        monkeypatch.setenv("MY_API_KEY", "key-123")

        options = {
            "base_url": "https://api.example.com",
            "endpoint": "/v1/items",
            "auth_type": "api_key",
            "api_key": "${MY_API_KEY}",
            "api_key_header": "X-Custom-Key",
        }

        source = create_api_source_from_options(
            system="test",
            entity="items",
            options=options,
            target_path="/output/",
        )

        assert source.auth is not None
        assert source.auth.auth_type == AuthType.API_KEY
        assert source.auth.api_key_header == "X-Custom-Key"

    def test_creates_with_pagination(self):
        """Should configure pagination from options."""
        options = {
            "base_url": "https://api.example.com",
            "endpoint": "/v1/items",
            "pagination_type": "offset",
            "page_size": 50,
            "offset_param": "skip",
            "limit_param": "take",
        }

        source = create_api_source_from_options(
            system="test",
            entity="items",
            options=options,
            target_path="/output/",
        )

        assert source.pagination is not None
        assert source.pagination.strategy == PaginationStrategy.OFFSET
        assert source.pagination.page_size == 50
        assert source.pagination.offset_param == "skip"

    def test_creates_with_cursor_pagination(self):
        """Should configure cursor pagination from options."""
        options = {
            "base_url": "https://api.example.com",
            "endpoint": "/v1/items",
            "pagination_type": "cursor",
            "cursor_param": "after",
            "cursor_path": "meta.next_cursor",
        }

        source = create_api_source_from_options(
            system="test",
            entity="items",
            options=options,
            target_path="/output/",
        )

        assert source.pagination is not None
        assert source.pagination.strategy == PaginationStrategy.CURSOR
        assert source.pagination.cursor_param == "after"
        assert source.pagination.cursor_path == "meta.next_cursor"

    def test_creates_with_data_path(self):
        """Should configure data_path from options."""
        options = {
            "base_url": "https://api.example.com",
            "endpoint": "/v1/items",
            "data_path": "data.items",
        }

        source = create_api_source_from_options(
            system="test",
            entity="items",
            options=options,
            target_path="/output/",
        )

        assert source.data_path == "data.items"

    def test_creates_with_rate_limiting(self):
        """Should configure rate limiting from options."""
        options = {
            "base_url": "https://api.example.com",
            "endpoint": "/v1/items",
            "requests_per_second": 5.0,
        }

        source = create_api_source_from_options(
            system="test",
            entity="items",
            options=options,
            target_path="/output/",
        )

        assert source.requests_per_second == 5.0


class TestApiOutputMetadata:
    """Tests for API output metadata."""

    def test_serialization(self):
        """Metadata should serialize to JSON correctly."""
        metadata = ApiOutputMetadata(
            row_count=100,
            columns=[{"name": "id", "type": "int64"}],
            written_at="2025-01-15T10:00:00Z",
            system="test",
            entity="items",
            base_url="https://api.example.com",
            endpoint="/items",
            run_date="2025-01-15",
            pages_fetched=5,
            total_requests=5,
        )

        json_str = metadata.to_json()
        data = json.loads(json_str)

        assert data["row_count"] == 100
        assert data["base_url"] == "https://api.example.com"
        assert data["pages_fetched"] == 5

    def test_to_dict(self):
        """to_dict should return all fields."""
        metadata = ApiOutputMetadata(
            row_count=10,
            columns=[],
            written_at="2025-01-15",
            system="test",
            entity="items",
            base_url="https://api.example.com",
            endpoint="/items",
            run_date="2025-01-15",
            pages_fetched=1,
            total_requests=1,
        )

        d = metadata.to_dict()

        assert d["row_count"] == 10
        assert d["system"] == "test"
        assert d["pages_fetched"] == 1

    def test_to_dict_roundtrip(self):
        """to_dict output can be used to create new metadata."""
        metadata = ApiOutputMetadata(
            row_count=50,
            columns=[{"name": "id"}],
            written_at="2025-01-15",
            system="test",
            entity="items",
            base_url="https://api.example.com",
            endpoint="/items",
            run_date="2025-01-15",
            pages_fetched=2,
            total_requests=2,
        )

        d = metadata.to_dict()

        # Can create new metadata from dict
        new_metadata = ApiOutputMetadata(**d)

        assert new_metadata.row_count == 50
        assert new_metadata.pages_fetched == 2


class TestApiSourceConfiguration:
    """Tests for ApiSource configuration options."""

    def test_path_params_substitution(self, tmp_path):
        """path_params should be substituted in endpoint."""
        source = ApiSource(
            system="test",
            entity="repos",
            base_url="https://api.example.com",
            endpoint="/users/{user}/repos",
            target_path=str(tmp_path / "output/"),
            path_params={"user": "testuser"},
        )

        # Verify path_params are stored
        assert source.path_params == {"user": "testuser"}

    def test_params_configuration(self, tmp_path):
        """params (query params) should be configurable."""
        source = ApiSource(
            system="test",
            entity="items",
            base_url="https://api.example.com",
            endpoint="/items",
            target_path=str(tmp_path / "output/"),
            params={"status": "active", "type": "widget"},
        )

        assert source.params["status"] == "active"
        assert source.params["type"] == "widget"

    def test_headers_configuration(self, tmp_path):
        """Custom headers should be configurable."""
        source = ApiSource(
            system="test",
            entity="items",
            base_url="https://api.example.com",
            endpoint="/items",
            target_path=str(tmp_path / "output/"),
            headers={"X-Custom-Header": "value"},
        )

        assert source.headers["X-Custom-Header"] == "value"

    def test_timeout_configuration(self, tmp_path):
        """Request timeout should be configurable."""
        source = ApiSource(
            system="test",
            entity="items",
            base_url="https://api.example.com",
            endpoint="/items",
            target_path=str(tmp_path / "output/"),
            timeout=120.0,
        )

        assert source.timeout == 120.0

    def test_watermark_configuration(self, tmp_path):
        """Watermark settings should be configurable."""
        source = ApiSource(
            system="test",
            entity="events",
            base_url="https://api.example.com",
            endpoint="/events",
            target_path=str(tmp_path / "output/"),
            watermark_column="updated_at",
            watermark_param="since",
        )

        assert source.watermark_column == "updated_at"
        assert source.watermark_param == "since"
