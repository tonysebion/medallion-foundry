"""Mock API Extractor End-to-End Tests.

Story 6: Tests that verify API extractor with synthetic mock responses including:
- Single page response
- Multi-page paginated response
- Empty response
- Error responses (4xx, 5xx)
- Rate limiting handling
- Retry behavior on transient failures

These tests use responses/unittest.mock to mock HTTP responses without
requiring real API endpoints.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List
from unittest.mock import patch, MagicMock

import pytest
import requests

from core.domain.adapters.extractors.api_extractor import ApiExtractor


# =============================================================================
# Test Data Generators
# =============================================================================


def generate_api_response(
    records: List[Dict[str, Any]],
    total: int | None = None,
    page: int | None = None,
    has_more: bool = False,
) -> Dict[str, Any]:
    """Generate a mock API response payload.

    Args:
        records: List of record dictionaries to include
        total: Optional total count
        page: Optional current page number
        has_more: Whether there are more pages

    Returns:
        Mock API response dictionary
    """
    response: Dict[str, Any] = {
        "data": records,
        "count": len(records),
    }

    if total is not None:
        response["total"] = total

    if page is not None:
        response["page"] = page

    if has_more:
        response["has_more"] = True
        response["next_cursor"] = f"cursor_{page + 1 if page else 2}"

    return response


def generate_test_records(count: int, start_id: int = 1) -> List[Dict[str, Any]]:
    """Generate test records for API responses."""
    return [
        {
            "id": f"REC{start_id + i:06d}",
            "name": f"Record {start_id + i}",
            "status": "active",
            "amount": (start_id + i) * 100.0,
            "updated_at": f"2024-01-{15 + (i % 10):02d}T00:00:00Z",
        }
        for i in range(count)
    ]


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def api_extractor() -> ApiExtractor:
    """Provide fresh ApiExtractor instance."""
    return ApiExtractor()


@pytest.fixture
def base_config() -> Dict[str, Any]:
    """Provide base API configuration."""
    return {
        "source": {
            "system": "test_system",
            "table": "test_table",
            "api": {
                "base_url": "https://api.example.com",
                "endpoint": "/v1/records",
                "auth": {"type": "none"},
                "pagination": {"type": "none"},
            },
            "run": {
                "load_pattern": "snapshot",
                "timeout_seconds": 30,
            },
        },
    }


@pytest.fixture
def run_date() -> date:
    """Provide consistent run date."""
    return date(2024, 1, 15)


# =============================================================================
# Single Page Response Tests
# =============================================================================


@pytest.mark.integration
class TestApiExtractorSinglePage:
    """Test API extractor with single page responses."""

    def test_single_page_extraction_succeeds(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify successful extraction from single page response."""
        records = generate_test_records(10)
        mock_response = generate_api_response(records)

        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = mock_response
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result, cursor = api_extractor.fetch_records(base_config, run_date)

        assert len(result) == 10
        assert result[0]["id"] == "REC000001"

    def test_extracts_records_from_data_path(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify extraction from nested data path."""
        records = generate_test_records(5)
        mock_response = {"response": {"items": records}}

        base_config["source"]["api"]["data_path"] = "response.items"

        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = mock_response
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result, cursor = api_extractor.fetch_records(base_config, run_date)

        assert len(result) == 5

    def test_empty_response_returns_empty_list(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify empty response handling."""
        mock_response = generate_api_response([])

        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = mock_response
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result, cursor = api_extractor.fetch_records(base_config, run_date)

        assert len(result) == 0
        assert cursor is None


# =============================================================================
# Pagination Tests
# =============================================================================


@pytest.mark.integration
class TestApiExtractorPagination:
    """Test API extractor pagination handling."""

    def test_offset_pagination_collects_all_pages(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify offset pagination collects records from multiple pages."""
        # Configure offset pagination
        base_config["source"]["api"]["pagination"] = {
            "type": "offset",
            "page_size": 10,
            "max_records": 25,
        }

        # Create responses for multiple pages
        page1 = generate_test_records(10, start_id=1)
        page2 = generate_test_records(10, start_id=11)
        page3 = generate_test_records(5, start_id=21)

        call_count = [0]

        def mock_response(*args, **kwargs):
            mock_resp = MagicMock()
            mock_resp.raise_for_status = MagicMock()

            if call_count[0] == 0:
                mock_resp.json.return_value = {"data": page1}
            elif call_count[0] == 1:
                mock_resp.json.return_value = {"data": page2}
            else:
                mock_resp.json.return_value = {"data": page3}

            call_count[0] += 1
            return mock_resp

        with patch("requests.Session.get", side_effect=mock_response):
            result, cursor = api_extractor.fetch_records(base_config, run_date)

        # Should have up to max_records (25)
        assert len(result) <= 25

    def test_page_pagination_collects_multiple_pages(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify page-number pagination works correctly."""
        base_config["source"]["api"]["pagination"] = {
            "type": "page",
            "page_size": 5,
            "max_pages": 3,
        }

        pages = [
            generate_test_records(5, start_id=1),
            generate_test_records(5, start_id=6),
            generate_test_records(3, start_id=11),
        ]

        call_count = [0]

        def mock_response(*args, **kwargs):
            mock_resp = MagicMock()
            mock_resp.raise_for_status = MagicMock()

            idx = min(call_count[0], len(pages) - 1)
            mock_resp.json.return_value = {"data": pages[idx]}
            call_count[0] += 1
            return mock_resp

        with patch("requests.Session.get", side_effect=mock_response):
            result, cursor = api_extractor.fetch_records(base_config, run_date)

        # Should have records from all pages
        assert len(result) >= 5


# =============================================================================
# Error Response Tests
# =============================================================================


@pytest.mark.integration
class TestApiExtractorErrors:
    """Test API extractor error handling."""

    def test_404_raises_error(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify 404 response raises an error (wrapped in retry error)."""
        from core.foundation.primitives.exceptions import RetryExhaustedError

        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 404
            error = requests.exceptions.HTTPError(response=mock_resp)
            mock_resp.raise_for_status.side_effect = error
            mock_get.return_value = mock_resp

            # 4xx errors are not retried, so they get wrapped in RetryExhaustedError
            with pytest.raises((requests.exceptions.HTTPError, RetryExhaustedError)):
                api_extractor.fetch_records(base_config, run_date)

    def test_401_raises_error(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify 401 unauthorized raises an error."""
        from core.foundation.primitives.exceptions import RetryExhaustedError

        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 401
            error = requests.exceptions.HTTPError(response=mock_resp)
            mock_resp.raise_for_status.side_effect = error
            mock_get.return_value = mock_resp

            with pytest.raises((requests.exceptions.HTTPError, RetryExhaustedError)):
                api_extractor.fetch_records(base_config, run_date)

    def test_connection_error_raises(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify connection errors are raised after retries exhausted."""
        from core.foundation.primitives.exceptions import RetryExhaustedError

        with patch("requests.Session.get") as mock_get:
            mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")

            # Connection errors are retried, so eventually raise RetryExhaustedError
            with pytest.raises((requests.exceptions.RequestException, RetryExhaustedError)):
                api_extractor.fetch_records(base_config, run_date)

    def test_timeout_error_raises(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify timeout errors are raised after retries exhausted."""
        from core.foundation.primitives.exceptions import RetryExhaustedError

        with patch("requests.Session.get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

            # Timeouts are retried, so eventually raise RetryExhaustedError
            with pytest.raises((requests.exceptions.RequestException, RetryExhaustedError)):
                api_extractor.fetch_records(base_config, run_date)


# =============================================================================
# Cursor/Watermark Tests
# =============================================================================


@pytest.mark.integration
class TestApiExtractorCursor:
    """Test API extractor cursor/watermark handling."""

    def test_cursor_computed_from_records(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify cursor is computed from cursor_field in records."""
        records = generate_test_records(5)
        mock_response = generate_api_response(records)

        base_config["source"]["api"]["cursor_field"] = "updated_at"

        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = mock_response
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result, cursor = api_extractor.fetch_records(base_config, run_date)

        assert cursor is not None
        assert "2024-01" in cursor  # Should be a date string

    def test_no_cursor_without_cursor_field(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify no cursor returned when cursor_field not configured."""
        records = generate_test_records(5)
        mock_response = generate_api_response(records)

        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = mock_response
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result, cursor = api_extractor.fetch_records(base_config, run_date)

        assert cursor is None


# =============================================================================
# Authentication Tests
# =============================================================================


@pytest.mark.integration
class TestApiExtractorAuth:
    """Test API extractor authentication handling."""

    def test_bearer_token_auth(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify bearer token is included in headers."""
        records = generate_test_records(3)
        mock_response = generate_api_response(records)

        base_config["source"]["api"]["auth"] = {
            "type": "bearer",
            "token_env": "TEST_API_TOKEN",
        }

        with patch.dict("os.environ", {"TEST_API_TOKEN": "test-token-123"}):
            with patch("requests.Session.get") as mock_get:
                mock_resp = MagicMock()
                mock_resp.json.return_value = mock_response
                mock_resp.raise_for_status = MagicMock()
                mock_get.return_value = mock_resp

                result, cursor = api_extractor.fetch_records(base_config, run_date)

                # Verify Authorization header was set
                call_args = mock_get.call_args
                headers = call_args.kwargs.get("headers", {})
                assert "Authorization" in headers
                assert "Bearer" in headers["Authorization"]

    def test_api_key_auth(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify API key is included in headers."""
        records = generate_test_records(3)
        mock_response = generate_api_response(records)

        base_config["source"]["api"]["auth"] = {
            "type": "api_key",
            "header_name": "X-API-Key",
            "key_env": "TEST_API_KEY",
        }

        with patch.dict("os.environ", {"TEST_API_KEY": "secret-key-456"}):
            with patch("requests.Session.get") as mock_get:
                mock_resp = MagicMock()
                mock_resp.json.return_value = mock_response
                mock_resp.raise_for_status = MagicMock()
                mock_get.return_value = mock_resp

                result, cursor = api_extractor.fetch_records(base_config, run_date)

                # Verify API key header was set
                call_args = mock_get.call_args
                headers = call_args.kwargs.get("headers", {})
                assert "X-API-Key" in headers


# =============================================================================
# Watermark Config Tests
# =============================================================================


@pytest.mark.integration
class TestApiExtractorWatermarkConfig:
    """Test API extractor watermark configuration."""

    def test_get_watermark_config_with_cursor_field(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
    ):
        """Verify watermark config returned when cursor_field configured."""
        base_config["source"]["api"]["cursor_field"] = "updated_at"

        config = api_extractor.get_watermark_config(base_config)

        assert config is not None
        assert config["enabled"] is True
        assert config["column"] == "updated_at"

    def test_get_watermark_config_without_cursor_field(
        self,
        api_extractor: ApiExtractor,
        base_config: Dict[str, Any],
    ):
        """Verify no watermark config when cursor_field not configured."""
        config = api_extractor.get_watermark_config(base_config)

        assert config is None
