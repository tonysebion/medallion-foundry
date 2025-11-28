"""Async HTTP client tests covering rate limiting, timeouts, and environment detection."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from typing import Any, Dict

from core.extractors.async_http import AsyncApiClient, is_async_enabled, HTTPX_AVAILABLE
from core.exceptions import RetryExhaustedError


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
def test_async_client_basic_get():
    async def _inner():
        with patch("core.extractors.async_http.httpx") as mock_httpx:
            mock_response = MagicMock()
            mock_response.json.return_value = {"status": "ok", "data": [1, 2, 3]}
            mock_response.raise_for_status = MagicMock()

            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get = AsyncMock(return_value=mock_response)

            mock_httpx.AsyncClient.return_value = mock_client

            client = AsyncApiClient(
                base_url="https://api.example.com",
                headers={"Authorization": "Bearer test"},
                timeout=10,
                max_concurrent=2,
            )

            result = await client.get("/users", params={"page": 1})

            assert result["status"] == "ok"
            assert len(result["data"]) == 3
            mock_client.get.assert_called_once()

    asyncio.run(_inner())


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
def test_async_client_retry_on_error():
    async def _inner():
        with patch("core.extractors.async_http.httpx") as mock_httpx:
            mock_response_fail = MagicMock()
            mock_response_fail.status_code = 503

            class MockHTTPError(Exception):
                def __init__(self, response):
                    self.response = response
                    super().__init__("Service unavailable")

            mock_error = MockHTTPError(mock_response_fail)
            type(mock_error).__name__ = "HTTPStatusError"
            type(mock_error).__module__ = "httpx"

            mock_response_success = MagicMock()
            mock_response_success.json.return_value = {"retry": "success"}
            mock_response_success.raise_for_status = MagicMock()

            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None

            call_count = 0

            async def mock_get(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise mock_error
                return mock_response_success

            mock_client.get = mock_get
            mock_httpx.AsyncClient.return_value = mock_client

            client = AsyncApiClient(
                base_url="https://api.example.com",
                headers={},
            )

            result = await client.get("/flaky")
            assert result["retry"] == "success"
            assert call_count == 3

    asyncio.run(_inner())


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
def test_async_client_handles_rate_limit():
    async def _inner():
        with patch("core.extractors.async_http.httpx") as mock_httpx:
            mock_limited = MagicMock()
            mock_limited.status_code = 429
            mock_limited.headers = {"Retry-After": "0.01"}

            class RateLimitException(Exception):
                response: Any

                def __init__(self, message: str, response: Any) -> None:
                    super().__init__(message)
                    self.response = response

            rate_exc = RateLimitException("rate limit", mock_limited)
            type(rate_exc).__name__ = "HTTPStatusError"
            type(rate_exc).__module__ = "httpx._exceptions"

            mock_limited.raise_for_status.side_effect = rate_exc
            mock_success = MagicMock()
            mock_success.json.return_value = {"ok": "after-rate"}
            mock_success.raise_for_status = MagicMock()

            call_counter: Dict[str, int] = {"count": 0}

            async def mock_get(*args, **kwargs):
                call_counter["count"] += 1
                if call_counter["count"] == 1:
                    raise rate_exc
                return mock_success

            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get = mock_get
            mock_httpx.AsyncClient.return_value = mock_client

            client = AsyncApiClient("https://api.example.com", headers={}, timeout=1)
            result = await client.get("/rate")
            assert result["ok"] == "after-rate"
            assert call_counter["count"] == 2

            call_counter["count"] = 0
            sleep_calls = []

            async def fake_sleep(delay):
                sleep_calls.append(delay)

            with patch(
                "core.extractors.async_http.asyncio.sleep", side_effect=fake_sleep
            ):
                await client.get("/rate")

                assert any(call >= 0.01 for call in sleep_calls)

    asyncio.run(_inner())


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
def test_async_client_handles_timeout():
    async def _inner():
        with patch("core.extractors.async_http.httpx") as mock_httpx:

            class TimeoutError(Exception):
                pass

            timeout_exc = TimeoutError("timeout")
            type(timeout_exc).__name__ = "TimeoutException"
            type(timeout_exc).__module__ = "httpx._exceptions"

            mock_success = MagicMock()
            mock_success.json.return_value = {"ok": "timeout-recovered"}
            mock_success.raise_for_status = MagicMock()

            call_counter: Dict[str, int] = {"count": 0}

            async def mock_get(*args, **kwargs):
                call_counter["count"] += 1
                if call_counter["count"] == 1:
                    raise timeout_exc
                return mock_success

            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get = mock_get
            mock_httpx.AsyncClient.return_value = mock_client

            client = AsyncApiClient("https://api.example.com", headers={}, timeout=1)
            result = await client.get("/timeout")
            assert result["ok"] == "timeout-recovered"
            assert call_counter["count"] == 2

    asyncio.run(_inner())


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
def test_async_client_retry_exhausts(monkeypatch):
    async def _inner():
        with patch("core.extractors.async_http.httpx") as mock_httpx:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.headers = {}

            class HTTPStatusError(Exception):
                def __init__(self, response):
                    self.response = response
                    super().__init__("server error")

            exc = HTTPStatusError(mock_response)
            type(exc).__module__ = "httpx"
            type(exc).__name__ = "HTTPStatusError"

            async def mock_get(*args, **kwargs):
                raise exc

            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get = mock_get
            mock_httpx.AsyncClient.return_value = mock_client

            client = AsyncApiClient("https://api.example.com", headers={}, timeout=1)
            with pytest.raises(RetryExhaustedError):
                await client.get("/fail")

    asyncio.run(_inner())


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
def test_async_client_get_many_respects_max_concurrent():
    async def _inner():
        with patch("core.extractors.async_http.httpx") as mock_httpx:
            mock_response = MagicMock()
            mock_response.json.return_value = {"ok": True}
            mock_response.raise_for_status = MagicMock()

            async def fake_get(*args, **kwargs):
                await asyncio.sleep(0)
                return mock_response

            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get = fake_get
            mock_httpx.AsyncClient.return_value = mock_client

            client = AsyncApiClient(
                "https://api.example.com", headers={}, max_concurrent=2
            )

            class TrackingSemaphore(asyncio.Semaphore):
                def __init__(self, value: int) -> None:
                    super().__init__(value)
                    self.active = 0
                    self.max_seen = 0

                async def __aenter__(self):
                    await super().__aenter__()
                    self.active += 1
                    self.max_seen = max(self.max_seen, self.active)
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    self.active -= 1
                    await super().__aexit__(exc_type, exc, tb)

            client._semaphore = TrackingSemaphore(2)

            await client.get_many([("/one", {}), ("/two", {}), ("/three", {})])
            assert client._semaphore.max_seen <= 2

    asyncio.run(_inner())


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
def test_async_client_concurrency_limit():
    async def _inner():
        with patch("core.extractors.async_http.httpx") as mock_httpx:
            mock_response = MagicMock()
            mock_response.json.return_value = {"ok": True}
            mock_response.raise_for_status = MagicMock()

            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get = AsyncMock(return_value=mock_response)

            mock_httpx.AsyncClient.return_value = mock_client

            client = AsyncApiClient(
                base_url="https://api.example.com",
                headers={},
                max_concurrent=2,
            )

            tasks = [client.get(f"/endpoint{i}") for i in range(5)]
            results = await asyncio.gather(*tasks)

            assert len(results) == 5
            assert all(r["ok"] for r in results)

    asyncio.run(_inner())


def test_is_async_enabled_env_overrides(monkeypatch):
    monkeypatch.setenv("BRONZE_ASYNC_HTTP", "1")
    assert is_async_enabled({"async": False}) is (True if HTTPX_AVAILABLE else False)

    monkeypatch.setenv("BRONZE_ASYNC_HTTP", "0")
    assert is_async_enabled({}) is False


def test_is_async_enabled_checks_httpx_availability(monkeypatch):
    monkeypatch.setattr("core.extractors.async_http.HTTPX_AVAILABLE", False)
    monkeypatch.setattr("core.extractors.async_http.httpx", None)
    assert is_async_enabled({"async": True}) is False


def test_is_async_enabled_via_config():
    assert is_async_enabled({"async": True}) is (True if HTTPX_AVAILABLE else False)
    assert is_async_enabled({"async": False}) is False
    assert is_async_enabled({}) is False


def test_is_async_enabled_via_env(monkeypatch):
    monkeypatch.setenv("BRONZE_ASYNC_HTTP", "1")
    assert is_async_enabled({}) is (True if HTTPX_AVAILABLE else False)

    monkeypatch.setenv("BRONZE_ASYNC_HTTP", "false")
    assert is_async_enabled({}) is False
