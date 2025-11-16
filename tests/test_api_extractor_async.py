from __future__ import annotations

import asyncio

import pytest

from core.extractors.api_extractor import ApiExtractor
from core.extractors.async_http import HTTPX_AVAILABLE


class DummySpan:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
def test_api_extractor_async_pagination(monkeypatch):
    extractor = ApiExtractor()
    api_cfg = {
        "base_url": "https://api.example.com",
        "endpoint": "/users",
        "pagination": {"type": "none"},
    }
    run_cfg = {"timeout_seconds": 5}

    class DummyClient:
        async def get(self, endpoint, **kwargs):
            return {"items": [{"id": 42}]}

    class DummyLimiter:
        @staticmethod
        def from_config(*args, **kwargs):
            return None

    class DummySpan:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("core.extractors.api_extractor.AsyncApiClient", lambda *args, **kwargs: DummyClient())
    monkeypatch.setattr("core.extractors.api_extractor.RateLimiter.from_config", DummyLimiter.from_config)
    monkeypatch.setattr("core.extractors.api_extractor.trace_span", lambda name: DummySpan())

    async def _run():
        return await extractor._paginate_async(
            api_cfg["base_url"],
            api_cfg["endpoint"],
            headers={},
            api_cfg=api_cfg,
            run_cfg=run_cfg,
        )

    results = asyncio.run(_run())
    assert results == [{"id": 42}]


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
def test_api_extractor_async_rate_limiter(monkeypatch):
    limiter = None

    class DummyLimiter:
        def __init__(self):
            self.count = 0

        async def async_acquire(self):
            self.count += 1

    async def _run():
        nonlocal limiter
        extractor = ApiExtractor()
        api_cfg = {
            "base_url": "https://api.example.com",
            "endpoint": "/users",
            "pagination": {"type": "none"},
        }
        run_cfg = {"timeout_seconds": 5}

        class DummyClient:
            async def get(self, endpoint, **kwargs):
                return {"items": [{"id": 99}]}

        limiter = DummyLimiter()
        monkeypatch.setattr("core.extractors.api_extractor.AsyncApiClient", lambda *args, **kwargs: DummyClient())
        monkeypatch.setattr("core.extractors.api_extractor.RateLimiter.from_config", lambda *args, **kwargs: limiter)
        monkeypatch.setattr("core.extractors.api_extractor.trace_span", lambda name: DummySpan())

        response = await extractor._paginate_async(
            api_cfg["base_url"],
            api_cfg["endpoint"],
            headers={},
            api_cfg=api_cfg,
            run_cfg=run_cfg,
        )
        return response

    results = asyncio.run(_run())
    assert results == [{"id": 99}]
    assert limiter and limiter.count == 1
