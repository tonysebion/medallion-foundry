from __future__ import annotations

import pytest

from core.extractors.api_extractor import ApiExtractor
from core.exceptions import RetryExhaustedError


class DummySession:
    def get(self, url, headers, params, timeout, auth=None):
        class Resp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"items": [{"id": 1}]}

        return Resp()

    def close(self):
        return None


def _build_cfg(pagination: dict):
    return {
        "source": {
            "api": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                "pagination": pagination,
                "auth_type": "none",
            },
            "run": {},
        }
    }


def test_api_extractor_invalid_pagination(monkeypatch):
    extractor = ApiExtractor()
    cfg = _build_cfg({"type": "unknown"})
    with pytest.raises(ValueError):
        extractor.fetch_records(cfg, None)


def test_api_extractor_request_exception(monkeypatch):
    class BadSession(DummySession):
        def get(self, url, headers, params, timeout, auth=None):
            raise ConnectionError("boom")

    monkeypatch.setattr("requests.Session", lambda: BadSession())
    extractor = ApiExtractor()
    cfg = _build_cfg({"type": "none"})
    with pytest.raises(RetryExhaustedError):
        extractor.fetch_records(cfg, None)
