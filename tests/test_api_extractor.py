from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pytest
import requests

from core.extractors.api_extractor import ApiExtractor


class FakeResponse:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload

    def json(self) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    def __init__(self, responses: List[Dict[str, Any]]) -> None:
        self.responses = responses
        self.calls: List[
            Tuple[str, Dict[str, str], Dict[str, Any], Optional[Tuple[str, str]]]
        ] = []

    def get(
        self,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        timeout: int,
        auth: Optional[Tuple[str, str]] = None,
    ):
        idx = len(self.calls)
        payload = self.responses[idx]
        self.calls.append((url, headers.copy(), params.copy(), auth))
        return FakeResponse(payload)

    def close(self) -> None:
        return None


def _build_config(
    pagination: Dict[str, Any], auth_cfg: Dict[str, Any], cursor_field: str
) -> Dict[str, Any]:
    return {
        "source": {
            "api": {
                "base_url": "https://example.com",
                "endpoint": "/data",
                **auth_cfg,
                "pagination": pagination,
                "cursor_field": cursor_field,
            },
            "run": {"load_pattern": "full"},
        }
    }


def test_api_extractor_offset_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ["OFFSET_TOKEN"] = "token-123"
    responses: List[Dict[str, Any]] = [
        {"items": [{"id": 1}, {"id": 2}]},
        {"items": [{"id": 3}]},
    ]
    session = FakeSession(responses)
    monkeypatch.setattr(requests, "Session", lambda: session)

    extractor = ApiExtractor()
    cfg = _build_config(
        pagination={
            "type": "offset",
            "offset_param": "offset",
            "limit_param": "limit",
            "page_size": 2,
        },
        auth_cfg={"auth_type": "bearer", "auth_token_env": "OFFSET_TOKEN"},
        cursor_field="id",
    )

    records, cursor = extractor.fetch_records(cfg, date.today())

    assert len(records) == 3
    assert cursor == "3"
    assert session.calls[0][1]["Authorization"] == "Bearer token-123"
    assert session.calls[1][2]["offset"] == 2


def test_api_extractor_cursor_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ["API_KEY"] = "key-xyz"
    responses: List[Dict[str, Any]] = [
        {"results": [{"id": 10}], "next_cursor": "abc"},
        {"results": [{"id": 20}], "next_cursor": None},
    ]
    session = FakeSession(responses)
    monkeypatch.setattr(requests, "Session", lambda: session)

    extractor = ApiExtractor()
    cfg = _build_config(
        pagination={
            "type": "cursor",
            "cursor_param": "cursor",
            "cursor_path": "next_cursor",
        },
        auth_cfg={"auth_type": "api_key", "auth_key_env": "API_KEY"},
        cursor_field="id",
    )

    records, cursor = extractor.fetch_records(cfg, date.today())

    assert len(records) == 2
    assert cursor == "20"
    assert session.calls[0][1]["X-API-Key"] == "key-xyz"
    assert session.calls[1][2]["cursor"] == "abc"
