import httpx
import pytest

from pipelines.lib.api import ApiSource, create_api_source_from_options


def _make_source(tmp_path, **overrides):
    config = {
        "system": "api",
        "entity": "items",
        "base_url": "https://api.example.com",
        "endpoint": "/v1/items",
        "target_path": str(tmp_path / "bronze"),
    }
    config.update(overrides)
    return ApiSource(**config)


def test_format_endpoint_substitutes_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv("ITEM_VALUE", "42")
    source = _make_source(
        tmp_path,
        endpoint="/v1/items/{value}",
        path_params={"value": "${ITEM_VALUE}"},
    )

    assert source._format_endpoint() == "/v1/items/42"


def test_extract_records_respects_data_path(tmp_path):
    source = _make_source(tmp_path, data_path="meta.payload.items")
    data = {"meta": {"payload": {"items": [{"id": 1}, {"id": 2}]}}}

    assert source._extract_records(data) == data["meta"]["payload"]["items"]


def test_extract_records_wraps_single_dict(tmp_path):
    source = _make_source(tmp_path)
    payload = {"single": "value"}

    assert source._extract_records(payload) == [payload]


def test_compute_max_watermark_returns_string(tmp_path):
    source = _make_source(tmp_path, watermark_column="value")
    records = [{"value": "2025-01-01"}, {"value": "2025-01-15"}]

    assert source._compute_max_watermark(records) == "2025-01-15"


def test_compute_max_watermark_handles_missing_column(tmp_path):
    source = _make_source(tmp_path, watermark_column="missing")

    assert source._compute_max_watermark([{"value": 1}]) is None


@pytest.mark.parametrize(
    "status_code,expected",
    ((500, True), (418, False)),
)
def test_should_retry_http_status_error(status_code, expected, tmp_path):
    source = _make_source(tmp_path)
    request = httpx.Request("GET", "https://api.example.com/items")
    response = httpx.Response(status_code, request=request)
    exc = httpx.HTTPStatusError("boom", request=request, response=response)

    assert source._should_retry(exc) is expected


def test_should_retry_request_error(tmp_path):
    source = _make_source(tmp_path)
    request = httpx.Request("GET", "https://api.example.com/items")
    exc = httpx.RequestError("timeout", request=request)

    assert source._should_retry(exc) is True


def test_respect_retry_after_sleeps(tmp_path, monkeypatch):
    source = _make_source(tmp_path)
    request = httpx.Request("GET", "https://api.example.com/items")
    response = httpx.Response(429, headers={"Retry-After": "0.01"}, request=request)

    sleep_calls: list[float] = []
    monkeypatch.setattr(
        "pipelines.lib.api.time.sleep", lambda secs: sleep_calls.append(secs)
    )

    source._respect_retry_after(response)

    assert sleep_calls == [0.01]


def test_respect_retry_after_ignores_invalid_header(tmp_path, monkeypatch):
    source = _make_source(tmp_path)
    request = httpx.Request("GET", "https://api.example.com/items")
    response = httpx.Response(429, headers={"Retry-After": "abc"}, request=request)

    sleep_calls: list[float] = []
    monkeypatch.setattr(
        "pipelines.lib.api.time.sleep", lambda secs: sleep_calls.append(secs)
    )

    source._respect_retry_after(response)

    assert sleep_calls == []


def test_create_api_source_from_options_rejects_invalid_auth():
    with pytest.raises(ValueError, match="Unsupported auth_type"):
        create_api_source_from_options(
            system="api",
            entity="items",
            options={
                "auth_type": "nope",
                "base_url": "https://api.example.com",
                "endpoint": "/v1",
            },
            target_path="/tmp/bronze",
        )
