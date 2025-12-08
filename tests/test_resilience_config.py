import os

import pytest

from core.platform.resilience.config import parse_retry_config, resolve_rate_limit_config
from core.platform.resilience.constants import (
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_BASE_DELAY,
    DEFAULT_JITTER,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_DELAY,
)


def test_parse_retry_config_defaults():
    defaults = parse_retry_config(None)
    assert defaults["max_attempts"] == DEFAULT_MAX_ATTEMPTS
    assert defaults["base_delay"] == DEFAULT_BASE_DELAY
    assert defaults["max_delay"] == DEFAULT_MAX_DELAY
    assert defaults["backoff_multiplier"] == DEFAULT_BACKOFF_MULTIPLIER
    assert defaults["jitter"] == DEFAULT_JITTER


@pytest.mark.parametrize(
    ("key", "value", "expected"),
    [
        ("max_attempts", "10", 10),
        ("base_delay", "0.2", 0.2),
        ("max_delay", 5.5, 5.5),
        ("backoff_multiplier", 1.5, 1.5),
        ("jitter", "0.15", 0.15),
    ],
)
def test_parse_retry_config_overrides(key, value, expected):
    overrides = parse_retry_config({key: value})
    assert overrides[key] == expected


def test_resolve_rate_limit_config_precedence(monkeypatch):
    cfg = {"rate_limit": {"rps": 12, "burst": 4}}
    run_cfg = {"rate_limit_rps": 8}
    result = resolve_rate_limit_config(cfg, run_cfg, env_var="BRONZE_TEST_RPS")
    assert result == (12.0, 4)


def test_resolve_rate_limit_config_run_fallback(monkeypatch):
    cfg = {}
    run_cfg = {"rate_limit_rps": 2.5}
    result = resolve_rate_limit_config(cfg, run_cfg, env_var="BRONZE_TEST_RPS")
    assert result == (2.5, None)


def test_resolve_rate_limit_config_env_fallback(monkeypatch):
    cfg = {}
    run_cfg = {}
    monkeypatch.setenv("BRONZE_TEST_RPS", "1.2")
    result = resolve_rate_limit_config(cfg, run_cfg, env_var="BRONZE_TEST_RPS")
    assert result == (1.2, None)


def test_resolve_rate_limit_config_invalid(monkeypatch):
    result = resolve_rate_limit_config({}, {}, env_var="BRONZE_TEST_RPS")
    assert result is None
