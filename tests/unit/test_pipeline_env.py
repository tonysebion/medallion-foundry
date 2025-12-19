"""Tests for pipelines environment helpers."""

import pytest

from pipelines.lib.env import expand_env_vars, expand_options


def test_expand_env_vars_simple(monkeypatch):
    monkeypatch.setenv("TOKEN", "abc123")
    assert expand_env_vars("${TOKEN}") == "abc123"


def test_expand_env_vars_no_match(monkeypatch):
    monkeypatch.delenv("MISSING", raising=False)
    assert expand_env_vars("${MISSING}") == "${MISSING}"


def test_expand_options_recurses(monkeypatch):
    monkeypatch.setenv("KEY", "value")
    cfg = {"path": "${KEY}", "nested": {"key": "${KEY}"}, "list": ["${KEY}", 1]}
    expanded = expand_options(cfg)
    assert expanded["path"] == "value"
    assert expanded["nested"]["key"] == "value"
    assert expanded["list"][0] == "value"
    assert expanded["list"][1] == 1


def test_expand_options_strict_missing(monkeypatch):
    monkeypatch.delenv("MISSING", raising=False)
    with pytest.raises(KeyError):
        expand_env_vars("${MISSING}", strict=True)
