"""Tests for shared environment variable resolution helpers."""


from core.infrastructure.config import resolve_env_vars


def test_resolve_simple_string(tmp_path, monkeypatch):
    monkeypatch.setenv("TOKEN", "abc123")
    assert resolve_env_vars("${TOKEN}") == "abc123"


def test_resolve_nested_structures(monkeypatch):
    monkeypatch.setenv("KEY", "value")
    cfg = {"path": "${KEY}", "list": ["${KEY}", {"nested": "${KEY}"}]}
    resolved = resolve_env_vars(cfg)
    assert resolved["path"] == "value"
    assert resolved["list"][0] == "value"
    assert resolved["list"][1]["nested"] == "value"


def test_missing_env_var_leaves_placeholder(monkeypatch):
    monkeypatch.delenv("MISSING", raising=False)
    result = resolve_env_vars("${MISSING}")
    assert result == "${MISSING}"
