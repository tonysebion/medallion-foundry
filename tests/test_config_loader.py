"""Tests for config loader helpers."""

import os
from pathlib import Path

import pytest

from core.infrastructure.config import loader
from core.infrastructure.config.loader import substitute_env_vars, apply_env_substitution


def test_read_yaml_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        loader._read_yaml(str(tmp_path / "missing.yaml"))


def test_read_yaml_non_dict(tmp_path):
    target = tmp_path / "list.yaml"
    target.write_text("- item\n- another\n")
    with pytest.raises(ValueError, match="Config must be a YAML dictionary"):
        loader._read_yaml(str(target))


def test_load_config_missing_run(tmp_path):
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        """
platform:
  bronze:
    storage_backend: local
    local_path: ./out
source:
  system: sys
  table: tbl
  type: api
  api:
    base_url: https://example.com
    endpoint: /
"""
    )
    with pytest.raises(ValueError, match="Missing required key 'source.run'"):
        loader.load_config(str(cfg_path))


class TestEnvSubstitution:
    """Tests for environment variable substitution."""

    def test_substitute_simple_var(self, monkeypatch):
        """Test ${VAR} substitution when var is set."""
        monkeypatch.setenv("TEST_VAR", "hello")
        result = substitute_env_vars("prefix_${TEST_VAR}_suffix")
        assert result == "prefix_hello_suffix"

    def test_substitute_with_default_when_set(self, monkeypatch):
        """Test ${VAR:default} uses VAR value when set."""
        monkeypatch.setenv("TEST_VAR", "actual_value")
        result = substitute_env_vars("${TEST_VAR:default_value}")
        assert result == "actual_value"

    def test_substitute_with_default_when_not_set(self, monkeypatch):
        """Test ${VAR:default} uses default when VAR not set."""
        monkeypatch.delenv("UNSET_VAR", raising=False)
        result = substitute_env_vars("${UNSET_VAR:default_value}")
        assert result == "default_value"

    def test_substitute_empty_default(self, monkeypatch):
        """Test ${VAR:} (empty default) returns empty string."""
        monkeypatch.delenv("UNSET_VAR", raising=False)
        result = substitute_env_vars("prefix_${UNSET_VAR:}_suffix")
        assert result == "prefix__suffix"

    def test_substitute_missing_no_default_raises(self, monkeypatch):
        """Test ${VAR} raises when var is not set and no default."""
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(ValueError, match="Environment variable 'MISSING_VAR' is not set"):
            substitute_env_vars("${MISSING_VAR}")

    def test_substitute_nested_dict(self, monkeypatch):
        """Test env vars substituted in nested dicts."""
        monkeypatch.setenv("DB_HOST", "localhost")
        monkeypatch.setenv("DB_PORT", "5432")
        config = {
            "database": {
                "host": "${DB_HOST}",
                "port": "${DB_PORT}",
            }
        }
        result = substitute_env_vars(config)
        assert result["database"]["host"] == "localhost"
        assert result["database"]["port"] == "5432"

    def test_substitute_in_list(self, monkeypatch):
        """Test env vars substituted in list items."""
        monkeypatch.setenv("ITEM1", "first")
        monkeypatch.setenv("ITEM2", "second")
        config = ["${ITEM1}", "${ITEM2}", "literal"]
        result = substitute_env_vars(config)
        assert result == ["first", "second", "literal"]

    def test_substitute_non_string_passthrough(self):
        """Test non-string values pass through unchanged."""
        config = {
            "number": 42,
            "float": 3.14,
            "bool": True,
            "none": None,
        }
        result = substitute_env_vars(config)
        assert result == config

    def test_multiple_vars_in_single_string(self, monkeypatch):
        """Test multiple ${VAR} substitutions in one string."""
        monkeypatch.setenv("HOST", "myhost")
        monkeypatch.setenv("PORT", "8080")
        result = substitute_env_vars("http://${HOST}:${PORT}/api")
        assert result == "http://myhost:8080/api"

    def test_apply_env_substitution_full_config(self, monkeypatch):
        """Test apply_env_substitution on a full config structure."""
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("API_KEY", "secret123")
        config = {
            "platform": {
                "bronze": {
                    "s3_bucket": "${S3_BUCKET}",
                }
            },
            "source": {
                "api": {
                    "auth": {
                        "api_key": "${API_KEY}"
                    }
                }
            }
        }
        result = apply_env_substitution(config)
        assert result["platform"]["bronze"]["s3_bucket"] == "my-bucket"
        assert result["source"]["api"]["auth"]["api_key"] == "secret123"


class TestMultiSourceConfig:
    """Tests for multi-source config loading."""

    def test_sources_must_be_list(self, tmp_path):
        """Test that 'sources' must be a list."""
        cfg_path = tmp_path / "invalid.yaml"
        cfg_path.write_text(
            """
platform:
  bronze:
    storage_backend: local
sources: "not a list"
"""
        )
        with pytest.raises(ValueError, match="'sources' must be a non-empty list"):
            loader.load_configs(str(cfg_path))

    def test_sources_cannot_coexist_with_source(self, tmp_path):
        """Test that 'source' and 'sources' cannot both exist."""
        cfg_path = tmp_path / "invalid.yaml"
        cfg_path.write_text(
            """
platform:
  bronze:
    storage_backend: local
source:
  system: sys
  table: tbl
sources:
  - source:
      system: sys2
      table: tbl2
"""
        )
        with pytest.raises(ValueError, match="cannot contain both 'source' and 'sources'"):
            loader.load_configs(str(cfg_path))

    def test_datasets_must_be_list(self, tmp_path):
        """Test that 'datasets' must be a list."""
        cfg_path = tmp_path / "invalid.yaml"
        cfg_path.write_text(
            """
domain: test
datasets: "not a list"
"""
        )
        with pytest.raises(ValueError, match="'datasets' must be a non-empty list"):
            loader.load_configs(str(cfg_path))
