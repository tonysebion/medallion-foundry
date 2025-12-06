"""Tests for config loader helpers."""

from pathlib import Path

import pytest

from core.infrastructure.config import loader


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
