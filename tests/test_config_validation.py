"""Tests for config validation helpers."""

import copy
import pytest

from core.infrastructure.config import validate_config_dict


@pytest.fixture
def base_config() -> dict:
    cfg = {
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_path": "./out",
            }
        },
        "source": {
            "system": "sys",
            "table": "tbl",
            "type": "api",
            "api": {"base_url": "https://example.com", "endpoint": "/"},
            "run": {},
        },
    }
    return cfg


def test_validate_config_returns_copy(base_config):
    validated = validate_config_dict(base_config)
    assert validated is not base_config
    assert validated["source"]["run"]["load_pattern"] == "snapshot"


def test_validate_missing_platform(base_config):
    cfg = copy.deepcopy(base_config)
    cfg.pop("platform")
    with pytest.raises(ValueError, match="Config must contain a 'platform' section"):
        validate_config_dict(cfg)


def test_validate_invalid_storage_backend(base_config):
    cfg = copy.deepcopy(base_config)
    cfg["platform"]["bronze"]["storage_backend"] = "unknown"
    with pytest.raises(ValueError, match="platform.bronze.storage_backend must be one of"):
        validate_config_dict(cfg)


def test_validate_invalid_source_type(base_config):
    cfg = copy.deepcopy(base_config)
    cfg["source"]["type"] = "unknown"
    with pytest.raises(ValueError, match="Invalid source.type"):
        validate_config_dict(cfg)


def test_validate_invalid_data_classification(base_config):
    cfg = copy.deepcopy(base_config)
    cfg["data_classification"] = "ultra-secret"
    with pytest.raises(ValueError, match="data_classification must be one of"):
        validate_config_dict(cfg)
