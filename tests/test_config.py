"""Tests for configuration loading and validation."""

import pytest
import tempfile
from pathlib import Path
import yaml

from core.config import load_config, build_relative_path
from datetime import date


class TestConfigLoading:
    """Test configuration file loading."""

    def test_load_valid_config(self, tmp_path):
        """Test loading a valid configuration file."""
        config = {
            "platform": {
                "bronze": {
                    "s3_bucket": "test-bucket",
                    "s3_prefix": "bronze",
                    "partitioning": {"use_dt_partition": True},
                    "output_defaults": {
                        "allow_csv": True,
                        "allow_parquet": True,
                        "parquet_compression": "snappy"
                    }
                },
                "s3_connection": {
                    "endpoint_url_env": "TEST_ENDPOINT",
                    "access_key_env": "TEST_KEY",
                    "secret_key_env": "TEST_SECRET"
                }
            },
            "source": {
                "type": "api",
                "system": "test_system",
                "table": "test_table",
                "api": {
                    "base_url": "https://api.example.com",
                    "endpoint": "/test"
                },
                "run": {
                    "max_rows_per_file": 1000,
                    "write_csv": True,
                    "write_parquet": True,
                    "s3_enabled": False,
                    "local_output_dir": "./output"
                }
            }
        }
        
        config_file = tmp_path / "test_config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)
        
        loaded_config = load_config(str(config_file))
        assert loaded_config["source"]["system"] == "test_system"
        assert loaded_config["platform"]["bronze"]["s3_bucket"] == "test-bucket"

    def test_missing_config_file(self):
        """Test that missing config file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent.yaml")

    def test_missing_platform_section(self, tmp_path):
        """Test that missing platform section raises ValueError."""
        config = {"source": {"system": "test"}}
        config_file = tmp_path / "bad_config.yaml"
        
        with open(config_file, "w") as f:
            yaml.dump(config, f)
        
        with pytest.raises(ValueError, match="'platform'"):
            load_config(str(config_file))

    def test_missing_source_section(self, tmp_path):
        """Test that missing source section raises ValueError."""
        config = {
            "platform": {
                "bronze": {"s3_bucket": "test"},
                "s3_connection": {}
            }
        }
        config_file = tmp_path / "bad_config.yaml"
        
        with open(config_file, "w") as f:
            yaml.dump(config, f)
        
        with pytest.raises(ValueError, match="'source'"):
            load_config(str(config_file))

    def test_invalid_source_type(self, tmp_path):
        """Test that invalid source type raises ValueError."""
        config = {
            "platform": {
                "bronze": {"s3_bucket": "test", "s3_prefix": "bronze"},
                "s3_connection": {}
            },
            "source": {
                "type": "invalid_type",
                "system": "test",
                "table": "test",
                "run": {}
            }
        }
        config_file = tmp_path / "bad_config.yaml"
        
        with open(config_file, "w") as f:
            yaml.dump(config, f)
        
        with pytest.raises(ValueError, match="Invalid source.type"):
            load_config(str(config_file))


class TestBuildRelativePath:
    """Test relative path building."""

    def test_build_path_with_dt_partition(self):
        """Test path building with dt partition."""
        config = {
            "platform": {
                "bronze": {
                    "partitioning": {"use_dt_partition": True}
                }
            },
            "source": {
                "system": "test_sys",
                "table": "test_tbl"
            }
        }
        
        run_date = date(2025, 11, 12)
        path = build_relative_path(config, run_date)
        
        assert path == "system=test_sys/table=test_tbl/dt=2025-11-12/"

    def test_build_path_without_dt_partition(self):
        """Test path building without dt partition."""
        config = {
            "platform": {
                "bronze": {
                    "partitioning": {"use_dt_partition": False}
                }
            },
            "source": {
                "system": "test_sys",
                "table": "test_tbl"
            }
        }
        
        run_date = date(2025, 11, 12)
        path = build_relative_path(config, run_date)
        
        assert path == "system=test_sys/table=test_tbl/"
