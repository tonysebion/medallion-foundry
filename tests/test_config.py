"""Tests for configuration loading and validation."""

import textwrap
from datetime import date

import pytest
import yaml

from core.config import build_relative_path, load_config, load_configs
from core.silver.models import SilverModel


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
                        "parquet_compression": "snappy",
                    },
                },
                "s3_connection": {
                    "endpoint_url_env": "TEST_ENDPOINT",
                    "access_key_env": "TEST_KEY",
                    "secret_key_env": "TEST_SECRET",
                },
            },
            "source": {
                "type": "api",
                "system": "test_system",
                "table": "test_table",
                "api": {"base_url": "https://api.example.com", "endpoint": "/test"},
                "run": {
                    "max_rows_per_file": 1000,
                    "write_csv": True,
                    "write_parquet": True,
                    "s3_enabled": False,
                    "local_output_dir": "./output",
                },
            },
        }

        config_file = tmp_path / "test_config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        loaded_config = load_config(str(config_file))
        assert loaded_config["source"]["system"] == "test_system"
        assert loaded_config["platform"]["bronze"]["s3_bucket"] == "test-bucket"
        assert loaded_config["silver"]["domain"] == "test_system"
        assert loaded_config["silver"]["entity"] == "test_table"
        assert loaded_config["silver"]["partitioning"]["columns"] == []
        assert loaded_config["silver"]["require_checksum"] is False
        assert loaded_config["source"]["run"]["load_pattern"] == "full"

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
        config = {"platform": {"bronze": {"s3_bucket": "test"}, "s3_connection": {}}}
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
                "s3_connection": {},
            },
            "source": {
                "type": "invalid_type",
                "system": "test",
                "table": "test",
                "run": {},
            },
        }
        config_file = tmp_path / "bad_config.yaml"

        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with pytest.raises(ValueError, match="Invalid source.type"):
            load_config(str(config_file))

    def test_file_source_requires_path(self, tmp_path):
        """Test that file sources require a path."""
        config = {
            "platform": {
                "bronze": {"s3_bucket": "test", "s3_prefix": "bronze"},
                "s3_connection": {},
            },
            "source": {
                "type": "file",
                "system": "offline",
                "table": "sample",
                "file": {},
                "run": {},
            },
        }
        config_file = tmp_path / "file_config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with pytest.raises(ValueError, match="source.file requires 'path'"):
            load_config(str(config_file))

    def test_file_source_validates_format(self, tmp_path):
        """Test that invalid file formats are rejected."""
        config = {
            "platform": {
                "bronze": {"s3_bucket": "test", "s3_prefix": "bronze"},
                "s3_connection": {},
            },
            "source": {
                "type": "file",
                "system": "offline",
                "table": "sample",
                "file": {"path": "./data/sample.txt", "format": "unsupported"},
                "run": {},
            },
        }
        config_file = tmp_path / "file_config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with pytest.raises(ValueError, match="source.file.format"):
            load_config(str(config_file))

    def test_load_configs_multiple_sources(self, tmp_path):
        """Test loading configs with multiple sources in one file."""
        config = {
            "platform": {
                "bronze": {
                    "s3_bucket": "test-bucket",
                    "s3_prefix": "bronze",
                    "output_defaults": {"allow_csv": True, "allow_parquet": True},
                },
                "s3_connection": {},
            },
            "silver": {
                "output_dir": "./silver_output",
                "primary_keys": ["id"],
            },
            "sources": [
                {
                    "name": "full_orders",
                    "source": {
                        "type": "file",
                        "system": "demo",
                        "table": "orders",
                        "file": {"path": "./data/orders_full.csv", "format": "csv"},
                        "run": {"load_pattern": "full"},
                    },
                },
                {
                    "name": "cdc_orders",
                    "source": {
                        "type": "file",
                        "system": "demo",
                        "table": "orders_cdc",
                        "file": {"path": "./data/orders_cdc.csv", "format": "csv"},
                        "run": {"load_pattern": "cdc"},
                    },
                },
            ],
        }
        config_file = tmp_path / "multi.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        configs = load_configs(str(config_file))
        assert len(configs) == 2
        names = [cfg["source"]["config_name"] for cfg in configs]
        assert names == ["full_orders", "cdc_orders"]

    def test_current_history_requires_primary_keys(self, tmp_path):
        config = {
            "platform": {
                "bronze": {"s3_bucket": "test", "s3_prefix": "bronze"},
                "s3_connection": {},
            },
            "silver": {},
            "source": {
                "type": "file",
                "system": "demo",
                "table": "orders",
                "file": {"path": "./data/orders.csv", "format": "csv"},
                "run": {"load_pattern": "current_history"},
            },
        }
        config_file = tmp_path / "current_history.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with pytest.raises(ValueError, match="silver\\.primary_keys"):
            load_config(str(config_file))

    def test_invalid_load_pattern(self, tmp_path):
        """Test that invalid load patterns raise ValueError."""
        config = {
            "platform": {
                "bronze": {"s3_bucket": "test", "s3_prefix": "bronze"},
                "s3_connection": {},
            },
            "source": {
                "type": "api",
                "system": "offline",
                "table": "sample",
                "api": {"base_url": "https://example.com", "endpoint": "/records"},
                "run": {"load_pattern": "invalid"},
            },
        }
        config_file = tmp_path / "pattern_config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with pytest.raises(ValueError, match="load pattern"):
            load_config(str(config_file))

    def test_silver_require_checksum_toggle(self, tmp_path):
        config = {
            "platform": {
                "bronze": {"s3_bucket": "test", "s3_prefix": "bronze"},
                "s3_connection": {},
            },
            "silver": {"require_checksum": True},
            "source": {
                "type": "file",
                "system": "demo",
                "table": "orders",
                "file": {"path": "./data/orders.csv", "format": "csv"},
                "run": {"load_pattern": "full"},
            },
        }
        config_file = tmp_path / "silver_toggle.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        loaded_config = load_config(str(config_file))
        assert loaded_config["silver"]["require_checksum"] is True

    def test_silver_require_checksum_must_be_boolean(self, tmp_path):
        config = {
            "platform": {
                "bronze": {"s3_bucket": "test", "s3_prefix": "bronze"},
                "s3_connection": {},
            },
            "silver": {"require_checksum": "yes"},
            "source": {
                "type": "file",
                "system": "demo",
                "table": "orders",
                "file": {"path": "./data/orders.csv", "format": "csv"},
                "run": {"load_pattern": "full"},
            },
        }
        config_file = tmp_path / "silver_toggle_bad.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with pytest.raises(ValueError, match="silver\\.require_checksum"):
            load_config(str(config_file))

    def test_silver_model_profile_applied(self, tmp_path):
        config = {
            "platform": {
                "bronze": {"s3_bucket": "test", "s3_prefix": "bronze"},
                "s3_connection": {},
            },
            "silver": {"model_profile": "analytics"},
            "source": {
                "type": "file",
                "system": "demo",
                "table": "orders",
                "file": {"path": "./data/orders.csv", "format": "csv"},
                "run": {"load_pattern": "full"},
            },
        }
        config_file = tmp_path / "silver_profile.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        loaded_config = load_config(str(config_file))
        assert loaded_config["silver"]["model"] == SilverModel.SCD_TYPE_2.value

    def test_intent_style_config_parses(self, tmp_path):
        yaml_text = textwrap.dedent(
            """
            environment: dev
            domain: finance
            system: crm
            entity: orders

            bronze:
              enabled: true
              source_type: file
              path_pattern: ./docs/examples/data/orders_daily.csv
              options:
                file:
                  format: csv

            silver:
              enabled: true
              entity_kind: event
              input_mode: append_log
              delete_mode: ignore
              schema_mode: strict
              natural_keys:
                - order_id
              event_ts_column: event_ts
              change_ts_column: event_ts
              attributes:
                - status
                - amount
              partition_by:
                - event_ts_dt
            """
        )
        config_file = tmp_path / "intent.yaml"
        config_file.write_text(yaml_text, encoding="utf-8")

        cfg = load_config(str(config_file))
        assert cfg["_intent_config"] is True
        dataset = cfg["__dataset__"]
        assert dataset.system == "crm"
        assert dataset.entity == "orders"
        assert dataset.silver.entity_kind.value == "event"
        assert cfg["source"]["system"] == "crm"
        assert cfg["silver"]["primary_keys"] == ["order_id"]
        assert cfg["source"]["run"]["local_output_dir"]

    def test_intent_multi_dataset_config(self, tmp_path):
        yaml_text = textwrap.dedent(
            """
            environment: test
            domain: claims
            datasets:
              - name: adjusters
                system: guidewire
                entity: adjusters
                bronze:
                  enabled: true
                  source_type: db
                  connection_name: GW_DB
                  source_query: SELECT * FROM adjusters
                silver:
                  enabled: true
                  entity_kind: state
                  history_mode: scd2
                  natural_keys: [adjuster_id]
                  change_ts_column: updated_at
                  attributes: [status]
              - name: claim_events
                system: guidewire
                entity: claim_events
                bronze:
                  enabled: true
                  source_type: api
                  connection_name: GW_API
                  source_query: /claims/events
                silver:
                  enabled: true
                  entity_kind: event
                  input_mode: append_log
                  natural_keys: [claim_event_id]
                  event_ts_column: occurred_at
                  change_ts_column: occurred_at
                  attributes: [claim_id, status]
            """
        )
        config_file = tmp_path / "multi_intent.yaml"
        config_file.write_text(yaml_text, encoding="utf-8")

        configs = load_configs(str(config_file))
        assert len(configs) == 2
        names = {cfg["source"]["config_name"] for cfg in configs}
        assert names == {"adjusters", "claim_events"}
        for cfg in configs:
            assert cfg["_intent_config"] is True
            dataset = cfg["__dataset__"]
            assert dataset.system == "guidewire"
            assert cfg["platform"]["bronze"]["local_path"]


class TestBuildRelativePath:
    """Test relative path building."""

    def test_build_path_with_dt_partition(self):
        """Test path building with dt partition."""
        config = {
            "platform": {"bronze": {"partitioning": {"use_dt_partition": True}}},
            "source": {"system": "test_sys", "table": "test_tbl"},
        }

        run_date = date(2025, 11, 12)
        path = build_relative_path(config, run_date)

        assert path == "system=test_sys/table=test_tbl/pattern=full/dt=2025-11-12/"

    def test_build_path_without_dt_partition(self):
        """Test path building without dt partition."""
        config = {
            "platform": {"bronze": {"partitioning": {"use_dt_partition": False}}},
            "source": {"system": "test_sys", "table": "test_tbl"},
        }

        run_date = date(2025, 11, 12)
        path = build_relative_path(config, run_date)

        assert path == "system=test_sys/table=test_tbl/pattern=full/"
