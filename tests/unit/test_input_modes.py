"""Tests for InputMode functionality.

Tests the input_mode feature which controls how Silver interprets Bronze partitions:
- REPLACE_DAILY: Each partition is a complete snapshot (read latest only)
- APPEND_LOG: Partitions are additive (union all partitions)
"""

import pytest
from pathlib import Path

from pipelines.lib.bronze import BronzeSource, InputMode, SourceType
from pipelines.lib.silver import SilverEntity
from pipelines.lib.config_loader import (
    load_bronze_from_yaml,
    load_silver_from_yaml,
    load_pipeline,
    YAMLConfigError,
)


class TestInputModeEnum:
    """Tests for InputMode enum."""

    def test_input_mode_values(self):
        """Test InputMode enum has expected values."""
        assert InputMode.REPLACE_DAILY.value == "replace_daily"
        assert InputMode.APPEND_LOG.value == "append_log"

    def test_input_mode_from_string(self):
        """Test InputMode can be accessed by value."""
        assert InputMode("replace_daily") == InputMode.REPLACE_DAILY
        assert InputMode("append_log") == InputMode.APPEND_LOG


class TestBronzeInputMode:
    """Tests for InputMode in BronzeSource."""

    def test_bronze_source_with_input_mode(self, tmp_path: Path):
        """Test BronzeSource accepts input_mode."""
        source = BronzeSource(
            system="test",
            entity="orders",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "orders.csv"),
            input_mode=InputMode.REPLACE_DAILY,
        )
        assert source.input_mode == InputMode.REPLACE_DAILY

    def test_bronze_source_input_mode_append_log(self, tmp_path: Path):
        """Test BronzeSource with append_log mode."""
        source = BronzeSource(
            system="test",
            entity="events",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "events.csv"),
            input_mode=InputMode.APPEND_LOG,
        )
        assert source.input_mode == InputMode.APPEND_LOG

    def test_bronze_source_input_mode_optional(self, tmp_path: Path):
        """Test BronzeSource input_mode is optional (None by default)."""
        source = BronzeSource(
            system="test",
            entity="orders",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "orders.csv"),
        )
        assert source.input_mode is None


class TestSilverInputMode:
    """Tests for InputMode in SilverEntity."""

    def test_silver_entity_with_input_mode(self):
        """Test SilverEntity accepts input_mode."""
        entity = SilverEntity(
            natural_keys=["order_id"],
            change_timestamp="updated_at",
            source_path="./bronze/system=test/entity=orders/dt=2025-01-15/*.parquet",
            target_path="./silver/orders/",
            input_mode=InputMode.REPLACE_DAILY,
        )
        assert entity.input_mode == InputMode.REPLACE_DAILY

    def test_silver_entity_input_mode_append_log(self):
        """Test SilverEntity with append_log mode."""
        entity = SilverEntity(
            natural_keys=["event_id"],
            change_timestamp="event_time",
            source_path="./bronze/system=test/entity=events/dt=2025-01-15/*.parquet",
            target_path="./silver/events/",
            input_mode=InputMode.APPEND_LOG,
        )
        assert entity.input_mode == InputMode.APPEND_LOG

    def test_silver_entity_input_mode_optional(self):
        """Test SilverEntity input_mode is optional (None by default)."""
        entity = SilverEntity(
            natural_keys=["order_id"],
            change_timestamp="updated_at",
            source_path="./bronze/orders/*.parquet",
            target_path="./silver/orders/",
        )
        assert entity.input_mode is None


class TestExpandToAllPartitions:
    """Tests for the _expand_to_all_partitions method."""

    def test_expands_date_partition_yyyy_mm_dd(self):
        """Test expansion of YYYY-MM-DD date format."""
        entity = SilverEntity(
            natural_keys=["order_id"],
            change_timestamp="updated_at",
            source_path="./bronze/orders/*.parquet",
            target_path="./silver/orders/",
            input_mode=InputMode.APPEND_LOG,
        )
        source = "s3://bucket/bronze/system=retail/entity=orders/dt=2025-01-15/*.parquet"
        expanded = entity._expand_to_all_partitions(source)
        assert expanded == "s3://bucket/bronze/system=retail/entity=orders/dt=*/*.parquet"

    def test_expands_date_partition_yyyymmdd(self):
        """Test expansion of YYYYMMDD date format."""
        entity = SilverEntity(
            natural_keys=["order_id"],
            change_timestamp="updated_at",
            source_path="./bronze/orders/*.parquet",
            target_path="./silver/orders/",
            input_mode=InputMode.APPEND_LOG,
        )
        source = "s3://bucket/bronze/dt=20250115/orders.parquet"
        expanded = entity._expand_to_all_partitions(source)
        assert expanded == "s3://bucket/bronze/dt=*/orders.parquet"

    def test_preserves_path_without_date_partition(self):
        """Test path without date partition is preserved with warning."""
        entity = SilverEntity(
            natural_keys=["order_id"],
            change_timestamp="updated_at",
            source_path="./bronze/orders/*.parquet",
            target_path="./silver/orders/",
            input_mode=InputMode.APPEND_LOG,
        )
        source = "s3://bucket/bronze/orders/*.parquet"
        expanded = entity._expand_to_all_partitions(source)
        # Should return unchanged since no dt= partition found
        assert expanded == source


class TestConfigLoaderInputMode:
    """Tests for input_mode parsing in config_loader."""

    def test_load_bronze_with_replace_daily(self, tmp_path: Path):
        """Test loading Bronze config with replace_daily input_mode."""
        config = {
            "system": "test",
            "entity": "orders",
            "source_type": "file_csv",
            "source_path": "./data/orders.csv",
            "input_mode": "replace_daily",
        }
        bronze = load_bronze_from_yaml(config, tmp_path)
        assert bronze.input_mode == InputMode.REPLACE_DAILY

    def test_load_bronze_with_append_log(self, tmp_path: Path):
        """Test loading Bronze config with append_log input_mode."""
        config = {
            "system": "test",
            "entity": "events",
            "source_type": "file_csv",
            "source_path": "./data/events.csv",
            "input_mode": "append_log",
        }
        bronze = load_bronze_from_yaml(config, tmp_path)
        assert bronze.input_mode == InputMode.APPEND_LOG

    def test_load_bronze_without_input_mode(self, tmp_path: Path):
        """Test loading Bronze config without input_mode (defaults to None)."""
        config = {
            "system": "test",
            "entity": "orders",
            "source_type": "file_csv",
            "source_path": "./data/orders.csv",
        }
        bronze = load_bronze_from_yaml(config, tmp_path)
        assert bronze.input_mode is None

    def test_load_bronze_invalid_input_mode_raises(self, tmp_path: Path):
        """Test invalid input_mode raises YAMLConfigError."""
        config = {
            "system": "test",
            "entity": "orders",
            "source_type": "file_csv",
            "source_path": "./data/orders.csv",
            "input_mode": "invalid_mode",
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_bronze_from_yaml(config, tmp_path)
        assert "Invalid input_mode" in str(exc_info.value)

    def test_load_silver_with_input_mode(self, tmp_path: Path):
        """Test loading Silver config with explicit input_mode."""
        config = {
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/orders/",
            "input_mode": "append_log",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.input_mode == InputMode.APPEND_LOG


class TestPipelineInputModeWiring:
    """Tests for input_mode wiring between Bronze and Silver in PipelineFromYAML."""

    def test_pipeline_wires_input_mode_from_bronze_to_silver(self, tmp_path: Path):
        """Test that input_mode is auto-wired from Bronze to Silver."""
        yaml_content = """
name: test_pipeline
bronze:
  system: test
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv
  input_mode: append_log

silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.bronze.input_mode == InputMode.APPEND_LOG
        assert pipeline.silver.input_mode == InputMode.APPEND_LOG

    def test_pipeline_silver_explicit_overrides_bronze(self, tmp_path: Path):
        """Test that explicit Silver input_mode overrides Bronze."""
        yaml_content = """
name: test_pipeline
bronze:
  system: test
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv
  input_mode: append_log

silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
  input_mode: replace_daily
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.bronze.input_mode == InputMode.APPEND_LOG
        # Silver's explicit setting is preserved (not overwritten by Bronze)
        assert pipeline.silver.input_mode == InputMode.REPLACE_DAILY

    def test_pipeline_no_input_mode_both_none(self, tmp_path: Path):
        """Test that both layers have None when input_mode not specified."""
        yaml_content = """
name: test_pipeline
bronze:
  system: test
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv

silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.bronze.input_mode is None
        assert pipeline.silver.input_mode is None
