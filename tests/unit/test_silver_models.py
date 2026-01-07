"""Tests for SilverModel functionality.

Tests the SilverModel feature which provides pre-built transformation patterns:
- periodic_snapshot: Simple dimension refresh (state + current_only + replace_daily)
- full_merge_dedupe: Dedupe accumulated changes (state + current_only + append_log)
- incremental_merge: CDC with merge (state + current_only + append_log)
- scd_type_2: Full history tracking (state + full_history + append_log)
- event_log: Immutable event stream (event + current_only + append_log)
"""

import pytest
from pathlib import Path

from pipelines.lib.bronze import InputMode
from pipelines.lib.silver import (
    SilverModel,
    SILVER_MODEL_PRESETS,
    EntityKind,
    HistoryMode,
)
from pipelines.lib.config_loader import (
    load_silver_from_yaml,
    load_pipeline,
    YAMLConfigError,
)


class TestSilverModelEnum:
    """Tests for SilverModel enum."""

    def test_silver_model_values(self):
        """Test SilverModel enum has expected values."""
        assert SilverModel.PERIODIC_SNAPSHOT.value == "periodic_snapshot"
        assert SilverModel.FULL_MERGE_DEDUPE.value == "full_merge_dedupe"
        assert SilverModel.INCREMENTAL_MERGE.value == "incremental_merge"
        assert SilverModel.SCD_TYPE_2.value == "scd_type_2"
        assert SilverModel.EVENT_LOG.value == "event_log"

    def test_silver_model_from_string(self):
        """Test SilverModel can be accessed by value."""
        assert SilverModel("periodic_snapshot") == SilverModel.PERIODIC_SNAPSHOT
        assert SilverModel("scd_type_2") == SilverModel.SCD_TYPE_2
        assert SilverModel("event_log") == SilverModel.EVENT_LOG


class TestSilverModelPresets:
    """Tests for SILVER_MODEL_PRESETS configuration."""

    def test_all_models_have_presets(self):
        """Test every SilverModel has a corresponding preset."""
        for model in SilverModel:
            assert model.value in SILVER_MODEL_PRESETS, f"Missing preset for {model.value}"

    def test_presets_have_all_required_keys(self):
        """Test every preset has entity_kind, history_mode, and input_mode."""
        required_keys = {"entity_kind", "history_mode", "input_mode"}
        for model_name, preset in SILVER_MODEL_PRESETS.items():
            for key in required_keys:
                assert key in preset, f"Preset {model_name} missing {key}"

    def test_periodic_snapshot_preset(self):
        """Test periodic_snapshot preset values."""
        preset = SILVER_MODEL_PRESETS["periodic_snapshot"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "current_only"
        assert preset["input_mode"] == "replace_daily"

    def test_full_merge_dedupe_preset(self):
        """Test full_merge_dedupe preset values."""
        preset = SILVER_MODEL_PRESETS["full_merge_dedupe"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "current_only"
        assert preset["input_mode"] == "append_log"

    def test_incremental_merge_preset(self):
        """Test incremental_merge preset values."""
        preset = SILVER_MODEL_PRESETS["incremental_merge"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "current_only"
        assert preset["input_mode"] == "append_log"

    def test_scd_type_2_preset(self):
        """Test scd_type_2 preset values."""
        preset = SILVER_MODEL_PRESETS["scd_type_2"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "full_history"
        assert preset["input_mode"] == "append_log"

    def test_event_log_preset(self):
        """Test event_log preset values."""
        preset = SILVER_MODEL_PRESETS["event_log"]
        assert preset["entity_kind"] == "event"
        assert preset["history_mode"] == "current_only"
        assert preset["input_mode"] == "append_log"


class TestConfigLoaderModelExpansion:
    """Tests for model expansion in config_loader."""

    def test_load_silver_with_periodic_snapshot_model(self, tmp_path: Path):
        """Test loading Silver config with periodic_snapshot model."""
        config = {
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/orders/",
            "model": "periodic_snapshot",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.CURRENT_ONLY
        assert silver.input_mode == InputMode.REPLACE_DAILY

    def test_load_silver_with_full_merge_dedupe_model(self, tmp_path: Path):
        """Test loading Silver config with full_merge_dedupe model."""
        config = {
            "natural_keys": ["customer_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/customers/",
            "model": "full_merge_dedupe",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.CURRENT_ONLY
        assert silver.input_mode == InputMode.APPEND_LOG

    def test_load_silver_with_scd_type_2_model(self, tmp_path: Path):
        """Test loading Silver config with scd_type_2 model."""
        config = {
            "natural_keys": ["product_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/products/",
            "model": "scd_type_2",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.FULL_HISTORY
        assert silver.input_mode == InputMode.APPEND_LOG

    def test_load_silver_with_event_log_model(self, tmp_path: Path):
        """Test loading Silver config with event_log model."""
        config = {
            "natural_keys": ["event_id"],
            "change_timestamp": "event_time",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/events/",
            "model": "event_log",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.EVENT
        assert silver.history_mode == HistoryMode.CURRENT_ONLY
        assert silver.input_mode == InputMode.APPEND_LOG

    def test_explicit_settings_override_model(self, tmp_path: Path):
        """Test that explicit settings override model defaults."""
        config = {
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/orders/",
            "model": "periodic_snapshot",  # Would set current_only, replace_daily
            "history_mode": "full_history",  # Override to full_history
            "input_mode": "append_log",  # Override to append_log
        }
        silver = load_silver_from_yaml(config, tmp_path)
        # entity_kind from model (not overridden)
        assert silver.entity_kind == EntityKind.STATE
        # history_mode overridden
        assert silver.history_mode == HistoryMode.FULL_HISTORY
        # input_mode overridden
        assert silver.input_mode == InputMode.APPEND_LOG

    def test_invalid_model_raises(self, tmp_path: Path):
        """Test invalid model raises YAMLConfigError."""
        config = {
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "model": "invalid_model",
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config, tmp_path)
        assert "Invalid model" in str(exc_info.value)


class TestPipelineWithModels:
    """Tests for models in full pipeline configuration."""

    def test_pipeline_with_scd_type_2_model(self, tmp_path: Path):
        """Test loading full pipeline with scd_type_2 model."""
        yaml_content = """
name: test_pipeline
bronze:
  system: test
  entity: customers
  source_type: file_csv
  source_path: ./data/customers.csv

silver:
  natural_keys: [customer_id]
  change_timestamp: updated_at
  model: scd_type_2
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.silver.entity_kind == EntityKind.STATE
        assert pipeline.silver.history_mode == HistoryMode.FULL_HISTORY
        assert pipeline.silver.input_mode == InputMode.APPEND_LOG

    def test_pipeline_with_event_log_model(self, tmp_path: Path):
        """Test loading full pipeline with event_log model."""
        yaml_content = """
name: test_pipeline
bronze:
  system: test
  entity: events
  source_type: file_csv
  source_path: ./data/events.csv

silver:
  natural_keys: [event_id]
  change_timestamp: event_time
  model: event_log
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.silver.entity_kind == EntityKind.EVENT
        assert pipeline.silver.history_mode == HistoryMode.CURRENT_ONLY
        assert pipeline.silver.input_mode == InputMode.APPEND_LOG

    def test_pipeline_model_with_bronze_input_mode(self, tmp_path: Path):
        """Test model's input_mode takes precedence over Bronze wiring."""
        yaml_content = """
name: test_pipeline
bronze:
  system: test
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv
  input_mode: replace_daily  # Bronze specifies replace_daily

silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
  model: scd_type_2  # Model specifies append_log
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        # Model's input_mode (append_log) should win over Bronze's (replace_daily)
        assert pipeline.silver.input_mode == InputMode.APPEND_LOG

    def test_model_without_other_settings(self, tmp_path: Path):
        """Test that model works as the only behavioral setting."""
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
  model: periodic_snapshot
  # No entity_kind, history_mode, or input_mode specified
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        # All settings should come from the model
        assert pipeline.silver.entity_kind == EntityKind.STATE
        assert pipeline.silver.history_mode == HistoryMode.CURRENT_ONLY
        assert pipeline.silver.input_mode == InputMode.REPLACE_DAILY
