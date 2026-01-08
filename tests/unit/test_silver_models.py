"""Tests for SilverModel functionality.

Tests the SilverModel feature which provides pre-built transformation patterns:
- periodic_snapshot: Simple dimension refresh (state + current_only + replace_daily)
- full_merge_dedupe: Dedupe accumulated changes (state + current_only + append_log)
- incremental_merge: CDC with merge (state + current_only + append_log)
- scd_type_2: Full history tracking (state + full_history + append_log)
- event_log: Immutable event stream (event + current_only + append_log)
- cdc_current: CDC stream to SCD1, ignore deletes
- cdc_current_tombstone: CDC stream to SCD1 with soft deletes
- cdc_current_hard_delete: CDC stream to SCD1, remove deletes
- cdc_history: CDC stream to SCD2, ignore deletes
- cdc_history_tombstone: CDC stream to SCD2 with soft deletes
- cdc_history_hard_delete: CDC stream to SCD2, remove deletes
"""

import pytest
from pathlib import Path

from pipelines.lib.bronze import InputMode
from pipelines.lib.silver import (
    SilverModel,
    SILVER_MODEL_PRESETS,
    EntityKind,
    HistoryMode,
    DeleteMode,
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
        # CDC presets
        assert SilverModel.CDC_CURRENT.value == "cdc_current"
        assert SilverModel.CDC_CURRENT_TOMBSTONE.value == "cdc_current_tombstone"
        assert SilverModel.CDC_CURRENT_HARD_DELETE.value == "cdc_current_hard_delete"
        assert SilverModel.CDC_HISTORY.value == "cdc_history"
        assert SilverModel.CDC_HISTORY_TOMBSTONE.value == "cdc_history_tombstone"
        assert SilverModel.CDC_HISTORY_HARD_DELETE.value == "cdc_history_hard_delete"

    def test_silver_model_from_string(self):
        """Test SilverModel can be accessed by value."""
        assert SilverModel("periodic_snapshot") == SilverModel.PERIODIC_SNAPSHOT
        assert SilverModel("scd_type_2") == SilverModel.SCD_TYPE_2
        assert SilverModel("event_log") == SilverModel.EVENT_LOG
        # CDC presets
        assert SilverModel("cdc_current") == SilverModel.CDC_CURRENT
        assert SilverModel("cdc_current_tombstone") == SilverModel.CDC_CURRENT_TOMBSTONE
        assert SilverModel("cdc_history_hard_delete") == SilverModel.CDC_HISTORY_HARD_DELETE


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

    # CDC preset tests
    def test_cdc_current_preset(self):
        """Test cdc_current preset values."""
        preset = SILVER_MODEL_PRESETS["cdc_current"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "current_only"
        assert preset["input_mode"] == "append_log"
        assert preset["delete_mode"] == "ignore"

    def test_cdc_current_tombstone_preset(self):
        """Test cdc_current_tombstone preset values."""
        preset = SILVER_MODEL_PRESETS["cdc_current_tombstone"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "current_only"
        assert preset["input_mode"] == "append_log"
        assert preset["delete_mode"] == "tombstone"

    def test_cdc_current_hard_delete_preset(self):
        """Test cdc_current_hard_delete preset values."""
        preset = SILVER_MODEL_PRESETS["cdc_current_hard_delete"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "current_only"
        assert preset["input_mode"] == "append_log"
        assert preset["delete_mode"] == "hard_delete"

    def test_cdc_history_preset(self):
        """Test cdc_history preset values."""
        preset = SILVER_MODEL_PRESETS["cdc_history"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "full_history"
        assert preset["input_mode"] == "append_log"
        assert preset["delete_mode"] == "ignore"

    def test_cdc_history_tombstone_preset(self):
        """Test cdc_history_tombstone preset values."""
        preset = SILVER_MODEL_PRESETS["cdc_history_tombstone"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "full_history"
        assert preset["input_mode"] == "append_log"
        assert preset["delete_mode"] == "tombstone"

    def test_cdc_history_hard_delete_preset(self):
        """Test cdc_history_hard_delete preset values."""
        preset = SILVER_MODEL_PRESETS["cdc_history_hard_delete"]
        assert preset["entity_kind"] == "state"
        assert preset["history_mode"] == "full_history"
        assert preset["input_mode"] == "append_log"
        assert preset["delete_mode"] == "hard_delete"

    def test_cdc_presets_include_delete_mode(self):
        """Test that all CDC presets include delete_mode key."""
        cdc_models = [
            "cdc_current", "cdc_current_tombstone", "cdc_current_hard_delete",
            "cdc_history", "cdc_history_tombstone", "cdc_history_hard_delete",
        ]
        for model_name in cdc_models:
            preset = SILVER_MODEL_PRESETS[model_name]
            assert "delete_mode" in preset, f"CDC preset {model_name} missing delete_mode"


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

    def test_load_silver_periodic_snapshot_without_keys(self, tmp_path: Path):
        """Test periodic_snapshot model does not require natural_keys or change_timestamp."""
        config = {
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/orders/",
            "model": "periodic_snapshot",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.natural_keys is None
        assert silver.change_timestamp is None
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

    def test_load_silver_with_column_mapping(self, tmp_path: Path):
        """Test loading Silver config with column_mapping."""
        config = {
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/orders/",
            "model": "periodic_snapshot",
            "column_mapping": {
                "ORDER_ID": "order_id",
                "CUST_NBR": "customer_number",
                "LastModified": "updated_at",
            },
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.column_mapping == {
            "ORDER_ID": "order_id",
            "CUST_NBR": "customer_number",
            "LastModified": "updated_at",
        }

    def test_load_silver_without_column_mapping(self, tmp_path: Path):
        """Test loading Silver config without column_mapping defaults to None."""
        config = {
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/orders/",
            "model": "periodic_snapshot",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.column_mapping is None


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


class TestCDCPresetConfigLoader:
    """Tests for CDC preset expansion in config_loader."""

    def test_load_silver_with_cdc_current_model(self, tmp_path: Path):
        """Test loading Silver config with cdc_current model."""
        config = {
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/orders/",
            "model": "cdc_current",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.CURRENT_ONLY
        assert silver.input_mode == InputMode.APPEND_LOG
        assert silver.delete_mode == DeleteMode.IGNORE

    def test_load_silver_with_cdc_current_tombstone_model(self, tmp_path: Path):
        """Test loading Silver config with cdc_current_tombstone model."""
        config = {
            "natural_keys": ["customer_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/customers/",
            "model": "cdc_current_tombstone",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.CURRENT_ONLY
        assert silver.input_mode == InputMode.APPEND_LOG
        assert silver.delete_mode == DeleteMode.TOMBSTONE

    def test_load_silver_with_cdc_history_model(self, tmp_path: Path):
        """Test loading Silver config with cdc_history model."""
        config = {
            "natural_keys": ["product_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/products/",
            "model": "cdc_history",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.FULL_HISTORY
        assert silver.input_mode == InputMode.APPEND_LOG
        assert silver.delete_mode == DeleteMode.IGNORE

    def test_load_silver_with_cdc_history_tombstone_model(self, tmp_path: Path):
        """Test loading Silver config with cdc_history_tombstone model."""
        config = {
            "natural_keys": ["account_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/accounts/",
            "model": "cdc_history_tombstone",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.FULL_HISTORY
        assert silver.input_mode == InputMode.APPEND_LOG
        assert silver.delete_mode == DeleteMode.TOMBSTONE

    def test_load_silver_with_cdc_current_hard_delete_model(self, tmp_path: Path):
        """Test loading Silver config with cdc_current_hard_delete model."""
        config = {
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/orders/",
            "model": "cdc_current_hard_delete",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.CURRENT_ONLY
        assert silver.input_mode == InputMode.APPEND_LOG
        assert silver.delete_mode == DeleteMode.HARD_DELETE

    def test_load_silver_with_cdc_history_hard_delete_model(self, tmp_path: Path):
        """Test loading Silver config with cdc_history_hard_delete model."""
        config = {
            "natural_keys": ["user_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/users/",
            "model": "cdc_history_hard_delete",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.FULL_HISTORY
        assert silver.input_mode == InputMode.APPEND_LOG
        assert silver.delete_mode == DeleteMode.HARD_DELETE

    def test_explicit_delete_mode_overrides_cdc_preset(self, tmp_path: Path):
        """Test that explicit delete_mode overrides CDC preset default."""
        config = {
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/orders/",
            "model": "cdc_current",  # Default delete_mode is "ignore"
            "delete_mode": "tombstone",  # Override to tombstone
        }
        silver = load_silver_from_yaml(config, tmp_path)
        # delete_mode should be overridden
        assert silver.delete_mode == DeleteMode.TOMBSTONE
        # Other settings from preset
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.CURRENT_ONLY
        assert silver.input_mode == InputMode.APPEND_LOG

    def test_pipeline_with_cdc_preset(self, tmp_path: Path):
        """Test loading full pipeline with CDC preset model."""
        yaml_content = """
name: test_cdc_pipeline
bronze:
  system: test
  entity: orders
  source_type: file_csv
  source_path: ./data/cdc_feed.csv
  load_pattern: cdc
  cdc_operation_column: op

silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
  model: cdc_current_tombstone
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.silver.entity_kind == EntityKind.STATE
        assert pipeline.silver.history_mode == HistoryMode.CURRENT_ONLY
        assert pipeline.silver.input_mode == InputMode.APPEND_LOG
        assert pipeline.silver.delete_mode == DeleteMode.TOMBSTONE
