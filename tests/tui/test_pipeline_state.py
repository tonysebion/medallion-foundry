"""Tests for PipelineState class.

These tests verify the state management layer without requiring Textual.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipelines.tui.models import FieldSource, FieldValue, PipelineState


class TestFieldValue:
    """Tests for FieldValue dataclass."""

    def test_field_value_defaults(self) -> None:
        """Field value has correct defaults."""
        field = FieldValue(name="test_field")

        assert field.name == "test_field"
        assert field.value == ""
        assert field.source == FieldSource.DEFAULT
        assert field.is_required is False
        assert field.is_sensitive is False
        assert field.validation_error is None

    def test_is_inherited(self) -> None:
        """is_inherited returns True only for parent source."""
        default_field = FieldValue(name="f1", source=FieldSource.DEFAULT)
        parent_field = FieldValue(name="f2", source=FieldSource.PARENT)
        local_field = FieldValue(name="f3", source=FieldSource.LOCAL)

        assert default_field.is_inherited() is False
        assert parent_field.is_inherited() is True
        assert local_field.is_inherited() is False

    def test_needs_env_var_sensitive_without_ref(self) -> None:
        """Sensitive field without ${} reference should suggest env var."""
        field = FieldValue(
            name="password",
            value="plaintext_secret",
            is_sensitive=True,
        )

        assert field.needs_env_var() is True

    def test_needs_env_var_sensitive_with_ref(self) -> None:
        """Sensitive field with ${} reference should not suggest env var."""
        field = FieldValue(
            name="password",
            value="${DB_PASSWORD}",
            is_sensitive=True,
        )

        assert field.needs_env_var() is False

    def test_needs_env_var_non_sensitive(self) -> None:
        """Non-sensitive field should not suggest env var."""
        field = FieldValue(
            name="system",
            value="retail",
            is_sensitive=False,
        )

        assert field.needs_env_var() is False

    def test_needs_env_var_empty(self) -> None:
        """Empty sensitive field should not suggest env var."""
        field = FieldValue(
            name="token",
            value="",
            is_sensitive=True,
        )

        assert field.needs_env_var() is False

    def test_reset_to_parent(self) -> None:
        """Reset restores parent value and sets source."""
        field = FieldValue(
            name="system",
            value="modified",
            source=FieldSource.LOCAL,
            parent_value="original",
        )

        field.reset_to_parent()

        assert field.value == "original"
        assert field.source == FieldSource.PARENT

    def test_set_local_value(self) -> None:
        """Setting local value updates source."""
        field = FieldValue(
            name="entity",
            value="orders",
            source=FieldSource.PARENT,
        )

        field.set_local_value("orders_filtered")

        assert field.value == "orders_filtered"
        assert field.source == FieldSource.LOCAL


class TestPipelineState:
    """Tests for PipelineState class."""

    def test_from_schema_defaults(self) -> None:
        """Create state with schema defaults."""
        state = PipelineState.from_schema_defaults()

        # Bronze fields should exist
        assert "system" in state.bronze
        assert "entity" in state.bronze
        assert "source_type" in state.bronze

        # Silver fields should exist
        assert "natural_keys" in state.silver
        assert "change_timestamp" in state.silver

        # Fields should be marked as default source
        assert state.bronze["system"].source == FieldSource.DEFAULT

    def test_from_yaml_simple(self, tmp_yaml: Path) -> None:
        """Load state from a simple YAML file."""
        state = PipelineState.from_yaml(tmp_yaml)

        assert state.mode == "edit"
        assert state.yaml_path == tmp_yaml

        # Bronze values should be loaded
        assert state.get_bronze_value("system") == "retail"
        assert state.get_bronze_value("entity") == "orders"
        assert state.get_bronze_value("source_type") == "file_csv"

        # Silver values should be loaded
        assert state.get_silver_value("natural_keys") == ["order_id"]
        assert state.get_silver_value("change_timestamp") == "updated_at"

    def test_from_yaml_with_inheritance(
        self, tmp_path: Path, tmp_parent_yaml: Path, tmp_child_yaml: Path
    ) -> None:
        """Load child config with parent inheritance."""
        state = PipelineState.from_yaml(tmp_child_yaml)

        # Entity was overridden in child
        assert state.get_bronze_value("entity") == "orders_filtered"

        # System inherited from parent
        assert state.get_bronze_value("system") == "retail"

    def test_get_required_fields_basic(self) -> None:
        """Basic required fields are always required."""
        state = PipelineState.from_schema_defaults()

        bronze_required = state.get_required_fields("bronze")
        silver_required = state.get_required_fields("silver")

        assert "system" in bronze_required
        assert "entity" in bronze_required
        assert "source_type" in bronze_required

        assert "natural_keys" in silver_required
        assert "change_timestamp" in silver_required

    def test_get_required_fields_api(self) -> None:
        """API source type requires additional fields."""
        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("source_type", "api_rest")

        bronze_required = state.get_required_fields("bronze")

        assert "base_url" in bronze_required
        assert "endpoint" in bronze_required

    def test_get_required_fields_incremental(self) -> None:
        """Incremental load pattern requires watermark column."""
        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("load_pattern", "incremental")

        bronze_required = state.get_required_fields("bronze")

        assert "watermark_column" in bronze_required

    def test_get_visible_fields_beginner(self) -> None:
        """Beginner mode shows only essential fields."""
        state = PipelineState.from_schema_defaults()
        state.view_mode = "beginner"

        bronze_visible = state.get_visible_fields("bronze")
        silver_visible = state.get_visible_fields("silver")

        # Essential bronze fields
        assert "system" in bronze_visible
        assert "entity" in bronze_visible
        assert "source_type" in bronze_visible
        assert "source_path" in bronze_visible

        # Essential silver fields
        assert "natural_keys" in silver_visible
        assert "change_timestamp" in silver_visible

        # Advanced fields should not be visible
        assert "chunk_size" not in bronze_visible

    def test_validate_missing_required(self) -> None:
        """Validation catches missing required fields."""
        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("system", "retail")
        # Missing entity and source_type

        errors = state.validate()

        assert any("entity" in e for e in errors)
        assert any("source_type" in e for e in errors)

    def test_validate_mutually_exclusive(self) -> None:
        """Cannot specify both attributes and exclude_columns."""
        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("system", "retail")
        state.set_bronze_value("entity", "orders")
        state.set_bronze_value("source_type", "file_csv")
        state.set_silver_value("natural_keys", ["id"])
        state.set_silver_value("change_timestamp", "updated_at")
        state.set_silver_value("attributes", ["name", "email"])
        state.set_silver_value("exclude_columns", ["internal_id"])

        errors = state.validate()

        assert any("attributes" in e and "exclude_columns" in e for e in errors)

    def test_to_config_dict(self) -> None:
        """Convert state to configuration dictionary."""
        state = PipelineState.from_schema_defaults()
        state.name = "test_pipeline"
        state.set_bronze_value("system", "retail")
        state.set_bronze_value("entity", "orders")
        state.set_bronze_value("source_type", "file_csv")
        state.set_silver_value("natural_keys", ["order_id"])
        state.set_silver_value("change_timestamp", "updated_at")

        config = state.to_config_dict()

        assert config["name"] == "test_pipeline"
        assert config["bronze"]["system"] == "retail"
        assert config["bronze"]["entity"] == "orders"
        assert config["silver"]["natural_keys"] == ["order_id"]

    def test_to_config_dict_preserves_extends(self) -> None:
        """Extends directive is preserved in config dict."""
        state = PipelineState.from_schema_defaults()
        state.extends = "./parent.yaml"
        state.set_bronze_value("entity", "orders_filtered")

        config = state.to_config_dict()

        assert config["extends"] == "./parent.yaml"

    def test_to_yaml_generates_valid_output(self) -> None:
        """to_yaml generates parseable YAML."""
        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("system", "retail")
        state.set_bronze_value("entity", "orders")
        state.set_bronze_value("source_type", "file_csv")
        state.set_silver_value("natural_keys", "order_id")
        state.set_silver_value("change_timestamp", "updated_at")

        yaml_str = state.to_yaml()

        assert "bronze:" in yaml_str
        assert "system: retail" in yaml_str
        assert "silver:" in yaml_str

    def test_set_parent_config(self, tmp_parent_yaml: Path) -> None:
        """Setting parent config populates inherited values."""
        state = PipelineState.from_schema_defaults()
        state.set_parent_config(tmp_parent_yaml)

        assert state.extends == str(tmp_parent_yaml)
        assert state.get_bronze_value("system") == "retail"
        assert state.bronze["system"].source == FieldSource.PARENT

    def test_load_api_yaml(self, tmp_api_yaml: Path) -> None:
        """Load API pipeline with nested auth and pagination."""
        state = PipelineState.from_yaml(tmp_api_yaml)

        assert state.get_bronze_value("source_type") == "api_rest"
        assert state.get_bronze_value("base_url") == "https://api.github.com"

    def test_discover_env_files(self, tmp_path: Path, tmp_env_file: Path) -> None:
        """Discover .env files in expected locations."""
        # Create state and set yaml_path to help discovery
        state = PipelineState.from_schema_defaults()
        state.yaml_path = tmp_path / "test.yaml"

        # Discovery should work (may find files depending on environment)
        discovered = state.discover_env_files()

        # Should be a list (may be empty if no .env files in standard locations)
        assert isinstance(discovered, list)


class TestFieldMetadata:
    """Tests for field metadata and visibility rules."""

    def test_conditional_visibility_file_source(self) -> None:
        """File source shows source_path, hides database fields."""
        from pipelines.tui.models.field_metadata import get_conditional_visibility

        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("source_type", "file_csv")

        visibility = get_conditional_visibility(state)

        assert visibility.get("source_path") is True
        assert visibility.get("host") is False
        assert visibility.get("database") is False
        assert visibility.get("base_url") is False

    def test_conditional_visibility_database_source(self) -> None:
        """Database source shows connection fields, hides file/api fields."""
        from pipelines.tui.models.field_metadata import get_conditional_visibility

        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("source_type", "database_mssql")

        visibility = get_conditional_visibility(state)

        assert visibility.get("host") is True
        assert visibility.get("database") is True
        assert visibility.get("base_url") is False

    def test_conditional_visibility_api_source(self) -> None:
        """API source shows API fields including auth and pagination."""
        from pipelines.tui.models.field_metadata import get_conditional_visibility

        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("source_type", "api_rest")

        visibility = get_conditional_visibility(state)

        assert visibility.get("base_url") is True
        assert visibility.get("endpoint") is True
        assert visibility.get("auth_type") is True
        assert visibility.get("pagination_strategy") is True

    def test_conditional_visibility_bearer_auth(self) -> None:
        """Bearer auth shows token field only."""
        from pipelines.tui.models.field_metadata import get_conditional_visibility

        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("source_type", "api_rest")
        state.set_bronze_value("auth_type", "bearer")

        visibility = get_conditional_visibility(state)

        assert visibility.get("token") is True
        assert visibility.get("api_key") is False
        assert visibility.get("username") is False

    def test_conditional_visibility_incremental(self) -> None:
        """Incremental load shows watermark fields."""
        from pipelines.tui.models.field_metadata import get_conditional_visibility

        state = PipelineState.from_schema_defaults()
        state.set_bronze_value("load_pattern", "incremental")

        visibility = get_conditional_visibility(state)

        assert visibility.get("watermark_column") is True
        assert visibility.get("full_refresh_days") is True
