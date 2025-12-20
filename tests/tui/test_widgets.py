"""Tests for TUI prompt_toolkit components.

These tests verify the TUI application logic without requiring
interactive terminal input.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from pipelines.tui.app import PipelineConfigApp, Field
from pipelines.tui.models import PipelineState, FieldSource


class TestField:
    """Tests for Field class."""

    def test_field_defaults(self) -> None:
        """Field has correct default values."""
        field = Field("test", "Test Label", "bronze")
        assert field.name == "test"
        assert field.label == "Test Label"
        assert field.section == "bronze"
        assert field.required is False
        assert field.is_sensitive is False
        assert field.field_type == "text"
        assert field.enum_options == []
        assert field.buffer.text == ""

    def test_field_with_default_value(self) -> None:
        """Field buffer is initialized with default value."""
        field = Field("name", "Name", "metadata", default="my_pipeline")
        assert field.buffer.text == "my_pipeline"

    def test_field_visibility_always_visible(self) -> None:
        """Field without visible_when is always visible."""
        field = Field("system", "System", "bronze")
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        assert field.is_visible(app) is True

    def test_field_visibility_conditional(self) -> None:
        """Field with visible_when respects condition."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # source_path should be visible when source_type starts with file_
        source_path_field = next(f for f in app.fields if f.name == "source_path")
        source_type_field = next(f for f in app.fields if f.name == "source_type")

        # Default is file_csv, so source_path should be visible
        source_type_field.buffer.text = "file_csv"
        assert source_path_field.is_visible(app) is True

        # Change to api_rest, source_path should be hidden
        source_type_field.buffer.text = "api_rest"
        assert source_path_field.is_visible(app) is False

    def test_enum_field(self) -> None:
        """Enum field has options."""
        field = Field(
            "source_type", "Source Type", "bronze",
            field_type="enum",
            enum_options=[
                ("file_csv", "CSV File"),
                ("api_rest", "REST API"),
            ],
            default="file_csv",
        )
        assert field.field_type == "enum"
        assert len(field.enum_options) == 2
        assert field.buffer.text == "file_csv"


class TestPipelineConfigApp:
    """Tests for PipelineConfigApp initialization."""

    def test_create_mode_initialization(self) -> None:
        """App initializes correctly in create mode."""
        app = PipelineConfigApp(mode="create")
        assert app.mode == "create"
        assert app.yaml_path is None
        assert app.parent_path is None

    def test_edit_mode_initialization(self) -> None:
        """App initializes correctly in edit mode."""
        app = PipelineConfigApp(mode="edit", yaml_path="/path/to/config.yaml")
        assert app.mode == "edit"
        assert app.yaml_path == "/path/to/config.yaml"

    def test_create_with_parent(self) -> None:
        """App initializes with parent path for inheritance."""
        app = PipelineConfigApp(mode="create", parent_path="/path/to/parent.yaml")
        assert app.mode == "create"
        assert app.parent_path == "/path/to/parent.yaml"

    def test_list_to_str_with_list(self) -> None:
        """_list_to_str converts list to comma-separated string."""
        app = PipelineConfigApp()
        result = app._list_to_str(["a", "b", "c"])
        assert result == "a, b, c"

    def test_list_to_str_with_string(self) -> None:
        """_list_to_str returns string as-is."""
        app = PipelineConfigApp()
        result = app._list_to_str("already_a_string")
        assert result == "already_a_string"

    def test_list_to_str_with_empty(self) -> None:
        """_list_to_str returns empty string for empty input."""
        app = PipelineConfigApp()
        assert app._list_to_str([]) == ""
        assert app._list_to_str("") == ""
        assert app._list_to_str(None) == ""


class TestFieldCreation:
    """Tests for field creation."""

    def test_creates_all_fields(self) -> None:
        """App creates all expected fields."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        field_names = [f.name for f in app.fields]

        # Metadata fields
        assert "name" in field_names
        assert "description" in field_names

        # Bronze fields
        assert "system" in field_names
        assert "entity" in field_names
        assert "source_type" in field_names
        assert "load_pattern" in field_names

        # Silver fields
        assert "natural_keys" in field_names
        assert "change_timestamp" in field_names
        assert "entity_kind" in field_names
        assert "history_mode" in field_names

    def test_required_fields_marked(self) -> None:
        """Required fields are marked correctly."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        system_field = next(f for f in app.fields if f.name == "system")
        name_field = next(f for f in app.fields if f.name == "name")

        assert system_field.required is True
        assert name_field.required is False

    def test_sensitive_fields_marked(self) -> None:
        """Sensitive fields are marked correctly."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        host_field = next(f for f in app.fields if f.name == "host")
        token_field = next(f for f in app.fields if f.name == "token")

        assert host_field.is_sensitive is True
        assert token_field.is_sensitive is True


class TestFieldVisibility:
    """Tests for conditional field visibility."""

    def test_get_visible_fields_file_source(self) -> None:
        """File source shows file-specific fields."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set source type to file_csv
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "file_csv"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        assert "source_path" in visible_names
        assert "host" not in visible_names
        assert "base_url" not in visible_names

    def test_get_visible_fields_database_source(self) -> None:
        """Database source shows database-specific fields."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set source type to database_mssql
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "database_mssql"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        assert "host" in visible_names
        assert "database" in visible_names
        assert "query" in visible_names
        assert "source_path" not in visible_names
        assert "base_url" not in visible_names

    def test_get_visible_fields_api_source(self) -> None:
        """API source shows API-specific fields."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set source type to api_rest
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "api_rest"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        assert "base_url" in visible_names
        assert "endpoint" in visible_names
        assert "auth_type" in visible_names
        assert "source_path" not in visible_names
        assert "host" not in visible_names

    def test_get_visible_fields_bearer_auth(self) -> None:
        """Bearer auth shows token field."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set source type to api_rest and auth to bearer
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "api_rest"
        auth_type = next(f for f in app.fields if f.name == "auth_type")
        auth_type.buffer.text = "bearer"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        assert "token" in visible_names

    def test_get_visible_fields_watermark(self) -> None:
        """Incremental load pattern shows watermark field."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set load pattern to incremental
        load_pattern = next(f for f in app.fields if f.name == "load_pattern")
        load_pattern.buffer.text = "incremental"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        assert "watermark_column" in visible_names


class TestFieldSource:
    """Tests for field source detection."""

    def test_get_source_default(self) -> None:
        """Default source returns 'default'."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        source = app._get_source("bronze", "system")
        assert source == "default"

    def test_get_source_after_set(self) -> None:
        """Source changes to 'local' after setting value."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.state.set_bronze_value("system", "retail")

        source = app._get_source("bronze", "system")
        assert source == "local"


class TestFieldValueRetrieval:
    """Tests for getting field values."""

    def test_get_field_value(self) -> None:
        """Can retrieve field value by name."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set a field value
        system_field = next(f for f in app.fields if f.name == "system")
        system_field.buffer.text = "retail"

        assert app._get_field_value("system") == "retail"

    def test_get_field_value_nonexistent(self) -> None:
        """Nonexistent field returns empty string."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        assert app._get_field_value("nonexistent") == ""


class TestStateSynchronization:
    """Tests for syncing field values to state."""

    def test_sync_fields_to_state(self) -> None:
        """Field values are synced to state correctly."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set field values via buffers
        for field in app.fields:
            if field.name == "name":
                field.buffer.text = "my_pipeline"
            elif field.name == "system":
                field.buffer.text = "retail"
            elif field.name == "entity":
                field.buffer.text = "orders"
            elif field.name == "natural_keys":
                field.buffer.text = "order_id, customer_id"

        app._sync_fields_to_state()

        assert app.state.name == "my_pipeline"
        assert app.state.get_bronze_value("system") == "retail"
        assert app.state.get_bronze_value("entity") == "orders"
        assert app.state.get_silver_value("natural_keys") == ["order_id", "customer_id"]


class TestYAMLPreview:
    """Tests for YAML preview generation."""

    def test_generate_yaml_preview(self) -> None:
        """YAML preview is generated from fields."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set some values
        for field in app.fields:
            if field.name == "system":
                field.buffer.text = "retail"
            elif field.name == "entity":
                field.buffer.text = "orders"
            elif field.name == "source_type":
                field.buffer.text = "file_csv"
            elif field.name == "natural_keys":
                field.buffer.text = "order_id"
            elif field.name == "change_timestamp":
                field.buffer.text = "updated_at"

        yaml_content = app._generate_yaml_preview()

        assert "bronze:" in yaml_content
        assert "system: retail" in yaml_content
        assert "entity: orders" in yaml_content


class TestStateIntegration:
    """Integration tests for app with state."""

    def test_state_initialization_in_create_mode(self) -> None:
        """State is properly initialized in create mode."""
        app = PipelineConfigApp(mode="create")
        app.state = PipelineState.from_schema_defaults()

        assert app.state is not None
        assert "system" in app.state.bronze
        assert "entity" in app.state.bronze

    def test_state_values_after_setting(self) -> None:
        """State values are properly set."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        app.state.set_bronze_value("system", "retail")
        app.state.set_bronze_value("entity", "orders")
        app.state.set_bronze_value("source_type", "file_csv")

        assert app.state.get_bronze_value("system") == "retail"
        assert app.state.get_bronze_value("entity") == "orders"
        assert app.state.get_bronze_value("source_type") == "file_csv"

    def test_silver_values_after_setting(self) -> None:
        """Silver state values are properly set."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        app.state.set_silver_value("natural_keys", ["order_id", "customer_id"])
        app.state.set_silver_value("change_timestamp", "updated_at")

        assert app.state.get_silver_value("natural_keys") == ["order_id", "customer_id"]
        assert app.state.get_silver_value("change_timestamp") == "updated_at"


class TestYAMLGeneration:
    """Tests for YAML generation from app state."""

    def test_to_yaml_generates_valid_content(self) -> None:
        """to_yaml generates valid YAML content."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        app.state.name = "test_pipeline"
        app.state.set_bronze_value("system", "retail")
        app.state.set_bronze_value("entity", "orders")
        app.state.set_bronze_value("source_type", "file_csv")
        app.state.set_silver_value("natural_keys", ["order_id"])
        app.state.set_silver_value("change_timestamp", "updated_at")

        yaml_content = app.state.to_yaml()

        assert "bronze:" in yaml_content
        assert "system: retail" in yaml_content
        assert "entity: orders" in yaml_content
        assert "silver:" in yaml_content


class TestAPIConfiguration:
    """Tests for API source configuration."""

    def test_api_source_fields(self) -> None:
        """API source type sets appropriate fields."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        app.state.set_bronze_value("source_type", "api_rest")
        app.state.set_bronze_value("base_url", "https://api.example.com")
        app.state.set_bronze_value("endpoint", "/v1/data")
        app.state.set_bronze_value("auth_type", "bearer")
        app.state.set_bronze_value("token", "${API_TOKEN}")

        assert app.state.get_bronze_value("source_type") == "api_rest"
        assert app.state.get_bronze_value("base_url") == "https://api.example.com"
        assert app.state.get_bronze_value("token") == "${API_TOKEN}"

    def test_pagination_fields(self) -> None:
        """Pagination configuration is stored correctly."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        app.state.set_bronze_value("pagination_strategy", "offset")
        app.state.set_bronze_value("page_size", "100")

        assert app.state.get_bronze_value("pagination_strategy") == "offset"
        assert app.state.get_bronze_value("page_size") == "100"
