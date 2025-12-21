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
from pipelines.tui.models.field_metadata import get_dynamic_required_fields


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
        """Field without visible_when is always visible in advanced mode."""
        field = Field("system", "System", "bronze", is_basic=True)
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.advanced_mode = True  # Need advanced mode or is_basic=True
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
        app.advanced_mode = True  # Enable advanced mode to see all fields
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
        app.advanced_mode = True  # Enable advanced mode to see all fields
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
        app.advanced_mode = True  # Enable advanced mode to see all fields
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
        app.advanced_mode = True  # Enable advanced mode to see all fields
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
        app.advanced_mode = True  # Enable advanced mode to see all fields
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


class TestUndoRedo:
    """Tests for undo/redo functionality."""

    def test_undo_stack_initially_empty(self) -> None:
        """Undo stack is empty initially."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        assert len(app._undo_stack) == 0
        assert len(app._redo_stack) == 0

    def test_undo_records_changes(self) -> None:
        """Field changes are recorded in undo stack."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Find system field and modify it
        system_field = next(f for f in app.fields if f.name == "system")
        system_field.buffer.text = "test_system"

        # The change should be recorded
        assert len(app._undo_stack) == 1
        assert app._undo_stack[0][0] == "system"  # field_name
        assert app._undo_stack[0][1] == ""  # old_value
        assert app._undo_stack[0][2] == "test_system"  # new_value

    def test_undo_restores_value(self) -> None:
        """Undo restores previous value."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Modify field
        system_field = next(f for f in app.fields if f.name == "system")
        system_field.buffer.text = "test_system"

        # Undo
        app._undo()

        # Value should be restored
        assert system_field.buffer.text == ""
        assert len(app._undo_stack) == 0
        assert len(app._redo_stack) == 1

    def test_redo_restores_undone_value(self) -> None:
        """Redo restores undone value."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Modify field
        system_field = next(f for f in app.fields if f.name == "system")
        system_field.buffer.text = "test_system"

        # Undo then redo
        app._undo()
        app._redo()

        # Value should be restored
        assert system_field.buffer.text == "test_system"
        assert len(app._undo_stack) == 1
        assert len(app._redo_stack) == 0


class TestSearchFilter:
    """Tests for search/filter functionality."""

    def test_search_initially_empty(self) -> None:
        """Search text is empty initially."""
        app = PipelineConfigApp()
        assert app._search_text == ""

    def test_search_filters_fields(self) -> None:
        """Search filters visible fields."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.advanced_mode = True  # Search only works in advanced mode
        app._create_fields()

        # Get all visible fields
        all_visible = app._get_visible_fields()
        initial_count = len(all_visible)

        # Set search text
        app._search_text = "system"
        filtered = app._get_visible_fields()

        # Should have fewer fields
        assert len(filtered) < initial_count
        # All filtered fields should contain "system"
        for field in filtered:
            assert "system" in field.name.lower() or "system" in field.label.lower() or "system" in field.help_text.lower()

    def test_search_disabled_in_basic_mode(self) -> None:
        """Search is ignored in basic mode."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.advanced_mode = False  # Basic mode
        app._create_fields()

        # Get all visible fields
        all_visible = app._get_visible_fields()
        initial_count = len(all_visible)

        # Set search text
        app._search_text = "xyz_nonexistent"
        filtered = app._get_visible_fields()

        # In basic mode, search should not filter
        assert len(filtered) == initial_count


class TestUnsavedChanges:
    """Tests for unsaved changes tracking."""

    def test_no_unsaved_changes_initially(self) -> None:
        """No unsaved changes initially."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        assert app._has_unsaved_changes is False

    def test_unsaved_changes_after_edit(self) -> None:
        """Unsaved changes tracked after edit."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Modify field
        system_field = next(f for f in app.fields if f.name == "system")
        system_field.buffer.text = "test_system"

        # Should have unsaved changes
        assert app._has_unsaved_changes is True


class TestFieldValidation:
    """Tests for inline field validation."""

    def test_required_field_validation(self) -> None:
        """Required fields fail validation when empty."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # System field is required
        system_field = next(f for f in app.fields if f.name == "system")
        system_field.required = True
        system_field.buffer.text = ""

        is_valid, error = app._is_field_valid(system_field)
        assert is_valid is False
        assert "required" in error.lower()

    def test_valid_field_passes_validation(self) -> None:
        """Valid field passes validation."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # System field with value
        system_field = next(f for f in app.fields if f.name == "system")
        system_field.buffer.text = "retail"

        is_valid, error = app._is_field_valid(system_field)
        assert is_valid is True
        assert error == ""


class TestDynamicRequiredFields:
    """Tests for dynamic required field detection based on current state.

    These tests verify the medallion architecture business rules:
    - API source requires base_url, endpoint
    - Database source requires host, database
    - File source requires source_path
    - Incremental load requires watermark_column
    - Auth types require their credentials
    - Cursor pagination requires cursor_path
    """

    def test_api_source_requires_base_url_endpoint(self) -> None:
        """API source type requires base_url and endpoint."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set source type to API
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "api_rest"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "base_url" in required["bronze"]
        assert "endpoint" in required["bronze"]

    def test_database_source_requires_host_database_query(self) -> None:
        """Database source type requires host, database, and query."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set source type to database
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "database_mssql"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "host" in required["bronze"]
        assert "database" in required["bronze"]
        assert "query" in required["bronze"]

    def test_file_source_requires_source_path(self) -> None:
        """File source type requires source_path."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set source type to file
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "file_csv"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "source_path" in required["bronze"]

    def test_incremental_load_requires_watermark_column(self) -> None:
        """Incremental load pattern requires watermark_column."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set load pattern to incremental
        load_pattern = next(f for f in app.fields if f.name == "load_pattern")
        load_pattern.buffer.text = "incremental"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "watermark_column" in required["bronze"]

    def test_bearer_auth_requires_token(self) -> None:
        """Bearer auth type requires token."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set auth type to bearer
        auth_type = next(f for f in app.fields if f.name == "auth_type")
        auth_type.buffer.text = "bearer"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "token" in required["bronze"]

    def test_api_key_auth_requires_api_key(self) -> None:
        """API key auth type requires api_key."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set auth type to api_key
        auth_type = next(f for f in app.fields if f.name == "auth_type")
        auth_type.buffer.text = "api_key"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "api_key" in required["bronze"]

    def test_basic_auth_requires_username_password(self) -> None:
        """Basic auth type requires username and password."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set auth type to basic
        auth_type = next(f for f in app.fields if f.name == "auth_type")
        auth_type.buffer.text = "basic"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "username" in required["bronze"]
        assert "password" in required["bronze"]

    def test_cursor_pagination_requires_cursor_path(self) -> None:
        """Cursor pagination strategy requires cursor_path."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set pagination strategy to cursor
        pagination = next(f for f in app.fields if f.name == "pagination_strategy")
        pagination.buffer.text = "cursor"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "cursor_path" in required["bronze"]

    def test_full_snapshot_does_not_require_watermark(self) -> None:
        """Full snapshot load pattern does not require watermark."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set load pattern to full_snapshot (default)
        load_pattern = next(f for f in app.fields if f.name == "load_pattern")
        load_pattern.buffer.text = "full_snapshot"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "watermark_column" not in required["bronze"]

    def test_cdc_load_requires_watermark_column(self) -> None:
        """CDC load pattern requires watermark_column."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set load pattern to CDC
        load_pattern = next(f for f in app.fields if f.name == "load_pattern")
        load_pattern.buffer.text = "cdc"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "watermark_column" in required["bronze"]


class TestCrossFieldValidation:
    """Tests for cross-field validation rules.

    These tests verify business logic that spans multiple fields:
    - Events (facts) should not have full_history (SCD2)
    - Cursor pagination requires cursor_path
    - API watermark needs watermark_param
    """

    def test_event_entity_cannot_have_full_history(self) -> None:
        """Events/facts are immutable and should not use SCD2."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set entity_kind to event
        entity_kind = next(f for f in app.fields if f.name == "entity_kind")
        entity_kind.buffer.text = "event"

        # Set history_mode to full_history
        history_mode = next(f for f in app.fields if f.name == "history_mode")
        history_mode.buffer.text = "full_history"

        # Validation should fail
        is_valid, error = app._is_field_valid(history_mode)
        assert is_valid is False
        assert "event" in error.lower() or "immutable" in error.lower()

    def test_state_entity_can_have_full_history(self) -> None:
        """State entities can use SCD2/full_history."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set entity_kind to state
        entity_kind = next(f for f in app.fields if f.name == "entity_kind")
        entity_kind.buffer.text = "state"

        # Set history_mode to full_history
        history_mode = next(f for f in app.fields if f.name == "history_mode")
        history_mode.buffer.text = "full_history"

        # Validation should pass
        is_valid, error = app._is_field_valid(history_mode)
        assert is_valid is True

    def test_cursor_pagination_needs_cursor_path_value(self) -> None:
        """Cursor pagination must have cursor_path specified."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set pagination strategy to cursor
        pagination = next(f for f in app.fields if f.name == "pagination_strategy")
        pagination.buffer.text = "cursor"

        # cursor_path field is empty
        cursor_path = next(f for f in app.fields if f.name == "cursor_path")
        cursor_path.buffer.text = ""

        # Validation should fail
        is_valid, error = app._is_field_valid(cursor_path)
        assert is_valid is False
        assert "cursor" in error.lower()

    def test_offset_pagination_does_not_require_cursor_path(self) -> None:
        """Offset pagination does not require cursor_path."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set pagination strategy to offset
        pagination = next(f for f in app.fields if f.name == "pagination_strategy")
        pagination.buffer.text = "offset"

        # cursor_path can be empty
        cursor_path = next(f for f in app.fields if f.name == "cursor_path")
        cursor_path.buffer.text = ""

        # Validation should pass (cursor_path not required for offset)
        is_valid, error = app._is_field_valid(cursor_path)
        assert is_valid is True


class TestMedallionArchitectureGuidance:
    """Tests for medallion architecture educational features.

    These tests verify that help text and guidance are appropriate
    for users new to the medallion architecture.
    """

    def test_entity_kind_has_educational_help_text(self) -> None:
        """entity_kind field has help text explaining the concept."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        entity_kind = next(f for f in app.fields if f.name == "entity_kind")
        # Should explain what state vs event means
        assert "state" in entity_kind.help_text.lower() or "event" in entity_kind.help_text.lower()
        assert len(entity_kind.help_text) > 20  # More than just "Entity kind"

    def test_history_mode_has_educational_help_text(self) -> None:
        """history_mode field has help text explaining SCD types."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        history_mode = next(f for f in app.fields if f.name == "history_mode")
        # Should mention SCD or history concepts
        assert "history" in history_mode.help_text.lower() or "scd" in history_mode.help_text.lower()

    def test_natural_keys_has_educational_help_text(self) -> None:
        """natural_keys field has help text explaining the concept."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        natural_keys = next(f for f in app.fields if f.name == "natural_keys")
        # Should explain what natural keys are
        assert "unique" in natural_keys.help_text.lower() or "identify" in natural_keys.help_text.lower()

    def test_load_pattern_has_educational_help_text(self) -> None:
        """load_pattern field has help text explaining the patterns."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        load_pattern = next(f for f in app.fields if f.name == "load_pattern")
        # Should explain the pattern options
        assert len(load_pattern.help_text) > 20


class TestBeginnerGuidance:
    """Tests for the beginner-friendly contextual guidance feature."""

    def test_beginner_guidance_for_system_field(self) -> None:
        """System field has beginner guidance."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        guidance = app._get_beginner_guidance("system")
        assert guidance  # Not empty
        assert "source" in guidance.lower() or "data" in guidance.lower()

    def test_beginner_guidance_for_natural_keys(self) -> None:
        """Natural keys field has beginner guidance."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        guidance = app._get_beginner_guidance("natural_keys")
        assert guidance  # Not empty
        assert "unique" in guidance.lower()

    def test_beginner_guidance_for_entity_kind(self) -> None:
        """Entity kind field has beginner guidance explaining state vs event."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        guidance = app._get_beginner_guidance("entity_kind")
        assert guidance  # Not empty
        assert "state" in guidance.lower()
        assert "event" in guidance.lower()

    def test_beginner_guidance_for_history_mode(self) -> None:
        """History mode field has beginner guidance explaining SCD types."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        guidance = app._get_beginner_guidance("history_mode")
        assert guidance  # Not empty
        assert "scd1" in guidance.lower() or "current only" in guidance.lower()
        assert "scd2" in guidance.lower() or "full history" in guidance.lower()

    def test_beginner_guidance_for_pagination(self) -> None:
        """Pagination field has beginner guidance."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        guidance = app._get_beginner_guidance("pagination_strategy")
        assert guidance  # Not empty
        assert "offset" in guidance.lower() or "cursor" in guidance.lower()

    def test_beginner_guidance_for_load_pattern(self) -> None:
        """Load pattern field has beginner guidance."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        guidance = app._get_beginner_guidance("load_pattern")
        assert guidance  # Not empty
        assert "snapshot" in guidance.lower() or "incremental" in guidance.lower()

    def test_no_guidance_for_unknown_field(self) -> None:
        """Unknown field returns empty guidance."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        guidance = app._get_beginner_guidance("nonexistent_field")
        assert guidance == ""

    def test_api_fields_have_guidance(self) -> None:
        """API-related fields have beginner guidance."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        # All API fields should have guidance
        api_fields = ["base_url", "endpoint", "data_path", "cursor_path"]
        for field_name in api_fields:
            guidance = app._get_beginner_guidance(field_name)
            assert guidance, f"Missing guidance for {field_name}"


class TestPaginationVisibility:
    """Tests for pagination field visibility rules."""

    def test_pagination_fields_hidden_for_file_source(self) -> None:
        """Pagination fields are hidden when source is file."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.advanced_mode = True
        app._create_fields()

        # Set source type to file
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "file_csv"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        assert "pagination_strategy" not in visible_names
        assert "cursor_path" not in visible_names
        assert "page_size" not in visible_names

    def test_pagination_fields_visible_for_api_source(self) -> None:
        """Pagination fields are visible when source is API."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.advanced_mode = True
        app._create_fields()

        # Set source type to API
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "api_rest"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        assert "pagination_strategy" in visible_names

    def test_cursor_path_visible_only_for_cursor_pagination(self) -> None:
        """cursor_path field only visible when pagination_strategy is cursor."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.advanced_mode = True
        app._create_fields()

        # Set source type to API and pagination to offset
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "api_rest"
        pagination = next(f for f in app.fields if f.name == "pagination_strategy")
        pagination.buffer.text = "offset"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        # cursor_path should NOT be visible for offset pagination
        assert "cursor_path" not in visible_names

        # Now change to cursor pagination
        pagination.buffer.text = "cursor"
        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        # cursor_path SHOULD be visible for cursor pagination
        assert "cursor_path" in visible_names


class TestInheritance:
    """Tests for parent-child configuration inheritance.

    These tests verify that:
    - extends field is correctly placed in YAML output
    - Parent values are inherited correctly
    - Child overrides work properly
    - Field sources are tracked correctly
    """

    def test_yaml_output_includes_extends(self) -> None:
        """YAML output includes extends field when parent is set."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        # Set parent path
        app.state.extends = "./base.yaml"
        app.state.name = "child_pipeline"
        app.state.set_bronze_value("system", "retail")
        app.state.set_bronze_value("entity", "orders")
        app.state.set_bronze_value("source_type", "file_csv")

        yaml_content = app.state.to_yaml()

        # extends should appear near the top
        assert "extends:" in yaml_content
        assert "./base.yaml" in yaml_content

    def test_clear_parent_removes_extends(self) -> None:
        """Clearing parent config removes extends from state."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set and then clear parent
        app.state.extends = "./base.yaml"
        app._clear_parent_config()

        assert app.state.extends is None
        yaml_content = app.state.to_yaml()
        assert "extends:" not in yaml_content

    def test_editor_title_shows_current_file_only(self) -> None:
        """Editor title shows current file, not parent."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.yaml_path = "./pipelines/my_pipeline.yaml"
        app.state.extends = "./base.yaml"

        title = app._get_editor_title()

        # Should show current file, not parent
        assert "my_pipeline.yaml" in title
        assert "base.yaml" not in title

    def test_yaml_preview_title_shows_parent_info(self) -> None:
        """YAML preview title shows parent info when inheriting."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.state.extends = "./base.yaml"

        title = app._get_yaml_preview_title()

        # Should indicate inheritance
        assert "extends" in title.lower()
        assert "base.yaml" in title

    def test_yaml_preview_title_simple_when_no_parent(self) -> None:
        """YAML preview title is simple when no parent."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.state.extends = None

        title = app._get_yaml_preview_title()

        # Should be simple without extends info
        assert title == "YAML Preview"

    def test_basic_fields_always_visible(self) -> None:
        """Essential fields are visible in basic mode even when empty."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.advanced_mode = False  # Basic mode
        app._create_fields()

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        # Essential bronze fields
        assert "system" in visible_names
        assert "entity" in visible_names
        assert "source_type" in visible_names
        assert "load_pattern" in visible_names

        # Essential silver fields
        assert "natural_keys" in visible_names
        assert "change_timestamp" in visible_names
        assert "entity_kind" in visible_names
        assert "history_mode" in visible_names


class TestDatabaseQueries:
    """Tests for database query field handling."""

    def test_full_query_required_for_database(self) -> None:
        """Full query is required for database sources."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set source type to database
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "database_mssql"
        app._sync_fields_to_state()

        required = get_dynamic_required_fields(app.state)
        assert "query" in required["bronze"]

    def test_incremental_query_visible_for_incremental_load(self) -> None:
        """Incremental query field visible for incremental load patterns."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.advanced_mode = True
        app._create_fields()

        # Set database source and incremental load
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "database_mssql"
        load_pattern = next(f for f in app.fields if f.name == "load_pattern")
        load_pattern.buffer.text = "incremental"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        assert "query" in visible_names
        assert "incremental_query" in visible_names

    def test_incremental_query_hidden_for_full_snapshot(self) -> None:
        """Incremental query hidden for full snapshot load pattern."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.advanced_mode = True
        app._create_fields()

        # Set database source and full snapshot load
        source_type = next(f for f in app.fields if f.name == "source_type")
        source_type.buffer.text = "database_mssql"
        load_pattern = next(f for f in app.fields if f.name == "load_pattern")
        load_pattern.buffer.text = "full_snapshot"

        visible = app._get_visible_fields()
        visible_names = [f.name for f in visible]

        assert "query" in visible_names
        assert "incremental_query" not in visible_names

    def test_query_field_is_multiline(self) -> None:
        """Query field supports multiline editing."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        query_field = next(f for f in app.fields if f.name == "query")
        assert query_field.multiline is True

    def test_incremental_query_field_is_multiline(self) -> None:
        """Incremental query field also supports multiline editing."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        inc_query_field = next(f for f in app.fields if f.name == "incremental_query")
        assert inc_query_field.multiline is True


class TestYAMLPreviewToggle:
    """Tests for YAML preview panel collapsible functionality."""

    def test_yaml_preview_initially_expanded(self) -> None:
        """YAML preview is expanded by default."""
        app = PipelineConfigApp()
        assert app.yaml_preview_collapsed is False

    def test_toggle_yaml_preview_collapses(self) -> None:
        """Toggling YAML preview changes collapsed state."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()

        app._toggle_yaml_preview()

        assert app.yaml_preview_collapsed is True
        assert "hidden" in app.status_message.lower()

    def test_toggle_yaml_preview_expands(self) -> None:
        """Toggling collapsed preview expands it."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app.yaml_preview_collapsed = True

        app._toggle_yaml_preview()

        assert app.yaml_preview_collapsed is False
        assert "shown" in app.status_message.lower()


class TestFieldMultiline:
    """Tests for multiline field handling."""

    def test_field_multiline_default_false(self) -> None:
        """Field multiline defaults to False."""
        field = Field("test", "Test", "bronze")
        assert field.multiline is False

    def test_field_multiline_true_creates_multiline_buffer(self) -> None:
        """Field with multiline=True creates multiline buffer."""
        field = Field("query", "Query", "bronze", multiline=True)
        assert field.multiline is True
        # Buffer.multiline is a filter object in prompt_toolkit, not a boolean
        # Check that the filter evaluates to True
        assert field.buffer.multiline()

    def test_regular_field_has_single_line_buffer(self) -> None:
        """Regular field has single-line buffer."""
        field = Field("system", "System", "bronze")
        # Buffer.multiline is a filter object in prompt_toolkit
        # Check that the filter evaluates to False
        assert not field.buffer.multiline()


class TestFormContentPadding:
    """Tests for form content scroll padding."""

    def test_form_content_has_scroll_padding(self) -> None:
        """Form content includes padding for scrolling."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        content = app._build_form_content()

        # Count empty Window elements at the end (padding)
        # The content should have padding windows added
        assert len(content) > len(app._get_visible_fields())


class TestValidationPanelScaling:
    """Tests for validation panel behavior."""

    def test_validation_errors_list_populated(self) -> None:
        """Validation errors list is populated after _update_validation."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # State has missing required fields, should have errors
        app._update_validation()

        # Should have some validation errors (missing system, entity, etc.)
        assert len(app.validation_errors) > 0

    def test_validation_errors_cleared_when_valid(self) -> None:
        """Validation errors are cleared when state is valid."""
        app = PipelineConfigApp()
        app.state = PipelineState.from_schema_defaults()
        app._create_fields()

        # Set all required fields in the state
        app.state.set_bronze_value("system", "retail")
        app.state.set_bronze_value("entity", "orders")
        app.state.set_bronze_value("source_type", "file_csv")
        app.state.set_bronze_value("source_path", "./data/orders.csv")
        app.state.set_silver_value("natural_keys", ["order_id"])
        app.state.set_silver_value("change_timestamp", "updated_at")

        # Also update the field buffers to match state (app reads from buffers)
        for field in app.fields:
            if field.name == "system":
                field.buffer.text = "retail"
            elif field.name == "entity":
                field.buffer.text = "orders"
            elif field.name == "source_type":
                field.buffer.text = "file_csv"
            elif field.name == "source_path":
                field.buffer.text = "./data/orders.csv"
            elif field.name == "natural_keys":
                field.buffer.text = "order_id"
            elif field.name == "change_timestamp":
                field.buffer.text = "updated_at"

        app._update_validation()

        # Should have no validation errors
        assert len(app.validation_errors) == 0
