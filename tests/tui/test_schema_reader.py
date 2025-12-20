"""Tests for schema_reader utility functions."""

from __future__ import annotations

import pytest

from pipelines.tui.utils.schema_reader import (
    get_all_bronze_fields,
    get_all_silver_fields,
    get_api_auth_fields,
    get_api_pagination_fields,
    get_enum_options,
    get_field_help_text,
    get_field_metadata,
    get_nested_field_parent,
    get_required_fields,
    is_nested_field,
    validate_field_type,
)


class TestGetFieldMetadata:
    """Tests for get_field_metadata function."""

    def test_bronze_required_field(self) -> None:
        """Required bronze fields have correct metadata."""
        meta = get_field_metadata("bronze", "system")

        assert meta["required"] is True
        assert meta["description"]  # Has a description

    def test_bronze_enum_field(self) -> None:
        """Enum fields return enum values."""
        meta = get_field_metadata("bronze", "source_type")

        assert meta["enum"]
        assert "file_csv" in meta["enum"]
        assert "database_mssql" in meta["enum"]
        assert "api_rest" in meta["enum"]

    def test_bronze_optional_field(self) -> None:
        """Optional fields are not marked required."""
        meta = get_field_metadata("bronze", "chunk_size")

        assert meta["required"] is False
        assert meta["type"] == "integer"

    def test_silver_required_field(self) -> None:
        """Required silver fields have correct metadata."""
        meta = get_field_metadata("silver", "natural_keys")

        assert meta["required"] is True

    def test_nested_auth_field(self) -> None:
        """Nested auth fields return correct metadata."""
        meta = get_field_metadata("bronze", "auth_type")

        assert "enum" in meta
        assert "bearer" in meta.get("enum", [])
        assert "api_key" in meta.get("enum", [])

    def test_nested_pagination_field(self) -> None:
        """Nested pagination fields return correct metadata."""
        meta = get_field_metadata("bronze", "pagination_strategy")

        assert "enum" in meta
        assert "offset" in meta.get("enum", [])
        assert "cursor" in meta.get("enum", [])

    def test_nonexistent_field(self) -> None:
        """Nonexistent fields return empty metadata."""
        meta = get_field_metadata("bronze", "nonexistent_field")

        assert meta["description"] == ""
        assert meta["required"] is False


class TestGetEnumOptions:
    """Tests for get_enum_options function."""

    def test_source_type_options(self) -> None:
        """Source type has human-readable labels."""
        options = get_enum_options("bronze", "source_type")

        # Should be list of (value, label) tuples
        assert len(options) > 0
        assert all(len(opt) == 2 for opt in options)

        # Find specific option
        csv_opt = next((opt for opt in options if opt[0] == "file_csv"), None)
        assert csv_opt is not None
        assert csv_opt[1] == "CSV File"

    def test_load_pattern_options(self) -> None:
        """Load pattern has descriptive labels."""
        options = get_enum_options("bronze", "load_pattern")

        incremental = next((opt for opt in options if opt[0] == "incremental"), None)
        assert incremental is not None
        assert "watermark" in incremental[1].lower()

    def test_history_mode_options(self) -> None:
        """History mode includes SCD references."""
        options = get_enum_options("silver", "history_mode")

        scd1 = next((opt for opt in options if "current" in opt[0]), None)
        assert scd1 is not None
        assert "SCD" in scd1[1]


class TestValidateFieldType:
    """Tests for validate_field_type function."""

    def test_required_field_empty(self) -> None:
        """Empty required field returns error."""
        error = validate_field_type("bronze", "system", "")

        assert error is not None
        assert "required" in error.lower()

    def test_required_field_valid(self) -> None:
        """Valid required field returns None."""
        error = validate_field_type("bronze", "system", "retail")

        assert error is None

    def test_integer_field_valid(self) -> None:
        """Valid integer returns None."""
        error = validate_field_type("bronze", "chunk_size", "100000")

        assert error is None

    def test_integer_field_invalid(self) -> None:
        """Invalid integer returns error."""
        error = validate_field_type("bronze", "chunk_size", "not_a_number")

        assert error is not None
        assert "integer" in error.lower()

    def test_integer_field_below_minimum(self) -> None:
        """Integer below minimum returns error."""
        error = validate_field_type("bronze", "chunk_size", "100")

        assert error is not None
        assert "1000" in error  # minimum value

    def test_number_field_valid(self) -> None:
        """Valid number returns None."""
        error = validate_field_type("bronze", "requests_per_second", "5.0")

        assert error is None

    def test_enum_field_valid(self) -> None:
        """Valid enum value returns None."""
        error = validate_field_type("bronze", "source_type", "file_csv")

        assert error is None

    def test_enum_field_invalid(self) -> None:
        """Invalid enum value returns error."""
        error = validate_field_type("bronze", "source_type", "invalid_type")

        assert error is not None
        assert "must be one of" in error.lower()


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_get_all_bronze_fields(self) -> None:
        """Returns all bronze field names."""
        fields = get_all_bronze_fields()

        assert "system" in fields
        assert "entity" in fields
        assert "source_type" in fields
        assert "base_url" in fields  # API field

    def test_get_all_silver_fields(self) -> None:
        """Returns all silver field names."""
        fields = get_all_silver_fields()

        assert "natural_keys" in fields
        assert "change_timestamp" in fields
        assert "history_mode" in fields

    def test_get_required_fields_bronze(self) -> None:
        """Returns required bronze fields."""
        required = get_required_fields("bronze")

        assert "system" in required
        assert "entity" in required
        assert "source_type" in required

    def test_get_required_fields_silver(self) -> None:
        """Returns required silver fields."""
        required = get_required_fields("silver")

        assert "natural_keys" in required
        assert "change_timestamp" in required

    def test_get_field_help_text(self) -> None:
        """Returns formatted help text."""
        help_text = get_field_help_text("bronze", "source_type")

        assert help_text  # Not empty
        assert "source" in help_text.lower()

    def test_get_api_auth_fields(self) -> None:
        """Returns auth field names."""
        fields = get_api_auth_fields()

        assert "auth_type" in fields
        assert "token" in fields
        assert "api_key" in fields
        assert "username" in fields
        assert "password" in fields

    def test_get_api_pagination_fields(self) -> None:
        """Returns pagination field names."""
        fields = get_api_pagination_fields()

        assert "pagination_strategy" in fields
        assert "page_size" in fields
        assert "cursor_param" in fields

    def test_is_nested_field_auth(self) -> None:
        """Auth fields are recognized as nested."""
        assert is_nested_field("token") is True
        assert is_nested_field("api_key") is True
        assert is_nested_field("auth_type") is True

    def test_is_nested_field_pagination(self) -> None:
        """Pagination fields are recognized as nested."""
        assert is_nested_field("page_size") is True
        assert is_nested_field("cursor_path") is True

    def test_is_nested_field_top_level(self) -> None:
        """Top-level fields are not nested."""
        assert is_nested_field("system") is False
        assert is_nested_field("base_url") is False

    def test_get_nested_field_parent_auth(self) -> None:
        """Auth fields have 'auth' parent."""
        assert get_nested_field_parent("token") == "auth"
        assert get_nested_field_parent("api_key") == "auth"

    def test_get_nested_field_parent_pagination(self) -> None:
        """Pagination fields have 'pagination' parent."""
        assert get_nested_field_parent("page_size") == "pagination"
        assert get_nested_field_parent("cursor_param") == "pagination"

    def test_get_nested_field_parent_none(self) -> None:
        """Top-level fields return None."""
        assert get_nested_field_parent("system") is None
        assert get_nested_field_parent("base_url") is None
