"""Field classification and visibility rules.

This module defines which fields are shown in beginner vs advanced mode,
which fields are sensitive (should encourage env var usage), and the
dynamic visibility rules based on other field values.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipelines.tui.models.pipeline_state import PipelineState

# Fields shown in Beginner mode (essential fields only)
BEGINNER_FIELDS: dict[str, list[str]] = {
    "bronze": [
        "system",
        "entity",
        "source_type",
        "source_path",
    ],
    "silver": [
        "natural_keys",
        "change_timestamp",
    ],
}

# All available fields by section (for Advanced mode)
ALL_BRONZE_FIELDS: list[str] = [
    # Required
    "system",
    "entity",
    "source_type",
    # File source
    "source_path",
    # Database source
    "host",
    "database",
    "query",
    "incremental_query",
    "connection",
    # API source
    "base_url",
    "endpoint",
    "data_path",
    "requests_per_second",
    "timeout",
    "max_retries",
    # API auth
    "auth_type",
    "token",
    "api_key",
    "api_key_header",
    "username",
    "password",
    # API pagination
    "pagination_strategy",
    "page_size",
    "offset_param",
    "limit_param",
    "page_param",
    "page_size_param",
    "cursor_param",
    "cursor_path",
    "max_pages",
    "max_records",
    # Load pattern
    "load_pattern",
    "watermark_column",
    "watermark_param",
    "full_refresh_days",
    # Output options
    "target_path",
    "chunk_size",
    "partition_by",
    "write_checksums",
    "write_metadata",
    # Source-specific options
    "csv_delimiter",
    "csv_header",
    "csv_skip_rows",
    "sheet",
    "flatten",
    "widths",
    "columns",
    # Headers and params
    "headers",
    "params",
    "path_params",
]

ALL_SILVER_FIELDS: list[str] = [
    # Required
    "natural_keys",
    "change_timestamp",
    # Entity configuration
    "entity_kind",
    "history_mode",
    # Column selection
    "attributes",
    "exclude_columns",
    # Paths
    "source_path",
    "target_path",
    # Output options
    "partition_by",
    "output_formats",
    "parquet_compression",
    "validate_source",
]

# Fields that contain sensitive data and should encourage ${VAR_NAME} usage
SENSITIVE_FIELDS: set[str] = {
    "password",
    "token",
    "api_key",
    "username",
    "secret_access_key",
}

# Fields that are always required (from schema)
ALWAYS_REQUIRED: dict[str, list[str]] = {
    "bronze": ["system", "entity", "source_type"],
    "silver": ["natural_keys", "change_timestamp"],
}


def _get_auth_visibility(is_api: bool, auth_type: str | None) -> dict[str, bool]:
    """Get visibility for authentication fields.

    Args:
        is_api: Whether source type is API
        auth_type: Current auth type selection

    Returns:
        Dict of auth field names to visibility
    """
    return {
        "auth_type": is_api,
        "token": is_api and auth_type == "bearer",
        "api_key": is_api and auth_type == "api_key",
        "api_key_header": is_api and auth_type == "api_key",
        "username": is_api and auth_type == "basic",
        "password": is_api and auth_type == "basic",
    }


def _get_pagination_visibility(
    is_api: bool, pagination_strategy: str | None
) -> dict[str, bool]:
    """Get visibility for pagination fields.

    Args:
        is_api: Whether source type is API
        pagination_strategy: Current pagination strategy selection

    Returns:
        Dict of pagination field names to visibility
    """
    has_pagination = pagination_strategy and pagination_strategy != "none"
    is_offset = pagination_strategy == "offset"
    is_page = pagination_strategy == "page"
    is_cursor = pagination_strategy == "cursor"

    return {
        "pagination_strategy": is_api,
        "page_size": is_api and has_pagination,
        "max_pages": is_api and has_pagination,
        "max_records": is_api and has_pagination,
        # Offset pagination
        "offset_param": is_api and is_offset,
        "limit_param": is_api and is_offset,
        # Page pagination
        "page_param": is_api and is_page,
        "page_size_param": is_api and is_page,
        # Cursor pagination
        "cursor_param": is_api and is_cursor,
        "cursor_path": is_api and is_cursor,
    }


def get_conditional_visibility(state: "PipelineState") -> dict[str, bool]:
    """Get visibility status for all conditional fields based on current state.

    Returns a dict mapping field names to whether they should be visible.

    Args:
        state: Current pipeline state

    Returns:
        Dict of field_name -> is_visible
    """
    from pipelines.tui.constants import (
        is_api_source,
        is_csv_source,
        is_database_source,
        is_file_source,
        is_json_source,
    )

    visibility: dict[str, bool] = {}

    # Get current bronze values
    source_type = state.get_bronze_value("source_type")
    load_pattern = state.get_bronze_value("load_pattern")
    auth_type = state.get_bronze_value("auth_type")
    pagination_strategy = state.get_bronze_value("pagination_strategy")

    # Source type checks
    is_file = is_file_source(source_type)
    is_database = is_database_source(source_type)
    is_api = is_api_source(source_type)

    # File source fields
    visibility["source_path"] = is_file or not source_type  # Show by default

    # Database source fields
    visibility["host"] = is_database
    visibility["database"] = is_database
    visibility["query"] = is_database
    visibility["connection"] = is_database

    # API source fields
    visibility["base_url"] = is_api
    visibility["endpoint"] = is_api
    visibility["data_path"] = is_api
    visibility["requests_per_second"] = is_api
    visibility["timeout"] = is_api
    visibility["max_retries"] = is_api
    visibility["headers"] = is_api
    visibility["params"] = is_api
    visibility["path_params"] = is_api

    # Add auth and pagination visibility from helpers
    visibility.update(_get_auth_visibility(is_api, auth_type))
    visibility.update(_get_pagination_visibility(is_api, pagination_strategy))

    # Load pattern fields
    is_incremental = load_pattern in ("incremental", "incremental_append", "cdc")
    visibility["watermark_column"] = is_incremental
    visibility["watermark_param"] = is_api and is_incremental
    visibility["full_refresh_days"] = is_incremental

    # CSV-specific options
    visibility["csv_delimiter"] = is_csv_source(source_type)
    visibility["csv_header"] = is_csv_source(source_type)
    visibility["csv_skip_rows"] = is_csv_source(source_type)

    # Excel-specific
    visibility["sheet"] = source_type == "file_excel"

    # JSON-specific
    visibility["flatten"] = is_json_source(source_type)

    # Fixed-width specific
    is_fixed = source_type == "file_fixed_width"
    visibility["widths"] = is_fixed
    visibility["columns"] = is_fixed or source_type == "file_space_delimited"

    return visibility


def _get_auth_required_fields(auth_type: str | None) -> list[str]:
    """Get required fields based on auth type.

    Args:
        auth_type: Current auth type selection

    Returns:
        List of required field names for the auth type
    """
    if auth_type == "bearer":
        return ["token"]
    elif auth_type == "api_key":
        return ["api_key"]
    elif auth_type == "basic":
        return ["username", "password"]
    return []


def _get_pagination_required_fields(pagination_strategy: str | None) -> list[str]:
    """Get required fields based on pagination strategy.

    Args:
        pagination_strategy: Current pagination strategy selection

    Returns:
        List of required field names for the pagination strategy
    """
    if pagination_strategy == "cursor":
        return ["cursor_path"]  # Must know where next cursor is in response
    return []


def get_dynamic_required_fields(state: "PipelineState") -> dict[str, list[str]]:
    """Get required fields based on current state.

    Some fields become required based on other field values:
    - base_url, endpoint required when source_type = api_rest
    - watermark_column required when load_pattern = incremental
    - token required when auth_type = bearer
    - etc.

    Args:
        state: Current pipeline state

    Returns:
        Dict with "bronze" and "silver" keys, each containing list of required field names
    """
    from pipelines.tui.constants import is_api_source, is_database_source, is_file_source

    bronze_required = list(ALWAYS_REQUIRED["bronze"])
    silver_required = list(ALWAYS_REQUIRED["silver"])

    # Get current values
    source_type = state.get_bronze_value("source_type")
    load_pattern = state.get_bronze_value("load_pattern")
    auth_type = state.get_bronze_value("auth_type")
    pagination_strategy = state.get_bronze_value("pagination_strategy")

    # API source requirements
    if is_api_source(source_type):
        bronze_required.extend(["base_url", "endpoint"])

    # Database source requirements
    if is_database_source(source_type):
        bronze_required.extend(["host", "database", "query"])

    # File source requirements
    if is_file_source(source_type):
        bronze_required.append("source_path")

    # Incremental/CDC load requirements
    if load_pattern in ("incremental", "incremental_append", "cdc"):
        bronze_required.append("watermark_column")

    # Add auth and pagination requirements from helpers
    bronze_required.extend(_get_auth_required_fields(auth_type))
    bronze_required.extend(_get_pagination_required_fields(pagination_strategy))

    return {
        "bronze": bronze_required,
        "silver": silver_required,
    }


def is_field_sensitive(field_name: str) -> bool:
    """Check if a field contains sensitive data."""
    return field_name in SENSITIVE_FIELDS


def get_visible_fields(
    section: str,
    view_mode: str,
    state: "PipelineState",
) -> list[str]:
    """Get list of fields that should be visible in the current mode.

    Args:
        section: "bronze" or "silver"
        view_mode: "beginner" or "advanced"
        state: Current pipeline state for conditional visibility

    Returns:
        List of field names that should be shown
    """
    if view_mode == "beginner":
        base_fields = BEGINNER_FIELDS.get(section, [])
    else:
        base_fields = ALL_BRONZE_FIELDS if section == "bronze" else ALL_SILVER_FIELDS

    # Apply conditional visibility
    visibility = get_conditional_visibility(state)

    visible = []
    for field_name in base_fields:
        # If field has a visibility rule, use it; otherwise show it
        if field_name in visibility:
            if visibility[field_name]:
                visible.append(field_name)
        else:
            visible.append(field_name)

    return visible
