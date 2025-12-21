"""Shared constants for TUI modules.

Centralizes mappings and constants used across multiple TUI files.
"""

from __future__ import annotations

# Mapping of flat field names to their nested schema locations
# Format: "flat_name": ("parent_key", "child_key")
NESTED_FIELD_MAPPINGS: dict[str, tuple[str, str]] = {
    # Auth fields (auth.*)
    "auth_type": ("auth", "auth_type"),
    "token": ("auth", "token"),
    "api_key": ("auth", "api_key"),
    "api_key_header": ("auth", "api_key_header"),
    "username": ("auth", "username"),
    "password": ("auth", "password"),
    # Pagination fields (pagination.*)
    "pagination_strategy": ("pagination", "strategy"),
    "page_size": ("pagination", "page_size"),
    "offset_param": ("pagination", "offset_param"),
    "limit_param": ("pagination", "limit_param"),
    "page_param": ("pagination", "page_param"),
    "page_size_param": ("pagination", "page_size_param"),
    "cursor_param": ("pagination", "cursor_param"),
    "cursor_path": ("pagination", "cursor_path"),
    "max_pages": ("pagination", "max_pages"),
    "max_records": ("pagination", "max_records"),
}

# Auth fields for quick lookup
AUTH_FIELDS = frozenset(
    field for field, (parent, _) in NESTED_FIELD_MAPPINGS.items() if parent == "auth"
)

# Pagination fields for quick lookup
PAGINATION_FIELDS = frozenset(
    field for field, (parent, _) in NESTED_FIELD_MAPPINGS.items() if parent == "pagination"
)


# =============================================================================
# Source Type Helpers
# =============================================================================

def is_file_source(source_type: str | None) -> bool:
    """Check if source type is a file-based source."""
    return bool(source_type and source_type.startswith("file_"))


def is_database_source(source_type: str | None) -> bool:
    """Check if source type is a database source."""
    return bool(source_type and source_type.startswith("database_"))


def is_api_source(source_type: str | None) -> bool:
    """Check if source type is a REST API source."""
    return source_type == "api_rest"


def is_csv_source(source_type: str | None) -> bool:
    """Check if source type is CSV or space-delimited."""
    return source_type in ("file_csv", "file_space_delimited")


def is_json_source(source_type: str | None) -> bool:
    """Check if source type is JSON or JSONL."""
    return source_type in ("file_json", "file_jsonl")
