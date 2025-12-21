"""Extract metadata from JSON Schema for TUI help and validation.

Reads the pipeline.schema.json file to provide:
- Field descriptions for help panels
- Enum values for dropdowns
- Default values for form fields
- Examples for placeholder text
- Nested field support (auth.*, pagination.*)
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from pipelines.tui.constants import NESTED_FIELD_MAPPINGS

# Path to the JSON Schema file
SCHEMA_PATH = Path(__file__).parent.parent.parent / "schema" / "pipeline.schema.json"


@lru_cache(maxsize=1)
def _load_schema() -> dict[str, Any]:
    """Load and cache the JSON Schema."""
    if not SCHEMA_PATH.exists():
        return {}
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        return json.load(f)


def get_field_metadata(section: str, field: str) -> dict[str, Any]:
    """Get metadata for a specific field.

    Supports both top-level fields and nested fields (auth.*, pagination.*).

    Args:
        section: "bronze" or "silver"
        field: Field name (e.g., "source_type", "auth_type", "pagination_strategy")

    Returns:
        Dict with keys: description, examples, enum, default, type, required
    """
    schema = _load_schema()
    definitions = schema.get("definitions", {})
    section_schema = definitions.get(section, {})
    properties = section_schema.get("properties", {})

    # Check if this is a nested field
    if field in NESTED_FIELD_MAPPINGS:
        parent_key, child_key = NESTED_FIELD_MAPPINGS[field]
        parent_schema = properties.get(parent_key, {})
        nested_properties = parent_schema.get("properties", {})
        field_schema = nested_properties.get(child_key, {})
    else:
        field_schema = properties.get(field, {})

    # Handle oneOf (like natural_keys which can be string or array)
    if "oneOf" in field_schema:
        # Combine info from all variants
        combined_type = [v.get("type") for v in field_schema["oneOf"] if v.get("type")]
        field_schema = {
            **field_schema,
            "type": combined_type,
        }

    required_fields = section_schema.get("required", [])

    return {
        "description": field_schema.get("description", ""),
        "examples": field_schema.get("examples", []),
        "enum": field_schema.get("enum", []),
        "default": field_schema.get("default"),
        "type": field_schema.get("type"),
        "required": field in required_fields,
        "minimum": field_schema.get("minimum"),
        "items": field_schema.get("items", {}),
    }


def get_enum_options(section: str, field: str) -> list[tuple[str, str]]:
    """Get enum options as (value, display_label) tuples.

    Args:
        section: "bronze" or "silver"
        field: Field name with enum constraint

    Returns:
        List of (value, human_readable_label) tuples
    """
    metadata = get_field_metadata(section, field)
    enum_values = metadata.get("enum", [])

    # Create human-readable labels
    labels = {
        # Source types
        "file_csv": "CSV File",
        "file_parquet": "Parquet File",
        "file_space_delimited": "Space-Delimited File",
        "file_fixed_width": "Fixed-Width File",
        "file_json": "JSON File",
        "file_jsonl": "JSON Lines File",
        "file_excel": "Excel File",
        "database_mssql": "SQL Server Database",
        "database_postgres": "PostgreSQL Database",
        "database_mysql": "MySQL Database",
        "database_db2": "DB2 Database",
        "api_rest": "REST API",
        # Load patterns
        "full_snapshot": "Full Snapshot (replace all each run)",
        "incremental": "Incremental (new records via watermark)",
        "incremental_append": "Incremental Append",
        "cdc": "CDC (change data capture)",
        # Entity kinds
        "state": "State (dimension - slowly changing)",
        "event": "Event (fact - immutable)",
        # History modes
        "current_only": "Current Only (SCD Type 1)",
        "scd1": "SCD Type 1 (keep latest)",
        "full_history": "Full History (SCD Type 2)",
        "scd2": "SCD Type 2 (keep all versions)",
        # Compression
        "snappy": "Snappy (fast, moderate compression)",
        "gzip": "GZip (slow, high compression)",
        "zstd": "ZStandard (balanced)",
        "lz4": "LZ4 (fastest, low compression)",
        "none": "None (no compression)",
        # Validation
        "skip": "Skip (no validation)",
        "warn": "Warn (log warning, continue)",
        "strict": "Strict (fail on error)",
    }

    return [(v, labels.get(v, v.replace("_", " ").title())) for v in enum_values]


def get_all_bronze_fields() -> list[str]:
    """Get all field names in the bronze section."""
    schema = _load_schema()
    bronze_schema = schema.get("definitions", {}).get("bronze", {})
    return list(bronze_schema.get("properties", {}).keys())


def get_all_silver_fields() -> list[str]:
    """Get all field names in the silver section."""
    schema = _load_schema()
    silver_schema = schema.get("definitions", {}).get("silver", {})
    return list(silver_schema.get("properties", {}).keys())


def get_required_fields(section: str) -> list[str]:
    """Get required field names for a section."""
    schema = _load_schema()
    section_schema = schema.get("definitions", {}).get(section, {})
    return section_schema.get("required", [])


def get_field_help_text(section: str, field: str) -> str:
    """Get formatted help text for a field.

    Combines description, examples, and default into readable help.
    """
    metadata = get_field_metadata(section, field)

    parts = []

    if metadata["description"]:
        parts.append(metadata["description"])

    if metadata["examples"]:
        examples = metadata["examples"]
        if len(examples) == 1:
            parts.append(f"Example: {examples[0]}")
        else:
            parts.append(f"Examples: {', '.join(str(e) for e in examples[:3])}")

    if metadata["default"] is not None:
        parts.append(f"Default: {metadata['default']}")

    if metadata["required"]:
        parts.append("(Required)")

    return "\n".join(parts)


def validate_field_type(section: str, field: str, value: Any) -> str | None:
    """Basic type validation for a field value.

    Returns error message if invalid, None if valid.
    """
    import re
    from urllib.parse import urlparse

    if value is None or value == "":
        metadata = get_field_metadata(section, field)
        if metadata["required"]:
            return f"{field} is required"
        return None

    metadata = get_field_metadata(section, field)
    field_type = metadata.get("type")

    if field_type == "integer":
        try:
            int_val = int(value)
            minimum = metadata.get("minimum")
            if minimum is not None and int_val < minimum:
                return f"{field} must be at least {minimum}"
        except (ValueError, TypeError):
            return f"{field} must be an integer"

    if field_type == "number":
        try:
            float_val = float(value)
            minimum = metadata.get("minimum")
            if minimum is not None and float_val < minimum:
                return f"{field} must be at least {minimum}"
        except (ValueError, TypeError):
            return f"{field} must be a number"

    if field_type == "boolean":
        if not isinstance(value, bool) and value not in ("true", "false", "True", "False"):
            return f"{field} must be true or false"

    if isinstance(field_type, list):
        # Multiple allowed types (e.g., string or array)
        pass  # Accept any for now

    enum_values = metadata.get("enum", [])
    if enum_values and value not in enum_values:
        return f"{field} must be one of: {', '.join(enum_values)}"

    # Field-specific validation
    if field == "base_url" and isinstance(value, str) and value:
        # Allow env var references
        if not value.startswith("${"):
            parsed = urlparse(value)
            if not parsed.scheme or not parsed.netloc:
                return f"{field} must be a valid URL (e.g., https://api.example.com)"

    if field == "endpoint" and isinstance(value, str) and value:
        # Endpoint should start with /
        if not value.startswith("/"):
            return f"{field} should start with / (e.g., /v1/customers)"

    if field == "source_path" and isinstance(value, str) and value:
        # Allow env var references and {run_date} placeholders
        if not value.startswith("${"):
            # Basic path validation - should have some path structure
            if not ("/" in value or "\\" in value or value.startswith("s3://") or value.startswith("gs://")):
                if "." not in value:  # Simple filename should have extension
                    return f"{field} should be a valid file path or S3/GCS URL"

    if field in ("system", "entity") and isinstance(value, str) and value:
        # These should be valid identifiers (alphanumeric + underscores)
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', value):
            return f"{field} should be alphanumeric with underscores (e.g., my_system)"

    return None


def get_api_auth_fields() -> list[str]:
    """Get all authentication field names."""
    return [
        "auth_type",
        "token",
        "api_key",
        "api_key_header",
        "username",
        "password",
    ]


def get_api_pagination_fields() -> list[str]:
    """Get all pagination field names."""
    return [
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
    ]


def get_nested_field_parent(field: str) -> str | None:
    """Get the parent object key for a nested field.

    Args:
        field: Field name (e.g., "token", "page_size")

    Returns:
        Parent key ("auth" or "pagination") or None if not nested
    """
    if field in NESTED_FIELD_MAPPINGS:
        return NESTED_FIELD_MAPPINGS[field][0]
    return None


def is_nested_field(field: str) -> bool:
    """Check if a field is nested under auth or pagination."""
    return field in NESTED_FIELD_MAPPINGS
