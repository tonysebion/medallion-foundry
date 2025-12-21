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


# Extended help for beginners - supplements schema descriptions
# Explains WHY a choice matters and reassures about flexibility
BEGINNER_GUIDANCE: dict[str, str] = {
    # Bronze essential fields
    "system": (
        "Tip: This is just a label to group related data sources. "
        "Use your company name, department, or data source name."
    ),
    "entity": (
        "Tip: Think of this as the table name in your data warehouse. "
        "Use singular nouns like 'customer', 'order', 'transaction'."
    ),
    "source_type": (
        "Pick where your data comes from. "
        "You can always create a new pipeline later for different sources."
    ),
    "source_path": (
        "Use {run_date} placeholder for daily files (e.g., data_{run_date}.csv). "
        "Supports local paths, S3 (s3://bucket/path), and Azure (abfss://)."
    ),
    # Silver essential fields
    "natural_keys": (
        "Important: Choose columns that uniquely identify each record. "
        "Like a primary key. Multiple columns OK (e.g., order_id + line_number)."
    ),
    "change_timestamp": (
        "Important: Pick the column that shows when records were last updated. "
        "This enables incremental processing and change detection."
    ),
    # Advanced fields - reassure about defaults
    "entity_kind": (
        "State = slowly changing data (customers, products). "
        "Event = immutable facts (orders, clicks). "
        "Default is fine for most cases; you can change this later."
    ),
    "history_mode": (
        "SCD Type 1 = keep only latest (simpler, uses less storage). "
        "SCD Type 2 = keep all history (enables time-travel queries). "
        "Start with Type 1; you can migrate to Type 2 later if needed."
    ),
    "load_pattern": (
        "Full = replace everything each run (simplest, always correct). "
        "Incremental = only load new/changed records (faster for large datasets). "
        "Start with Full unless your data is very large."
    ),
    "parquet_compression": (
        "ZStandard is recommended (best balance of speed and size). "
        "This is a storage optimization - you can change it anytime without losing data."
    ),
    "partition_by": (
        "Partitioning improves query speed for very large tables. "
        "Common choices: date columns for time-series, region for geo data. "
        "Skip for small tables (<1GB); you can add partitioning later."
    ),
    # Sensitive fields
    "password": (
        "Security: Use ${PASSWORD} syntax to reference an environment variable. "
        "Never put actual passwords in config files."
    ),
    "token": (
        "Security: Use ${API_TOKEN} syntax to reference an environment variable. "
        "Store tokens in .env files, not in config files."
    ),
    "api_key": (
        "Security: Use ${API_KEY} syntax to reference an environment variable. "
        "Store keys in .env files, not in config files."
    ),
    # API fields
    "base_url": (
        "The root URL of the API (e.g., https://api.example.com). "
        "Don't include endpoints - those go in the 'endpoint' field."
    ),
    "endpoint": (
        "The specific API path to call (e.g., /v1/customers). "
        "Always starts with a forward slash."
    ),
    "pagination_strategy": (
        "How the API returns multiple pages of results. "
        "Check the API docs - most modern APIs use cursor pagination."
    ),
    "requests_per_second": (
        "Rate limiting protects against API throttling. "
        "Check the API's rate limits; 10 req/s is a safe default."
    ),
}


def get_field_help_text(section: str, field: str) -> str:
    """Get formatted help text for a field.

    Combines description, examples, default, and beginner guidance.
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

    # Add beginner guidance if available
    if field in BEGINNER_GUIDANCE:
        parts.append("")  # Blank line separator
        parts.append(BEGINNER_GUIDANCE[field])

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


def get_schema_default(section: str, field: str) -> Any:
    """Get the default value for a field from the schema.

    Args:
        section: "bronze" or "silver"
        field: Field name

    Returns:
        Default value from schema, or None if not specified
    """
    metadata = get_field_metadata(section, field)
    return metadata.get("default")


# Centralized schema defaults for fields that have them
# These are the defaults used in yaml_generator to skip outputting redundant values
SCHEMA_DEFAULTS: dict[str, dict[str, Any]] = {
    "bronze": {
        "load_pattern": "full_snapshot",
    },
    "silver": {
        "parquet_compression": "snappy",
        "validate_source": "skip",
        "entity_kind": "state",
        "history_mode": "current_only",
    },
}


# Suggested placeholder values for empty fields
# These help users understand the expected format and encourage best practices
FIELD_PLACEHOLDERS: dict[str, str] = {
    # URL fields - show expected format
    "base_url": "https://api.example.com",
    "endpoint": "/v1/",
    # Path fields - show common patterns
    "source_path": "./data/{run_date}/",
    "target_path": "./output/",
    # Sensitive fields - encourage env var usage
    "password": "${DB_PASSWORD}",
    "token": "${API_TOKEN}",
    "api_key": "${API_KEY}",
    "username": "${DB_USERNAME}",
    # Identifier fields - show naming convention
    "system": "my_system",
    "entity": "my_entity",
    # API fields
    "api_key_header": "X-API-Key",
    "data_path": "data.items",
    # Pagination
    "offset_param": "offset",
    "limit_param": "limit",
    "page_param": "page",
    "page_size_param": "page_size",
    "cursor_param": "cursor",
    "cursor_path": "meta.next_cursor",
    # Database
    "host": "${DB_HOST}",
    "database": "my_database",
    # Numeric defaults
    "requests_per_second": "10.0",
    "timeout": "30",
    "page_size": "100",
    "chunk_size": "10000",
    # Keys and timestamps - common column names
    "natural_keys": "id",
    "change_timestamp": "updated_at",
    "watermark_column": "updated_at",
}


def get_field_placeholder(field: str) -> str | None:
    """Get a suggested placeholder value for a field.

    These are pre-filled values that help users understand the expected
    format. They encourage best practices (like env vars for secrets)
    and show common naming conventions.

    Args:
        field: Field name

    Returns:
        Placeholder string or None if no placeholder defined
    """
    return FIELD_PLACEHOLDERS.get(field)
