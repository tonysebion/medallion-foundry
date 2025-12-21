"""Generate YAML configuration from form data.

Converts the TUI form dictionary into properly formatted YAML output
with comments, schema reference, and consistent structure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class OrphanedField:
    """A field that has a value but is not currently active/visible."""

    name: str
    value: Any
    section: str  # "bronze" or "silver"
    reason: str  # Why this field is orphaned (e.g., "not used with offset pagination")


def generate_yaml(
    config: dict[str, Any],
    *,
    include_schema_ref: bool = True,
    include_comments: bool = True,
    parent_path: str | None = None,
    orphaned_fields: list[OrphanedField] | None = None,
) -> str:
    """Generate YAML configuration string from form data.

    Args:
        config: Dictionary with bronze/silver configuration
        include_schema_ref: Include yaml-language-server schema reference
        include_comments: Include helpful comments
        parent_path: If set, add extends: directive for inheritance
        orphaned_fields: Fields that have values but aren't currently active

    Returns:
        Formatted YAML string
    """
    lines: list[str] = []

    # Schema reference for IDE support
    if include_schema_ref:
        lines.append("# yaml-language-server: $schema=../schema/pipeline.schema.json")

    # Header comments with run commands
    if include_comments:
        name = config.get("name", "pipeline")
        lines.append("#")
        lines.append(f"# Pipeline: {name}")
        if config.get("description"):
            lines.append(f"# {config['description']}")
        lines.append("#")
        lines.append("# Run commands:")
        lines.append(f"#   python -m pipelines ./{name}.yaml --date 2025-01-15           # Run both layers")
        lines.append(f"#   python -m pipelines ./{name}.yaml:bronze --date 2025-01-15    # Bronze only")
        lines.append(f"#   python -m pipelines ./{name}.yaml:silver --date 2025-01-15    # Silver only")
        lines.append(f"#   python -m pipelines ./{name}.yaml --dry-run                   # Validate only")
        lines.append("#")
        lines.append("")

    # Parent inheritance
    if parent_path:
        lines.append(f"extends: {parent_path}")
        lines.append("")

    # Name and description
    if config.get("name"):
        lines.append(f"name: {config['name']}")
    if config.get("description"):
        lines.append(f"description: {config['description']}")
    if config.get("name") or config.get("description"):
        lines.append("")

    # Bronze section
    if "bronze" in config:
        if include_comments:
            lines.append("# Bronze layer - raw data extraction")
        lines.append("bronze:")
        lines.extend(_format_bronze(config["bronze"]))

        # Add orphaned bronze fields as comments
        bronze_orphans = [o for o in (orphaned_fields or []) if o.section == "bronze"]
        if bronze_orphans:
            lines.append("")
            lines.append("  # Unused fields (preserved for reference):")
            for orphan in bronze_orphans:
                value_str = _format_value_for_comment(orphan.value)
                lines.append(f"  # {orphan.name}: {value_str}  # ({orphan.reason})")

        lines.append("")

    # Silver section
    if "silver" in config:
        if include_comments:
            lines.append("# Silver layer - data curation")
        lines.append("silver:")
        lines.extend(_format_silver(config["silver"]))

        # Add orphaned silver fields as comments
        silver_orphans = [o for o in (orphaned_fields or []) if o.section == "silver"]
        if silver_orphans:
            lines.append("")
            lines.append("  # Unused fields (preserved for reference):")
            for orphan in silver_orphans:
                value_str = _format_value_for_comment(orphan.value)
                lines.append(f"  # {orphan.name}: {value_str}  # ({orphan.reason})")

        lines.append("")

    return "\n".join(lines)


def _format_value_for_comment(value: Any) -> str:
    """Format a value for inclusion in a YAML comment."""
    if isinstance(value, list):
        return "[" + ", ".join(str(v) for v in value) + "]"
    if isinstance(value, str) and ("\n" in value or len(value) > 50):
        return f'"{value[:47]}..."'  # Truncate long strings
    return str(value)


def _format_bronze(bronze: dict[str, Any]) -> list[str]:
    """Format bronze section fields."""
    from pipelines.tui.utils.schema_reader import SCHEMA_DEFAULTS

    bronze_defaults = SCHEMA_DEFAULTS.get("bronze", {})

    # Field definitions: (key, format_type)
    # format_type: "simple", "path" (needs quoting), "multiline", "skip_default"
    BRONZE_FIELDS = [
        # Required fields first
        ("system", "simple"),
        ("entity", "simple"),
        ("source_type", "simple"),
        # Paths
        ("source_path", "path"),
        ("target_path", "path"),
        # Database
        ("host", "simple"),
        ("database", "simple"),
        ("query", "multiline"),
        # Load pattern
        ("load_pattern", "skip_default"),
        # Incremental
        ("watermark_column", "simple"),
        ("full_refresh_days", "simple"),
        # Options
        ("chunk_size", "simple"),
    ]

    lines: list[str] = []
    for key, fmt_type in BRONZE_FIELDS:
        value = bronze.get(key)
        if not value:
            continue
        if fmt_type == "skip_default" and value == bronze_defaults.get(key):
            continue

        if fmt_type == "multiline":
            lines.append(f"  {key}: |")
            for line in str(value).split("\n"):
                lines.append(f"    {line}")
        elif fmt_type == "path" and _needs_quoting(str(value)):
            lines.append(f'  {key}: "{value}"')
        else:
            lines.append(f"  {key}: {value}")

    # Options dict handled separately
    if bronze.get("options"):
        lines.extend(_format_options(bronze["options"]))

    return lines


def _format_silver(silver: dict[str, Any]) -> list[str]:
    """Format silver section fields."""
    from pipelines.tui.utils.schema_reader import SCHEMA_DEFAULTS

    silver_defaults = SCHEMA_DEFAULTS.get("silver", {})

    # Field definitions: (key, format_type)
    # format_type: "simple", "path", "list", "keys_list", "skip_default"
    SILVER_FIELDS = [
        # Required
        ("natural_keys", "keys_list"),
        ("change_timestamp", "simple"),
        # Paths
        ("source_path", "path"),
        ("target_path", "path"),
        # Entity config
        ("entity_kind", "skip_default"),
        ("history_mode", "skip_default"),
        # Column selection
        ("attributes", "list"),
        ("exclude_columns", "list"),
        ("partition_by", "list"),
        # Output options
        ("parquet_compression", "skip_default"),
        ("validate_source", "skip_default"),
    ]

    lines: list[str] = []
    for key, fmt_type in SILVER_FIELDS:
        value = silver.get(key)
        if not value:
            continue
        if fmt_type == "skip_default" and value == silver_defaults.get(key):
            continue

        if fmt_type == "keys_list":
            # Special handling for natural_keys (single-line for 1 item)
            keys = value if isinstance(value, list) else [value]
            if len(keys) == 1:
                lines.append(f"  {key}: [{keys[0]}]")
            else:
                lines.append(f"  {key}:")
                for k in keys:
                    lines.append(f"    - {k}")
        elif fmt_type == "list":
            items = value if isinstance(value, list) else [value]
            lines.append(f"  {key}:")
            for item in items:
                lines.append(f"    - {item}")
        elif fmt_type == "path" and _needs_quoting(str(value)):
            lines.append(f'  {key}: "{value}"')
        else:
            lines.append(f"  {key}: {value}")

    return lines


def _format_options(options: dict[str, Any], indent: int = 2) -> list[str]:
    """Format the options nested dictionary."""
    lines: list[str] = []
    prefix = "  " * indent

    lines.append(f"{prefix}options:")

    for key, value in options.items():
        if isinstance(value, dict):
            lines.append(f"{prefix}  {key}:")
            for k, v in value.items():
                if isinstance(v, list):
                    lines.append(f"{prefix}    {k}:")
                    for item in v:
                        lines.append(f"{prefix}      - {item}")
                else:
                    lines.append(f"{prefix}    {k}: {v}")
        elif isinstance(value, list):
            lines.append(f"{prefix}  {key}:")
            for item in value:
                lines.append(f"{prefix}    - {item}")
        else:
            lines.append(f"{prefix}  {key}: {value}")

    return lines


def _needs_quoting(value: str) -> bool:
    """Check if a string value needs to be quoted in YAML."""
    special_chars = ["{", "}", "[", "]", "*", ":", "#", "&", "!", "|", ">", "'", '"']
    return any(c in value for c in special_chars) or value.startswith("./")


def form_to_config(form_data: dict[str, Any]) -> dict[str, Any]:
    """Convert flat form data to nested config structure.

    The TUI form may have flat keys like 'bronze_system', 'silver_natural_keys'.
    This converts them to the nested structure expected by generate_yaml.
    """
    config: dict[str, Any] = {}

    # Top-level fields
    if "name" in form_data:
        config["name"] = form_data["name"]
    if "description" in form_data:
        config["description"] = form_data["description"]

    # Bronze section
    bronze: dict[str, Any] = {}
    bronze_keys = [
        "system",
        "entity",
        "source_type",
        "source_path",
        "target_path",
        "host",
        "database",
        "query",
        "load_pattern",
        "watermark_column",
        "full_refresh_days",
        "chunk_size",
    ]
    for key in bronze_keys:
        form_key = f"bronze_{key}"
        if form_key in form_data and form_data[form_key]:
            bronze[key] = form_data[form_key]
        elif key in form_data and form_data[key]:
            bronze[key] = form_data[key]

    # Bronze options
    if "bronze_options" in form_data:
        bronze["options"] = form_data["bronze_options"]

    if bronze:
        config["bronze"] = bronze

    # Silver section
    silver: dict[str, Any] = {}
    silver_keys = [
        "natural_keys",
        "change_timestamp",
        "source_path",
        "target_path",
        "entity_kind",
        "history_mode",
        "attributes",
        "exclude_columns",
        "partition_by",
        "parquet_compression",
        "validate_source",
    ]
    for key in silver_keys:
        form_key = f"silver_{key}"
        if form_key in form_data and form_data[form_key]:
            silver[key] = form_data[form_key]
        elif key in form_data and form_data[key]:
            silver[key] = form_data[key]

    if silver:
        config["silver"] = silver

    return config
