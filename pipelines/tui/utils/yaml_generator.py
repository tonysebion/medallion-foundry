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
    lines: list[str] = []

    # Required fields first
    if bronze.get("system"):
        lines.append(f"  system: {bronze['system']}")
    if bronze.get("entity"):
        lines.append(f"  entity: {bronze['entity']}")
    if bronze.get("source_type"):
        lines.append(f"  source_type: {bronze['source_type']}")

    # Source path (quote if contains special chars)
    if bronze.get("source_path"):
        path = bronze["source_path"]
        if _needs_quoting(path):
            lines.append(f'  source_path: "{path}"')
        else:
            lines.append(f"  source_path: {path}")

    # Target path
    if bronze.get("target_path"):
        path = bronze["target_path"]
        if _needs_quoting(path):
            lines.append(f'  target_path: "{path}"')
        else:
            lines.append(f"  target_path: {path}")

    # Database connection
    if bronze.get("host"):
        lines.append(f"  host: {bronze['host']}")
    if bronze.get("database"):
        lines.append(f"  database: {bronze['database']}")
    if bronze.get("query"):
        lines.append("  query: |")
        for line in bronze["query"].split("\n"):
            lines.append(f"    {line}")

    # Load pattern (only if not default)
    if bronze.get("load_pattern") and bronze["load_pattern"] != "full_snapshot":
        lines.append(f"  load_pattern: {bronze['load_pattern']}")

    # Watermark
    if bronze.get("watermark_column"):
        lines.append(f"  watermark_column: {bronze['watermark_column']}")

    # Full refresh
    if bronze.get("full_refresh_days"):
        lines.append(f"  full_refresh_days: {bronze['full_refresh_days']}")

    # Chunk size
    if bronze.get("chunk_size"):
        lines.append(f"  chunk_size: {bronze['chunk_size']}")

    # Options
    if bronze.get("options"):
        lines.extend(_format_options(bronze["options"]))

    return lines


def _format_silver(silver: dict[str, Any]) -> list[str]:
    """Format silver section fields."""
    lines: list[str] = []

    # Natural keys (required)
    if silver.get("natural_keys"):
        keys = silver["natural_keys"]
        if isinstance(keys, list):
            if len(keys) == 1:
                lines.append(f"  natural_keys: [{keys[0]}]")
            else:
                lines.append("  natural_keys:")
                for key in keys:
                    lines.append(f"    - {key}")
        else:
            lines.append(f"  natural_keys: [{keys}]")

    # Change timestamp (required)
    if silver.get("change_timestamp"):
        lines.append(f"  change_timestamp: {silver['change_timestamp']}")

    # Source path
    if silver.get("source_path"):
        path = silver["source_path"]
        if _needs_quoting(path):
            lines.append(f'  source_path: "{path}"')
        else:
            lines.append(f"  source_path: {path}")

    # Target path
    if silver.get("target_path"):
        path = silver["target_path"]
        if _needs_quoting(path):
            lines.append(f'  target_path: "{path}"')
        else:
            lines.append(f"  target_path: {path}")

    # Entity kind (only if not default)
    if silver.get("entity_kind") and silver["entity_kind"] != "state":
        lines.append(f"  entity_kind: {silver['entity_kind']}")

    # History mode (only if not default)
    if silver.get("history_mode") and silver["history_mode"] != "current_only":
        lines.append(f"  history_mode: {silver['history_mode']}")

    # Attributes
    if silver.get("attributes"):
        lines.append("  attributes:")
        for attr in silver["attributes"]:
            lines.append(f"    - {attr}")

    # Exclude columns
    if silver.get("exclude_columns"):
        lines.append("  exclude_columns:")
        for col in silver["exclude_columns"]:
            lines.append(f"    - {col}")

    # Partition by
    if silver.get("partition_by"):
        lines.append("  partition_by:")
        for col in silver["partition_by"]:
            lines.append(f"    - {col}")

    # Compression (only if not default)
    if silver.get("parquet_compression") and silver["parquet_compression"] != "snappy":
        lines.append(f"  parquet_compression: {silver['parquet_compression']}")

    # Validate source (only if not default)
    if silver.get("validate_source") and silver["validate_source"] != "skip":
        lines.append(f"  validate_source: {silver['validate_source']}")

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
