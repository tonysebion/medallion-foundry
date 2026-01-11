"""Deprecation handling for pipeline configuration.

Provides runtime warnings for deprecated fields to guide users toward
best practices while maintaining backward compatibility.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pipelines.lib.observability import get_structlog_logger

logger = get_structlog_logger(__name__)


# Mapping of deprecated field paths to their deprecation info
# Each entry contains:
#   - message: Human-readable deprecation message
#   - replacement: Suggested replacement (or None if field is being removed)
#   - severity: "warning" or "info" (info for soft deprecations)
DEPRECATED_FIELDS: Dict[str, Dict[str, Any]] = {
    # Bronze deprecated fields (not implemented or to be removed)
    "bronze.chunk_size": {
        "message": "chunk_size is not implemented and will be removed in a future version",
        "replacement": None,
        "severity": "warning",
    },
    "bronze.full_refresh_days": {
        "message": "full_refresh_days is not implemented and will be removed in a future version",
        "replacement": None,
        "severity": "warning",
    },
    "bronze.partition_by": {
        "message": "partition_by is not used and will be removed in a future version",
        "replacement": None,
        "severity": "warning",
    },
    "bronze.connection": {
        "message": "Named connections are not implemented and will be removed in a future version",
        "replacement": None,
        "severity": "warning",
    },
    "bronze.input_mode": {
        "message": "input_mode on Bronze is deprecated",
        "replacement": "input_mode is auto-wired to Silver based on load_pattern",
        "severity": "info",
    },
    "bronze.watermark_column": {
        "message": "watermark_column has been renamed to incremental_column",
        "replacement": "Use 'incremental_column' instead (pairs with 'load_pattern: incremental')",
        "severity": "warning",
    },
    # Silver renamed fields (backwards compatible)
    "silver.natural_keys": {
        "message": "natural_keys has been renamed to unique_columns",
        "replacement": "Use 'unique_columns' instead (describes what the column(s) do)",
        "severity": "warning",
    },
    "silver.change_timestamp": {
        "message": "change_timestamp has been renamed to last_updated_column",
        "replacement": "Use 'last_updated_column' instead (describes what the column does)",
        "severity": "warning",
    },
    # Silver deprecated fields (prefer model presets)
    "silver.entity_kind": {
        "message": "Manual entity_kind configuration is deprecated",
        "replacement": "Use 'model' preset instead (e.g., 'scd_type_2', 'event_log')",
        "severity": "warning",
    },
    "silver.history_mode": {
        "message": "Manual history_mode configuration is deprecated",
        "replacement": "Use 'model' preset instead (e.g., 'scd_type_2' for full_history, 'full_merge_dedupe' for current_only)",
        "severity": "warning",
    },
    "silver.delete_mode": {
        "message": "Manual delete_mode configuration is deprecated for CDC",
        "replacement": "Use CDC model preset (e.g., 'cdc_current_tombstone', 'cdc_history_hard_delete')",
        "severity": "warning",
    },
    "silver.input_mode": {
        "message": "Manual input_mode configuration is rarely needed",
        "replacement": "input_mode is auto-wired from Bronze load_pattern (full_snapshot → replace_daily, incremental/cdc → append_log)",
        "severity": "info",
    },
    "silver.exclude_columns": {
        "message": "exclude_columns is deprecated",
        "replacement": "Use 'attributes' to explicitly list columns to include",
        "severity": "info",
    },
    "silver.column_mapping": {
        "message": "column_mapping is deprecated",
        "replacement": "Use SQL transforms in Bronze query or handle in Gold layer",
        "severity": "info",
    },
    "silver.output_formats": {
        "message": "output_formats is rarely needed",
        "replacement": "Default parquet format is recommended for Silver layer",
        "severity": "info",
    },
    "silver.parquet_compression": {
        "message": "parquet_compression is rarely needed",
        "replacement": "Default snappy compression is recommended",
        "severity": "info",
    },
}


# CDC model names that are deprecated in favor of model: cdc + options
DEPRECATED_CDC_MODELS: Dict[str, Dict[str, str]] = {
    "cdc_current": {
        "keep_history": "false",
        "handle_deletes": "ignore",
    },
    "cdc_current_tombstone": {
        "keep_history": "false",
        "handle_deletes": "flag",
    },
    "cdc_current_hard_delete": {
        "keep_history": "false",
        "handle_deletes": "remove",
    },
    "cdc_history": {
        "keep_history": "true",
        "handle_deletes": "ignore",
    },
    "cdc_history_tombstone": {
        "keep_history": "true",
        "handle_deletes": "flag",
    },
    "cdc_history_hard_delete": {
        "keep_history": "true",
        "handle_deletes": "remove",
    },
}


def get_cdc_model_migration_suggestion(model_name: str) -> Optional[str]:
    """Get migration suggestion for deprecated CDC model names.

    Args:
        model_name: The model name (e.g., 'cdc_current_tombstone')

    Returns:
        Migration suggestion string, or None if model is not deprecated
    """
    if model_name not in DEPRECATED_CDC_MODELS:
        return None

    config = DEPRECATED_CDC_MODELS[model_name]
    return (
        f"model: cdc\n"
        f"  keep_history: {config['keep_history']}\n"
        f"  handle_deletes: {config['handle_deletes']}"
    )


def find_deprecated_fields(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Find all deprecated fields used in a pipeline configuration.

    Args:
        config: The parsed YAML configuration dict

    Returns:
        List of deprecation info dicts with keys: path, message, replacement, severity
    """
    found: List[Dict[str, Any]] = []

    # Check Bronze section
    if "bronze" in config and isinstance(config["bronze"], dict):
        for key in config["bronze"]:
            path = f"bronze.{key}"
            if path in DEPRECATED_FIELDS:
                found.append(
                    {
                        "path": path,
                        **DEPRECATED_FIELDS[path],
                    }
                )

    # Check Silver section
    if "silver" in config and isinstance(config["silver"], dict):
        silver = config["silver"]
        for key in silver:
            path = f"silver.{key}"
            if path in DEPRECATED_FIELDS:
                # Special case: entity_kind/history_mode are OK if no model preset is used
                # But we still want to suggest using model presets
                if (
                    key in ("entity_kind", "history_mode", "delete_mode")
                    and "model" in silver
                ):
                    # They're using both - that's fine, model takes precedence
                    continue
                found.append(
                    {
                        "path": path,
                        **DEPRECATED_FIELDS[path],
                    }
                )

        # Check for deprecated CDC model names
        model_name = silver.get("model", "").lower() if silver.get("model") else ""
        if model_name in DEPRECATED_CDC_MODELS:
            migration = get_cdc_model_migration_suggestion(model_name)
            found.append(
                {
                    "path": "silver.model",
                    "message": f"CDC model '{model_name}' is deprecated",
                    "replacement": f"Use the unified CDC model with explicit options:\n{migration}",
                    "severity": "warning",
                }
            )

    return found


def warn_deprecated_fields(
    config: Dict[str, Any], yaml_path: Optional[str] = None
) -> None:
    """Log warnings for any deprecated fields found in config.

    Args:
        config: The parsed YAML configuration dict
        yaml_path: Optional path to the YAML file (for better error messages)
    """
    deprecated = find_deprecated_fields(config)

    for field in deprecated:
        severity = field.get("severity", "warning")
        path = field["path"]
        message = field["message"]
        replacement = field.get("replacement")

        log_message = f"Deprecated field '{path}': {message}"
        if replacement:
            log_message += f". {replacement}"

        if yaml_path:
            log_message = f"[{yaml_path}] {log_message}"

        if severity == "warning":
            logger.warning(
                "deprecated_field", field=path, message=message, replacement=replacement
            )
        else:
            logger.info(
                "deprecated_field", field=path, message=message, replacement=replacement
            )


def get_deprecation_summary() -> str:
    """Get a human-readable summary of all deprecated fields.

    Returns:
        Formatted string listing all deprecated fields and their replacements
    """
    lines = ["Deprecated Pipeline Configuration Fields:", "=" * 50, ""]

    # Group by section
    bronze_fields = [
        (k, v) for k, v in DEPRECATED_FIELDS.items() if k.startswith("bronze.")
    ]
    silver_fields = [
        (k, v) for k, v in DEPRECATED_FIELDS.items() if k.startswith("silver.")
    ]

    if bronze_fields:
        lines.append("Bronze Layer:")
        lines.append("-" * 30)
        for path, info in bronze_fields:
            field = path.split(".")[-1]
            lines.append(f"  {field}: {info['message']}")
            if info.get("replacement"):
                lines.append(f"    → {info['replacement']}")
        lines.append("")

    if silver_fields:
        lines.append("Silver Layer:")
        lines.append("-" * 30)
        for path, info in silver_fields:
            field = path.split(".")[-1]
            lines.append(f"  {field}: {info['message']}")
            if info.get("replacement"):
                lines.append(f"    → {info['replacement']}")
        lines.append("")

    return "\n".join(lines)
