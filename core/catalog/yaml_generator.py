"""YAML skeleton generator from OpenMetadata schema per spec Section 9.1.

Generates pipeline configuration YAML skeletons from OpenMetadata table schemas.
This enables users to quickly bootstrap their dataset configurations.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from core.om.client import OpenMetadataClient, TableSchema, ColumnSchema

logger = logging.getLogger(__name__)


def generate_yaml_skeleton(
    table_fqn: str,
    om_client: Optional[OpenMetadataClient] = None,
    output_path: Optional[Path] = None,
    include_bronze: bool = True,
    include_silver: bool = True,
    default_entity_kind: str = "state",
) -> Dict[str, Any]:
    """Generate a YAML configuration skeleton from an OpenMetadata table.

    Args:
        table_fqn: Fully qualified table name (e.g., "database.schema.table")
        om_client: OpenMetadata client instance (creates new if not provided)
        output_path: Optional path to write YAML file
        include_bronze: Include Bronze layer configuration
        include_silver: Include Silver layer configuration
        default_entity_kind: Default entity kind for Silver ("state" or "event")

    Returns:
        Dictionary with the generated configuration

    Example:
        # Generate YAML from OM schema
        config = generate_yaml_skeleton(
            table_fqn="source_db.public.orders",
            output_path=Path("configs/orders.yaml"),
        )
    """
    if om_client is None:
        om_client = OpenMetadataClient()

    # Fetch schema from OpenMetadata
    table_schema = om_client.get_table_schema(table_fqn)

    if table_schema is None:
        logger.warning(
            "Could not fetch schema from OpenMetadata for %s. "
            "Generating minimal skeleton.",
            table_fqn,
        )
        # Generate minimal skeleton without schema details
        return _generate_minimal_skeleton(
            table_fqn,
            include_bronze=include_bronze,
            include_silver=include_silver,
            default_entity_kind=default_entity_kind,
            output_path=output_path,
        )

    # Generate full skeleton from schema
    return _generate_skeleton_from_schema(
        table_schema,
        include_bronze=include_bronze,
        include_silver=include_silver,
        default_entity_kind=default_entity_kind,
        output_path=output_path,
    )


def _generate_minimal_skeleton(
    table_fqn: str,
    include_bronze: bool,
    include_silver: bool,
    default_entity_kind: str,
    output_path: Optional[Path],
) -> Dict[str, Any]:
    """Generate a minimal skeleton without schema details."""
    parts = table_fqn.split(".")
    entity_name = parts[-1] if parts else "entity"
    system_name = parts[0] if len(parts) > 1 else "source_system"

    config: Dict[str, Any] = {
        "dataset_id": f"{system_name}.{entity_name}",
        "system": system_name,
        "entity": entity_name,
        "domain": "TODO_SET_DOMAIN",
        "environment": "dev",
    }

    if include_bronze:
        config["bronze"] = {
            "enabled": True,
            "source_type": "db_table",
            "connection_string_env": "TODO_SET_CONNECTION_ENV",
            "table_name": table_fqn,
            "load_pattern": "incremental_append",
            "watermark_column": "TODO_SET_WATERMARK_COLUMN",
        }

    if include_silver:
        config["silver"] = {
            "enabled": True,
            "entity_kind": default_entity_kind,
            "natural_keys": ["TODO_SET_PRIMARY_KEY"],
            "attributes": ["TODO_ADD_ATTRIBUTES"],
        }
        if default_entity_kind == "state":
            config["silver"]["change_ts_column"] = "TODO_SET_CHANGE_TS"
        else:
            config["silver"]["event_ts_column"] = "TODO_SET_EVENT_TS"

    config["schema"] = {
        "expected_columns": [
            {
                "name": "TODO_ADD_COLUMNS",
                "type": "string",
                "nullable": True,
            }
        ]
    }

    if output_path:
        _write_yaml(config, output_path)

    return config


def _generate_skeleton_from_schema(
    table_schema: TableSchema,
    include_bronze: bool,
    include_silver: bool,
    default_entity_kind: str,
    output_path: Optional[Path],
) -> Dict[str, Any]:
    """Generate a full skeleton from OpenMetadata schema."""
    config: Dict[str, Any] = {
        "dataset_id": f"{table_schema.database}.{table_schema.name}",
        "system": table_schema.database,
        "entity": table_schema.name,
        "domain": _infer_domain_from_tags(table_schema.tags),
        "environment": "dev",
    }

    if table_schema.description:
        config["description"] = table_schema.description

    # Bronze configuration
    if include_bronze:
        watermark_col = _infer_watermark_column(table_schema.columns)
        config["bronze"] = {
            "enabled": True,
            "source_type": "db_table",
            "connection_string_env": f"{table_schema.database.upper()}_CONNECTION",
            "table_name": f"{table_schema.schema}.{table_schema.name}",
            "load_pattern": "incremental_append" if watermark_col else "snapshot",
        }
        if watermark_col:
            config["bronze"]["watermark_column"] = watermark_col

    # Silver configuration
    if include_silver:
        primary_keys = table_schema.primary_keys
        if not primary_keys:
            primary_keys = ["TODO_SET_PRIMARY_KEY"]

        entity_kind = _infer_entity_kind(table_schema.columns, default_entity_kind)
        ts_column = _infer_timestamp_column(table_schema.columns)

        config["silver"] = {
            "enabled": True,
            "entity_kind": entity_kind,
            "natural_keys": primary_keys,
            "attributes": _get_attribute_columns(table_schema.columns, primary_keys),
        }

        if entity_kind == "state":
            config["silver"]["change_ts_column"] = ts_column or "updated_at"
        else:
            config["silver"]["event_ts_column"] = ts_column or "event_ts"

    # Schema configuration
    config["schema"] = {
        "expected_columns": [col.to_dict() for col in table_schema.columns]
    }
    if table_schema.primary_keys:
        config["schema"]["primary_keys"] = table_schema.primary_keys

    if output_path:
        _write_yaml(config, output_path)

    return config


def _infer_domain_from_tags(tags: List[str]) -> str:
    """Infer domain from OpenMetadata tags."""
    domain_tags = [t for t in tags if t.startswith("domain:")]
    if domain_tags:
        return domain_tags[0].replace("domain:", "")
    return "TODO_SET_DOMAIN"


def _infer_watermark_column(columns: List[ColumnSchema]) -> Optional[str]:
    """Infer watermark column from column names."""
    watermark_candidates = [
        "updated_at",
        "modified_at",
        "last_modified",
        "created_at",
        "event_ts",
        "event_time",
        "timestamp",
    ]
    column_names = {c.name.lower(): c.name for c in columns}

    for candidate in watermark_candidates:
        if candidate in column_names:
            return column_names[candidate]

    # Check for timestamp columns
    for col in columns:
        if col.data_type.lower() in ("timestamp", "datetime", "timestamptz"):
            return col.name

    return None


def _infer_entity_kind(columns: List[ColumnSchema], default: str) -> str:
    """Infer entity kind from column patterns."""
    column_names = {c.name.lower() for c in columns}

    # Event entity patterns
    event_patterns = {"event_id", "event_ts", "event_type", "event_name"}
    if event_patterns & column_names:
        return "event"

    # State entity patterns
    state_patterns = {"effective_from", "effective_to", "is_current", "version"}
    if state_patterns & column_names:
        return "state"

    return default


def _infer_timestamp_column(columns: List[ColumnSchema]) -> Optional[str]:
    """Infer the primary timestamp column."""
    ts_candidates = [
        "event_ts",
        "event_time",
        "updated_at",
        "modified_at",
        "created_at",
        "timestamp",
    ]
    column_names = {c.name.lower(): c.name for c in columns}

    for candidate in ts_candidates:
        if candidate in column_names:
            return column_names[candidate]

    return None


def _get_attribute_columns(
    columns: List[ColumnSchema],
    exclude: List[str],
) -> List[str]:
    """Get attribute columns (non-key, non-timestamp)."""
    exclude_set = {e.lower() for e in exclude}
    exclude_set.update(
        {
            "created_at",
            "updated_at",
            "modified_at",
            "deleted_at",
            "effective_from",
            "effective_to",
            "is_current",
            "version",
            "load_date",
            "batch_id",
        }
    )

    return [
        c.name
        for c in columns
        if c.name.lower() not in exclude_set and not c.is_primary_key
    ]


def _write_yaml(config: Dict[str, Any], output_path: Path) -> None:
    """Write configuration to YAML file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Generated from OpenMetadata schema\n")
        f.write("# Review and update TODO items before running\n\n")
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    logger.info("YAML skeleton written to %s", output_path)


def generate_multi_table_skeletons(
    database: str,
    schema: Optional[str] = None,
    om_client: Optional[OpenMetadataClient] = None,
    output_dir: Optional[Path] = None,
    **kwargs,
) -> List[Dict[str, Any]]:
    """Generate YAML skeletons for multiple tables in a database.

    Args:
        database: Database name
        schema: Optional schema to filter
        om_client: OpenMetadata client
        output_dir: Directory to write YAML files
        **kwargs: Additional args passed to generate_yaml_skeleton

    Returns:
        List of generated configurations
    """
    if om_client is None:
        om_client = OpenMetadataClient()

    tables = om_client.get_database_tables(database, schema)

    if not tables:
        logger.warning(
            "No tables found in %s.%s (or OM client in stub mode)",
            database,
            schema or "*",
        )
        return []

    configs = []
    for table in tables:
        output_path = None
        if output_dir:
            output_path = output_dir / f"{table.name}.yaml"

        config = generate_yaml_skeleton(
            table_fqn=table.fully_qualified_name,
            om_client=om_client,
            output_path=output_path,
            **kwargs,
        )
        configs.append(config)

    return configs
