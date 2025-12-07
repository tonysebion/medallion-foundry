"""Configuration migration utilities.

This module provides functions to convert between legacy config formats and the
new intent-based DatasetConfig format.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from core.primitives.foundations.exceptions import emit_compat
from core.primitives.foundations.patterns import LoadPattern
from core.config.models.enums import (
    EntityKind,
    HistoryMode,
    InputMode,
    DeleteMode,
    SchemaMode,
)
from core.config.models.intent import BronzeIntent, SilverIntent
from core.config.models.dataset import DatasetConfig, PathStructure
from core.config.models.helpers import require_bool

logger = logging.getLogger(__name__)


def dataset_to_runtime_config(dataset: DatasetConfig) -> Dict[str, Any]:
    """Build a legacy-style runtime config dict from a DatasetConfig."""
    bronze_base_path = dataset.bronze_base_path
    bronze_override = dataset.bronze.options.get("local_output_dir")
    if bronze_override:
        bronze_base_path = Path(bronze_override).resolve()
    bronze_base = str(bronze_base_path)
    pattern_folder = dataset.bronze.options.get("pattern_folder")

    if dataset.silver.output_dir:
        silver_base_path = Path(dataset.silver.output_dir).resolve()
    else:
        silver_base_path = dataset.silver_base_path
    silver_base = str(silver_base_path)
    pattern_override = dataset.bronze.options.get("load_pattern")
    if pattern_override:
        load_pattern_value = LoadPattern.normalize(pattern_override).value
    else:
        load_pattern_value = LoadPattern.SNAPSHOT.value

    bronze_backend = dataset.bronze.output_storage or "local"
    local_run = {
        "load_pattern": load_pattern_value,
        "local_output_dir": bronze_base,
        "write_parquet": dataset.silver.write_parquet,
        "write_csv": dataset.silver.write_csv,
        "storage_enabled": bronze_backend == "s3",
    }
    reference_mode = dataset.bronze.options.get("reference_mode")
    if isinstance(reference_mode, dict) and reference_mode:
        local_run["reference_mode"] = dict(reference_mode)

    source_cfg: Dict[str, Any] = {
        "type": dataset.bronze.source_type,
        "system": dataset.system,
        "table": dataset.entity,
        "run": local_run,
    }
    if pattern_folder:
        source_cfg["run"]["pattern_folder"] = pattern_folder
    if dataset.bronze.owner_team:
        source_cfg["owner_team"] = dataset.bronze.owner_team
    if dataset.bronze.owner_contact:
        source_cfg["owner_contact"] = dataset.bronze.owner_contact

    if dataset.bronze.source_type == "file":
        if not dataset.bronze.path_pattern:
            raise ValueError(
                f"{dataset.dataset_id} bronze.path_pattern must be provided for file sources"
            )
        file_cfg: Dict[str, Any] = {
            "path": dataset.bronze.path_pattern,
            "format": dataset.bronze.options.get("format", "csv"),
        }
        file_cfg.update(dataset.bronze.options.get("file", {}))
        source_cfg["file"] = file_cfg
    elif dataset.bronze.source_type == "db":
        db_cfg = dataset.bronze.options.get("db", {})
        conn = dataset.bronze.connection_name or db_cfg.get("conn_str_env")
        if not conn:
            raise ValueError(
                f"{dataset.dataset_id} bronze.connection_name (or db.conn_str_env) is required for db sources"
            )
        if not dataset.bronze.source_query and not db_cfg.get("base_query"):
            raise ValueError(
                f"{dataset.dataset_id} bronze.source_query is required for db sources"
            )
        source_cfg["db"] = {
            "conn_str_env": conn,
            "base_query": dataset.bronze.source_query or db_cfg.get("base_query"),
        }
    elif dataset.bronze.source_type == "api":
        api_cfg = dataset.bronze.options.get("api", {}).copy()
        base_url = api_cfg.get("base_url") or dataset.bronze.connection_name
        if not base_url:
            raise ValueError(
                f"{dataset.dataset_id} bronze.connection_name (or api.base_url) is required for api sources"
            )
        endpoint = api_cfg.get("endpoint") or dataset.bronze.source_query or "/"
        api_cfg.setdefault("base_url", base_url)
        api_cfg.setdefault("endpoint", endpoint)
        source_cfg["api"] = api_cfg
    elif dataset.bronze.source_type == "custom":
        custom_cfg = dataset.bronze.options.get("custom_extractor", {})
        if not custom_cfg:
            raise ValueError(
                f"{dataset.dataset_id} requires bronze.options.custom_extractor for custom sources"
            )
        source_cfg["custom_extractor"] = custom_cfg

    partitioning = {"use_dt_partition": True, "partition_strategy": "date"}
    if dataset.bronze.partition_column:
        partitioning["column"] = dataset.bronze.partition_column

    bronze_cfg: Dict[str, Any] = {
        "storage_backend": bronze_backend,
        "local_path": bronze_base,
        "partitioning": partitioning,
        "output_defaults": dataset.bronze.options.get(
            "output_defaults",
            {
                "allow_csv": True,
                "allow_parquet": True,
                "parquet_compression": "snappy",
            },
        ),
    }
    if bronze_backend == "s3":
        bronze_cfg["s3_bucket"] = dataset.bronze.output_bucket
        bronze_cfg["s3_prefix"] = dataset.bronze.output_prefix
    bronze_options = bronze_cfg.setdefault("options", {})
    if pattern_folder:
        bronze_options["pattern_folder"] = pattern_folder

    platform_cfg = {
        "bronze": bronze_cfg,
        "s3_connection": {
            "endpoint_url_env": "BRONZE_S3_ENDPOINT",
            "access_key_env": "AWS_ACCESS_KEY_ID",
            "secret_key_env": "AWS_SECRET_ACCESS_KEY",
        },
    }

    order_column = (
        dataset.silver.change_ts_column
        if dataset.silver.entity_kind.is_state_like
        else dataset.silver.event_ts_column
    )

    from core.primitives.foundations.models import SilverModel

    if dataset.silver.entity_kind.is_state_like:
        if dataset.silver.history_mode == HistoryMode.SCD1:
            model = SilverModel.SCD_TYPE_1.value
        elif dataset.silver.history_mode == HistoryMode.LATEST_ONLY:
            model = SilverModel.FULL_MERGE_DEDUPE.value
        else:
            model = SilverModel.SCD_TYPE_2.value
    elif dataset.silver.entity_kind == EntityKind.DERIVED_EVENT:
        model = SilverModel.INCREMENTAL_MERGE.value
    else:
        model = SilverModel.PERIODIC_SNAPSHOT.value

    silver_cfg: Dict[str, Any] = {
        "output_dir": silver_base,
        "domain": dataset.domain or dataset.system,
        "entity": dataset.entity,
        "version": dataset.silver.version,
        "load_partition_name": dataset.silver.load_partition_name,
        "include_pattern_folder": dataset.silver.include_pattern_folder,
        "write_parquet": dataset.silver.write_parquet,
        "write_csv": dataset.silver.write_csv,
        "parquet_compression": "snappy",
        "primary_keys": list(dataset.silver.natural_keys),
        "order_column": order_column,
        "partitioning": {"columns": list(dataset.silver.partition_by)},
        "schema": {
            "rename_map": {},
            "column_order": list(dataset.silver.attributes) or None,
        },
        "normalization": {"trim_strings": False, "empty_strings_as_null": False},
        "error_handling": {
            "enabled": False,
            "max_bad_records": 0,
            "max_bad_percent": 0.0,
        },
        "model": model,
        "require_checksum": dataset.silver.require_checksum,
        "semantic_owner": dataset.silver.semantic_owner,
        "semantic_contact": dataset.silver.semantic_contact,
    }

    return {
        "config_version": 3,
        "platform": platform_cfg,
        "source": source_cfg,
        "silver": silver_cfg,
        "path_structure": {
            "bronze": dataset.path_structure.bronze,
            "silver": dataset.path_structure.silver,
        },
    }


def legacy_to_dataset(cfg: Dict[str, Any]) -> Optional[DatasetConfig]:
    """Best-effort translation from legacy config dictionaries to DatasetConfig."""
    try:
        source = cfg["source"]
        silver_raw = cfg.get("silver", {})
        system = source["system"]
        entity = source["table"]
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(
            "Legacy config missing required keys for dataset conversion: %s", exc
        )
        return None

    source_type = source.get("type", "file")
    run_cfg = source.get("run", {})
    partition_column = run_cfg.get("partition_column")
    connection_name = None
    source_query = None
    path_pattern = None
    options: Dict[str, Any] = {}

    if source_type == "db":
        db_cfg = source.get("db", {})
        connection_name = db_cfg.get("conn_str_env")
        source_query = db_cfg.get("base_query")
        options["db"] = db_cfg
    elif source_type == "api":
        api_cfg = source.get("api", {})
        connection_name = api_cfg.get("base_url")
        source_query = api_cfg.get("endpoint")
        options["api"] = api_cfg
    elif source_type == "file":
        file_cfg = source.get("file", {})
        path_pattern = file_cfg.get("path")
        options["file"] = file_cfg

    bronze_intent = BronzeIntent(
        enabled=True,
        source_type=source_type,
        connection_name=connection_name,
        source_query=source_query,
        path_pattern=path_pattern,
        partition_column=partition_column,
        options=options,
    )

    primary_keys = silver_raw.get("primary_keys") or []
    order_column = silver_raw.get("order_column") or run_cfg.get("order_column")
    attributes = (silver_raw.get("schema") or {}).get("column_order") or []
    partition_by = (silver_raw.get("partitioning") or {}).get("columns") or []
    model = (silver_raw.get("model") or "").lower()

    if model in {"scd_type_1", "scd_type_2"}:
        entity_kind = EntityKind.STATE
    elif model == "full_merge_dedupe":
        entity_kind = EntityKind.STATE
    else:
        entity_kind = EntityKind.EVENT

    history_mode = None
    if entity_kind.is_state_like:
        if model == "scd_type_1":
            history_mode = HistoryMode.SCD1
        elif model == "full_merge_dedupe":
            history_mode = HistoryMode.LATEST_ONLY
        else:
            history_mode = HistoryMode.SCD2

    input_mode = None
    if entity_kind.is_event_like:
        load_pattern = run_cfg.get("load_pattern", LoadPattern.SNAPSHOT.value)
        input_mode = (
            InputMode.REPLACE_DAILY
            if load_pattern == LoadPattern.SNAPSHOT.value
            else InputMode.APPEND_LOG
        )

    if not order_column:
        if entity_kind.is_state_like:
            order_column = "_change_ts"
        else:
            order_column = "_event_ts"
        emit_compat(
            "Legacy config missing silver.order_column; defaulting to placeholder column",
            code="CFG_NEW_ORDER",
        )

    write_parquet = require_bool(
        silver_raw.get("write_parquet"), "silver.write_parquet", True
    )
    write_csv = require_bool(silver_raw.get("write_csv"), "silver.write_csv", False)
    silver_intent = SilverIntent(
        enabled=True,
        entity_kind=entity_kind,
        history_mode=history_mode,
        input_mode=input_mode,
        delete_mode=DeleteMode.IGNORE,
        schema_mode=SchemaMode.STRICT,
        natural_keys=primary_keys,
        event_ts_column=order_column if entity_kind.is_event_like else None,
        change_ts_column=order_column if entity_kind.is_state_like else None,
        attributes=attributes,
        partition_by=partition_by,
        write_parquet=write_parquet,
        write_csv=write_csv,
    )

    domain = silver_raw.get("domain")
    env = cfg.get("environment")

    return DatasetConfig(
        system=system,
        entity=entity,
        environment=env,
        domain=domain,
        bronze=bronze_intent,
        silver=silver_intent,
        path_structure=PathStructure(),
    )


__all__ = [
    "dataset_to_runtime_config",
    "legacy_to_dataset",
]
