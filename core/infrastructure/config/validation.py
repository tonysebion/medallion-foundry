from __future__ import annotations

import copy
import logging
from typing import Any, Dict

from .models.root import SilverConfig, DataClassification
from pydantic import ValidationError
from core.foundation.primitives.exceptions import emit_compat, emit_deprecation, DeprecationSpec
from core.foundation.primitives.patterns import LoadPattern
from core.infrastructure.io.storage import validate_storage_metadata

logger = logging.getLogger(__name__)

# Valid values for new spec fields
VALID_DATA_CLASSIFICATIONS = [e.value for e in DataClassification]
VALID_ENVIRONMENTS = ["dev", "staging", "prod", "test", "local"]
VALID_LAYERS = ["bronze", "silver"]
VALID_SCHEMA_EVOLUTION_MODES = ["strict", "allow_new_nullable", "ignore_unknown"]


def _normalize_silver_config(
    raw_silver: Dict[str, Any] | None,
    source: Dict[str, Any],
    load_pattern: LoadPattern,
) -> Dict[str, Any]:
    try:
        model = SilverConfig.from_raw(raw_silver, source, load_pattern)
    except ValidationError as exc:
        error_fields = {err.get("loc", (None,))[0] for err in exc.errors()}
        if "require_checksum" in error_fields:
            raise ValueError("silver.require_checksum must be a boolean") from exc
        raise
    return model.to_dict()


def _validate_spec_fields(cfg: Dict[str, Any]) -> None:
    """Validate new spec fields per Section 2.

    Validates:
    - pipeline_id: Optional string
    - layer: "bronze" or "silver"
    - domain: Optional string
    - environment: "dev", "staging", "prod", "test", "local"
    - data_classification: "public", "internal", "confidential", "restricted"
    - owners: dict with semantic_owner and technical_owner
    - schema_evolution: dict with mode and allow_type_relaxation
    """
    # Validate layer
    layer = cfg.get("layer", "bronze")
    if layer not in VALID_LAYERS:
        raise ValueError(f"layer must be one of {VALID_LAYERS}, got '{layer}'")

    # Validate environment
    environment = cfg.get("environment", "dev")
    if environment not in VALID_ENVIRONMENTS:
        raise ValueError(
            f"environment must be one of {VALID_ENVIRONMENTS}, got '{environment}'"
        )

    # Validate data_classification
    classification = cfg.get("data_classification", "internal")
    if isinstance(classification, str):
        classification = classification.lower()
    if classification not in VALID_DATA_CLASSIFICATIONS:
        raise ValueError(
            f"data_classification must be one of {VALID_DATA_CLASSIFICATIONS}, got '{classification}'"
        )

    # Validate owners
    owners = cfg.get("owners")
    if owners is not None:
        if not isinstance(owners, dict):
            raise ValueError("owners must be a dictionary")
        for key in ("semantic_owner", "technical_owner"):
            if key in owners and owners[key] is not None:
                if not isinstance(owners[key], str):
                    raise ValueError(f"owners.{key} must be a string")

    # Validate schema_evolution
    schema_evolution = cfg.get("schema_evolution")
    if schema_evolution is not None:
        if not isinstance(schema_evolution, dict):
            raise ValueError("schema_evolution must be a dictionary")
        mode = schema_evolution.get("mode", "strict")
        if mode not in VALID_SCHEMA_EVOLUTION_MODES:
            raise ValueError(
                f"schema_evolution.mode must be one of {VALID_SCHEMA_EVOLUTION_MODES}, got '{mode}'"
            )
        allow_relaxation = schema_evolution.get("allow_type_relaxation", False)
        if not isinstance(allow_relaxation, bool):
            raise ValueError("schema_evolution.allow_type_relaxation must be a boolean")

    # Validate pipeline_id if provided
    pipeline_id = cfg.get("pipeline_id")
    if pipeline_id is not None and not isinstance(pipeline_id, str):
        raise ValueError("pipeline_id must be a string")

    # Validate domain if provided
    domain = cfg.get("domain")
    if domain is not None and not isinstance(domain, str):
        raise ValueError("domain must be a string")


def validate_config_dict(cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = copy.deepcopy(cfg)

    # Validate top-level sections
    if "platform" not in cfg:
        raise ValueError("Config must contain a 'platform' section")
    if "source" not in cfg:
        raise ValueError("Config must contain a 'source' section")

    # Validate new spec fields (Section 2)
    _validate_spec_fields(cfg)

    platform = cfg["platform"]
    if not isinstance(platform, dict):
        raise ValueError("'platform' must be a dictionary")

    if "bronze" not in platform:
        raise ValueError("Missing platform.bronze section in config")
    bronze = platform["bronze"]
    if not isinstance(bronze, dict):
        raise ValueError("'platform.bronze' must be a dictionary")

    storage_backend = bronze.get("storage_backend", "s3")
    valid_backends = ["s3", "azure", "local"]
    if storage_backend not in valid_backends:
        raise ValueError(
            f"platform.bronze.storage_backend must be one of {valid_backends}"
        )

    if storage_backend == "s3":
        if "s3_connection" not in platform:
            raise ValueError(
                "Missing platform.s3_connection section (required for S3 backend)"
            )
        for key in ("s3_bucket", "s3_prefix"):
            if key not in bronze:
                raise ValueError(
                    f"Missing required key 'platform.bronze.{key}' in config"
                )
    elif storage_backend == "azure":
        if "azure_connection" not in platform:
            raise ValueError(
                "Missing platform.azure_connection section (required for Azure backend)"
            )
        if "azure_container" not in bronze:
            raise ValueError("Azure backend requires platform.bronze.azure_container")
    elif storage_backend == "local":
        if "local_path" not in bronze:
            legacy_output_dir = bronze.get("output_dir")
            if legacy_output_dir:
                bronze["local_path"] = legacy_output_dir
                emit_compat(
                    "Using platform.bronze.output_dir as local_path; add explicit local_path to silence warning",
                    code="CFG001",
                )
                emit_deprecation(
                    DeprecationSpec(
                        code="CFG001",
                        message="Implicit local_path fallback will be removed; define platform.bronze.local_path",
                        since="1.1.0",
                        remove_in="1.3.0",
                    )
                )
            else:
                raise ValueError("Local backend requires platform.bronze.local_path")

    logger.debug("Validated storage backend: %s", storage_backend)
    validate_storage_metadata(cfg["platform"])

    source = cfg["source"]
    if not isinstance(source, dict):
        raise ValueError("'source' must be a dictionary")

    for key in ("system", "table", "run"):
        if key not in source:
            raise ValueError(f"Missing required key 'source.{key}' in config")

    source_type = source.get("type", "api")
    # Per spec Section 3: db_table, db_query, db_multi, file_batch, api
    valid_source_types = ["api", "db", "db_table", "db_query", "db_multi", "file", "file_batch", "custom"]
    if source_type not in valid_source_types:
        raise ValueError(
            f"Invalid source.type: '{source_type}'. Must be one of {valid_source_types}"
        )

    if source_type == "api" and "api" not in source:
        raise ValueError("source.type='api' requires 'source.api' section")
    if source_type in ("db", "db_table", "db_query") and "db" not in source:
        raise ValueError(f"source.type='{source_type}' requires 'source.db' section")
    if source_type == "db_multi" and "entities" not in source:
        raise ValueError("source.type='db_multi' requires 'source.entities' list")
    if source_type == "custom" and "custom_extractor" not in source:
        raise ValueError(
            "source.type='custom' requires 'source.custom_extractor' section"
        )
    if source_type in ("file", "file_batch") and "file" not in source:
        raise ValueError(f"source.type='{source_type}' requires 'source.file' section")

    if source_type == "custom":
        custom = source["custom_extractor"]
        if "module" not in custom or "class_name" not in custom:
            raise ValueError(
                "source.custom_extractor requires 'module' and 'class_name'"
            )

    if source_type == "file":
        file_cfg = source["file"]
        if not isinstance(file_cfg, dict):
            raise ValueError("source.file must be a dictionary of options")
        if "path" not in file_cfg:
            raise ValueError(
                "source.file requires 'path' to point to the local dataset"
            )
        file_format = file_cfg.get("format")
        valid_formats = ["csv", "tsv", "json", "jsonl", "parquet"]
        if file_format and file_format.lower() not in valid_formats:
            raise ValueError(
                f"source.file.format must be one of {valid_formats} (got '{file_format}')"
            )
        if "delimiter" in file_cfg and not isinstance(file_cfg["delimiter"], str):
            raise ValueError("source.file.delimiter must be a string when provided")
        if "limit_rows" in file_cfg:
            limit_rows = file_cfg["limit_rows"]
            if not isinstance(limit_rows, int) or limit_rows <= 0:
                raise ValueError("source.file.limit_rows must be a positive integer")

    if source_type in ("db", "db_table", "db_query"):
        db = source["db"]
        if "conn_str_env" not in db:
            raise ValueError(
                "source.db requires 'conn_str_env' to specify connection string env var"
            )
        if "base_query" not in db:
            raise ValueError("source.db requires 'base_query' for SQL query")

    if source_type == "db_multi":
        entities = source.get("entities", [])
        if not entities:
            raise ValueError("source.type='db_multi' requires at least one entity")
        if not isinstance(entities, list):
            raise ValueError("source.entities must be a list")
        for i, entity in enumerate(entities):
            if not isinstance(entity, dict):
                raise ValueError(f"source.entities[{i}] must be a dictionary")
            if "name" not in entity:
                raise ValueError(f"source.entities[{i}] requires 'name'")
        # db_multi requires connection_ref or db.conn_str_env
        if "connection_ref" not in source and "db" not in source:
            raise ValueError(
                "source.type='db_multi' requires 'connection_ref' or 'source.db.conn_str_env'"
            )

    if source_type == "api":
        api = source["api"]
        if "base_url" not in api and "url" in api:
            api["base_url"] = api["url"]
            emit_compat("source.api.url key is deprecated; use base_url", code="CFG002")
            emit_deprecation(
                DeprecationSpec(
                    code="CFG002",
                    message="Legacy 'url' field will be removed; rename to base_url",
                    since="1.1.0",
                    remove_in="1.3.0",
                )
            )
        if "endpoint" not in api:
            api["endpoint"] = "/"
            emit_compat("Defaulting missing endpoint to '/'", code="CFG003")
            emit_deprecation(
                DeprecationSpec(
                    code="CFG003",
                    message="Implicit endpoint default will be removed; specify source.api.endpoint explicitly",
                    since="1.1.0",
                    remove_in="1.3.0",
                )
            )
        if "base_url" not in api:
            raise ValueError("source.api requires 'base_url'")
        if "endpoint" not in api:
            raise ValueError("source.api requires 'endpoint'")

    run_cfg = source.get("run", {})
    pattern_value = run_cfg.get("load_pattern", LoadPattern.SNAPSHOT.value)
    pattern = LoadPattern.normalize(pattern_value)
    run_cfg["load_pattern"] = pattern.value

    if "max_file_size_mb" in run_cfg:
        max_size = run_cfg["max_file_size_mb"]
        if not isinstance(max_size, (int, float)) or max_size <= 0:
            raise ValueError("source.run.max_file_size_mb must be a positive number")

    if "parallel_workers" in run_cfg:
        workers = run_cfg["parallel_workers"]
        if not isinstance(workers, int) or workers < 1:
            raise ValueError(
                "source.run.parallel_workers must be a positive integer (1 or greater)"
            )
        if workers > 32:
            logger.warning(
                f"source.run.parallel_workers={workers} is very high and may cause resource issues"
            )

    reference_mode = run_cfg.get("reference_mode")
    if reference_mode is not None:
        if not isinstance(reference_mode, dict):
            raise ValueError("source.run.reference_mode must be a dictionary")
        enabled = reference_mode.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("source.run.reference_mode.enabled must be a boolean")
        role = reference_mode.get("role", "reference")
        if role not in {"reference", "delta", "auto"}:
            raise ValueError(
                "source.run.reference_mode.role must be one of 'reference', 'delta', or 'auto'"
            )
        cadence = reference_mode.get("cadence_days")
        if cadence is not None:
            if not isinstance(cadence, int) or cadence <= 0:
                raise ValueError(
                    "source.run.reference_mode.cadence_days must be a positive integer"
                )
        delta_patterns = reference_mode.get("delta_patterns", [])
        if delta_patterns and not all(isinstance(p, str) for p in delta_patterns):
            raise ValueError(
                "source.run.reference_mode.delta_patterns must be a list of strings"
            )
        run_cfg["reference_mode"] = {
            "enabled": enabled,
            "role": role,
            "cadence_days": cadence,
            "delta_patterns": delta_patterns,
        }

    # Validate late_data config per spec Section 4
    late_data = run_cfg.get("late_data")
    if late_data is not None:
        if not isinstance(late_data, dict):
            raise ValueError("source.run.late_data must be a dictionary")
        mode = late_data.get("mode", "allow")
        valid_late_data_modes = ["allow", "reject", "quarantine"]
        if mode not in valid_late_data_modes:
            raise ValueError(
                f"source.run.late_data.mode must be one of {valid_late_data_modes}, got '{mode}'"
            )
        threshold_days = late_data.get("threshold_days", 7)
        if not isinstance(threshold_days, int) or threshold_days <= 0:
            raise ValueError(
                "source.run.late_data.threshold_days must be a positive integer"
            )
        quarantine_path = late_data.get("quarantine_path", "_quarantine")
        if not isinstance(quarantine_path, str):
            raise ValueError("source.run.late_data.quarantine_path must be a string")
        timestamp_column = late_data.get("timestamp_column")
        if timestamp_column is not None and not isinstance(timestamp_column, str):
            raise ValueError(
                "source.run.late_data.timestamp_column must be a string when provided"
            )

    # Validate backfill config per spec Section 4
    backfill = run_cfg.get("backfill")
    if backfill is not None:
        if not isinstance(backfill, dict):
            raise ValueError("source.run.backfill must be a dictionary")
        start_date = backfill.get("start_date")
        end_date = backfill.get("end_date")
        if start_date is None or end_date is None:
            raise ValueError(
                "source.run.backfill requires both 'start_date' and 'end_date'"
            )
        # Validate date format
        from datetime import date as date_cls
        if isinstance(start_date, str):
            try:
                date_cls.fromisoformat(start_date)
            except ValueError:
                raise ValueError(
                    "source.run.backfill.start_date must be a valid ISO date (YYYY-MM-DD)"
                )
        if isinstance(end_date, str):
            try:
                date_cls.fromisoformat(end_date)
            except ValueError:
                raise ValueError(
                    "source.run.backfill.end_date must be a valid ISO date (YYYY-MM-DD)"
                )
        force_full = backfill.get("force_full", False)
        if not isinstance(force_full, bool):
            raise ValueError("source.run.backfill.force_full must be a boolean")

    partition_strategy = (
        platform.get("bronze", {})
        .get("partitioning", {})
        .get("partition_strategy", "date")
    )
    valid_strategies = ["date", "hourly", "timestamp", "batch_id"]
    if partition_strategy not in valid_strategies:
        raise ValueError(
            f"platform.bronze.partitioning.partition_strategy must be one of {valid_strategies}"
        )

    # Validate quality_rules config per spec Section 7
    quality_rules = cfg.get("quality_rules", [])
    if quality_rules:
        if not isinstance(quality_rules, list):
            raise ValueError("quality_rules must be a list")
        valid_rule_levels = ["error", "warn"]
        for i, rule in enumerate(quality_rules):
            if not isinstance(rule, dict):
                raise ValueError(f"quality_rules[{i}] must be a dictionary")
            if "id" not in rule:
                raise ValueError(f"quality_rules[{i}] requires 'id'")
            if "expression" not in rule:
                raise ValueError(f"quality_rules[{i}] requires 'expression'")
            rule_level = rule.get("level", "error").lower()
            if rule_level not in valid_rule_levels:
                raise ValueError(
                    f"quality_rules[{i}].level must be one of {valid_rule_levels}, got '{rule_level}'"
                )

    # Validate schema config per spec Section 6
    schema_cfg = cfg.get("schema")
    if schema_cfg is not None:
        if not isinstance(schema_cfg, dict):
            raise ValueError("schema must be a dictionary")
        expected_columns = schema_cfg.get("expected_columns", [])
        if expected_columns:
            if not isinstance(expected_columns, list):
                raise ValueError("schema.expected_columns must be a list")
            valid_column_types = [
                "string", "integer", "bigint", "decimal", "float", "double",
                "boolean", "date", "timestamp", "datetime", "binary", "array", "map", "struct", "any"
            ]
            for i, col in enumerate(expected_columns):
                if not isinstance(col, dict):
                    raise ValueError(f"schema.expected_columns[{i}] must be a dictionary")
                if "name" not in col:
                    raise ValueError(f"schema.expected_columns[{i}] requires 'name'")
                col_type = col.get("type", "string").lower()
                if col_type not in valid_column_types:
                    raise ValueError(
                        f"schema.expected_columns[{i}].type must be one of {valid_column_types}, got '{col_type}'"
                    )
                if "nullable" in col and not isinstance(col["nullable"], bool):
                    raise ValueError(f"schema.expected_columns[{i}].nullable must be a boolean")
                if "primary_key" in col and not isinstance(col["primary_key"], bool):
                    raise ValueError(f"schema.expected_columns[{i}].primary_key must be a boolean")
        primary_keys = schema_cfg.get("primary_keys", [])
        if primary_keys and not isinstance(primary_keys, list):
            raise ValueError("schema.primary_keys must be a list")
        partition_columns = schema_cfg.get("partition_columns", [])
        if partition_columns and not isinstance(partition_columns, list):
            raise ValueError("schema.partition_columns must be a list")

    normalized_silver = _normalize_silver_config(cfg.get("silver"), source, pattern)
    run_cfg["silver"] = normalized_silver
    cfg["silver"] = normalized_silver

    source.setdefault("config_name", f"{source['system']}.{source['table']}")
    logger.info(
        "Successfully validated config for %s.%s", source["system"], source["table"]
    )
    return cfg
