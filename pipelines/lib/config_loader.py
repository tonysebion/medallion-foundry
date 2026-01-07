"""YAML configuration loader for Bronze-Foundry pipelines.

Allows non-Python developers to define pipelines using simple YAML files.

Example YAML (retail_orders.yaml):
    bronze:
      system: retail
      entity: orders
      source_type: file_csv
      source_path: "./data/orders_{run_date}.csv"

    silver:
      natural_keys: [order_id]
      change_timestamp: updated_at
      attributes: [customer_id, order_total, status]

Usage:
    # Command line
    bronze-foundry run ./pipelines/retail_orders.yaml --date 2025-01-15

    # Python API
    from pipelines.lib.config_loader import load_pipeline
    pipeline = load_pipeline("./pipelines/retail_orders.yaml")
    result = pipeline.run("2025-01-15")
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

from pipelines.lib.bronze import BronzeSource, InputMode, LoadPattern, SourceType
from pipelines.lib.silver import (
    DeleteMode,
    EntityKind,
    HistoryMode,
    SilverEntity,
    SilverModel,
    SILVER_MODEL_PRESETS,
)
from pipelines.lib.validate import LoggingConfig

logger = logging.getLogger(__name__)

__all__ = [
    "load_pipeline",
    "load_bronze_from_yaml",
    "load_silver_from_yaml",
    "load_logging_from_yaml",
    "validate_yaml_config",
    "YAMLConfigError",
    "load_with_inheritance",
]


class YAMLConfigError(Exception):
    """Error in YAML pipeline configuration."""

    pass


# Mapping from YAML string values to enum types
SOURCE_TYPE_MAP = {
    "file_csv": SourceType.FILE_CSV,
    "file_parquet": SourceType.FILE_PARQUET,
    "file_space_delimited": SourceType.FILE_SPACE_DELIMITED,
    "file_fixed_width": SourceType.FILE_FIXED_WIDTH,
    "file_json": SourceType.FILE_JSON,
    "file_jsonl": SourceType.FILE_JSONL,
    "file_excel": SourceType.FILE_EXCEL,
    "database_mssql": SourceType.DATABASE_MSSQL,
    "database_postgres": SourceType.DATABASE_POSTGRES,
    "database_mysql": SourceType.DATABASE_MYSQL,
    "database_db2": SourceType.DATABASE_DB2,
    "api_rest": SourceType.API_REST,
}

LOAD_PATTERN_MAP = {
    "full_snapshot": LoadPattern.FULL_SNAPSHOT,
    "incremental": LoadPattern.INCREMENTAL_APPEND,
    "incremental_append": LoadPattern.INCREMENTAL_APPEND,
    "cdc": LoadPattern.CDC,
}

INPUT_MODE_MAP = {
    "replace_daily": InputMode.REPLACE_DAILY,
    "append_log": InputMode.APPEND_LOG,
}

ENTITY_KIND_MAP = {
    "state": EntityKind.STATE,
    "event": EntityKind.EVENT,
}

HISTORY_MODE_MAP = {
    "current_only": HistoryMode.CURRENT_ONLY,
    "scd1": HistoryMode.CURRENT_ONLY,
    "full_history": HistoryMode.FULL_HISTORY,
    "scd2": HistoryMode.FULL_HISTORY,
}

SILVER_MODEL_MAP = {
    "periodic_snapshot": SilverModel.PERIODIC_SNAPSHOT,
    "full_merge_dedupe": SilverModel.FULL_MERGE_DEDUPE,
    "incremental_merge": SilverModel.INCREMENTAL_MERGE,
    "scd_type_2": SilverModel.SCD_TYPE_2,
    "event_log": SilverModel.EVENT_LOG,
}

DELETE_MODE_MAP = {
    "ignore": DeleteMode.IGNORE,
    "tombstone": DeleteMode.TOMBSTONE,
    "hard_delete": DeleteMode.HARD_DELETE,
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dictionaries, with override taking precedence.

    Args:
        base: Base dictionary (parent config)
        override: Override dictionary (child config)

    Returns:
        New dictionary with merged values
    """
    result = dict(base)

    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            # Recursively merge nested dicts
            result[key] = _deep_merge(result[key], value)
        else:
            # Override the value
            result[key] = value

    return result


def load_with_inheritance(
    config_path: Union[str, Path],
) -> tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """Load a YAML config, resolving parent inheritance via 'extends' key.

    Child configs can extend parent configs using:
        extends: ./base_config.yaml

    The child config is deep-merged with the parent, with child values
    taking precedence over parent values.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Tuple of (merged_config, parent_config_or_none)

    Raises:
        FileNotFoundError: If config or parent file doesn't exist
        YAMLConfigError: If YAML is invalid

    Example YAML:
        # child.yaml
        extends: ./base.yaml

        bronze:
          entity: specific_table  # Override parent's entity
          # system, host, database inherited from parent
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        try:
            child_config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise YAMLConfigError(f"Invalid YAML syntax in {config_path}: {e}")

    if "extends" not in child_config:
        return child_config, None

    # Resolve parent path relative to child config file
    parent_ref = child_config["extends"]
    parent_path = (config_path.parent / parent_ref).resolve()

    if not parent_path.exists():
        raise FileNotFoundError(
            f"Parent config not found: {parent_path} "
            f"(referenced from {config_path})"
        )

    with open(parent_path, "r", encoding="utf-8") as f:
        try:
            parent_config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise YAMLConfigError(f"Invalid YAML syntax in {parent_path}: {e}")

    # Deep merge: child values override parent values
    merged = _deep_merge(parent_config, child_config)

    # Remove the 'extends' key from the final merged config
    merged.pop("extends", None)

    logger.debug(
        "Loaded config with inheritance",
        extra={
            "child_path": str(config_path),
            "parent_path": str(parent_path),
        }
    )

    return merged, parent_config


def load_logging_from_yaml(
    config: Dict[str, Any],
    config_dir: Optional[Path] = None,
) -> LoggingConfig:
    """Create a LoggingConfig from YAML configuration.

    Args:
        config: Dictionary from parsed YAML (the 'logging' section)
        config_dir: Directory containing the YAML file (for relative path resolution)

    Returns:
        Configured LoggingConfig instance

    Example YAML:
        logging:
          level: INFO
          format: json          # 'json' for Splunk, 'console' for human-readable
          file: ./logs/pipeline.log  # Optional file output
          console: true         # Also output to console
    """
    config_dir = config_dir or Path.cwd()

    # Resolve file path if provided
    log_file = config.get("file")
    if log_file:
        log_file = _resolve_path(log_file, config_dir)

    # Build LoggingConfig using Pydantic validation
    try:
        return LoggingConfig(
            level=config.get("level", "INFO"),
            format=config.get("format", "console"),
            file=log_file,
            console=config.get("console", True),
            include_timestamp=config.get("include_timestamp", True),
            include_source=config.get("include_source", True),
        )
    except ValueError as e:
        raise YAMLConfigError(f"Invalid logging configuration: {e}")


def _resolve_path(path: str, config_dir: Path) -> str:
    """Resolve relative paths based on config file location.

    Paths starting with "./" are resolved relative to the YAML config file.
    Paths with environment variables like ${VAR} are left as-is for runtime expansion.
    Absolute paths and paths starting with s3:// or abfs:// are unchanged.
    """
    if not path:
        return path

    # Cloud paths are unchanged
    if path.startswith(("s3://", "abfs://", "http://", "https://")):
        return path

    # Absolute paths are unchanged
    if os.path.isabs(path):
        return path

    # Relative paths: resolve from config file directory
    if path.startswith("./") or path.startswith("../"):
        resolved = config_dir / path
        return str(resolved)

    # Other paths (like templates with {run_date}) - leave as-is
    return path


def load_bronze_from_yaml(
    config: Dict[str, Any],
    config_dir: Optional[Path] = None,
) -> BronzeSource:
    """Create a BronzeSource from YAML configuration.

    Args:
        config: Dictionary from parsed YAML (the 'bronze' section)
        config_dir: Directory containing the YAML file (for relative path resolution)

    Returns:
        Configured BronzeSource instance

    Raises:
        YAMLConfigError: If configuration is invalid
    """
    config_dir = config_dir or Path.cwd()

    # Required fields
    if "system" not in config:
        raise YAMLConfigError("bronze.system is required")
    if "entity" not in config:
        raise YAMLConfigError("bronze.entity is required")
    if "source_type" not in config:
        raise YAMLConfigError("bronze.source_type is required")

    # Convert source_type string to enum
    source_type_str = config["source_type"].lower()
    if source_type_str not in SOURCE_TYPE_MAP:
        valid = ", ".join(sorted(SOURCE_TYPE_MAP.keys()))
        raise YAMLConfigError(
            f"Invalid source_type '{config['source_type']}'. "
            f"Valid options: {valid}"
        )
    source_type = SOURCE_TYPE_MAP[source_type_str]

    # Convert load_pattern string to enum (optional)
    load_pattern = LoadPattern.FULL_SNAPSHOT
    if "load_pattern" in config:
        pattern_str = config["load_pattern"].lower()
        if pattern_str not in LOAD_PATTERN_MAP:
            valid = ", ".join(sorted(LOAD_PATTERN_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid load_pattern '{config['load_pattern']}'. "
                f"Valid options: {valid}"
            )
        load_pattern = LOAD_PATTERN_MAP[pattern_str]

    # Convert input_mode string to enum (optional)
    input_mode = None
    if "input_mode" in config:
        mode_str = config["input_mode"].lower()
        if mode_str not in INPUT_MODE_MAP:
            valid = ", ".join(sorted(INPUT_MODE_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid input_mode '{config['input_mode']}'. "
                f"Valid options: {valid}"
            )
        input_mode = INPUT_MODE_MAP[mode_str]

    # Resolve source_path relative to config file
    source_path = config.get("source_path", "")
    if source_path:
        source_path = _resolve_path(source_path, config_dir)

    # Resolve target_path relative to config file
    target_path = config.get("target_path", "")
    if target_path:
        target_path = _resolve_path(target_path, config_dir)

    # Build options dict from YAML
    options = dict(config.get("options", {}))

    # Build BronzeSource
    return BronzeSource(
        system=config["system"],
        entity=config["entity"],
        source_type=source_type,
        source_path=source_path,
        target_path=target_path,
        load_pattern=load_pattern,
        input_mode=input_mode,
        watermark_column=config.get("watermark_column"),
        connection=config.get("connection"),
        host=config.get("host"),
        database=config.get("database"),
        query=config.get("query"),
        options=options,
        partition_by=config.get("partition_by", ["_load_date"]),
        chunk_size=config.get("chunk_size"),
        full_refresh_days=config.get("full_refresh_days"),
        write_checksums=config.get("write_checksums", True),
        write_metadata=config.get("write_metadata", True),
    )


def load_silver_from_yaml(
    config: Dict[str, Any],
    config_dir: Optional[Path] = None,
    bronze: Optional[BronzeSource] = None,
) -> SilverEntity:
    """Create a SilverEntity from YAML configuration.

    Args:
        config: Dictionary from parsed YAML (the 'silver' section)
        config_dir: Directory containing the YAML file (for relative path resolution)
        bronze: Optional BronzeSource for auto-wiring source_path

    Returns:
        Configured SilverEntity instance

    Raises:
        YAMLConfigError: If configuration is invalid
    """
    config_dir = config_dir or Path.cwd()

    # Required fields
    if "natural_keys" not in config:
        raise YAMLConfigError("silver.natural_keys is required")
    if "change_timestamp" not in config:
        raise YAMLConfigError("silver.change_timestamp is required")

    # Ensure natural_keys is a list
    natural_keys = config["natural_keys"]
    if isinstance(natural_keys, str):
        natural_keys = [natural_keys]

    # Expand model preset if specified (before parsing individual settings)
    # Model provides defaults; explicit settings can override them
    model_defaults: Dict[str, str] = {}
    if "model" in config:
        model_str = config["model"].lower()
        if model_str not in SILVER_MODEL_MAP:
            valid = ", ".join(sorted(SILVER_MODEL_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid model '{config['model']}'. "
                f"Valid options: {valid}"
            )
        model_defaults = SILVER_MODEL_PRESETS[model_str]
        logger.debug(
            "Expanding Silver model preset",
            extra={"model": model_str, "defaults": model_defaults}
        )

    # Convert entity_kind string to enum (from config or model preset)
    entity_kind_str = config.get("entity_kind") or model_defaults.get("entity_kind") or "state"
    entity_kind_str = entity_kind_str.lower()
    if entity_kind_str not in ENTITY_KIND_MAP:
        valid = ", ".join(sorted(ENTITY_KIND_MAP.keys()))
        raise YAMLConfigError(
            f"Invalid entity_kind '{entity_kind_str}'. "
            f"Valid options: {valid}"
        )
    entity_kind = ENTITY_KIND_MAP[entity_kind_str]

    # Convert history_mode string to enum (from config or model preset)
    history_mode_str = config.get("history_mode") or model_defaults.get("history_mode") or "current_only"
    history_mode_str = history_mode_str.lower()
    if history_mode_str not in HISTORY_MODE_MAP:
        valid = ", ".join(sorted(HISTORY_MODE_MAP.keys()))
        raise YAMLConfigError(
            f"Invalid history_mode '{history_mode_str}'. "
            f"Valid options: {valid}"
        )
    history_mode = HISTORY_MODE_MAP[history_mode_str]

    # Resolve source_path
    source_path = config.get("source_path", "")
    if source_path:
        source_path = _resolve_path(source_path, config_dir)

    # Resolve target_path
    target_path = config.get("target_path", "")
    if target_path:
        target_path = _resolve_path(target_path, config_dir)

    # Handle attributes - can be list or None
    attributes = config.get("attributes")
    if isinstance(attributes, str):
        attributes = [attributes]

    # Handle exclude_columns - can be list or None
    exclude_columns = config.get("exclude_columns")
    if isinstance(exclude_columns, str):
        exclude_columns = [exclude_columns]

    # Handle partition_by
    partition_by = config.get("partition_by")
    if isinstance(partition_by, str):
        partition_by = [partition_by]

    # Handle output_formats
    output_formats = config.get("output_formats", ["parquet"])
    if isinstance(output_formats, str):
        output_formats = [output_formats]

    # Get system and entity from config or infer from bronze
    system = config.get("system", "")
    entity = config.get("entity", "")

    # Convert input_mode string to enum (from config, model preset, or auto-wired from Bronze)
    input_mode = None
    input_mode_str = config.get("input_mode") or model_defaults.get("input_mode")
    if input_mode_str:
        input_mode_str = input_mode_str.lower()
        if input_mode_str not in INPUT_MODE_MAP:
            valid = ", ".join(sorted(INPUT_MODE_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid input_mode '{input_mode_str}'. "
                f"Valid options: {valid}"
            )
        input_mode = INPUT_MODE_MAP[input_mode_str]

    # Convert delete_mode string to enum (for CDC processing)
    delete_mode = DeleteMode.IGNORE  # Default to ignoring deletes
    if "delete_mode" in config:
        delete_mode_str = config["delete_mode"].lower()
        if delete_mode_str not in DELETE_MODE_MAP:
            valid = ", ".join(sorted(DELETE_MODE_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid delete_mode '{config['delete_mode']}'. "
                f"Valid options: {valid}"
            )
        delete_mode = DELETE_MODE_MAP[delete_mode_str]

    # Parse cdc_options (for CDC load pattern)
    cdc_options = None
    if "cdc_options" in config:
        cdc_options = config["cdc_options"]
        if not isinstance(cdc_options, dict):
            raise YAMLConfigError("cdc_options must be an object")
        if "operation_column" not in cdc_options:
            raise YAMLConfigError("cdc_options.operation_column is required")

    # Auto-wire from Bronze if not specified in Silver config
    storage_options = None
    if bronze:
        if not system:
            system = bronze.system
        if not entity:
            entity = bronze.entity
        # Auto-wire storage options from Bronze (for S3-compatible storage)
        # This passes s3_signature_version, s3_addressing_style, etc.
        if bronze.options:
            storage_keys = [
                "s3_signature_version",
                "s3_addressing_style",
                "endpoint_url",
                "key",
                "secret",
                "region",
            ]
            storage_options = {
                k: v for k, v in bronze.options.items() if k in storage_keys
            }
            if not storage_options:
                storage_options = None

    # Build SilverEntity
    return SilverEntity(
        natural_keys=natural_keys,
        change_timestamp=config["change_timestamp"],
        system=system,
        entity=entity,
        source_path=source_path,
        target_path=target_path,
        attributes=attributes,
        exclude_columns=exclude_columns,
        entity_kind=entity_kind,
        history_mode=history_mode,
        input_mode=input_mode,
        delete_mode=delete_mode,
        cdc_options=cdc_options,
        partition_by=partition_by,
        output_formats=output_formats,
        parquet_compression=config.get("parquet_compression", "snappy"),
        validate_source=config.get("validate_source", "skip"),
        storage_options=storage_options,
    )


def load_pipeline(
    config_path: Union[str, Path],
) -> "PipelineFromYAML":
    """Load a pipeline from a YAML configuration file.

    Supports parent-child inheritance via the 'extends' key:
        extends: ./base_config.yaml

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        PipelineFromYAML instance with run() methods

    Raises:
        YAMLConfigError: If configuration is invalid
        FileNotFoundError: If config file doesn't exist

    Example:
        pipeline = load_pipeline("./pipelines/retail_orders.yaml")
        result = pipeline.run("2025-01-15")

        # Child config inheriting from parent:
        # child.yaml:
        #   extends: ./base.yaml
        #   bronze:
        #     entity: specific_table  # Override
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config_dir = config_path.parent.resolve()

    # Load with inheritance support
    config, parent_config = load_with_inheritance(config_path)

    if parent_config:
        logger.info(
            "Loaded config with inheritance from parent",
            extra={"config_path": str(config_path)}
        )

    if not config:
        raise YAMLConfigError("Empty configuration file")

    # Parse logging section (optional)
    logging_config = None
    if "logging" in config:
        logging_config = load_logging_from_yaml(config["logging"], config_dir)

    # Parse bronze section
    bronze = None
    if "bronze" in config:
        bronze = load_bronze_from_yaml(config["bronze"], config_dir)

    # Parse silver section
    silver = None
    if "silver" in config:
        silver = load_silver_from_yaml(config["silver"], config_dir, bronze)

    if not bronze and not silver:
        raise YAMLConfigError(
            "Configuration must have at least 'bronze' or 'silver' section"
        )

    # Create and return pipeline wrapper
    return PipelineFromYAML(
        bronze=bronze,
        silver=silver,
        config_path=config_path,
        config=config,
        logging_config=logging_config,
    )


def validate_yaml_config(config_path: Union[str, Path]) -> List[str]:
    """Validate a YAML configuration file without creating the pipeline.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        List of validation errors (empty if valid)
    """
    errors: List[str] = []

    try:
        pipeline = load_pipeline(config_path)

        # Validate bronze
        if pipeline.bronze:
            bronze_issues = pipeline.bronze.validate(check_connectivity=False)
            errors.extend(f"bronze: {issue}" for issue in bronze_issues)

        # Validate silver
        if pipeline.silver:
            silver_issues = pipeline.silver.validate(check_source=False)
            errors.extend(f"silver: {issue}" for issue in silver_issues)

    except YAMLConfigError as e:
        errors.append(str(e))
    except FileNotFoundError as e:
        errors.append(str(e))
    except ValueError as e:
        errors.append(str(e))

    return errors


class PipelineFromYAML:
    """Pipeline wrapper created from YAML configuration.

    Provides the same interface as the Python Pipeline class.
    """

    def __init__(
        self,
        bronze: Optional[BronzeSource],
        silver: Optional[SilverEntity],
        config_path: Path,
        config: Dict[str, Any],
        logging_config: Optional[LoggingConfig] = None,
    ):
        self.bronze = bronze
        self.silver = silver
        self.config_path = config_path
        self.config = config
        self.logging_config = logging_config

        # If both bronze and silver are present, wire them together
        if bronze and silver and not silver.source_path:
            # Auto-wire Silver source from Bronze target
            silver.source_path = bronze.target_path.rstrip("/") + "/*.parquet"

        if bronze and silver and not silver.target_path:
            # Auto-generate Silver target using Hive-style partitioning for consistency
            silver.target_path = f"./silver/system={bronze.system}/entity={bronze.entity}/"

        # Auto-wire input_mode from Bronze to Silver if not explicitly set
        if bronze and silver and silver.input_mode is None and bronze.input_mode is not None:
            silver.input_mode = bronze.input_mode

    def setup_logging(self) -> None:
        """Configure logging based on the YAML logging config.

        This initializes structlog with settings from the logging section
        of the pipeline YAML file. Call this before running the pipeline
        to enable proper structured logging.

        If no logging config is present, this is a no-op.

        Example:
            pipeline = load_pipeline("./my_pipeline.yaml")
            pipeline.setup_logging()  # Configure logging from YAML
            result = pipeline.run("2025-01-15")
        """
        if not self.logging_config:
            return

        from pipelines.lib.observability import setup_structlog

        setup_structlog(
            verbose=(self.logging_config.level == "DEBUG"),
            json_format=(self.logging_config.format == "json"),
            log_file=self.logging_config.file,
        )

    @property
    def name(self) -> str:
        """Get pipeline name from config or derive from file."""
        if "name" in self.config:
            return self.config["name"]
        if self.bronze:
            return f"{self.bronze.system}.{self.bronze.entity}"
        return self.config_path.stem

    def run(
        self,
        run_date: str,
        *,
        dry_run: bool = False,
        target_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run the full Bronze -> Silver pipeline.

        Args:
            run_date: The date for this pipeline run (YYYY-MM-DD format)
            dry_run: If True, validate without executing
            target_override: Override target paths for local development

        Returns:
            Dictionary with results from both layers
        """
        result: Dict[str, Any] = {}

        if self.bronze:
            result["bronze"] = self.bronze.run(
                run_date,
                dry_run=dry_run,
                target_override=target_override,
            )

        if self.silver:
            result["silver"] = self.silver.run(
                run_date,
                dry_run=dry_run,
                target_override=target_override,
            )

        return result

    def run_bronze(
        self,
        run_date: str,
        *,
        dry_run: bool = False,
        target_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run only the Bronze extraction layer.

        Args:
            run_date: The date for this pipeline run
            dry_run: If True, validate without executing
            target_override: Override target path

        Returns:
            Bronze layer results
        """
        if not self.bronze:
            raise ValueError("This pipeline has no bronze layer defined")

        return self.bronze.run(
            run_date,
            dry_run=dry_run,
            target_override=target_override,
        )

    def run_silver(
        self,
        run_date: str,
        *,
        dry_run: bool = False,
        target_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run only the Silver curation layer.

        Args:
            run_date: The date for this pipeline run
            dry_run: If True, validate without executing
            target_override: Override target path

        Returns:
            Silver layer results
        """
        if not self.silver:
            raise ValueError("This pipeline has no silver layer defined")

        return self.silver.run(
            run_date,
            dry_run=dry_run,
            target_override=target_override,
        )

    def validate(self, run_date: Optional[str] = None) -> List[str]:
        """Validate the pipeline configuration.

        Args:
            run_date: Optional date for path validation

        Returns:
            List of validation issues (empty if valid)
        """
        issues: List[str] = []

        if self.bronze:
            bronze_issues = self.bronze.validate(run_date, check_connectivity=False)
            issues.extend(f"bronze: {issue}" for issue in bronze_issues)

        if self.silver:
            silver_issues = self.silver.validate(run_date, check_source=False)
            issues.extend(f"silver: {issue}" for issue in silver_issues)

        return issues

    def explain(self) -> str:
        """Return a human-readable explanation of what this pipeline does."""
        lines = [
            f"Pipeline: {self.name}",
            f"Config:   {self.config_path}",
            "",
        ]

        if self.bronze:
            input_mode_str = self.bronze.input_mode.value if self.bronze.input_mode else "(not set)"
            lines.extend([
                "BRONZE LAYER:",
                f"  System:       {self.bronze.system}",
                f"  Entity:       {self.bronze.entity}",
                f"  Source Type:  {self.bronze.source_type.value}",
                f"  Source Path:  {self.bronze.source_path or '(from database)'}",
                f"  Target Path:  {self.bronze.target_path}",
                f"  Load Pattern: {self.bronze.load_pattern.value}",
                f"  Input Mode:   {input_mode_str}",
                "",
            ])

        if self.silver:
            lines.extend([
                "SILVER LAYER:",
                f"  Natural Keys: {', '.join(self.silver.natural_keys)}",
                f"  Change Col:   {self.silver.change_timestamp}",
                f"  Entity Kind:  {self.silver.entity_kind.value}",
                f"  History Mode: {self.silver.history_mode.value}",
                f"  Source Path:  {self.silver.source_path}",
                f"  Target Path:  {self.silver.target_path}",
                "",
            ])

        return "\n".join(lines)
