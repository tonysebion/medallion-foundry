from typing import Dict, Any, List
from pathlib import Path
import datetime as dt
import logging
import copy

import yaml

from core.patterns import LoadPattern

logger = logging.getLogger(__name__)


def _read_yaml(path: str) -> Dict[str, Any]:
    logger.info(f"Loading config from {path}")

    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in config file: {e}")

    if not isinstance(cfg, dict):
        raise ValueError("Config must be a YAML dictionary/object")

    return cfg


def load_config(path: str) -> Dict[str, Any]:
    """Load and validate a YAML config file with a single source."""
    cfg = _read_yaml(path)
    if "sources" in cfg:
        raise ValueError("Config contains multiple sources; use load_configs() instead.")
    return _validate_config(cfg)

    # Validate top-level sections
    if "platform" not in cfg:
        raise ValueError("Config must contain a 'platform' section")
    if "source" not in cfg:
        raise ValueError("Config must contain a 'source' section")

    # Validate platform section
    platform = cfg["platform"]
    if not isinstance(platform, dict):
        raise ValueError("'platform' must be a dictionary")
    
    if "bronze" not in platform:
        raise ValueError("Missing platform.bronze section in config")
    
    # Validate bronze section
    bronze = platform["bronze"]
    if not isinstance(bronze, dict):
        raise ValueError("'platform.bronze' must be a dictionary")
    
    # Validate storage backend (optional, defaults to s3)
    storage_backend = bronze.get("storage_backend", "s3")
    valid_backends = ["s3", "azure", "gcs", "local"]
    if storage_backend not in valid_backends:
        raise ValueError(f"platform.bronze.storage_backend must be one of {valid_backends}")
    
    # Backend-specific validation
    if storage_backend == "s3":
        # S3 requires s3_connection section
        if "s3_connection" not in platform:
            raise ValueError("Missing platform.s3_connection section (required for S3 backend)")
        
        required_bronze_keys = ["s3_bucket", "s3_prefix"]
        for key in required_bronze_keys:
            if key not in bronze:
                raise ValueError(f"Missing required key 'platform.bronze.{key}' in config")
    
    elif storage_backend == "azure":
        # Azure requires azure_connection section
        if "azure_connection" not in platform:
            raise ValueError("Missing platform.azure_connection section (required for Azure backend)")
        
        if "azure_container" not in bronze:
            raise ValueError("Azure backend requires platform.bronze.azure_container")
    
    elif storage_backend == "gcs":
        # GCS requires gcs_connection section
        if "gcs_connection" not in platform:
            raise ValueError("Missing platform.gcs_connection section (required for GCS backend)")
        
        if "gcs_bucket" not in bronze or "gcs_prefix" not in bronze:
            raise ValueError("GCS backend requires platform.bronze.gcs_bucket and gcs_prefix")
    
    elif storage_backend == "local":
        # Local backend requires local_path
        if "local_path" not in bronze:
            raise ValueError("Local backend requires platform.bronze.local_path")
    
    logger.debug(f"Validated storage backend: {storage_backend}")
    
    # Validate source section
    source = cfg["source"]
    if not isinstance(source, dict):
        raise ValueError("'source' must be a dictionary")
    
    required_source_keys = ["system", "table", "run"]
    for key in required_source_keys:
        if key not in source:
            raise ValueError(f"Missing required key 'source.{key}' in config")
    
    # Validate source type
    source_type = source.get("type", "api")
    valid_source_types = ["api", "db", "custom", "file"]
    if source_type not in valid_source_types:
        raise ValueError(
            f"Invalid source.type: '{source_type}'. Must be one of {valid_source_types}"
        )
    
    # Type-specific validation
    if source_type == "api" and "api" not in source:
        raise ValueError("source.type='api' requires 'source.api' section")
    if source_type == "db" and "db" not in source:
        raise ValueError("source.type='db' requires 'source.db' section")
    if source_type == "custom" and "custom_extractor" not in source:
        raise ValueError("source.type='custom' requires 'source.custom_extractor' section")
    if source_type == "file" and "file" not in source:
        raise ValueError("source.type='file' requires 'source.file' section")
    
    # Validate custom extractor configuration
    if source_type == "custom":
        custom = source["custom_extractor"]
        if "module" not in custom or "class_name" not in custom:
            raise ValueError("source.custom_extractor requires 'module' and 'class_name'")

    # Validate file configuration
    if source_type == "file":
        file_cfg = source["file"]
        if not isinstance(file_cfg, dict):
            raise ValueError("source.file must be a dictionary of options")
        if "path" not in file_cfg:
            raise ValueError("source.file requires 'path' to point to the local dataset")
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
    
    # Validate DB configuration
    if source_type == "db":
        db = source["db"]
        if "conn_str_env" not in db:
            raise ValueError("source.db requires 'conn_str_env' to specify connection string env var")
        if "base_query" not in db:
            raise ValueError("source.db requires 'base_query' for SQL query")
    
    # Validate API configuration
    if source_type == "api":
        api = source["api"]
        if "base_url" not in api:
            raise ValueError("source.api requires 'base_url'")
        if "endpoint" not in api:
            raise ValueError("source.api requires 'endpoint'")
    
    # Validate run configuration options
    run_cfg = source.get("run", {})

    # Coerce load pattern (defaults to full)
    pattern_value = run_cfg.get("load_pattern", LoadPattern.FULL.value)
    pattern = LoadPattern.normalize(pattern_value)
    run_cfg["load_pattern"] = pattern.value
    
    # Validate max_file_size_mb if provided
    if "max_file_size_mb" in run_cfg:
        max_size = run_cfg["max_file_size_mb"]
        if not isinstance(max_size, (int, float)) or max_size <= 0:
            raise ValueError("source.run.max_file_size_mb must be a positive number")
    
    # Validate parallel_workers if provided
    if "parallel_workers" in run_cfg:
        workers = run_cfg["parallel_workers"]
        if not isinstance(workers, int) or workers < 1:
            raise ValueError("source.run.parallel_workers must be a positive integer (1 or greater)")
        if workers > 32:
            logger.warning(f"source.run.parallel_workers={workers} is very high and may cause resource issues")
    
    # Validate partition_strategy if provided
    partition_strategy = platform.get("bronze", {}).get("partitioning", {}).get("partition_strategy", "date")
    valid_strategies = ["date", "hourly", "timestamp", "batch_id"]
    if partition_strategy not in valid_strategies:
        raise ValueError(f"platform.bronze.partitioning.partition_strategy must be one of {valid_strategies}")
    
    # Validate silver section (optional)
    silver_cfg = cfg.get("silver")
    if silver_cfg is None:
        silver_cfg = {}
    elif not isinstance(silver_cfg, dict):
        raise ValueError("'silver' must be a dictionary when provided")

    if "output_dir" in silver_cfg and not isinstance(silver_cfg["output_dir"], str):
        raise ValueError("silver.output_dir must be a string path")

    for flag in ["write_parquet", "write_csv"]:
        if flag in silver_cfg and not isinstance(silver_cfg[flag], bool):
            raise ValueError(f"silver.{flag} must be a boolean")

    if "parquet_compression" in silver_cfg and not isinstance(silver_cfg["parquet_compression"], str):
        raise ValueError("silver.parquet_compression must be a string")

    if "primary_keys" in silver_cfg:
        primary_keys = silver_cfg["primary_keys"]
        if not isinstance(primary_keys, list) or any(not isinstance(pk, str) for pk in primary_keys):
            raise ValueError("silver.primary_keys must be a list of strings")
    else:
        silver_cfg["primary_keys"] = []

    for key in ["order_column", "current_output_name", "history_output_name", "cdc_output_name", "full_output_name"]:
        if key in silver_cfg and silver_cfg[key] is not None and not isinstance(silver_cfg[key], str):
            raise ValueError(f"silver.{key} must be a string when provided")

    silver_cfg.setdefault("output_dir", "./silver_output")
    silver_cfg.setdefault("write_parquet", True)
    silver_cfg.setdefault("write_csv", False)
    silver_cfg.setdefault("parquet_compression", "snappy")
    silver_cfg.setdefault("order_column", None)
    silver_cfg.setdefault("full_output_name", "full_snapshot")
    silver_cfg.setdefault("current_output_name", "current")
    silver_cfg.setdefault("history_output_name", "history")
    silver_cfg.setdefault("cdc_output_name", "cdc_changes")

    cfg["silver"] = silver_cfg
    
    source.setdefault("config_name", source.get("config_name") or f"{source['system']}.{source['table']}")

    logger.info(f"Successfully validated config for {source['system']}.{source['table']}")
    return cfg


def load_configs(path: str) -> List[Dict[str, Any]]:
    """Load a YAML config file and return a list of normalized configs (one per source)."""
    raw = _read_yaml(path)
    if "sources" not in raw:
        return [_validate_config(raw)]

    sources = raw["sources"]
    if not isinstance(sources, list) or not sources:
        raise ValueError("'sources' must be a non-empty list")
    if "source" in raw:
        raise ValueError("Config cannot contain both 'source' and 'sources'")

    platform = raw.get("platform")
    base_silver = raw.get("silver")
    results = []
    for idx, entry in enumerate(sources):
        if not isinstance(entry, dict):
            raise ValueError("Each item in 'sources' must be a dictionary")
        if "source" not in entry:
            raise ValueError("Each item in 'sources' must include a 'source' section")

        merged_cfg: Dict[str, Any] = {
            "platform": copy.deepcopy(platform),
            "source": copy.deepcopy(entry["source"]),
        }

        entry_silver = copy.deepcopy(base_silver) if base_silver else {}
        if "silver" in entry:
            entry_silver = entry_silver or {}
            entry_silver.update(copy.deepcopy(entry["silver"]))
        if entry_silver:
            merged_cfg["silver"] = entry_silver

        name = entry.get("name")
        if name:
            merged_cfg["source"]["config_name"] = name
        merged_cfg["source"]["_source_list_index"] = idx
        results.append(_validate_config(merged_cfg))

    return results


def _validate_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = copy.deepcopy(cfg)

    # Validate top-level sections
    if "platform" not in cfg:
        raise ValueError("Config must contain a 'platform' section")
    if "source" not in cfg:
        raise ValueError("Config must contain a 'source' section")

    # Validate platform section
    platform = cfg["platform"]
    if not isinstance(platform, dict):
        raise ValueError("'platform' must be a dictionary")

    if "bronze" not in platform:
        raise ValueError("Missing platform.bronze section in config")

    # Validate bronze section
    bronze = platform["bronze"]
    if not isinstance(bronze, dict):
        raise ValueError("'platform.bronze' must be a dictionary")

    # Validate storage backend (optional, defaults to s3)
    storage_backend = bronze.get("storage_backend", "s3")
    valid_backends = ["s3", "azure", "gcs", "local"]
    if storage_backend not in valid_backends:
        raise ValueError(f"platform.bronze.storage_backend must be one of {valid_backends}")

    # Backend-specific validation
    if storage_backend == "s3":
        if "s3_connection" not in platform:
            raise ValueError("Missing platform.s3_connection section (required for S3 backend)")

        required_bronze_keys = ["s3_bucket", "s3_prefix"]
        for key in required_bronze_keys:
            if key not in bronze:
                raise ValueError(f"Missing required key 'platform.bronze.{key}' in config")

    elif storage_backend == "azure":
        if "azure_connection" not in platform:
            raise ValueError("Missing platform.azure_connection section (required for Azure backend)")

        if "azure_container" not in bronze:
            raise ValueError("Azure backend requires platform.bronze.azure_container")

    elif storage_backend == "gcs":
        if "gcs_connection" not in platform:
            raise ValueError("Missing platform.gcs_connection section (required for GCS backend)")

        if "gcs_bucket" not in bronze or "gcs_prefix" not in bronze:
            raise ValueError("GCS backend requires platform.bronze.gcs_bucket and gcs_prefix")

    elif storage_backend == "local":
        if "local_path" not in bronze:
            raise ValueError("Local backend requires platform.bronze.local_path")

    logger.debug(f"Validated storage backend: {storage_backend}")

    # Validate source section
    source = cfg["source"]
    if not isinstance(source, dict):
        raise ValueError("'source' must be a dictionary")

    required_source_keys = ["system", "table", "run"]
    for key in required_source_keys:
        if key not in source:
            raise ValueError(f"Missing required key 'source.{key}' in config")

    # Validate source type
    source_type = source.get("type", "api")
    valid_source_types = ["api", "db", "custom", "file"]
    if source_type not in valid_source_types:
        raise ValueError(
            f"Invalid source.type: '{source_type}'. Must be one of {valid_source_types}"
        )

    # Type-specific validation
    if source_type == "api" and "api" not in source:
        raise ValueError("source.type='api' requires 'source.api' section")
    if source_type == "db" and "db" not in source:
        raise ValueError("source.type='db' requires 'source.db' section")
    if source_type == "custom" and "custom_extractor" not in source:
        raise ValueError("source.type='custom' requires 'source.custom_extractor' section")
    if source_type == "file" and "file" not in source:
        raise ValueError("source.type='file' requires 'source.file' section")

    # Validate custom extractor configuration
    if source_type == "custom":
        custom = source["custom_extractor"]
        if "module" not in custom or "class_name" not in custom:
            raise ValueError("source.custom_extractor requires 'module' and 'class_name'")

    # Validate file configuration
    if source_type == "file":
        file_cfg = source["file"]
        if not isinstance(file_cfg, dict):
            raise ValueError("source.file must be a dictionary of options")
        if "path" not in file_cfg:
            raise ValueError("source.file requires 'path' to point to the local dataset")
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

    # Validate DB configuration
    if source_type == "db":
        db = source["db"]
        if "conn_str_env" not in db:
            raise ValueError("source.db requires 'conn_str_env' to specify connection string env var")
        if "base_query" not in db:
            raise ValueError("source.db requires 'base_query' for SQL query")

    # Validate API configuration
    if source_type == "api":
        api = source["api"]
        if "base_url" not in api:
            raise ValueError("source.api requires 'base_url'")
        if "endpoint" not in api:
            raise ValueError("source.api requires 'endpoint'")

    # Validate run configuration options
    run_cfg = source.get("run", {})

    # Coerce load pattern (defaults to full)
    pattern_value = run_cfg.get("load_pattern", LoadPattern.FULL.value)
    pattern = LoadPattern.normalize(pattern_value)
    run_cfg["load_pattern"] = pattern.value

    # Validate max_file_size_mb if provided
    if "max_file_size_mb" in run_cfg:
        max_size = run_cfg["max_file_size_mb"]
        if not isinstance(max_size, (int, float)) or max_size <= 0:
            raise ValueError("source.run.max_file_size_mb must be a positive number")

    # Validate parallel_workers if provided
    if "parallel_workers" in run_cfg:
        workers = run_cfg["parallel_workers"]
        if not isinstance(workers, int) or workers < 1:
            raise ValueError("source.run.parallel_workers must be a positive integer (1 or greater)")
        if workers > 32:
            logger.warning(f"source.run.parallel_workers={workers} is very high and may cause resource issues")

    # Validate partitioning strategy if provided
    partition_strategy = platform.get("bronze", {}).get("partitioning", {}).get("partition_strategy", "date")
    valid_strategies = ["date", "hourly", "timestamp", "batch_id"]
    if partition_strategy not in valid_strategies:
        raise ValueError(f"platform.bronze.partitioning.partition_strategy must be one of {valid_strategies}")

    # Validate silver section (optional)
    silver_cfg = cfg.get("silver")
    if silver_cfg is None:
        silver_cfg = {}
    elif not isinstance(silver_cfg, dict):
        raise ValueError("'silver' must be a dictionary when provided")

    if "output_dir" in silver_cfg and not isinstance(silver_cfg["output_dir"], str):
        raise ValueError("silver.output_dir must be a string path")

    for flag in ["write_parquet", "write_csv"]:
        if flag in silver_cfg and not isinstance(silver_cfg[flag], bool):
            raise ValueError(f"silver.{flag} must be a boolean")

    if "parquet_compression" in silver_cfg and not isinstance(silver_cfg["parquet_compression"], str):
        raise ValueError("silver.parquet_compression must be a string")

    if "primary_keys" in silver_cfg:
        primary_keys = silver_cfg["primary_keys"]
        if not isinstance(primary_keys, list) or any(not isinstance(pk, str) for pk in primary_keys):
            raise ValueError("silver.primary_keys must be a list of strings")
    else:
        silver_cfg["primary_keys"] = []

    for key in ["order_column", "current_output_name", "history_output_name", "cdc_output_name", "full_output_name"]:
        if key in silver_cfg and silver_cfg[key] is not None and not isinstance(silver_cfg[key], str):
            raise ValueError(f"silver.{key} must be a string when provided")

    silver_cfg.setdefault("output_dir", "./silver_output")
    silver_cfg.setdefault("write_parquet", True)
    silver_cfg.setdefault("write_csv", False)
    silver_cfg.setdefault("parquet_compression", "snappy")
    silver_cfg.setdefault("order_column", None)
    silver_cfg.setdefault("full_output_name", "full_snapshot")
    silver_cfg.setdefault("current_output_name", "current")
    silver_cfg.setdefault("history_output_name", "history")
    silver_cfg.setdefault("cdc_output_name", "cdc_changes")

    cfg["silver"] = silver_cfg

    source.setdefault("config_name", source.get("config_name") or f"{source['system']}.{source['table']}")

    logger.info(f"Successfully validated config for {source['system']}.{source['table']}")
    return cfg


def build_relative_path(cfg: Dict[str, Any], run_date: dt.date) -> str:
    """Build the relative Bronze path for a given config and run date."""
    platform_cfg = cfg["platform"]
    source_cfg = cfg["source"]

    bronze = platform_cfg["bronze"]
    partitioning = bronze.get("partitioning", {})
    use_dt = partitioning.get("use_dt_partition", True)
    
    # Support for multiple daily loads with additional partition strategies
    partition_strategy = partitioning.get("partition_strategy", "date")  # date, hourly, timestamp, batch_id
    
    system = source_cfg["system"]
    table = source_cfg["table"]

    base_path = f"system={system}/table={table}/"

    run_cfg = source_cfg.get("run", {})
    load_pattern = run_cfg.get("load_pattern", LoadPattern.FULL.value)
    if load_pattern:
        base_path += f"pattern={load_pattern}/"
    
    if not use_dt:
        return base_path
    
    # Build partition path based on strategy
    if partition_strategy == "date":
        # Traditional daily partition
        return f"{base_path}dt={run_date.isoformat()}/"
    
    elif partition_strategy == "hourly":
        # Hourly partitions for multiple daily loads
        from datetime import datetime
        current_hour = datetime.now().strftime("%H")
        return f"{base_path}dt={run_date.isoformat()}/hour={current_hour}/"
    
    elif partition_strategy == "timestamp":
        # Timestamp-based partitions (down to minute)
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        return f"{base_path}dt={run_date.isoformat()}/batch={timestamp}/"
    
    elif partition_strategy == "batch_id":
        # Custom batch ID from config or auto-generated
        from datetime import datetime
        import uuid
        batch_id = source_cfg.get("run", {}).get("batch_id") or f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        return f"{base_path}dt={run_date.isoformat()}/batch_id={batch_id}/"
    
    else:
        logger.warning(f"Unknown partition_strategy '{partition_strategy}', falling back to date")
        return f"{base_path}dt={run_date.isoformat()}/"
