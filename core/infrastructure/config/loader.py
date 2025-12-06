from __future__ import annotations

import copy
import logging
import os
from datetime import date as _date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from .dataset import (
    DatasetConfig,
    dataset_to_runtime_config,
    is_new_intent_config,
    legacy_to_dataset,
)
from .env_substitution import apply_env_substitution
from .typed_models import RootConfig, parse_root_config
from core.primitives.foundations.exceptions import emit_compat
from core.pipeline.runtime.paths import build_bronze_relative_path
from core.infrastructure.config.environment import EnvironmentConfig, S3ConnectionConfig
from .validation import validate_config_dict
from .v2_validation import validate_v2_config_dict

logger = logging.getLogger(__name__)


def _read_yaml(path: str) -> Dict[str, Any]:
    logger.info(f"Loading config from {path}")

    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as handle:
            cfg = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in config file: {exc}")

    if not isinstance(cfg, dict):
        raise ValueError("Config must be a YAML dictionary/object")

    return cfg


def load_config(
    path: str, *, strict: bool = False, enable_env_substitution: bool = True
) -> RootConfig:
    """Load a single config file and return both dict and typed model.

    For backward compatibility we still return a validated dict, but attach
    a typed model instance under reserved key '__typed_model__'.

    Args:
        path: Path to config YAML file
        strict: Enable strict validation (require config_version, enforce v2 rules)
        enable_env_substitution: Substitute ${VAR} and ${VAR:default} with environment variables
    """
    cfg = _read_yaml(path)
    if enable_env_substitution:
        cfg = apply_env_substitution(cfg)

    if "datasets" in cfg:
        datasets = _load_intent_datasets(cfg, config_path=Path(path))
        if len(datasets) != 1:
            raise ValueError(
                "Config file contains multiple datasets; use load_configs() instead."
            )
        runtime = datasets[0]
        # Parse into typed RootConfig
        typed = parse_root_config(runtime)
        env_config = runtime.get("__env_config__")
        if env_config:
            setattr(typed, "__env_config__", env_config)
        return typed

    if is_new_intent_config(cfg):
        dataset = DatasetConfig.from_dict(cfg)
        env_config = _load_environment_config(Path(path), dataset.environment)
        runtime = _build_dataset_runtime(dataset, env_config)
        typed = parse_root_config(runtime)
        if env_config:
            setattr(typed, "__env_config__", env_config)
        return typed

    if "sources" in cfg:
        raise ValueError(
            "Config contains multiple sources; use load_configs() instead."
        )
    validated = validate_config_dict(cfg)
    typed = parse_root_config(validated)
    if "config_version" not in validated:
        if strict:
            raise ValueError("Missing required config_version in strict mode")
        emit_compat("Config missing config_version; defaulting to 1", code="CFG004")
    if strict and int(validated.get("config_version", 1) or 1) >= 2:
        validate_v2_config_dict(validated)
    # Return typed RootConfig instance
    return typed


def load_configs(
    path: str, *, strict: bool = False, enable_env_substitution: bool = True
) -> List[RootConfig]:
    """Load multi-source config file.

    Args:
        path: Path to config YAML file
        strict: Enable strict validation
        enable_env_substitution: Substitute ${VAR} with environment variables
    """
    raw = _read_yaml(path)
    if enable_env_substitution:
        raw = apply_env_substitution(raw)

    if "datasets" in raw or is_new_intent_config(raw):
        datasets = _load_intent_datasets(raw, config_path=Path(path))
        if datasets:
            results = []
            for ds in datasets:
                typed = parse_root_config(ds)
                env_cfg = ds.get("__env_config__")
                if env_cfg:
                    setattr(typed, "__env_config__", env_cfg)
                results.append(typed)
            return results
        raise ValueError("'datasets' must be a non-empty list when provided")

    if "sources" not in raw:
        validated = validate_config_dict(raw)
        dataset_intent = legacy_to_dataset(validated)
        if dataset_intent:
            validated["__dataset__"] = dataset_intent
        return [parse_root_config(validated)]

    sources = raw["sources"]
    if not isinstance(sources, list) or not sources:
        raise ValueError("'sources' must be a non-empty list")
    if "source" in raw:
        raise ValueError("Config cannot contain both 'source' and 'sources'")

    platform = raw.get("platform")
    base_silver = raw.get("silver")
    results: List[RootConfig] = []

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
        validated = validate_config_dict(merged_cfg)
        try:
            typed = parse_root_config(validated)
            env_cfg = validated.get("__env_config__")
            if env_cfg:
                setattr(typed, "__env_config__", env_cfg)
            if "config_version" not in validated:
                if strict:
                    raise ValueError(
                        "Missing required config_version in strict mode (multi-config load)"
                    )
                emit_compat(
                    "Config missing config_version; defaulting to 1", code="CFG004"
                )
            if strict and int(validated.get("config_version", 1) or 1) >= 2:
                validate_v2_config_dict(validated)
            validated["__typed_model__"] = typed
        except Exception as exc:  # pragma: no cover
            logger.warning(
                "Typed config parse failed for source index %s: %s", idx, exc
            )
        dataset_intent = legacy_to_dataset(validated)
        if dataset_intent:
            validated["__dataset__"] = dataset_intent
        results.append(parse_root_config(validated))

    return results


def build_relative_path(cfg: Dict[str, Any], run_date: _date) -> str:
    return build_bronze_relative_path(cfg, run_date)


def _load_environment_config(
    config_path: Path, env_name: Optional[str]
) -> Optional[EnvironmentConfig]:
    if not env_name:
        return None

    env_config_path = config_path.parent.parent / "environments"
    if not env_config_path.exists():
        env_config_path = Path("environments")

    env_file = env_config_path / f"{env_name}.yaml"

    if env_file.exists():
        logger.info(f"Loading environment config: {env_file}")
        env_config = EnvironmentConfig.from_yaml(env_file)
        _export_s3_env_vars(env_config.s3)
        return env_config

    logger.warning(
        f"Environment '{env_name}' referenced in config but not found at {env_file}"
    )
    return None


def _build_dataset_runtime(
    dataset: DatasetConfig, env_config: Optional[EnvironmentConfig] = None
) -> Dict[str, Any]:
    runtime = dataset_to_runtime_config(dataset)
    validated = validate_config_dict(runtime)
    validated["__dataset__"] = dataset
    validated["_intent_config"] = True
    if env_config:
        validated["__env_config__"] = env_config
    return validated


def _export_s3_env_vars(config: Optional[S3ConnectionConfig]) -> None:
    if not config:
        return
    if config.access_key_id:
        os.environ["AWS_ACCESS_KEY_ID"] = config.access_key_id
    if config.secret_access_key:
        os.environ["AWS_SECRET_ACCESS_KEY"] = config.secret_access_key
    if config.region:
        os.environ["AWS_DEFAULT_REGION"] = config.region
    if config.endpoint_url:
        os.environ["BRONZE_S3_ENDPOINT"] = config.endpoint_url


def ensure_root_config(cfg: Dict[str, Any]) -> RootConfig:
    """Given a config dict, return a parsed RootConfig model.

    This ensures that callers downstream can work with typed config models.
    """
    try:
        typed = parse_root_config(cfg)
        return typed
    except Exception:
        # If parsing fails, raise so callers must handle invalid configs explicitly
        raise


def _load_intent_datasets(
    raw: Dict[str, Any], *, config_path: Path
) -> List[Dict[str, Any]]:
    """Load intent-style configs (single dict or list under 'datasets')."""
    entries: List[Dict[str, Any]]
    if "datasets" in raw:
        datasets = raw["datasets"]
        if not isinstance(datasets, list) or not datasets:
            raise ValueError("'datasets' must be a non-empty list")
        defaults = {
            key: raw.get(key)
            for key in ("environment", "domain")
            if raw.get(key) is not None
        }
        entries = []
        for idx, item in enumerate(datasets):
            if not isinstance(item, dict):
                raise ValueError("Each entry in 'datasets' must be a dictionary")
            merged = {**defaults, **item}
            dataset = DatasetConfig.from_dict(merged)
            env_config = _load_environment_config(config_path, dataset.environment)
            if env_config and env_config.s3:
                if dataset.bronze.output_bucket:
                    dataset.bronze.output_bucket = env_config.s3.get_bucket(
                        dataset.bronze.output_bucket
                    )
                if dataset.silver.output_bucket:
                    dataset.silver.output_bucket = env_config.s3.get_bucket(
                        dataset.silver.output_bucket
                    )
            runtime = _build_dataset_runtime(dataset, env_config)
            runtime["source"]["config_name"] = item.get("name") or dataset.dataset_id
            entries.append(runtime)
        return entries

    dataset = DatasetConfig.from_dict(raw)
    env_config = _load_environment_config(config_path, dataset.environment)
    if env_config and env_config.s3:
        if dataset.bronze.output_bucket:
            dataset.bronze.output_bucket = env_config.s3.get_bucket(
                dataset.bronze.output_bucket
            )
        if dataset.silver.output_bucket:
            dataset.silver.output_bucket = env_config.s3.get_bucket(
                dataset.silver.output_bucket
            )
    return [_build_dataset_runtime(dataset, env_config)]


def load_config_with_env(
    config_path: Path, env_config_path: Optional[Path] = None
) -> Tuple[DatasetConfig, Optional["EnvironmentConfig"]]:  # noqa: F821
    """Load dataset config and optional environment config.

    Args:
        config_path: Path to pattern YAML file
        env_config_path: Optional path to environment config directory
            If not provided, looks for environments/ directory relative to config

    Returns:
        Tuple of (dataset_config, environment_config)

    Example:
        >>> dataset, env = load_config_with_env(Path("patterns/pattern_full.yaml"))
        >>> if env:
        ...     print(f"Using environment: {env.name}")
    """
    from core.infrastructure.config.environment import EnvironmentConfig

    # Load the pattern config
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError("Config must be a YAML dictionary/object")

    # Apply environment variable substitution
    raw = apply_env_substitution(raw)

    # Parse into DatasetConfig
    if not is_new_intent_config(raw):
        raise ValueError(
            "load_config_with_env requires intent-style config. "
            "Use load_config() for legacy configs."
        )

    dataset = DatasetConfig.from_dict(raw)

    # Load environment config if referenced
    env_config = None
    env_name = raw.get("environment")

    if env_name:
        # Determine environment config directory
        if env_config_path is None:
            # Look for environments/ directory relative to config file
            env_config_path = config_path.parent.parent / "environments"
            if not env_config_path.exists():
                # Try project root
                env_config_path = Path("environments")

        env_file = env_config_path / f"{env_name}.yaml"

        if env_file.exists():
            logger.info(f"Loading environment config: {env_file}")
            env_config = EnvironmentConfig.from_yaml(env_file)
        else:
            logger.warning(
                f"Environment '{env_name}' referenced in config but not found at {env_file}"
            )

    return dataset, env_config
