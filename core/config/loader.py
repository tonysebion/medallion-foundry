from __future__ import annotations

from datetime import date as _date
from pathlib import Path
from typing import Any, Dict, List
import copy
import logging

import yaml

from .validation import validate_config_dict
from .typed_models import parse_root_config, RootConfig
from .v2_validation import validate_v2_config_dict
from .env_substitution import apply_env_substitution
from core.deprecation import emit_compat
from core.paths import build_bronze_relative_path

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
) -> Dict[str, Any | RootConfig]:
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
    if "sources" in cfg:
        raise ValueError(
            "Config contains multiple sources; use load_configs() instead."
        )
    validated = validate_config_dict(cfg)
    try:
        typed = parse_root_config(validated)
        if "config_version" not in validated:
            if strict:
                raise ValueError("Missing required config_version in strict mode")
            emit_compat("Config missing config_version; defaulting to 1", code="CFG004")
        if strict and int(validated.get("config_version", 1) or 1) >= 2:
            validate_v2_config_dict(validated)
        validated["__typed_model__"] = typed
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Typed config parse failed; proceeding with dict only: %s", exc)
    return validated


def load_configs(
    path: str, *, strict: bool = False, enable_env_substitution: bool = True
) -> List[Dict[str, Any | RootConfig]]:
    """Load multi-source config file.

    Args:
        path: Path to config YAML file
        strict: Enable strict validation
        enable_env_substitution: Substitute ${VAR} with environment variables
    """
    raw = _read_yaml(path)
    if enable_env_substitution:
        raw = apply_env_substitution(raw)
    if "sources" not in raw:
        return [validate_config_dict(raw)]

    sources = raw["sources"]
    if not isinstance(sources, list) or not sources:
        raise ValueError("'sources' must be a non-empty list")
    if "source" in raw:
        raise ValueError("Config cannot contain both 'source' and 'sources'")

    platform = raw.get("platform")
    base_silver = raw.get("silver")
    results: List[Dict[str, Any]] = []

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
        results.append(validated)

    return results


def build_relative_path(cfg: Dict[str, Any], run_date: _date) -> str:
    return build_bronze_relative_path(cfg, run_date)
