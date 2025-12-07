"""Silver asset models and helpers."""

from __future__ import annotations

from typing import Any, Dict

# Import SilverModel and SILVER_MODEL_ALIASES from primitives and re-export
from core.foundation.primitives.models import (
    SilverModel,
    SILVER_MODEL_ALIASES as _SILVER_MODEL_ALIASES,
)

# Re-export from infrastructure for backward compatibility
from core.infrastructure.config.profiles import (
    MODEL_PROFILES as _MODEL_PROFILES,
    resolve_profile as _resolve_profile,
)

SILVER_MODEL_ALIASES = _SILVER_MODEL_ALIASES
MODEL_PROFILES = _MODEL_PROFILES
resolve_profile = _resolve_profile

# =============================================================================
# Default Configuration Values
# =============================================================================

DEFAULT_NORMALIZATION: Dict[str, Any] = {
    "trim_strings": False,
    "empty_strings_as_null": False,
}
DEFAULT_ERROR_HANDLING: Dict[str, Any] = {
    "enabled": False,
    "max_bad_records": 0,
    "max_bad_percent": 0.0,
}
DEFAULT_SCHEMA: Dict[str, Any] = {"rename_map": {}, "column_order": None}
DEFAULT_ARTIFACT_OUTPUT_NAMES: Dict[str, str] = {
    "full_output_name": "full_snapshot",
    "cdc_output_name": "cdc_changes",
    "history_output_name": "history",
    "current_output_name": "current",
}


def default_silver_config() -> Dict[str, Any]:
    """Return a fresh dict with the default Silver configuration."""
    return {
        "schema": DEFAULT_SCHEMA.copy(),
        "normalization": DEFAULT_NORMALIZATION.copy(),
        "partitioning": {"columns": []},
        "error_handling": DEFAULT_ERROR_HANDLING.copy(),
        "primary_keys": [],
        "order_column": None,
        "write_parquet": True,
        "write_csv": False,
        "parquet_compression": "snappy",
        **DEFAULT_ARTIFACT_OUTPUT_NAMES,
        "domain": "default",
        "entity": "dataset",
        "version": 1,
        "load_partition_name": "load_date",
        "include_pattern_folder": False,
        "require_checksum": False,
        "model": SilverModel.PERIODIC_SNAPSHOT.value,
    }
