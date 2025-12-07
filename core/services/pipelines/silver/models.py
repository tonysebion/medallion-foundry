"""Silver asset models and helpers."""

from __future__ import annotations

from typing import Any, Dict

# Import SilverModel and SILVER_MODEL_ALIASES from primitives and re-export
from core.primitives.foundations.models import SilverModel, SILVER_MODEL_ALIASES


# =============================================================================
# Model Profiles - Silver-layer specific mappings
# =============================================================================

MODEL_PROFILES: Dict[str, SilverModel] = {
    "analytics": SilverModel.SCD_TYPE_2,
    "operational": SilverModel.SCD_TYPE_1,
    "merge_ready": SilverModel.FULL_MERGE_DEDUPE,
    "cdc_delta": SilverModel.INCREMENTAL_MERGE,
    "snapshot": SilverModel.PERIODIC_SNAPSHOT,
}


def resolve_profile(profile_name: str | None) -> SilverModel | None:
    """Resolve a profile name to a SilverModel."""
    if profile_name is None:
        return None
    key = profile_name.strip().lower()
    return MODEL_PROFILES.get(key)


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
