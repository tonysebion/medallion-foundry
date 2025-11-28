"""Verify generated Silver sample metadata matches the configs that produced them."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, cast

import pytest
import yaml

from core.silver.defaults import (
    DEFAULT_ARTIFACT_OUTPUT_NAMES,
    DEFAULT_ERROR_HANDLING,
    DEFAULT_NORMALIZATION,
    DEFAULT_SCHEMA,
)
from core.silver.models import SilverModel

REPO_ROOT = Path(__file__).resolve().parents[1]
SILVER_ROOT = REPO_ROOT / "sampledata" / "silver_samples"


def _load_expected_silver_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found at {config_path}")
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    silver_cfg = dict(cfg.get("silver", {}))
    bronze_cfg = cfg.get("bronze", {}) or {}
    bronze_options = bronze_cfg.get("options", {}) or {}
    silver_cfg.setdefault("load_pattern", bronze_options.get("load_pattern", "full"))
    source = cfg.get("source")
    if source:
        domain_value = source["system"]
        entity_value = source["table"]
    else:
        domain_value = cfg.get("system") or cfg.get("domain")
        entity_value = cfg.get("entity")
    silver_cfg.setdefault("domain", domain_value)
    silver_cfg.setdefault("entity", entity_value)
    silver_cfg.setdefault("version", 1)
    silver_cfg.setdefault("load_partition_name", "load_date")
    silver_cfg.setdefault("include_pattern_folder", False)
    natural_keys = silver_cfg.get("natural_keys") or []
    primary_keys = silver_cfg.get("primary_keys") or natural_keys
    silver_cfg["primary_keys"] = list(primary_keys)
    change_ts_column = silver_cfg.get("change_ts_column")
    event_ts_column = silver_cfg.get("event_ts_column")
    order_column = silver_cfg.get("order_column") or change_ts_column or event_ts_column
    silver_cfg["order_column"] = order_column
    silver_cfg.setdefault("parquet_compression", "snappy")
    silver_cfg.setdefault("normalization", DEFAULT_NORMALIZATION.copy())
    silver_cfg.setdefault("error_handling", DEFAULT_ERROR_HANDLING.copy())
    silver_cfg.setdefault("schema", DEFAULT_SCHEMA.copy())
    for key, value in DEFAULT_ARTIFACT_OUTPUT_NAMES.items():
        silver_cfg.setdefault(key, value)

    silver_cfg["write_parquet"] = True
    silver_cfg["write_csv"] = True
    partitioning = dict(silver_cfg.get("partitioning", {}))
    partitioning.setdefault("columns", list(silver_cfg.get("partition_by", [])))
    silver_cfg["partitioning"] = partitioning
    silver_cfg["dataset_id"] = f"{domain_value}.{entity_value}"
    silver_cfg.setdefault("entity_kind", "state")
    silver_cfg.setdefault("history_mode", silver_cfg.get("history_mode"))
    silver_cfg.setdefault("input_mode", silver_cfg.get("input_mode"))
    silver_cfg["bronze_owner"] = bronze_cfg.get("owner_team")
    silver_cfg["silver_owner"] = silver_cfg.get("semantic_owner")
    return silver_cfg


def _label_dir_from_metadata(metadata_path: Path) -> Path:
    relative_parts = metadata_path.relative_to(SILVER_ROOT).parts
    if not relative_parts:
        raise ValueError(f"Unexpected metadata path structure: {metadata_path}")
    return SILVER_ROOT / relative_parts[0]


def _find_intent_config(label_dir: Path) -> Path:
    pattern_name = label_dir.name.split("=", 1)[1] if "=" in label_dir.name else label_dir.name
    candidate = label_dir / f"intent_{pattern_name}.yaml"
    if candidate.exists():
        return candidate
    intent_files = sorted(label_dir.glob("intent_*.yaml"))
    if intent_files:
        return intent_files[0]
    raise FileNotFoundError(f"No intent config found under {label_dir}")


def _expected_artifact_names(
    model: SilverModel, silver_cfg: Dict[str, Any]
) -> List[str]:
    artifact_names: Dict[str, str] = {
        "full_snapshot": str(silver_cfg["full_output_name"]),
        "cdc": str(silver_cfg["cdc_output_name"]),
        "history": str(silver_cfg["history_output_name"]),
        "current": str(silver_cfg["current_output_name"]),
    }
    mapping = {
        SilverModel.SCD_TYPE_1: ["current"],
        SilverModel.SCD_TYPE_2: ["history", "current"],
        SilverModel.INCREMENTAL_MERGE: ["cdc"],
        SilverModel.FULL_MERGE_DEDUPE: ["full_snapshot"],
        SilverModel.PERIODIC_SNAPSHOT: ["full_snapshot"],
    }
    return [artifact_names[key] for key in mapping[model]]


def _extract_load_date(metadata_path: Path) -> str:
    relative_parts = metadata_path.relative_to(SILVER_ROOT).parts
    for part in relative_parts:
        if part.startswith("load_date="):
            return part.split("=", 1)[1]
    return ""


@pytest.fixture(scope="module")
def silver_metadata_files() -> List[Path]:
    if not SILVER_ROOT.exists():
        pytest.skip(
            "Silver samples are missing; run scripts/generate_silver_samples.py"
        )
    return list(SILVER_ROOT.rglob("_metadata.json"))


def test_silver_metadata_matches_config(silver_metadata_files: List[Path]) -> None:
    assert silver_metadata_files, "Expected at least one Silver metadata file"

    for metadata_path in silver_metadata_files:
        metadata: Dict[str, Any] = json.loads(metadata_path.read_text(encoding="utf-8"))
        label_dir = _label_dir_from_metadata(metadata_path)
        config_path = _find_intent_config(label_dir)
        expected_cfg = _load_expected_silver_config(config_path)

        bronze_path = metadata["bronze_path"]
        assert metadata["load_pattern"] == expected_cfg["load_pattern"]
        assert metadata["dataset_id"] == expected_cfg["dataset_id"]
        assert metadata["entity_kind"] == expected_cfg["entity_kind"]
        assert metadata["history_mode"] == expected_cfg["history_mode"]
        assert metadata["input_mode"] == expected_cfg["input_mode"]
        assert metadata["bronze_owner"] == expected_cfg["bronze_owner"]
        assert metadata["silver_owner"] == expected_cfg["silver_owner"]
        load_date = _extract_load_date(metadata_path)
        if load_date:
            assert metadata["load_batch_id"] == f"{expected_cfg['dataset_id']}-{load_date}"
        assert metadata["rows_written"] >= 0
        assert metadata["rows_read"] >= metadata["rows_written"]
        assert expected_cfg["domain"] in bronze_path
        assert expected_cfg["entity"] in bronze_path
