"""Verify generated Silver sample metadata matches the configs that produced them."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List

import pytest
import yaml

from core.silver.models import SilverModel
from scripts.generate_silver_samples import PATTERN_CONFIG

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIGS_DIR = REPO_ROOT / "docs" / "examples" / "configs" / "examples"
SILVER_ROOT = REPO_ROOT / "sampledata" / "silver_samples"
PATTERN_REGEX = re.compile(r"pattern=([^/\\]+)")
DEFAULT_NORMALIZATION = {"trim_strings": False, "empty_strings_as_null": False}
DEFAULT_ERROR_HANDLING = {
    "enabled": False,
    "max_bad_records": 0,
    "max_bad_percent": 0.0,
}
DEFAULT_SCHEMA = {"rename_map": {}, "column_order": None}


def _load_expected_silver_config(pattern: str) -> Dict[str, object]:
    config_name = PATTERN_CONFIG.get(pattern)
    if not config_name:
        raise ValueError(f"No config mapping for pattern '{pattern}'")
    config_path = CONFIGS_DIR / config_name
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    silver_cfg = dict(cfg.get("silver", {}))
    silver_cfg.setdefault("domain", cfg["source"]["system"])
    silver_cfg.setdefault("entity", cfg["source"]["table"])
    silver_cfg.setdefault("version", 1)
    silver_cfg.setdefault("load_partition_name", "load_date")
    silver_cfg.setdefault("include_pattern_folder", False)
    silver_cfg.setdefault("primary_keys", [])
    silver_cfg.setdefault("order_column", None)
    silver_cfg.setdefault("parquet_compression", "snappy")
    silver_cfg.setdefault("normalization", DEFAULT_NORMALIZATION.copy())
    silver_cfg.setdefault("error_handling", DEFAULT_ERROR_HANDLING.copy())
    silver_cfg.setdefault("schema", DEFAULT_SCHEMA.copy())
    silver_cfg.setdefault("full_output_name", "full_snapshot")
    silver_cfg.setdefault("cdc_output_name", "cdc_changes")
    silver_cfg.setdefault("history_output_name", "history")
    silver_cfg.setdefault("current_output_name", "current")

    silver_cfg["write_parquet"] = True
    silver_cfg["write_csv"] = True
    partitioning = dict(silver_cfg.get("partitioning", {}))
    partitioning["columns"] = []
    silver_cfg["partitioning"] = partitioning
    return silver_cfg


def _pattern_from_bronze_path(bronze_path: str) -> str:
    match = PATTERN_REGEX.search(bronze_path)
    if not match:
        raise ValueError(f"Unable to parse pattern from bronze_path '{bronze_path}'")
    return match.group(1)


def _expected_artifact_names(
    model: SilverModel, silver_cfg: Dict[str, object]
) -> List[str]:
    artifact_names = {
        "full_snapshot": silver_cfg["full_output_name"],
        "cdc": silver_cfg["cdc_output_name"],
        "history": silver_cfg["history_output_name"],
        "current": silver_cfg["current_output_name"],
    }
    mapping = {
        SilverModel.SCD_TYPE_1: ["current"],
        SilverModel.SCD_TYPE_2: ["history", "current"],
        SilverModel.INCREMENTAL_MERGE: ["cdc"],
        SilverModel.FULL_MERGE_DEDUPE: ["full_snapshot"],
        SilverModel.PERIODIC_SNAPSHOT: ["full_snapshot"],
    }
    return [artifact_names[key] for key in mapping[model]]


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
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        silver_model = SilverModel(metadata["silver_model"])
        expected_cfg = _load_expected_silver_config(
            _pattern_from_bronze_path(metadata["bronze_path"])
        )

        assert metadata["write_parquet"] is True
        assert metadata["write_csv"] is True
        assert metadata["partition_columns"] == []
        assert metadata["parquet_compression"] == expected_cfg["parquet_compression"]
        assert metadata["domain"] == expected_cfg["domain"]
        assert metadata["entity"] == expected_cfg["entity"]
        assert metadata["version"] == expected_cfg["version"]
        assert metadata["load_partition_name"] == expected_cfg["load_partition_name"]
        assert (
            metadata["include_pattern_folder"] == expected_cfg["include_pattern_folder"]
        )
        assert metadata["primary_keys"] == expected_cfg["primary_keys"]
        assert metadata["order_column"] == expected_cfg["order_column"]
        assert metadata["normalization"] == expected_cfg["normalization"]
        assert metadata["schema"] == expected_cfg["schema"]
        assert metadata["error_handling"] == expected_cfg["error_handling"]

        expected_artifacts = set(_expected_artifact_names(silver_model, expected_cfg))
        artifact_keys = set(metadata["artifacts"].keys())
        assert artifact_keys == expected_artifacts
        for artifact_name in expected_artifacts:
            output_files = metadata["artifacts"][artifact_name]
            assert set(output_files) == {
                f"{artifact_name}.parquet",
                f"{artifact_name}.csv",
            }
