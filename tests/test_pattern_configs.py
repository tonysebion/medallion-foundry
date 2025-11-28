"""Sanity checks for the pattern sample configs."""

from pathlib import Path

import pytest
import yaml

PATTERN_MAPPING = {
    "pattern_full.yaml": "pattern1_full_events",
    "pattern_cdc.yaml": "pattern2_cdc_events",
    "pattern_current_history.yaml": "pattern3_scd_state",
    "pattern_hybrid_cdc_point.yaml": "pattern4_hybrid_cdc_point",
    "pattern_hybrid_cdc_cumulative.yaml": "pattern5_hybrid_cdc_cumulative",
    "pattern_hybrid_incremental_point.yaml": "pattern6_hybrid_incremental_point",
    "pattern_hybrid_incremental_cumulative.yaml": "pattern7_hybrid_incremental_cumulative",
}


@pytest.mark.parametrize("config_name, pattern_folder", PATTERN_MAPPING.items())
def test_pattern_folder_is_set(config_name: str, pattern_folder: str) -> None:
    config_path = (
        Path("docs") / "examples" / "configs" / "patterns" / config_name
    )
    cfg = yaml.safe_load(config_path.read_text())
    bronze_config = cfg.get("bronze", {})
    options = bronze_config.get("options", {})

    assert options.get("pattern_folder") == pattern_folder, (
        f"{config_name} should write Bronze output under pattern={pattern_folder} "
        "via bronze.options.pattern_folder"
    )
