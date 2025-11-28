"""Ensure Silver writes both CSV and Parquet for all sample Bronze partitions."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(".").resolve()
BRONZE_SAMPLE_ROOT = REPO_ROOT / "sampledata" / "bronze_samples"
CONFIGS_DIR = REPO_ROOT / "docs" / "examples" / "configs" / "examples"

CONFIG_FILES = [
    "file_example.yaml",
    "file_cdc_example.yaml",
    "file_current_history_example.yaml",
]
PATTERN_DIRS = {
    "full": "pattern1_full_events",
    "cdc": "pattern2_cdc_events",
    "current_history": "pattern3_scd_state",
    "hybrid_cdc_point": "pattern4_hybrid_cdc_point",
    "hybrid_cdc_cumulative": "pattern5_hybrid_cdc_cumulative",
    "hybrid_incremental_point": "pattern6_hybrid_incremental_point",
    "hybrid_incremental_cumulative": "pattern7_hybrid_incremental_cumulative",
}
RUN_DATES = ["2025-11-13", "2025-11-14"]


def _ensure_source(cfg: dict) -> dict:
    if "source" in cfg:
        return cfg["source"]
    bronze = cfg.get("bronze", {})
    options = bronze.get("options", {}) or {}
    source = {
        "system": cfg.get("system"),
        "table": cfg.get("entity"),
        "run": {"load_pattern": options.get("load_pattern", "full")},
        "file": {"path": bronze.get("path_pattern")},
        "type": bronze.get("source_type", "file"),
    }
    cfg["source"] = source
    return source


def _resolve_pattern_folder(cfg: dict, source: dict) -> str:
    pattern_folder = source["run"].get("pattern_folder")
    if pattern_folder:
        return pattern_folder
    bronze = cfg.get("bronze", {})
    options = bronze.get("options", {}) or {}
    resolved = options.get("pattern_folder") or source["run"].get(
        "load_pattern", "full"
    )
    return PATTERN_DIRS.get(resolved, resolved)


def _build_sample_path(cfg: dict, run_date: str) -> Path:
    source = _ensure_source(cfg)
    pattern_folder = _resolve_pattern_folder(cfg, source)
    system = source["system"]
    table = source["table"]
    filename = Path(source["file"]["path"]).name
    return (
        BRONZE_SAMPLE_ROOT
        / f"sample={pattern_folder}"
        / f"system={system}"
        / f"table={table}"
        / f"dt={run_date}"
        / filename
    )


def _rewrite_config(original: Path, run_date: str, tmp_dir: Path) -> Path:
    cfg = yaml.safe_load(original.read_text())
    source = _ensure_source(cfg)
    pattern_folder = _resolve_pattern_folder(cfg, source)
    bronze_path = _build_sample_path(cfg, run_date)
    assert bronze_path.exists(), f"Missing Bronze sample data at {bronze_path}"
    bronze_out = tmp_dir / f"bronze_out_{run_date}"
    bronze_out.mkdir(parents=True, exist_ok=True)
    bronze_cfg = cfg.setdefault("bronze", {})
    bronze_cfg["path_pattern"] = str(bronze_path.resolve())
    source["file"]["path"] = str(bronze_path.resolve())
    source["run"]["local_output_dir"] = str(bronze_out.resolve())
    source["run"]["pattern_folder"] = pattern_folder
    target = tmp_dir / f"{original.stem}_bronze_{run_date}.yaml"
    target.write_text(yaml.safe_dump(cfg))
    return target


def _rewrite_silver_config(
    original: Path, run_date: str, tmp_dir: Path, fmt: str
) -> Path:
    cfg = yaml.safe_load(original.read_text())
    source = _ensure_source(cfg)
    bronze_path = _build_sample_path(cfg, run_date)
    bronze_out = tmp_dir / f"bronze_out_{run_date}"
    bronze_out.mkdir(parents=True, exist_ok=True)
    source["file"]["path"] = str(bronze_path.resolve())
    source["run"]["local_output_dir"] = str(bronze_out.resolve())
    pattern_folder = _resolve_pattern_folder(cfg, source)
    source["run"]["pattern_folder"] = pattern_folder
    silver_out = tmp_dir / f"silver_out_{fmt}_{run_date}"
    silver_out.mkdir(parents=True, exist_ok=True)
    cfg.setdefault("silver", {})
    cfg["silver"]["output_dir"] = str(silver_out.resolve())
    if fmt == "parquet":
        cfg["silver"]["write_parquet"] = True
        cfg["silver"]["write_csv"] = False
    else:
        cfg["silver"]["write_parquet"] = False
        cfg["silver"]["write_csv"] = True
    target = tmp_dir / f"{original.stem}_{fmt}_{run_date}.yaml"
    target.write_text(yaml.safe_dump(cfg))
    return target


def _run_cli(cmd: list[str]) -> None:
    subprocess.run([sys.executable, *cmd], check=True, cwd=REPO_ROOT)


@pytest.mark.parametrize("fmt", ["parquet", "csv"])
@pytest.mark.parametrize("config_name", CONFIG_FILES)
@pytest.mark.parametrize("run_date", RUN_DATES)
def test_silver_writer_formats(
    tmp_path: Path, config_name: str, run_date: str, fmt: str
) -> None:
    config_path = CONFIGS_DIR / config_name
    bronze_cfg = _rewrite_config(config_path, run_date, tmp_path)
    _run_cli(["bronze_extract.py", "--config", str(bronze_cfg), "--date", run_date])

    silver_cfg = _rewrite_silver_config(config_path, run_date, tmp_path, fmt)
    _run_cli(["silver_extract.py", "--config", str(silver_cfg), "--date", run_date])

    cfg = yaml.safe_load(silver_cfg.read_text())
    silver_root = Path(cfg["silver"]["output_dir"]).resolve()
    assert silver_root.exists()
    files = list(silver_root.rglob("*.parquet" if fmt == "parquet" else "*.csv"))
    assert files, f"No {fmt} files produced under {silver_root}"
