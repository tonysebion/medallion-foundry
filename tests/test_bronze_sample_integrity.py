"""Verify Bronze data that powers `docs/examples/configs` matches the sample inputs."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, cast

import pandas as pd
import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_ROOT = REPO_ROOT / "docs" / "examples" / "configs"
SOURCE_ROOT = REPO_ROOT / "sampledata" / "source_samples"

NULL_SENTINEL = "<BRONZE_NULL>"


def _replace_dt_in_path(path: str, run_date: str) -> str:
    """Substitute the first dt=YYYY-MM-DD segment so we can exercise other runs."""
    pattern = r"dt=\d{4}-\d{2}-\d{2}"
    if not re.search(pattern, path):
        raise ValueError(f"Config path_pattern {path!r} lacks a dt= segment")
    return re.sub(pattern, f"dt={run_date}", path, count=1)


def _value_counter(series: pd.Series) -> Counter[str]:
    """Normalize column values so we can compare source rows to Bronze output."""
    values = (
        NULL_SENTINEL if pd.isna(value) else str(value)
        for value in series.astype(object)
    )
    return Counter(values)


def _collect_bronze_partition(output_root: Path) -> Path:
    """Bronze writes metadata per partition; locate it to inspect the data."""
    metadata_files = list(output_root.rglob("_metadata.json"))
    assert metadata_files, f"No Bronze metadata found under {output_root}"
    return metadata_files[0].parent


def _read_bronze_dataframe(partition: Path) -> pd.DataFrame:
    parquet_files = sorted(partition.glob("*.parquet"))
    assert parquet_files, f"No Parquet files found in {partition}"
    frames = [pd.read_parquet(path) for path in parquet_files]
    return pd.concat(frames, ignore_index=True)


def _read_source_dataframe(source_path: Path) -> pd.DataFrame:
    suffix = source_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(source_path, dtype=str, keep_default_na=False)
    if suffix == ".json":
        return pd.read_json(source_path, dtype=str)
    if suffix == ".parquet":
        return pd.read_parquet(source_path).astype(str)
    raise ValueError(f"Unsupported sample format: {suffix}")


def _rewrite_bronze_config(
    config_path: Path, run_date: str, tmp_dir: Path
) -> tuple[Path, Path, Dict[str, Any]]:
    cfg = cast(Dict[str, Any], yaml.safe_load(config_path.read_text()))
    bronze_cfg = cfg.setdefault("bronze", {})
    path_pattern = bronze_cfg.get("path_pattern")
    if not path_pattern:
        raise ValueError(f"{config_path} missing bronze.path_pattern")

    updated_path = _replace_dt_in_path(path_pattern, run_date)
    bronze_path = Path(updated_path)
    if not bronze_path.is_absolute():
        bronze_path = (REPO_ROOT / bronze_path).resolve()
    bronze_cfg["path_pattern"] = str(bronze_path)

    bronze_out = tmp_dir / f"{config_path.stem}_{run_date.replace('-', '')}_bronze"
    options = bronze_cfg.setdefault("options", {})
    options["local_output_dir"] = str(bronze_out.resolve())

    rewritten = tmp_dir / f"{config_path.stem}_{run_date}_bronze.yaml"
    rewritten.write_text(yaml.safe_dump(cfg))
    return rewritten, bronze_out, cfg


def _run_cli(args: list[str]) -> None:
    subprocess.run(
        [sys.executable, *args],
        check=True,
        cwd=REPO_ROOT,
    )


BRONZE_TEST_CASES = [
    ("patterns/pattern_full.yaml", "2025-11-13"),
    ("patterns/pattern_full.yaml", "2025-11-14"),
    ("patterns/pattern_cdc.yaml", "2025-11-13"),
    ("patterns/pattern_current_history.yaml", "2025-11-13"),
    ("patterns/pattern_hybrid_incremental_cumulative.yaml", "2025-11-24"),
]


@pytest.fixture(scope="module")
def source_samples_root() -> Path:
    if not SOURCE_ROOT.exists():
        pytest.skip("Sample source data missing; run scripts/generate_sample_data.py")
    return SOURCE_ROOT


@pytest.mark.parametrize(
    "config_name,run_date",
    BRONZE_TEST_CASES,
    ids=[f"{cfg.split('/')[-1]}@{date}" for cfg, date in BRONZE_TEST_CASES],
)
def test_bronze_preserves_source_rows(
    tmp_path: Path,
    source_samples_root: Path,
    config_name: str,
    run_date: str,
) -> None:
    config_path = CONFIG_ROOT / config_name
    rewritten_cfg, bronze_out, cfg_data = _rewrite_bronze_config(
        config_path, run_date, tmp_path
    )

    _run_cli(["bronze_extract.py", "--config", str(rewritten_cfg), "--date", run_date])

    bronze_partition = _collect_bronze_partition(bronze_out)
    metadata = cast(
        Dict[str, Any], json.loads((bronze_partition / "_metadata.json").read_text())
    )

    source_path = Path(cfg_data["bronze"]["path_pattern"])
    assert source_path.exists(), f"Source path missing: {source_path}"
    assert (
        source_samples_root in source_path.parents
    ), "Sample data must live under sampledata/source_samples"
    source_df = _read_source_dataframe(source_path)
    expected_load_pattern = (
        cfg_data["bronze"].get("options", {}).get("load_pattern") or "full"
    )

    bronze_df = _read_bronze_dataframe(bronze_partition)
    assert set(source_df.columns) <= set(bronze_df.columns)
    for column in source_df.columns:
        assert _value_counter(bronze_df[column]) == _value_counter(
            source_df[column]
        ), f"Column {column!r} diverged between source and Bronze outputs"

    assert metadata["run_date"] == run_date
    assert metadata["system"] == cfg_data["system"]
    assert metadata["table"] == cfg_data["entity"]
    assert metadata["record_count"] == len(bronze_df) == len(source_df)
    assert metadata["chunk_count"] == len(list(bronze_partition.glob("*.parquet")))
    assert metadata["load_pattern"] == expected_load_pattern
