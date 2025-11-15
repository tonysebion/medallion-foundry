"""Integration tests for pattern-aware sample data."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

BRONZE_SAMPLE_ROOT = Path("docs/examples/data/bronze_samples")
REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def bronze_samples_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if not BRONZE_SAMPLE_ROOT.exists():
        pytest.skip("sample data missing; run scripts/generate_sample_data.py")

    dest = tmp_path_factory.mktemp("bronze_samples")
    shutil.copytree(BRONZE_SAMPLE_ROOT, dest, dirs_exist_ok=True)
    return dest


def _rewrite_config(original: Path, bronze_dir: Path, tmp_dir: Path) -> tuple[Path, Path, Path]:
    """Clone config and rewrite data/local output paths into the tmp dir."""
    target = tmp_dir / original.name
    bronze_out = (tmp_dir / "bronze_out").resolve()
    silver_out = (tmp_dir / "silver_out").resolve()
    replacement = original.read_text()
    replacement = replacement.replace(
        "./docs/examples/data/bronze_samples", str(bronze_dir).replace("\\", "/")
    )
    replacement = replacement.replace("./output", str(bronze_out).replace("\\", "/"))
    replacement = replacement.replace("./silver_output", str(silver_out).replace("\\", "/"))
    target.write_text(replacement)
    return target, bronze_out, silver_out


def _run_cli(args: list[str]) -> None:
    env = os.environ.copy()
    subprocess.run([sys.executable, *args], check=True, cwd=REPO_ROOT, env=env)


def _collect_bronze_partition(output_root: Path) -> Path:
    metadata_files = list(output_root.rglob("_metadata.json"))
    assert metadata_files, "Expected Bronze metadata to be created"
    return metadata_files[0].parent


def _read_metadata(metadata_path: Path) -> dict:
    return json.loads(metadata_path.read_text())


@pytest.mark.parametrize(
    "config_name, pattern, expected_silver_files",
    [
        ("file_example.yaml", "full", {"full_snapshot.parquet"}),
        ("file_cdc_example.yaml", "cdc", {"cdc_changes.parquet"}),
        ("file_current_history_example.yaml", "current_history", {"history.parquet", "current.parquet"}),
    ],
)
def test_bronze_to_silver_end_to_end(
    tmp_path: Path,
    bronze_samples_dir: Path,
    config_name: str,
    pattern: str,
    expected_silver_files: set[str],
) -> None:
    config_path = Path("docs/examples/configs") / config_name
    rewritten_cfg, bronze_out, silver_out = _rewrite_config(config_path, bronze_samples_dir, tmp_path)
    run_date = "2025-11-14"

    _run_cli(["bronze_extract.py", "--config", str(rewritten_cfg), "--date", run_date])

    bronze_partition = _collect_bronze_partition(bronze_out)
    metadata = _read_metadata(bronze_partition / "_metadata.json")
    assert metadata["load_pattern"] == pattern
    assert metadata["record_count"] > 0

    _run_cli(["silver_extract.py", "--config", str(rewritten_cfg), "--date", run_date])

    produced_files = {path.name for path in silver_out.rglob("*.parquet")}
    assert expected_silver_files <= produced_files

    # Sanity check row counts
    for parquet_file in silver_out.rglob("*.parquet"):
        df = pd.read_parquet(parquet_file)
        assert len(df) > 0, f"{parquet_file} should contain rows"
