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
import yaml

BRONZE_SAMPLE_ROOT = Path("docs/examples/data/bronze_samples")
BRONZE_EXAMPLE_ROOT = Path("docs/examples/data/bronze_examples")
REPO_ROOT = Path(__file__).resolve().parents[1]
GENERATE_SCRIPT = Path("scripts") / "generate_sample_data.py"


@pytest.fixture(scope="module")
def bronze_samples_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    subprocess.run([sys.executable, str(GENERATE_SCRIPT)], check=True, cwd=REPO_ROOT)
    if not BRONZE_SAMPLE_ROOT.exists():
        pytest.skip("sample data missing; run scripts/generate_sample_data.py")

    dest = tmp_path_factory.mktemp("bronze_samples")
    shutil.copytree(BRONZE_SAMPLE_ROOT, dest, dirs_exist_ok=True)
    return dest


def _build_sample_path(
    bronze_dir: Path, cfg: dict, run_date: str, config_path: Path
) -> Path:
    bronze_options = cfg.get("bronze", {}).get("options", {})
    if "source" in cfg:
        load_pattern = cfg["source"]["run"].get("load_pattern", "full")
        pattern_dir = load_pattern
        system = cfg["source"]["system"]
        table = cfg["source"]["table"]
        filename = Path(cfg["source"]["file"]["path"]).name
        tail = [filename]
    else:
        load_pattern = bronze_options.get("load_pattern", "full")
        pattern_dir = bronze_options.get("pattern_folder") or load_pattern
        system = cfg["system"]
        table = cfg["entity"]
        original_path = Path(cfg["bronze"].get("path_pattern", "sample.csv"))
        tail: list[str] = []
        parts = list(original_path.parts)
        for idx, part in enumerate(parts):
            if part.startswith("dt="):
                tail = parts[idx + 1 :]
                break
        if not tail:
            tail = [original_path.name]
    config_root = BRONZE_EXAMPLE_ROOT / config_path.stem
    return (
        config_root
        / pattern_dir
        / f"system={system}"
        / f"table={table}"
        / f"pattern={pattern_dir}"
        / f"dt={run_date}"
        / Path(*tail)
    )


def _rewrite_config(
    original: Path, bronze_dir: Path, tmp_dir: Path, run_date: str
) -> tuple[Path, Path, Path, dict]:
    """Clone config and rewrite data/local output paths into the tmp dir."""
    cfg = yaml.safe_load(original.read_text())
    bronze_out = (tmp_dir / f"bronze_out_{run_date}").resolve()
    silver_out = (tmp_dir / f"silver_out_{run_date}").resolve()

    if "source" in cfg:
        cfg["source"]["file"]["path"] = str(
            _build_sample_path(bronze_dir, cfg, run_date, original)
        )
        cfg["source"]["run"]["local_output_dir"] = str(bronze_out)
        cfg.setdefault("silver", {})
        cfg["silver"]["output_dir"] = str(silver_out)
    else:
        cfg.setdefault("bronze", {})
        cfg["bronze"]["path_pattern"] = str(
            _build_sample_path(bronze_dir, cfg, run_date, original)
        )
        bronze_options = cfg["bronze"].setdefault("options", {})
        bronze_options["local_output_dir"] = str(bronze_out)
        cfg.setdefault("silver", {})
        cfg["silver"]["output_dir"] = str(silver_out)

    target = tmp_dir / f"{original.stem}_{run_date}.yaml"
    target.write_text(yaml.safe_dump(cfg))
    return target, bronze_out, silver_out, cfg


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
    "config_name, load_pattern, pattern_dir, expected_silver_files, run_dates",
    [
        (
            "file_example.yaml",
            "full",
            "full",
            {"events.parquet"},
            ["2025-11-13", "2025-11-14"],
        ),
        (
            "file_cdc_example.yaml",
            "cdc",
            "cdc",
            {"events.parquet"},
            ["2025-11-13", "2025-11-14"],
        ),
        (
            "file_current_history_example.yaml",
            "current_history",
            "current_history",
            {"state_history.parquet", "state_current.parquet"},
            ["2025-11-13", "2025-11-14"],
        ),
        (
            "pattern_full.yaml",
            "full",
            "full",
            {"events.parquet"},
            ["2025-11-13"],
        ),
        (
            "pattern_cdc.yaml",
            "cdc",
            "cdc",
            {"events.parquet"},
            ["2025-11-13"],
        ),
        (
            "pattern_current_history.yaml",
            "current_history",
            "current_history",
            {"state_history.parquet", "state_current.parquet"},
            ["2025-11-13"],
        ),
        (
            "pattern_hybrid_cdc_point.yaml",
            "cdc",
            "hybrid_cdc_point",
            None,
            ["2025-11-24"],
        ),
        (
            "pattern_hybrid_cdc_cumulative.yaml",
            "cdc",
            "hybrid_cdc_cumulative",
            None,
            ["2025-11-24"],
        ),
        (
            "pattern_hybrid_incremental_point.yaml",
            "cdc",
            "hybrid_incremental_point",
            None,
            ["2025-11-24"],
        ),
        (
            "pattern_hybrid_incremental_cumulative.yaml",
            "cdc",
            "hybrid_incremental_cumulative",
            None,
            ["2025-11-24"],
        ),
    ],
)
def test_bronze_to_silver_end_to_end(
    tmp_path: Path,
    bronze_samples_dir: Path,
    config_name: str,
    load_pattern: str,
    pattern_dir: str,
    expected_silver_files: set[str] | None,
    run_dates: list[str],
) -> None:
    config_path = Path("docs/examples/configs") / config_name

    for run_date in run_dates:
        rewritten_cfg, bronze_out, silver_out, cfg_data = _rewrite_config(
            config_path, bronze_samples_dir, tmp_path, run_date
        )

        _run_cli(
            ["bronze_extract.py", "--config", str(rewritten_cfg), "--date", run_date]
        )

        bronze_partition = _collect_bronze_partition(bronze_out)
        metadata = _read_metadata(bronze_partition / "_metadata.json")
        assert metadata["load_pattern"] == load_pattern
        assert metadata["record_count"] > 0

        _run_cli(
            ["silver_extract.py", "--config", str(rewritten_cfg), "--date", run_date]
        )

        base_silver = Path(cfg_data["silver"]["output_dir"])
        if "source" in cfg_data:
            domain = cfg_data["silver"].get("domain", cfg_data["source"]["system"])
            entity = cfg_data["silver"].get("entity", cfg_data["source"]["table"])
        else:
            domain = cfg_data.get("domain") or cfg_data["silver"].get(
                "domain", cfg_data["system"]
            )
            entity = cfg_data["silver"].get("entity", cfg_data.get("entity"))
        version = cfg_data["silver"].get("version", 1)
        load_part = cfg_data["silver"].get("load_partition_name", "load_date")
        base_path = (
            base_silver / f"domain={domain}" / f"entity={entity}" / f"v{version}"
        )
        if cfg_data["silver"].get("include_pattern_folder"):
            base_path = base_path / f"pattern={pattern}"
        base_path = base_path / f"{load_part}={run_date}"
        assert base_path.exists()

        produced_files = {path.name for path in silver_out.rglob("*.parquet")}
        if expected_silver_files:
            assert expected_silver_files <= produced_files
        else:
            assert produced_files, "Expected at least one Silver artifact"

        for parquet_file in silver_out.rglob("*.parquet"):
            df = pd.read_parquet(parquet_file)
            assert len(df) > 0, f"{parquet_file} should contain rows"


def test_silver_require_checksum_succeeds(
    tmp_path: Path, bronze_samples_dir: Path
) -> None:
    config_path = Path("docs/examples/configs") / "file_example.yaml"
    run_date = "2025-11-13"

    rewritten_cfg, bronze_out, silver_out, cfg_data = _rewrite_config(
        config_path, bronze_samples_dir, tmp_path, run_date
    )
    cfg_data.setdefault("silver", {})["require_checksum"] = True
    rewritten_cfg.write_text(yaml.safe_dump(cfg_data))

    _run_cli(["bronze_extract.py", "--config", str(rewritten_cfg), "--date", run_date])

    # Should succeed because manifest remains intact
    _run_cli(["silver_extract.py", "--config", str(rewritten_cfg), "--date", run_date])

    assert any(
        silver_out.rglob("*.parquet")
    ), "Silver output should exist when manifest is present"


def test_silver_require_checksum_missing_manifest(
    tmp_path: Path, bronze_samples_dir: Path
) -> None:
    config_path = Path("docs/examples/configs") / "file_example.yaml"
    run_date = "2025-11-14"

    rewritten_cfg, bronze_out, silver_out, cfg_data = _rewrite_config(
        config_path, bronze_samples_dir, tmp_path, run_date
    )
    cfg_data.setdefault("silver", {})["require_checksum"] = True
    rewritten_cfg.write_text(yaml.safe_dump(cfg_data))

    _run_cli(["bronze_extract.py", "--config", str(rewritten_cfg), "--date", run_date])

    bronze_partition = _collect_bronze_partition(bronze_out)
    checksum_path = bronze_partition / "_checksums.json"
    checksum_path.unlink()

    with pytest.raises(subprocess.CalledProcessError):
        _run_cli(
            ["silver_extract.py", "--config", str(rewritten_cfg), "--date", run_date]
        )

    assert not any(
        silver_out.rglob("*.parquet")
    ), "Silver output should not be created when checksum is missing"
