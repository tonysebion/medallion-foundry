"""Integration tests for pattern-aware sample data."""

from __future__ import annotations

from datetime import date, timedelta
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

BRONZE_SAMPLE_ROOT = Path("sampledata/source_samples")
BRONZE_EXAMPLE_ROOT = BRONZE_SAMPLE_ROOT
PATTERN_DIRS = {
    "full": "pattern1_full_events",
    "cdc": "pattern2_cdc_events",
    "current_history": "pattern3_scd_state",
    "hybrid_cdc_point": "pattern4_hybrid_cdc_point",
    "hybrid_cdc_cumulative": "pattern5_hybrid_cdc_cumulative",
    "hybrid_incremental_point": "pattern6_hybrid_incremental_point",
    "hybrid_incremental_cumulative": "pattern7_hybrid_incremental_cumulative",
}

HYBRID_REFERENCE_INITIAL = date(2025, 11, 13)
HYBRID_REFERENCE_SECOND = HYBRID_REFERENCE_INITIAL + timedelta(days=9)
HYBRID_DELTA_DAYS = 28
HYBRID_DELTA_PATTERN = "incremental_merge"


def _pattern_dir(pattern: str) -> str:
    return PATTERN_DIRS.get(pattern, pattern)
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
        pattern_dir = _pattern_dir(load_pattern)
        system = cfg["source"]["system"]
        table = cfg["source"]["table"]
        filename = Path(cfg["source"]["file"]["path"]).name
        tail = [filename]
    else:
        load_pattern = bronze_options.get("load_pattern", "full")
        pattern_dir = _pattern_dir(bronze_options.get("pattern_folder") or load_pattern)
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
    return (
        BRONZE_SAMPLE_ROOT
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


def _read_bronze_parquet(bronze_partition: Path) -> pd.DataFrame:
    parquet_files = list(bronze_partition.glob("*.parquet"))
    assert parquet_files, f"No Parquet artifacts found under {bronze_partition}"
    return pd.concat((pd.read_parquet(path) for path in parquet_files), ignore_index=True)


def _source_csv_path_from_cfg(cfg: dict[str, object]) -> Path:
    if "source" in cfg:
        return Path(cfg["source"]["file"]["path"])
    bronze_cfg = cfg.get("bronze", {})
    if "path_pattern" in bronze_cfg:
        return Path(bronze_cfg["path_pattern"])
    raise ValueError("Cannot determine source CSV path")


def _reference_date_for_delta(delta_date: date) -> date:
    if delta_date <= HYBRID_REFERENCE_SECOND:
        return HYBRID_REFERENCE_INITIAL
    return HYBRID_REFERENCE_SECOND


def _reference_csv_path(delta_source: Path) -> Path:
    dt_dir = next(p for p in delta_source.parents if p.name.startswith("dt="))
    pattern_root = dt_dir.parent
    run_date_str = dt_dir.name.split("=", 1)[1]
    delta_date = date.fromisoformat(run_date_str)
    reference_date = _reference_date_for_delta(delta_date)
    return pattern_root / f"dt={reference_date.isoformat()}" / "reference" / "reference-part-0001.csv"


def _expected_hybrid_delta_tags(run_date: str) -> set[str]:
    run_date_obj = date.fromisoformat(run_date)
    delta_span = (run_date_obj - HYBRID_REFERENCE_INITIAL).days
    delta_span = min(max(delta_span, 0), HYBRID_DELTA_DAYS)
    return {
        f"{HYBRID_DELTA_PATTERN}-{(HYBRID_REFERENCE_INITIAL + timedelta(days=offset)).isoformat()}"
        for offset in range(1, delta_span + 1)
    }


@pytest.mark.parametrize(
    "config_name, load_pattern, pattern_dir, expected_silver_files, run_dates",
    [
        (
            "examples/file_example.yaml",
            "full",
            "full",
            {"events.parquet"},
            ["2025-11-13", "2025-11-14"],
        ),
        (
            "patterns/file_cdc_example.yaml",
            "cdc",
            "cdc",
            {"events.parquet"},
            ["2025-11-13", "2025-11-14"],
        ),
        (
            "patterns/file_current_history_example.yaml",
            "current_history",
            "current_history",
            {"state_history.parquet", "state_current.parquet"},
            ["2025-11-13", "2025-11-14"],
        ),
        (
            "patterns/pattern_full.yaml",
            "full",
            "full",
            {"events.parquet"},
            ["2025-11-13"],
        ),
        (
            "patterns/pattern_cdc.yaml",
            "cdc",
            "cdc",
            {"events.parquet"},
            ["2025-11-13"],
        ),
        (
            "patterns/pattern_current_history.yaml",
            "current_history",
            "current_history",
            {"state_history.parquet", "state_current.parquet"},
            ["2025-11-13"],
        ),
        (
            "patterns/pattern_hybrid_cdc_point.yaml",
            "cdc",
            "hybrid_cdc_point",
            None,
            ["2025-11-24"],
        ),
        (
            "patterns/pattern_hybrid_cdc_cumulative.yaml",
            "cdc",
            "hybrid_cdc_cumulative",
            None,
            ["2025-11-24"],
        ),
        (
            "patterns/pattern_hybrid_incremental_point.yaml",
            "cdc",
            "hybrid_incremental_point",
            None,
            ["2025-11-24"],
        ),
        (
            "patterns/pattern_hybrid_incremental_cumulative.yaml",
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
        if pattern_dir == "hybrid_incremental_cumulative":
            bronze_df = _read_bronze_parquet(bronze_partition)
            assert {"delta_tag", "change_type"} <= set(bronze_df.columns), "Hybrid samples must emit delta metadata"
            source_path = _source_csv_path_from_cfg(cfg_data)
            source_df = pd.read_csv(source_path)
            assert set(source_df.columns) <= set(bronze_df.columns), "Bronze schema should include source columns"
            bronze_tag_counts = bronze_df["delta_tag"].dropna().astype(str).value_counts().to_dict()
            source_tag_counts = source_df["delta_tag"].dropna().astype(str).value_counts().to_dict()
            assert bronze_tag_counts == source_tag_counts, "Per-tag row counts must match source"
            bronze_change_counts = bronze_df["change_type"].dropna().astype(str).value_counts().to_dict()
            source_change_counts = source_df["change_type"].dropna().astype(str).value_counts().to_dict()
            assert bronze_change_counts == source_change_counts, "Change-type counts must survive the Bronze layer"
            reference_path = _reference_csv_path(source_path)
            if reference_path.exists():
                reference_df = pd.read_csv(reference_path)
                delta_ids = {str(value) for value in source_df["order_id"].dropna()}
                reference_ids = {str(value) for value in reference_df["order_id"].dropna()}
                assert not delta_ids.intersection(reference_ids), "Delta records should not duplicate reference IDs"
            expected_tags = _expected_hybrid_delta_tags(run_date)
            assert expected_tags, "Expected at least one cumulative delta tag for this run_date"
            actual_tags = {str(tag) for tag in bronze_df["delta_tag"].dropna()}
            missing_tags = expected_tags - actual_tags
            assert not missing_tags, f"Bronze is missing cumulative tags for {run_date}: {sorted(missing_tags)}"
            change_types = {str(value) for value in bronze_df["change_type"].dropna()}
            assert {"insert", "update"} <= change_types, "Hybrid Bronze output should contain insert/update ops"

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
    config_path = Path("docs/examples/configs/examples/file_example.yaml")
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
    config_path = Path("docs/examples/configs/examples/file_example.yaml")
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
