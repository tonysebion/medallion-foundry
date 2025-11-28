"""Usage-focused regression tests that mirror the owner learning path."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
GENERATE_SCRIPT = Path("scripts") / "generate_sample_data.py"
BRONZE_SAMPLE_ROOT = Path("sampledata/source_samples")
PATTERN_DIRS = {
    "full": "pattern1_full_events",
    "cdc": "pattern2_cdc_events",
    "current_history": "pattern3_scd_state",
    "hybrid_cdc_point": "pattern4_hybrid_cdc_point",
    "hybrid_cdc_cumulative": "pattern5_hybrid_cdc_cumulative",
    "hybrid_incremental_point": "pattern6_hybrid_incremental_point",
    "hybrid_incremental_cumulative": "pattern7_hybrid_incremental_cumulative",
}


def _pattern_dir(pattern: str) -> str:
    return PATTERN_DIRS.get(pattern, pattern)


CONFIG_ROOT = Path("docs/examples/configs")


@pytest.fixture(scope="module")
def bronze_samples_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Ensure the sample data exists and copy it to an isolated workspace."""
    subprocess.run([sys.executable, str(GENERATE_SCRIPT)], check=True, cwd=REPO_ROOT)
    if not BRONZE_SAMPLE_ROOT.exists():
        pytest.skip("Sample data missing; run scripts/generate_sample_data.py")
    dest = tmp_path_factory.mktemp("usage_samples")
    shutil.copytree(BRONZE_SAMPLE_ROOT, dest, dirs_exist_ok=True)
    return dest


def _build_sample_path(
    bronze_dir: Path, cfg: dict[str, object], run_date: str, config_path: Path
) -> Path:
    """Recreate the pattern-aware bronze path logic used in docs configs."""
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
) -> tuple[Path, Path, Path, dict[str, object]]:
    """Copy the config and massage the paths to point at the sample data + tmp outputs."""
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


def _collect_metadata(output_root: Path) -> Path:
    metadata_files = list(output_root.rglob("_metadata.json"))
    assert metadata_files, "Expected Bronze metadata to be created"
    return metadata_files[0]


USAGE_CONFIGS = [
    ("examples/file_example.yaml", ["2025-11-13"], {"events.parquet"}),
    ("patterns/pattern_full.yaml", ["2025-11-13"], {"events.parquet"}),
    ("patterns/pattern_cdc.yaml", ["2025-11-13"], {"events.parquet"}),
    (
        "patterns/pattern_current_history.yaml",
        ["2025-11-13"],
        {"state_current.parquet", "state_history.parquet"},
    ),
]


@pytest.mark.parametrize(
    "config_name, run_dates, expected_silver_files",
    USAGE_CONFIGS,
    ids=[entry[0] for entry in USAGE_CONFIGS],
)
def test_quickstart_and_pattern_flow(
    tmp_path: Path,
    bronze_samples_dir: Path,
    config_name: str,
    run_dates: list[str],
    expected_silver_files: set[str],
) -> None:
    config_path = CONFIG_ROOT / config_name
    for run_date in run_dates:
        target, bronze_out, silver_out, cfg = _rewrite_config(
            config_path, bronze_samples_dir, tmp_path, run_date
        )
        _run_cli(["bronze_extract.py", "--config", str(target), "--date", run_date])
        meta_file = _collect_metadata(bronze_out)
        metadata = json.loads(meta_file.read_text())
        assert metadata.get("run_date"), "Run metadata should record the run_date"
        assert metadata.get("system"), "System identifier should exist in metadata"
        assert metadata.get("table"), "Table identifier should exist in metadata"

        _run_cli(["silver_extract.py", "--config", str(target), "--date", run_date])
        for filename in expected_silver_files:
            matches = list(silver_out.rglob(filename))
            assert matches, f"{filename} missing in {silver_out}"


def test_owner_intent_expansion(tmp_path: Path) -> None:
    output = tmp_path / "owner_intent_resolved.yaml"
    subprocess.run(
        [
            sys.executable,
            "scripts/expand_owner_intent.py",
            "--config",
            "docs/examples/configs/templates/owner_intent_template.yaml",
            "--output",
            str(output),
        ],
        check=True,
        cwd=REPO_ROOT,
    )
    intent = yaml.safe_load(output.read_text())
    assert intent.get("datasets"), "Intent template should define datasets"
    for dataset in intent["datasets"]:
        derived = dataset.get("derived_paths", {})
        assert derived.get("bronze"), "Derived bronze path must exist"
        assert derived.get("silver"), "Derived silver path must exist"
