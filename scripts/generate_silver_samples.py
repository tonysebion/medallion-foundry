"""Generate Silver artifacts for every Bronze sample partition."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_SAMPLE_ROOT = REPO_ROOT / "docs" / "examples" / "data" / "bronze_samples"
SILVER_SAMPLE_ROOT = REPO_ROOT / "docs" / "examples" / "data" / "silver_samples"
CONFIGS_DIR = REPO_ROOT / "docs" / "examples" / "configs"

SILVER_MODELS = [
    "scd_type_1",
    "scd_type_2",
    "incremental_merge",
    "full_merge_dedupe",
    "periodic_snapshot",
]
PATTERN_CONFIG = {
    "full": "file_example.yaml",
    "cdc": "file_cdc_example.yaml",
    "current_history": "file_current_history_example.yaml",
    "hybrid_cdc_point": "file_cdc_example.yaml",
    "hybrid_cdc_cumulative": "file_cdc_example.yaml",
    "hybrid_incremental_point": "file_example.yaml",
    "hybrid_incremental_cumulative": "file_example.yaml",
}
PATTERN_LOAD = {
    "hybrid_cdc_point": "cdc",
    "hybrid_cdc_cumulative": "cdc",
}


def _ensure_minimum_bronze_samples() -> None:
    """Create a minimal Bronze sample set if none exist (backwards compatibility).

    Tests expect sample generation to succeed even on a fresh clone. If the user
    hasn't run the Bronze sample generation yet, synthesize a tiny fixture set
    for the supported patterns so Silver samples can still be produced.
    """
    if BRONZE_SAMPLE_ROOT.exists() and any(BRONZE_SAMPLE_ROOT.rglob("*.csv")):
        return
    # Construct minimal partitions for today's date
    import datetime as _dt
    run_date = _dt.date.today().isoformat()
    samples = [
        ("full", ["order_id,customer_id,status,order_total,updated_at", f"1,10,NEW,12.50,{run_date}T00:00:00Z"]),
        ("cdc", ["order_id,customer_id,status,order_total,changed_at", f"1,10,NEW,12.50,{run_date}T01:00:00Z"]),
        ("current_history", ["order_id,customer_id,status,current_flag,effective_start,effective_end,updated_at", f"1,10,NEW,true,{run_date},{run_date},{run_date}T02:00:00Z"]),
    ]
    for pattern, lines in samples:
        dir_path = (
            BRONZE_SAMPLE_ROOT
            / pattern
            / "system=retail_demo"
            / "table=orders"
            / f"pattern={pattern}"
            / f"dt={run_date}"
        )
        dir_path.mkdir(parents=True, exist_ok=True)
        filename = {
            "full": "full-part-0001.csv",
            "cdc": "cdc-part-0001.csv",
            "current_history": "current-history-part-0001.csv",
        }[pattern]
        (dir_path / filename).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _find_brone_partitions() -> Iterable[Dict[str, object]]:
    _ensure_minimum_bronze_samples()
    seen: set[str] = set()
    # Accept any partition directory containing at least one CSV even if metadata file absent
    for dir_path in BRONZE_SAMPLE_ROOT.rglob("dt=*"):
        if not dir_path.is_dir():
            continue
        csv_files = sorted(dir_path.glob("*.csv"))
        if not csv_files:
            continue
        rel_parts = dir_path.relative_to(BRONZE_SAMPLE_ROOT).parts
        pattern_part = next((p for p in rel_parts if p.startswith("pattern=")), None)
        dt_part = next((p for p in rel_parts if p.startswith("dt=")), None)
        if not pattern_part or not dt_part:
            continue
        pattern = pattern_part.split("=", 1)[1]
        run_date = dt_part.split("=", 1)[1]
        suffix_parts = []
        for part in rel_parts:
            if part in (pattern_part, dt_part):
                continue
            if part.startswith("system=") or part.startswith("table="):
                continue
            suffix_parts.append(part.replace("=", "-"))
        suffix = "_".join(suffix_parts)
        label = f"{pattern}_{run_date}"
        if suffix:
            label = f"{label}_{suffix}"
        key = f"{pattern}|{run_date}|{dir_path}"
        if key in seen:
            continue
        seen.add(key)
        sample = {
            "pattern": pattern,
            "run_date": run_date,
            "label": label,
            "dir": dir_path,
            "file": csv_files[0],
        }
        yield sample


def _rewrite_config(
    template: Path,
    partition: Dict[str, object],
    tmp_dir: Path,
) -> Path:
    cfg = yaml.safe_load(template.read_text(encoding="utf-8"))
    bronze_file = partition["file"]
    cfg["source"]["file"]["path"] = str(bronze_file)
    run_cfg = cfg["source"]["run"]
    run_cfg["local_output_dir"] = str(tmp_dir / f"bronze_out_{partition['run_date']}")
    mapped_load = PATTERN_LOAD.get(partition["pattern"])
    if mapped_load:
        run_cfg["load_pattern"] = mapped_load
    target = tmp_dir / f"{template.stem}_{partition['label']}_{partition['run_date']}.yaml"
    target.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return target


def _rewrite_silver_config(
    template: Path,
    partition: Dict[str, object],
    tmp_dir: Path,
    silver_model: str,
    enable_parquet: bool,
    enable_csv: bool,
) -> Path:
    cfg = yaml.safe_load(template.read_text(encoding="utf-8"))
    bronze_file = partition["file"]
    cfg["source"]["file"]["path"] = str(bronze_file)
    cfg["source"]["run"]["local_output_dir"] = str(tmp_dir / f"bronze_out_{partition['run_date']}")
    silver_cfg = cfg.setdefault("silver", {})
    partition_cfg = dict(silver_cfg.get("partitioning", {}))
    partition_cfg["columns"] = []
    silver_cfg["partitioning"] = partition_cfg
    silver_base = SILVER_SAMPLE_ROOT / partition["label"] / silver_model
    silver_base.mkdir(parents=True, exist_ok=True)
    cfg["silver"]["output_dir"] = str(silver_base)
    cfg["silver"]["write_parquet"] = enable_parquet
    cfg["silver"]["write_csv"] = enable_csv
    target = tmp_dir / f"{template.stem}_{silver_model}_{partition['label']}_{partition['run_date']}.yaml"
    target.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return target


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Silver samples derived from the Bronze fixtures")
    parser.add_argument(
        "--formats",
        choices=["parquet", "csv", "both"],
        default="both",
        help="Which Silver artifact formats to write",
    )
    return parser.parse_args()


def _run_cli(cmd: list[str]) -> None:
    subprocess.run([sys.executable, *cmd], check=True, cwd=REPO_ROOT)


def _generate_for_partition(partition: Dict[str, object], tmp_dir: Path, enable_parquet: bool, enable_csv: bool) -> None:
    pattern = partition["pattern"]
    config_name = PATTERN_CONFIG.get(pattern, "file_example.yaml")
    template = CONFIGS_DIR / config_name
    silver_models = SILVER_MODELS
    for silver_model in silver_models:
        silver_cfg = _rewrite_silver_config(
            template,
            partition,
            tmp_dir,
            silver_model,
            enable_parquet=enable_parquet,
            enable_csv=enable_csv,
        )
        _run_cli(
            [
                "silver_extract.py",
                "--config",
                str(silver_cfg),
                "--bronze-path",
                str(partition["dir"]),
                "--date",
                partition["run_date"],
                "--silver-model",
                silver_model,
            ]
        )


def _synthesize_sample_readmes(root_dir: Path) -> None:
    for label_dir in root_dir.iterdir():
        if not label_dir.is_dir():
            continue
        for model_dir in label_dir.iterdir():
            if not model_dir.is_dir():
                continue
            readme_path = model_dir / "README.md"
            model_name = model_dir.name.replace("_", " ")
            content = f"""# Silver samples ({model_name})

Derived from Bronze partition `{label_dir.name}` using Silver model `{model_dir.name}`.
"""
            readme_path.write_text(content, encoding="utf-8")


def main() -> None:
    args = parse_args()
    enable_parquet = args.formats in {"parquet", "both"}
    enable_csv = args.formats in {"csv", "both"}

    partitions = list(_find_brone_partitions())
    if not partitions:
        raise RuntimeError("No Bronze partitions found; generate Bronze samples first.")

    if SILVER_SAMPLE_ROOT.exists():
        shutil.rmtree(SILVER_SAMPLE_ROOT)
    SILVER_SAMPLE_ROOT.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="silver_samples_gen_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        for partition in partitions:
            _generate_for_partition(partition, tmp_dir_path, enable_parquet, enable_csv)

    _synthesize_sample_readmes(SILVER_SAMPLE_ROOT)
    print(f"Silver samples materialized under {SILVER_SAMPLE_ROOT}")


if __name__ == "__main__":
    main()
