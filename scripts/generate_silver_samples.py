"""Generate Silver artifacts for every Bronze sample partition."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_SAMPLE_ROOT = REPO_ROOT / "sampledata" / "bronze_samples"
# Silver artifacts now land under sampledata/silver_samples for easy reuse
SILVER_SAMPLE_ROOT = REPO_ROOT / "sampledata" / "silver_samples"
CONFIGS_DIR = REPO_ROOT / "docs" / "examples" / "configs" / "patterns"


@dataclass(frozen=True)
class PatternConfig:
    path: Path
    match_dirs: tuple[str, ...] | None


# Ensure project root on sys.path when executed as standalone script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _find_brone_partitions() -> Iterable[Dict[str, object]]:
    seen: set[str] = set()
    # Accept any partition directory containing at least one CSV even if metadata file absent
    for dir_path in BRONZE_SAMPLE_ROOT.rglob("dt=*"):
        if not dir_path.is_dir():
            continue
        csv_files = sorted(dir_path.rglob("*.csv"))
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
        chunk_dir = csv_files[0].parent
        extra_parts = chunk_dir.relative_to(dir_path).parts
        for part in extra_parts:
            suffix_parts.append(part.replace("=", "-"))
        suffix = "_".join(suffix_parts)
        label = f"{pattern}_{run_date}"
        if suffix:
            label = f"{label}_{suffix}"
        key = f"{pattern}|{run_date}|{chunk_dir}"
        if key in seen:
            continue
        seen.add(key)
        sample = {
            "pattern": pattern,
            "run_date": run_date,
            "label": label,
            "dir": chunk_dir,
            "file": csv_files[0],
        }
        yield sample


def _pattern_from_config(cfg: Dict[str, object]) -> str | None:
    bronze = cfg.get("bronze", {})
    options = bronze.get("options", {}) or {}
    pattern_folder = options.get("pattern_folder") or bronze.get("pattern_folder")
    if pattern_folder:
        return pattern_folder
    return cfg.get("pattern")


def _discover_pattern_configs() -> Dict[str, List[PatternConfig]]:
    configs: Dict[str, List[PatternConfig]] = {}
    if not CONFIGS_DIR.is_dir():
        raise FileNotFoundError(f"Patterns directory {CONFIGS_DIR} not found")
    for path in sorted(CONFIGS_DIR.glob("pattern*.yaml")):
        if not path.is_file():
            continue
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
        pattern = _pattern_from_config(cfg)
        if not pattern:
            continue
        bronze = cfg.get("bronze", {})
        options = bronze.get("options", {}) or {}
        match_dir = options.get("match_dir")
        if isinstance(match_dir, str):
            match_dirs: tuple[str, ...] = (match_dir,)
        elif isinstance(match_dir, (list, tuple)):
            match_dirs = tuple(match_dir)
        else:
            match_dirs = None
        configs.setdefault(pattern, []).append(
            PatternConfig(path=path, match_dirs=match_dirs)
        )
    return configs


def _silver_label_from_partition(partition: Dict[str, object], config_path: Path) -> str:
    label_parts = [partition["pattern"], config_path.stem, partition["run_date"]]
    dir_name = partition["dir"].name
    dt_label = f"dt={partition['run_date']}"
    if dir_name and dir_name != dt_label:
        label_parts.append(dir_name)
    return "_".join(label_parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Silver samples derived from the Bronze fixtures"
    )
    parser.add_argument(
        "--formats",
        choices=["parquet", "csv", "both"],
        default="both",
        help="Which Silver artifact formats to write",
    )
    return parser.parse_args()


def _run_cli(cmd: list[str]) -> None:
    subprocess.run([sys.executable, *cmd], check=True, cwd=REPO_ROOT)


def _write_label_readme(
    label_dir: Path, partition: Dict[str, object], config_path: Path
) -> None:
    readme = label_dir / "README.md"
    content = f"""# Silver samples ({label_dir.name})

Derived from Bronze partition `{partition['dir']}` using config `{config_path.name}`.
"""
    readme.write_text(content, encoding="utf-8")


def _generate_for_partition(
    partition: Dict[str, object],
    config: PatternConfig,
    enable_parquet: bool,
    enable_csv: bool,
) -> None:
    dir_name = partition["dir"].name
    if config.match_dirs and dir_name not in config.match_dirs:
        return
    config_path = config.path
    silver_label = _silver_label_from_partition(partition, config_path)
    silver_base = SILVER_SAMPLE_ROOT / silver_label
    silver_base.mkdir(parents=True, exist_ok=True)
    cmd = [
        "silver_extract.py",
        "--config",
        str(config_path),
        "--bronze-path",
        str(partition["dir"]),
        "--date",
        partition["run_date"],
        "--silver-base",
        str(silver_base),
    ]
    if enable_parquet:
        cmd.append("--write-parquet")
    else:
        cmd.append("--no-write-parquet")
    if enable_csv:
        cmd.append("--write-csv")
    else:
        cmd.append("--no-write-csv")
    _run_cli(cmd)
    intent_dest = silver_base / "intent.yaml"
    shutil.copy(config_path, intent_dest)
    _write_label_readme(silver_base, partition, config_path)


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
    pattern_configs = _discover_pattern_configs()
    for partition in partitions:
        configs = pattern_configs.get(partition["pattern"])
        if not configs:
            raise ValueError(
                f"No pattern configs found for pattern '{partition['pattern']}'"
            )
        for config_variant in configs:
            _generate_for_partition(
                partition, config_variant, enable_parquet, enable_csv
            )
    print(f"Silver samples materialized under {SILVER_SAMPLE_ROOT}")


if __name__ == "__main__":
    main()
