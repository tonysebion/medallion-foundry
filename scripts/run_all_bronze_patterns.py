#!/usr/bin/env python
"""
Run Bronze extraction for every sample pattern so Bronze outputs mimic the source layout.

Usage:
    python scripts/run_all_bronze_patterns.py

By default this regenerates source samples before running Bronze. Pass
`--skip-sample-generation` to reuse whatever is already under sampledata/source_samples.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable, List

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]

PATTERN_CONFIGS = [
    {"config": "docs/examples/configs/patterns/pattern_full.yaml", "pattern": "pattern1_full_events"},
    {"config": "docs/examples/configs/patterns/pattern_cdc.yaml", "pattern": "pattern2_cdc_events"},
    {"config": "docs/examples/configs/patterns/pattern_current_history.yaml", "pattern": "pattern3_scd_state"},
    {"config": "docs/examples/configs/patterns/pattern_hybrid_cdc_point.yaml", "pattern": "pattern4_hybrid_cdc_point"},
    {"config": "docs/examples/configs/patterns/pattern_hybrid_cdc_cumulative.yaml", "pattern": "pattern5_hybrid_cdc_cumulative"},
    {"config": "docs/examples/configs/patterns/pattern_hybrid_incremental_point.yaml", "pattern": "pattern6_hybrid_incremental_point"},
    {"config": "docs/examples/configs/patterns/pattern_hybrid_incremental_cumulative.yaml", "pattern": "pattern7_hybrid_incremental_cumulative"},
    {"config": "docs/examples/configs/examples/file_example.yaml", "pattern": "pattern1_full_events"},
    {"config": "docs/examples/configs/patterns/file_cdc_example.yaml", "pattern": "pattern2_cdc_events"},
    {"config": "docs/examples/configs/patterns/file_current_history_example.yaml", "pattern": "pattern3_scd_state"},
]


def _extract_path_pattern_from_config(cfg: dict[str, object]) -> str:
    bronze_cfg = cfg.get("bronze", {})
    if "path_pattern" in bronze_cfg:
        return bronze_cfg["path_pattern"]
    source_cfg = cfg.get("source", {})
    file_cfg = source_cfg.get("file", {})
    if "path" in file_cfg:
        return file_cfg["path"]
    raise ValueError("Unable to determine Bronze source path in config")


def _pattern_base_dir(path_pattern: str) -> Path:
    candidate = Path(path_pattern)
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    for idx, part in enumerate(candidate.parts):
        if part.startswith("dt="):
            return Path(*candidate.parts[:idx])
    raise ValueError(f"No dt= directory found in path pattern {path_pattern}")


def _path_tail_after_dt(path_pattern: str) -> Path:
    candidate = Path(path_pattern)
    tail_parts: List[str] = []
    for idx, part in enumerate(candidate.parts):
        if part.startswith("dt="):
            tail_parts = list(candidate.parts[idx + 1 :])
            break
    return Path(*tail_parts) if tail_parts else Path()


def _discover_run_dates(
    config_path: Path, explicit_dates: Iterable[str] | None
) -> List[str]:
    if explicit_dates:
        return list(explicit_dates)

    cfg = yaml.safe_load(config_path.read_text())
    path_pattern = _extract_path_pattern_from_config(cfg)
    base_dir = _pattern_base_dir(path_pattern)
    tail = _path_tail_after_dt(path_pattern)
    dt_dirs = sorted(p for p in base_dir.glob("dt=*") if p.is_dir())
    if not dt_dirs:
        raise ValueError(f"No dt directories present under {base_dir}")

    valid_dates: List[str] = []
    for dt_dir in dt_dirs:
        candidate = dt_dir / tail if tail.parts else dt_dir
        if candidate.exists():
            valid_dates.append(dt_dir.name.split("=", 1)[1])

    if not valid_dates:
        raise ValueError(f"No valid files found for {config_path}")
    return valid_dates


def run_command(cmd: list[str], description: str) -> bool:
    """Execute a subprocess command."""
    print("\n" + "=" * 60)
    print(f"🔄 {description}")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ SUCCESS")
        return True
    print(f"❌ FAILED ({description})")
    if result.stdout:
        print("STDOUT:", result.stdout.strip())
    if result.stderr:
        print("STDERR:", result.stderr.strip())
    return False


def rewrite_config(
    original_path: str,
    run_date: str,
    temp_dir: Path,
    output_base: Path,
    pattern_folder: str | None,
) -> str:
    config = yaml.safe_load(Path(original_path).read_text())

    if "bronze" in config and "path_pattern" in config["bronze"]:
        import re

        config["bronze"]["path_pattern"] = re.sub(
            r"dt=\d{4}-\d{2}-\d{2}",
            f"dt={run_date}",
            config["bronze"]["path_pattern"],
        )

    if "bronze" in config:
        options = config["bronze"].setdefault("options", {})
        options["local_output_dir"] = str(output_base)
        if pattern_folder:
            options["pattern_folder"] = pattern_folder

    if "silver" in config:
        config["silver"]["output_dir"] = str(output_base / "silver")

    config_name = Path(original_path).name
    temp_config = temp_dir / f"temp_{config_name}_{run_date.replace('-', '')}"
    temp_config.write_text(yaml.safe_dump(config))
    return str(temp_config)


def process_run(task: dict[str, object]) -> tuple[str, str, bool]:
    config_path = task["config_path"]
    run_date = task["run_date"]
    temp_path = task["temp_path"]
    output_base = task["output_base"]
    total_runs = task["total_runs"]
    pattern_folder = task.get("pattern")

    run_count = task["run_count"]
    config_name = Path(config_path).name
    description = f"[{run_count}/{total_runs}] Bronze extraction: {config_name} ({run_date})"

    actual_config = rewrite_config(
        config_path,
        run_date,
        temp_path,
        output_base,
        pattern_folder,
    )

    success = run_command(
        [sys.executable, "bronze_extract.py", "--config", actual_config, "--date", run_date],
        description,
    )
    return config_name, run_date, success


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Bronze extraction for all pattern configs")
    parser.add_argument(
        "--skip-sample-generation",
        action="store_true",
        help="Skip generating the source sample data",
    )

    args = parser.parse_args()

    if not (REPO_ROOT / "scripts" / "generate_sample_data.py").exists():
        print("❌ Run me from the repository root.")
        return 1

    if not args.skip_sample_generation:
        if not run_command(
            [sys.executable, "scripts/generate_sample_data.py"],
            "Generating sample data",
        ):
            return 1

    pattern_runs: List[dict[str, object]] = []
    for entry in PATTERN_CONFIGS:
        config_path = entry["config"]
        run_dates = _discover_run_dates(REPO_ROOT / config_path, None)
        pattern_runs.append(
            {
                "config": config_path,
                "run_dates": run_dates,
                "pattern": entry.get("pattern"),
            }
        )

    total_runs = sum(len(entry["run_dates"]) for entry in pattern_runs)
    print(f"Configs to run: {len(pattern_runs)} ({total_runs} total Bronze runs)")

    bronze_out = Path("sampledata/bronze_outputs")
    bronze_out.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        tasks: list[dict[str, object]] = []
        run_count = 0
        for entry in pattern_runs:
            for run_date in entry["run_dates"]:
                run_count += 1
                tasks.append(
                    {
                        "config_path": entry["config"],
                        "run_date": run_date,
                        "run_count": run_count,
                        "temp_path": temp_path,
                        "output_base": bronze_out,
                        "total_runs": total_runs,
                        "pattern": entry["pattern"],
                    }
                )

        results: list[tuple[str, str, bool]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_run, task) for task in tasks]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

    successful = sum(1 for _, _, success in results if success)
    total = len(results)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"\nCompleted: {successful}/{total} runs")

    if successful == total:
        print("\n✅ ALL BRONZE EXTRACTIONS SUCCEEDED!")
        print(f"\n📁 Bronze outputs: {bronze_out.resolve()}")
    else:
        print("\n❌ SOME BRONZE EXTRACTIONS FAILED")
        print("\nFailed runs:")
        for config_name, run_date, success in results:
            if not success:
                print(f"   ❌ {config_name} ({run_date})")
    return 0 if successful == total else 1


if __name__ == "__main__":
    sys.exit(main())
