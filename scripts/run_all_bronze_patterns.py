#!/usr/bin/env python
"""
Run Bronze extraction for every sample pattern so Bronze outputs mirror the source layout.

Usage:
    python scripts/run_all_bronze_patterns.py

This script assumes each pattern's source samples already exist remotely (S3)
and writes Bronze outputs into temporary local staging directories before uploading
them using the config settings.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import boto3  # noqa: E402
import yaml  # noqa: E402

from botocore.exceptions import ClientError  # noqa: E402
from core.config.dataset import DatasetConfig, dataset_to_runtime_config  # noqa: E402
from core.config.loader import build_relative_path, load_config_with_env  # noqa: E402
from core.config.environment import EnvironmentConfig  # noqa: E402

PATTERN_CONFIGS = [
    {
        "config": "docs/examples/configs/patterns/pattern_full.yaml",
        "pattern": "pattern1_full_events",
    },
    {
        "config": "docs/examples/configs/patterns/pattern_cdc.yaml",
        "pattern": "pattern2_cdc_events",
    },
    {
        "config": "docs/examples/configs/patterns/pattern_current_history.yaml",
        "pattern": "pattern3_scd_state",
    },
    {
        "config": "docs/examples/configs/patterns/pattern_hybrid_cdc_point.yaml",
        "pattern": "pattern4_hybrid_cdc_point",
    },
    {
        "config": "docs/examples/configs/patterns/pattern_hybrid_cdc_cumulative.yaml",
        "pattern": "pattern5_hybrid_cdc_cumulative",
    },
    {
        "config": "docs/examples/configs/patterns/pattern_hybrid_incremental_point.yaml",
        "pattern": "pattern6_hybrid_incremental_point",
    },
    {
        "config": "docs/examples/configs/patterns/pattern_hybrid_incremental_cumulative.yaml",
        "pattern": "pattern7_hybrid_incremental_cumulative",
    },
]


def _extract_path_pattern_from_config(cfg: Dict[str, Any]) -> str:
    bronze_cfg = cfg.get("bronze", {})
    if "path_pattern" in bronze_cfg:
        return str(bronze_cfg["path_pattern"])
    source_cfg = cfg.get("source", {})
    file_cfg = source_cfg.get("file", {})
    if "path" in file_cfg:
        return str(file_cfg["path"])
    raise ValueError("Unable to determine Bronze source path in config")


def _pattern_base_dir(path_pattern: str) -> Path:
    candidate = Path(path_pattern)
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    for idx, part in enumerate(candidate.parts):
        if part.startswith("dt="):
            return Path(*candidate.parts[:idx])
    raise ValueError(f"No dt= directory found in path pattern {path_pattern}")


def _tail_after_dt(path_pattern: str) -> Path:
    candidate = Path(path_pattern)
    for idx, part in enumerate(candidate.parts):
        if part.startswith("dt="):
            tail_parts = list(candidate.parts[idx + 1 :])
            return Path(*tail_parts) if tail_parts else Path()
    return Path()


def _split_path_pattern(path_pattern: str) -> tuple[str, str, str]:
    normalized = path_pattern.replace("\\", "/").rstrip("/")
    dt_idx = normalized.find("dt=")
    if dt_idx == -1:
        raise ValueError(f"No dt= segment found in path_pattern: {path_pattern}")
    prefix = normalized[:dt_idx].rstrip("/")
    suffix = normalized[dt_idx:]
    if "/" in suffix:
        dt_segment, tail = suffix.split("/", 1)
    else:
        dt_segment, tail = suffix, ""
    return prefix, dt_segment, tail


def _build_s3_client(env_config: EnvironmentConfig):
    s3_cfg = env_config.s3
    client_kwargs = {}
    if s3_cfg.endpoint_url:
        client_kwargs["endpoint_url"] = s3_cfg.endpoint_url
    if s3_cfg.region:
        client_kwargs["region_name"] = s3_cfg.region
    return boto3.client(
        "s3",
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        **client_kwargs,
    )


def _object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return False
        raise


def _resolve_s3_sample_path(client, bucket: str, candidate: str) -> str | None:
    normalized = candidate.rstrip("/")

    # CRITICAL FIX: Skip metadata files to prevent reading them as data
    filename = Path(normalized).name
    if filename.startswith("_"):
        return None

    if "." in filename:
        return normalized if _object_exists(client, bucket, normalized) else None

    for ext in ("parquet", "csv"):
        candidate_with_ext = f"{normalized}.{ext}"
        if _object_exists(client, bucket, candidate_with_ext):
            return candidate_with_ext
    return None


def _relative_path_for_dataset(dataset: DatasetConfig, run_date_str: str) -> str:
    runtime_config = dataset_to_runtime_config(dataset)
    run_date_obj = date.fromisoformat(run_date_str)
    return build_relative_path(runtime_config, run_date_obj)


def _build_bronze_destination(
    dataset: DatasetConfig,
    run_date_str: str,
) -> str:
    runtime_config = dataset_to_runtime_config(dataset)
    bronze_cfg = runtime_config.get("platform", {}).get("bronze", {})
    bucket = bronze_cfg.get("s3_bucket")
    if not bucket:
        raise ValueError("Unable to determine S3 bucket for Bronze output")
    prefix = bronze_cfg.get("s3_prefix", "").strip("/")
    relative = _relative_path_for_dataset(dataset, run_date_str)
    combined = f"{prefix}/{relative}" if prefix else relative
    return f"s3://{bucket}/{combined}"


def _discover_run_dates_s3(
    cfg: Dict[str, Any],
    env_config: EnvironmentConfig,
    explicit_dates: Iterable[str] | None,
) -> list[dict]:
    if explicit_dates:
        return [{"run_date": date, "sample_path": None} for date in explicit_dates]

    storage = cfg.get("storage", {})
    source = storage.get("source", {})
    backend = source.get("backend", "local")
    if backend != "s3":
        raise ValueError("S3 discovery invoked but backend is not 's3'")

    bucket_ref = source.get("bucket")
    if not bucket_ref:
        raise ValueError("storage.source.bucket required for S3 discovery")
    prefix = source.get("prefix", "").strip("/")
    bronze_cfg = cfg.get("bronze", {})
    path_pattern = bronze_cfg.get("path_pattern")
    if not path_pattern:
        raise ValueError("bronze.path_pattern is required to discover dates")

    base, dt_segment, tail = _split_path_pattern(path_pattern)
    base_root = "/".join(part for part in (prefix, base) if part).rstrip("/")
    if base_root and not base_root.endswith("/"):
        base_root += "/"

    client = _build_s3_client(env_config)
    bucket = env_config.s3.get_bucket(bucket_ref)
    run_dates_dict: dict[str, str] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=base_root):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.startswith(base_root):
                continue

            # CRITICAL FIX: Skip metadata and checksum files
            if key.endswith("_metadata.json") or key.endswith("_checksums.json"):
                continue
            # Skip any files starting with underscore
            if "/_" in key or key.endswith("/"):
                continue

            remainder = key[len(base_root) :].lstrip("/")
            if not remainder.startswith("dt="):
                continue
            parts = remainder.split("/", 1)
            dt_name = parts[0]
            date_value = dt_name.split("=", 1)[1]
            tail_path = parts[1] if len(parts) > 1 else ""
            candidate = f"{base}/{dt_name}"
            if tail_path:
                candidate = f"{candidate}/{tail_path}"
            sample_path = (
                _resolve_s3_sample_path(client, bucket, candidate) or candidate
            )
            if sample_path and date_value not in run_dates_dict:
                run_dates_dict[date_value] = sample_path

    run_dates = [
        {"run_date": dt, "sample_path": sample}
        for dt, sample in sorted(run_dates_dict.items())
    ]

    if not run_dates:
        raise ValueError(f"No dt= prefixes found under s3://{bucket}/{base_root}")

    return run_dates


def _discover_run_dates_local(
    config_path: Path,
    explicit_dates: Iterable[str] | None,
    cfg: Dict[str, Any],
) -> list[dict]:
    if explicit_dates:
        return [{"run_date": date, "sample_path": None} for date in explicit_dates]

    base_dir = _pattern_base_dir(_extract_path_pattern_from_config(cfg))
    tail = _tail_after_dt(_extract_path_pattern_from_config(cfg))
    dt_dirs = sorted(p for p in base_dir.glob("dt=*") if p.is_dir())
    if not dt_dirs:
        raise ValueError(f"No dt directories under {base_dir}")

    valid_dates: list[dict] = []
    for dt_dir in dt_dirs:
        candidate = dt_dir / tail if tail.parts else dt_dir
        sample_path = _resolve_sample_path(dt_dir, candidate)
        if sample_path:
            valid_dates.append(
                {"run_date": dt_dir.name.split("=", 1)[1], "sample_path": sample_path}
            )
    if not valid_dates:
        raise ValueError(f"No valid files were found for {config_path}")

    return valid_dates


def _ensure_environment_bucket_refs(storage: Dict[str, Any]) -> None:
    def _ensure(section_name: str, default_bucket: str) -> None:
        section = storage.setdefault(section_name, {})
        backend = section.get("backend")
        if backend == "s3":
            section.setdefault("bucket", default_bucket)

    _ensure("source", "source_data")
    _ensure("bronze", "bronze_data")
    _ensure("silver", "silver_data")


def _load_config_dict(config_path: Path) -> Dict[str, Any]:
    cfg = yaml.safe_load(config_path.read_text())
    if not isinstance(cfg, dict):
        raise ValueError("Config must be a YAML dictionary/object")
    storage = cfg.setdefault("storage", {})
    _ensure_environment_bucket_refs(storage)
    return cfg


def _discover_run_dates(
    config_path: Path,
    explicit_dates: Iterable[str] | None,
    env_config: EnvironmentConfig,
    cfg: Dict[str, Any] | None = None,
) -> list[dict]:
    cfg = cfg or _load_config_dict(config_path)
    storage = cfg.get("storage", {})
    source_backend = storage.get("source", {}).get("backend", "local")
    if source_backend == "s3":
        return _discover_run_dates_s3(cfg, env_config, explicit_dates)
    return _discover_run_dates_local(config_path, explicit_dates, cfg)


def _resolve_sample_path(dt_dir: Path, candidate: Path) -> Path | None:
    """Return a data file path for the given dt directory, preferring the candidate."""
    if candidate.exists():
        return candidate

    # Try alternative extensions (csv <-> parquet) if candidate has a file name
    if candidate.name:
        for alt_ext in (".parquet", ".csv"):
            try:
                alt_path = candidate.with_suffix(alt_ext)
            except ValueError:
                continue
            if alt_path.exists():
                return alt_path

    # Fallback to any parquet/csv inside the dt directory
    for ext in ("parquet", "csv"):
        for path in sorted(dt_dir.rglob(f"*.{ext}")):
            if path.name.startswith("_"):
                continue
            return path

    return None


def run_command(cmd: list[str], description: str) -> bool:
    print("\n" + "=" * 60)
    print(f"Running: {description}")
    joined_cmd = " ".join(cmd)
    print(f"Command: {joined_cmd}")
    print("=" * 60)
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode == 0:
        print("SUCCESS")
        return True
    print(f"FAILED ({description})")
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
    sample_path: str | Path | None = None,
    limit_records: int | None = None,
) -> str:
    config = yaml.safe_load(Path(original_path).read_text())

    if "bronze" in config and "path_pattern" in config["bronze"]:
        import re

        if sample_path:
            sample_path_str = str(sample_path)
            config["bronze"]["path_pattern"] = sample_path_str
            bronze_options = config["bronze"].setdefault("options", {})
            extension = Path(sample_path_str).suffix.lower().lstrip(".")
            if extension:
                bronze_options["format"] = extension
        else:
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

    if limit_records:
        source_cfg = config.setdefault("source", {})
        file_cfg = source_cfg.setdefault("file", {})
        file_cfg["limit_rows"] = limit_records

    os.makedirs(output_base, exist_ok=True)
    temp_config = (
        temp_dir / f"temp_{Path(original_path).stem}_{run_date.replace('-', '')}.yaml"
    )
    temp_config.write_text(yaml.safe_dump(config))
    return str(temp_config), config


def process_run(task: Dict[str, Any]) -> tuple[str, str, bool]:
    config_path = task["config_path"]
    run_date = task["run_date"]
    temp_path = task["temp_path"]
    bronze_out = task["output_base"]
    total_runs = task["total_runs"]
    pattern_folder = task.get("pattern_folder")
    run_count = task["run_count"]
    env_config = task["env_config"]
    dataset = task["dataset"]

    config_name = Path(config_path).name
    description = (
        f"[{run_count}/{total_runs}] Bronze extraction: {config_name} ({run_date})"
    )

    actual_config, rewritten_cfg = rewrite_config(
        config_path,
        run_date,
        temp_path,
        bronze_out,
        pattern_folder,
        sample_path=task.get("sample_path"),
        limit_records=task.get("limit_records"),
    )
    dest_path = _build_bronze_destination(dataset, run_date)
    print(
        f"[{run_count}/{total_runs}] Bronze run for {config_name} ({run_date}) writes to {dest_path}",
        flush=True,
    )

    success = run_command(
        [
            sys.executable,
            "bronze_extract.py",
            "--config",
            actual_config,
            "--date",
            run_date,
        ],
        description,
    )
    return config_name, run_date, success


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Bronze extraction for all pattern configs"
    )
    parser.add_argument(
        "--limit-records",
        type=int,
        default=None,
        help="Limit each Bronze run to the first N rows per file (for quick dry-runs)",
    )

    args = parser.parse_args()

    if not (REPO_ROOT / "scripts" / "generate_sample_data.py").exists():
        print("Please run from the repository root.")
        return 1

    print("Preparing discovery (this can take a while)...", flush=True)

    print(
        "Scanning source_samples in S3 for available pattern dates (this may take a moment)...",
        flush=True,
    )
    pattern_runs: list[Dict[str, Any]] = []
    for entry in PATTERN_CONFIGS:
        config_path = entry["config"]
        config_full_path = REPO_ROOT / config_path
        cfg = _load_config_dict(config_full_path)
        dataset, env_config = load_config_with_env(config_full_path)
        if not env_config:
            print(f"Environment config missing for {config_path}")
            return 1
        pattern_folder = (
            cfg.get("bronze", {}).get("options", {}).get("pattern_folder")
            or cfg.get("pattern_id")
            or entry.get("pattern")
        )
        if pattern_folder:
            dataset.bronze.options["pattern_folder"] = pattern_folder
        run_dates = _discover_run_dates(config_full_path, None, env_config, cfg)
        print(
            f"Pattern {entry.get('pattern', config_path)} has {len(run_dates)} dates",
            flush=True,
        )
        for d in run_dates[:3]:
            print(
                f"  sample path: {d['sample_path']} (run_date={d['run_date']})",
                flush=True,
            )
        pattern_runs.append(
            {
                "config": config_path,
                "run_dates": run_dates,
                "pattern": entry.get("pattern"),
                "pattern_folder": pattern_folder,
                "env_config": env_config,
                "dataset": dataset,
            }
        )

    total_runs = sum(len(entry["run_dates"]) for entry in pattern_runs)
    print(f"Configs to run: {len(pattern_runs)} ({total_runs} Bronze runs)", flush=True)

    limit_records = args.limit_records
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        tasks: list[Dict[str, Any]] = []
        run_counter = 0
        for entry in pattern_runs:
            pattern_dir = temp_path / (entry["pattern"] or Path(entry["config"]).stem)
            for run_info in entry["run_dates"]:
                run_counter += 1
                run_output = pattern_dir / run_info["run_date"]
                run_output.mkdir(parents=True, exist_ok=True)
                tasks.append(
                    {
                        "config_path": entry["config"],
                        "run_date": run_info["run_date"],
                        "run_count": run_counter,
                        "temp_path": temp_path,
                        "output_base": run_output,
                        "total_runs": total_runs,
                        "pattern": entry["pattern"],
                        "pattern_folder": entry.get("pattern_folder"),
                        "sample_path": run_info.get("sample_path"),
                        "limit_records": limit_records,
                        "env_config": entry["env_config"],
                        "dataset": entry["dataset"],
                    }
                )

        print(
            f"Scheduling {len(tasks)} Bronze runs ({total_runs} total) across patterns",
            flush=True,
        )

        results: list[tuple[str, str, bool]] = []
        # Reduced from 10 to 3 workers to prevent S3 timeout/resource exhaustion
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
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
        print("ALL BRONZE EXTRACTIONS SUCCEEDED!")
        print(
            "Bronze outputs were uploaded per configuration and temporary staging directories were removed."
        )
    else:
        print("SOME BRONZE EXTRACTIONS FAILED")
        print("Failed runs:")
        for config_name, run_date, success in results:
            if not success:
                print(f"   - {config_name} ({run_date})")

    return 0 if successful == total else 1


if __name__ == "__main__":
    sys.exit(main())
