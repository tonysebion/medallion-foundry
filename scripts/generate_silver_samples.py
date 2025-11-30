"""Generate Silver artifacts into S3 storage for every Bronze sample partition.

Silver samples mirror the Bronze hierarchy under the configured S3 bucket.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import boto3
import yaml

if TYPE_CHECKING:
    from core.config.dataset import (
        DatasetConfig,
        PolybaseExternalDataSource,
        PolybaseExternalFileFormat,
        PolybaseExternalTable,
        PolybaseSetup,
    )
    from core.config.environment import EnvironmentConfig

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config.environment import EnvironmentConfig
from core.config.loader import load_config_with_env

CONFIGS_DIR = REPO_ROOT / "docs" / "examples" / "configs" / "patterns"

SILVER_MODEL_MAP = {
    ("state", "scd2"): "scd_type_2",
    ("state", "scd1"): "scd_type_1",
    ("state", None): "scd_type_1",
    ("event", "incremental"): "incremental_merge",
    ("event", None): "incremental_merge",
    ("events", "incremental"): "incremental_merge",
    ("events", None): "incremental_merge",
    ("snapshot", None): "periodic_snapshot",
    ("derived_event", None): "incremental_merge",
    ("derived_state", None): "scd_type_1",
    ("hybrid", "incremental"): "incremental_merge",
    ("hybrid", "cumulative"): "full_merge_dedupe",
}


@dataclass(frozen=True)
class PatternConfig:
    path: Path
    pattern_folder: str
    silver_model: str
    domain: str
    entity: str
    dataset: "DatasetConfig"
    env_config: Optional["EnvironmentConfig"] = None


@dataclass(frozen=True)
class BronzePartition:
    pattern: str
    run_date: str
    s3_bucket: str
    s3_prefix: str


def _clear_path(target: Path) -> None:
    if not target.exists():
        return
    try:
        shutil.rmtree(target)
    except OSError:
        for child in target.glob("**/*"):
            try:
                if child.is_file() or child.is_symlink():
                    child.unlink()
                elif child.is_dir():
                    shutil.rmtree(child)
            except OSError:
                continue
        try:
            target.rmdir()
        except OSError:
            pass


def _pattern_from_config(cfg: Dict[str, Any]) -> Optional[str]:
    bronze = cfg.get("bronze", {})
    options = bronze.get("options", {}) or {}
    pattern_folder = options.get("pattern_folder") or bronze.get("pattern_folder")
    if pattern_folder:
        return str(pattern_folder)
    val = cfg.get("pattern")
    return str(val) if val is not None else None


def _silver_model_from_config(cfg: Dict[str, Any]) -> str:
    silver = cfg.get("silver", {})
    if not isinstance(silver, dict):
        silver = {}
    if "model" in silver:
        return str(silver["model"])
    entity_kind = silver.get("entity_kind", "state")
    history_mode = silver.get("history_mode")
    return SILVER_MODEL_MAP.get((entity_kind, history_mode), "scd_type_1")


def _discover_pattern_configs() -> Dict[str, List[PatternConfig]]:
    configs: Dict[str, List[PatternConfig]] = {}
    if not CONFIGS_DIR.is_dir():
        raise FileNotFoundError(f"Patterns directory {CONFIGS_DIR} not found")
    for path in sorted(CONFIGS_DIR.glob("pattern*.yaml")):
        if not path.is_file():
            continue
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            continue
        pattern = _pattern_from_config(raw)
        if not pattern:
            continue
        silver_model = _silver_model_from_config(raw)
        try:
            dataset, env_config = load_config_with_env(path)
        except Exception as exc:
            print(f"[WARN] Skipping config {path.name}: {exc}")
            continue
        domain_value = dataset.domain or dataset.system or "default"
        entity_value = dataset.entity
        configs.setdefault(pattern, []).append(
            PatternConfig(
                path=path,
                pattern_folder=pattern,
                silver_model=silver_model,
                domain=domain_value,
                entity=entity_value,
                dataset=dataset,
                env_config=env_config,
            )
        )
    return configs


def _build_s3_client(env_config: EnvironmentConfig) -> boto3.client:
    client_kwargs: Dict[str, Any] = {}
    if env_config.s3.endpoint_url:
        client_kwargs["endpoint_url"] = env_config.s3.endpoint_url
    if env_config.s3.region:
        client_kwargs["region_name"] = env_config.s3.region
    return boto3.client(
        "s3",
        aws_access_key_id=env_config.s3.access_key_id,
        aws_secret_access_key=env_config.s3.secret_access_key,
        **client_kwargs,
    )


def _discover_s3_partitions(config: PatternConfig) -> List[BronzePartition]:
    if not config.env_config or not config.env_config.s3:
        raise RuntimeError(
            f"Pattern {config.pattern_folder} requires an S3 environment config"
        )
    client = _build_s3_client(config.env_config)
    bucket = config.env_config.s3.get_bucket(
        config.dataset.bronze.output_bucket or "bronze_data"
    )
    prefix_base = (config.dataset.bronze.output_prefix or "bronze_samples/").strip("/")
    path_root = "/".join(
        part
        for part in (
            prefix_base,
            f"system={config.dataset.system}",
            f"table={config.dataset.entity}",
            f"pattern={config.pattern_folder}",
        )
        if part
    )
    if not path_root.endswith("/"):
        path_root += "/"
    partitions: List[BronzePartition] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=path_root, Delimiter="/"):
        for common in page.get("CommonPrefixes", []):
            dt_prefix = common["Prefix"]
            dt_name = Path(dt_prefix.rstrip("/")).name
            if not dt_name.startswith("dt="):
                continue
            run_date = dt_name.split("=", 1)[1]
            has_data = False
            data_page = client.list_objects_v2(Bucket=bucket, Prefix=dt_prefix)
            for obj in data_page.get("Contents", []):
                filename = Path(obj["Key"]).name
                if filename.startswith("_"):
                    continue
                if filename.lower().endswith((".parquet", ".csv")):
                    has_data = True
                    break
            if not has_data:
                continue
            partitions.append(
                BronzePartition(pattern=config.pattern_folder, run_date=run_date, s3_bucket=bucket, s3_prefix=dt_prefix)
            )
    return partitions


def _list_all_s3_partitions(pattern_configs: Dict[str, List[PatternConfig]]) -> List[BronzePartition]:
    partitions: List[BronzePartition] = []
    for configs in pattern_configs.values():
        for config in configs:
            partitions.extend(_discover_s3_partitions(config))
    return partitions


def _build_s3_silver_prefix(config: PatternConfig) -> str:
    prefix_base = (config.dataset.silver.output_prefix or "silver_samples/").strip("/")
    silver_subpath = Path(f"sample={config.pattern_folder}") / f"silver_model={config.silver_model}"
    if prefix_base:
        return f"{prefix_base}/{silver_subpath.as_posix()}"
    return silver_subpath.as_posix()


def _download_partition_from_s3(partition: BronzePartition, env_config: EnvironmentConfig) -> Path:
    client = _build_s3_client(env_config)
    temp_dir = Path(tempfile.mkdtemp(prefix=f"bronze_{partition.pattern}_{partition.run_date}_"))
    files_copied = False
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=partition.s3_bucket, Prefix=partition.s3_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            filename = Path(key).name
            if filename.startswith("_"):
                continue
            relative = key[len(partition.s3_prefix):].lstrip("/")
            if not relative:
                continue
            dest = temp_dir / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(partition.s3_bucket, key, str(dest))
            files_copied = True
    if not files_copied:
        _clear_path(temp_dir)
        raise RuntimeError(f"No data files downloaded for partition {partition.s3_prefix}")
    return temp_dir


def _rewrite_local_silver_config(original: Path, target: Path) -> Path:
    cfg = yaml.safe_load(original.read_text(encoding="utf-8")) or {}
    bronze = cfg.setdefault("bronze", {})
    bronze.setdefault("source_storage", "local")
    bronze.setdefault("output_storage", "local")
    silver = cfg.setdefault("silver", {})
    silver.setdefault("input_storage", "local")
    silver.setdefault("output_storage", "local")
    target.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    return target


def _upload_directory_to_s3(local_root: Path, bucket: str, prefix: str, env_config: EnvironmentConfig) -> None:
    client = _build_s3_client(env_config)
    normalized_prefix = prefix.rstrip("/")
    for file_path in sorted(local_root.rglob("*")):
        if file_path.is_dir():
            continue
        relative = file_path.relative_to(local_root).as_posix()
        key = f"{normalized_prefix}/{relative}" if normalized_prefix else relative
        client.upload_file(str(file_path), bucket, key)


def _write_pattern_readme(pattern_dir: Path, pattern_id: str, silver_model: str) -> None:
    readme = pattern_dir / "README.md"
    content = f"""# Silver Samples - {pattern_id}

## Overview
Silver artifacts derived from Bronze `{pattern_id}` samples.

## Structure
```
{pattern_dir.name}/
  domain={{domain}}/
    entity={{entity}}/
      v{{version}}/
        load_date={{YYYY-MM-DD}}/
          {{artifacts}}
```

## Silver Model
- **Model Type**: `{silver_model}`
- Derived from Bronze pattern and silver configuration

## Files
- `intent.yaml` - Original intent config
- `_metadata.json` / `_checksums.json` - Batch metadata
- `*.parquet` / `*.csv` - Silver artifacts

## Generation
Generated by: `python scripts/generate_silver_samples.py`
"""
    readme.write_text(content, encoding="utf-8")


def _run_cli(cmd: List[str]) -> None:
    subprocess.run([sys.executable, *cmd], check=True, cwd=REPO_ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Silver samples into S3 storage matching Bronze sample patterns"
    )
    parser.add_argument(
        "--formats",
        choices=["parquet", "csv", "both"],
        default="both",
        help="Which Silver artifact formats to write",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of sample partitions generated",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent silver_extract worker subprocesses",
    )
    parser.add_argument(
        "--artifact-writer",
        choices=["default", "transactional"],
        default="transactional",
        help="Artifact writer kind (transactional recommended)",
    )
    parser.add_argument(
        "--use-locks",
        action="store_true",
        help="Acquire filesystem locks around each silver_extract run",
    )
    return parser.parse_args()


def _resolve_format_flags(config: PatternConfig, format_choice: str) -> tuple[bool, bool]:
    silver = config.dataset.silver
    requested_parquet = format_choice in {"parquet", "both"}
    requested_csv = format_choice in {"csv", "both"}
    allowed_parquet = bool(silver.write_parquet)
    allowed_csv = bool(silver.write_csv)
    effective_parquet = requested_parquet and allowed_parquet
    effective_csv = requested_csv and allowed_csv
    if requested_csv and not allowed_csv:
        print(
            f"[INFO] Skipping CSV output for pattern={config.pattern_folder}; "
            "pattern config disables `silver.write_csv`"
        )
    return effective_parquet, effective_csv



def _generate_for_partition(
    partition: BronzePartition,
    config: PatternConfig,
    enable_parquet: bool,
    enable_csv: bool,
    artifact_writer: str,
    chunk_tag: Optional[str],
    use_locks: bool,
) -> tuple[str, str]:
    run_date = partition.run_date
    bronze_dir = _download_partition_from_s3(partition, config.env_config)
    try:
        temp_silver_root = Path(tempfile.mkdtemp(prefix=f"silver_{partition.pattern}_{run_date}_"))
        try:
            silver_base = temp_silver_root / f"sample={config.pattern_folder}" / f"silver_model={config.silver_model}"
            silver_base.mkdir(parents=True, exist_ok=True)
            pattern_root = silver_base.parent
            intent_dest = pattern_root / f"intent_{config.path.stem}.yaml"
            shutil.copyfile(config.path, intent_dest)
            local_config = pattern_root / f"intent_{config.path.stem}_local.yaml"
            _rewrite_local_silver_config(config.path, local_config)
            cmd = [
                "silver_extract.py",
                "--config",
                str(local_config),
                "--bronze-path",
                str(bronze_dir),
                "--date",
                run_date,
                "--silver-base",
                str(silver_base),
            ]
            if artifact_writer:
                cmd.extend(["--artifact-writer", artifact_writer])
            if chunk_tag:
                cmd.extend(["--chunk-tag", chunk_tag])
            if use_locks:
                cmd.append("--use-locks")
            if enable_parquet:
                cmd.append("--write-parquet")
            else:
                cmd.append("--no-write-parquet")
            if enable_csv:
                cmd.append("--write-csv")
            else:
                cmd.append("--no-write-csv")
            print(f"Generating Silver for sample={partition.pattern}/silver_model={config.silver_model} run_date={run_date}")
            _run_cli(cmd)
            _write_pattern_readme(pattern_root, config.pattern_folder, config.silver_model)
            silver_prefix = _build_s3_silver_prefix(config)
            bucket_ref = config.env_config.s3.get_bucket(
                config.dataset.silver.output_bucket or "silver_data"
            )
            _upload_directory_to_s3(pattern_root, bucket_ref, silver_prefix, config.env_config)
            return bucket_ref, silver_prefix
        finally:
            _clear_path(silver_base.parent)
            _clear_path(temp_silver_root)
    finally:
        _clear_path(bronze_dir)


def main() -> None:
    args = parse_args()
    enable_parquet = args.formats in {"parquet", "both"}
    enable_csv = args.formats in {"csv", "both"}
    pattern_configs = _discover_pattern_configs()
    partitions = _list_all_s3_partitions(pattern_configs)
    if not partitions:
        raise RuntimeError(
            "No Bronze partitions found for any configured pattern; ensure Bronze outputs exist in S3"
        )
    limit = args.limit if args.limit and args.limit > 0 else None
    task_list: List[tuple] = []
    generated = 0
    for partition in partitions:
        configs = pattern_configs.get(partition.pattern)
        if not configs:
            continue
        for config in configs:
            if limit is not None and generated >= limit:
                break
            chunk_tag = None
            eff_parquet, eff_csv = _resolve_format_flags(config, args.formats)
            task_list.append(
                (
                    partition,
                    config,
                    eff_parquet,
                    eff_csv,
                    args.artifact_writer,
                    chunk_tag,
                    args.use_locks,
                )
            )
            generated += 1
        if limit is not None and generated >= limit:
            break
    if not task_list:
        print("[WARN] No Silver generation tasks found; nothing to run")
        return
    from concurrent.futures import ThreadPoolExecutor, as_completed

    failures: List[tuple] = []
    upload_locations: List[str] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(_generate_for_partition, *task) for task in task_list]
        for fut in as_completed(futures):
            try:
                bucket, prefix = fut.result()
                upload_locations.append(f"s3://{bucket}/{prefix}")
            except Exception as exc:
                failures.append(exc)
    if failures:
        print(f"[ERROR] {len(failures)} Silver generation tasks failed:")
        for exc in failures:
            print(f" - {exc}")
        raise RuntimeError("One or more Silver generation tasks failed")
    unique_locations = sorted(set(upload_locations))
    print("\n[OK] Generated Silver samples in S3 to the following prefixes:")
    for loc in unique_locations[:5]:
        print(f"  {loc}")
    if len(unique_locations) > 5:
        print(f"  ...and {len(unique_locations) - 5} more prefixes")


if __name__ == "__main__":
    main()
