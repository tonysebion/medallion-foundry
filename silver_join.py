"""Join two Silver assets into a curated third output asset."""

from __future__ import annotations

import argparse
import json
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import yaml

from core.io import write_batch_metadata
from core.patterns import LoadPattern
from core.run_options import RunOptions
from core.silver_models import SilverModel, resolve_profile
from core.storage import get_storage_backend
from core.storage_policy import enforce_storage_scope, validate_storage_metadata
from silver_extract import write_silver_outputs

logger = logging.getLogger(__name__)

VALID_JOIN_TYPES = {"inner", "left", "right", "outer"}
MODEL_FALLBACK_ORDER = [
    SilverModel.SCD_TYPE_2,
    SilverModel.FULL_MERGE_DEDUPE,
    SilverModel.SCD_TYPE_1,
    SilverModel.PERIODIC_SNAPSHOT,
    SilverModel.INCREMENTAL_MERGE,
]


def parse_config(path: Path) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if "silver_join" not in data:
        raise ValueError("Config must include a 'silver_join' section")
    return data, data["silver_join"]


def normalize_platform_config(cfg: Dict[str, Any] | None) -> Dict[str, Any]:
    cfg = cfg or {}
    if "bronze" in cfg:
        bronze_cfg = dict(cfg["bronze"])
    else:
        bronze_cfg = dict(cfg)
    normalized: Dict[str, Any] = {"bronze": bronze_cfg}
    for key, value in cfg.items():
        if key == "bronze":
            continue
        normalized[key] = value
    return normalized


def merge_platform_configs(base: Dict[str, Any] | None, override: Dict[str, Any] | None) -> Dict[str, Any]:
    base_norm = normalize_platform_config(base)
    override_norm = normalize_platform_config(override)
    merged = dict(base_norm)
    merged["bronze"] = {**base_norm.get("bronze", {}), **override_norm.get("bronze", {})}
    for key, value in override_norm.items():
        if key == "bronze":
            continue
        merged[key] = value
    return merged


def fetch_asset_local(entry: Dict[str, Any], tmp_dir: Path, global_platform: Dict[str, Any], storage_scope: str) -> Path:
    platform_cfg = merge_platform_configs(global_platform, entry.get("platform", {}))
    validate_storage_metadata(platform_cfg)
    enforce_storage_scope(platform_cfg, storage_scope)

    backend = get_storage_backend({"platform": platform_cfg})
    requested_path = Path(entry["path"])
    if requested_path.exists():
        return requested_path

    prefix = entry["path"].rstrip("/\\")
    if not prefix:
        raise ValueError("Remote path must be provided when the local path does not exist")

    target_dir = tmp_dir / (entry.get("name") or prefix.replace("/", "_").replace("\\", "_"))
    target_dir.mkdir(parents=True, exist_ok=True)

    files = backend.list_files(prefix)
    if not files:
        raise FileNotFoundError(f"No remote files found under {prefix}")

    for remote in files:
        rel = Path(remote).relative_to(prefix) if remote.startswith(prefix) else Path(remote).name
        local_path = target_dir / rel
        local_path.parent.mkdir(parents=True, exist_ok=True)
        backend.download_file(remote, str(local_path))

    return target_dir


def read_metadata(silver_path: Path) -> Dict[str, Any]:
    metadata_path = silver_path / "_metadata.json"
    if metadata_path.exists():
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    else:
        logger.warning("No metadata found for %s; falling back to minimal defaults", silver_path)
        payload = {}
    payload.setdefault("silver_path", str(silver_path))
    return payload


def read_silver_data(silver_path: Path) -> pd.DataFrame:
    parquet_paths = sorted(silver_path.glob("**/*.parquet"))
    csv_paths = sorted(silver_path.glob("**/*.csv"))
    frames: List[pd.DataFrame] = []
    for path in parquet_paths:
        frames.append(pd.read_parquet(path))
    for path in csv_paths:
        frames.append(pd.read_csv(path))
    if not frames:
        raise FileNotFoundError(f"No data files found under {silver_path}")
    return pd.concat(frames, ignore_index=True)


def chunk_dataframe(df: pd.DataFrame, chunk_size: int) -> List[pd.DataFrame]:
    if chunk_size <= 0:
        return [df]
    return [df.iloc[i : i + chunk_size] for i in range(0, len(df), chunk_size)]


def normalize_join_keys(raw_keys: Any) -> List[str]:
    if raw_keys is None:
        return []
    if isinstance(raw_keys, (list, tuple)):
        keys = list(raw_keys)
    else:
        keys = [raw_keys]
    return [str(key) for key in keys if key]


def perform_join(left: pd.DataFrame, right: pd.DataFrame, output_cfg: Dict[str, Any]) -> pd.DataFrame:
    join_type = output_cfg.get("join_type", "inner").strip().lower()
    if join_type not in VALID_JOIN_TYPES:
        raise ValueError(f"join_type must be one of {', '.join(VALID_JOIN_TYPES)}")

    join_keys = normalize_join_keys(output_cfg.get("join_keys"))
    if not join_keys:
        raise ValueError("silver_join.output.join_keys must list at least one key")

    missing = [key for key in join_keys if key not in left.columns or key not in right.columns]
    if missing:
        raise ValueError(f"Join keys missing from inputs: {missing}")

    chunk_size = max(int(output_cfg.get("chunk_size") or 0), 0)
    if chunk_size > 0:
        merged_parts: List[pd.DataFrame] = []
        for chunk in chunk_dataframe(left, chunk_size):
            merged_parts.append(pd.merge(chunk, right, how=join_type, on=join_keys))
        joined = pd.concat(merged_parts, ignore_index=True)
    else:
        joined = pd.merge(left, right, how=join_type, on=join_keys)

    projection = output_cfg.get("select_columns") or output_cfg.get("projection")
    if projection:
        projected = [str(col) for col in projection]
        missing_projection = [col for col in projected if col not in joined.columns]
        if missing_projection:
            raise ValueError(f"Projection references unknown columns: {missing_projection}")
        joined = joined[projected]

    return joined


def determine_model(requested: str | None, metadata: Dict[str, Any]) -> SilverModel:
    profile = resolve_profile(requested)
    if profile:
        return profile
    if requested:
        return SilverModel.normalize(requested)
    return SilverModel.default_for_load_pattern(LoadPattern.FULL)


def _model_supported(model: SilverModel, metadata_list: List[Dict[str, Any]]) -> bool:
    if not model.requires_dedupe:
        return True
    for meta in metadata_list:
        if not meta.get("primary_keys") or not meta.get("order_column"):
            return False
    return True


def select_model(requested_model: str | None, metadata_list: List[Dict[str, Any]]) -> SilverModel:
    desired = determine_model(requested_model, metadata_list[0])
    if _model_supported(desired, metadata_list):
        return desired
    for candidate in MODEL_FALLBACK_ORDER:
        if candidate == desired:
            continue
        if _model_supported(candidate, metadata_list):
            logger.warning(
                "Falling back from %s to %s because metadata lacked dedupe/order metadata",
                desired.value,
                candidate.value,
            )
            return candidate
    raise ValueError("No suitable Silver model could be satisfied by the inputs")


def build_run_options(output_cfg: Dict[str, Any], metadata_list: List[Dict[str, Any]]) -> RunOptions:
    formats = {fmt.strip().lower() for fmt in output_cfg.get("formats", ["parquet"])}
    write_parquet = "parquet" in formats
    write_csv = "csv" in formats
    if not (write_parquet or write_csv):
        raise ValueError("silver_join.output.formats must include at least one of 'parquet' or 'csv'")

    primary_keys = metadata_list[0].get("primary_keys") or output_cfg.get("primary_keys") or []
    order_column = metadata_list[0].get("order_column") or output_cfg.get("order_column")
    return RunOptions(
        load_pattern=LoadPattern.FULL,
        require_checksum=False,
        write_parquet=write_parquet,
        write_csv=write_csv,
        parquet_compression=output_cfg.get("parquet_compression", "snappy"),
        primary_keys=primary_keys,
        order_column=order_column,
        partition_columns=output_cfg.get("partition_columns", []),
        artifact_names=RunOptions.default_artifacts(),
    )


def build_schema_snapshot(df: pd.DataFrame) -> Dict[str, str]:
    return {col: str(df[col].dtype) for col in df.columns}


def write_output(
    df: pd.DataFrame,
    base_dir: Path,
    model: SilverModel,
    run_opts: RunOptions,
    metadata_list: List[Dict[str, Any]],
    output_cfg: Dict[str, Any],
    source_paths: List[str],
) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    outputs = write_silver_outputs(
        df,
        base_dir,
        LoadPattern.FULL,
        run_opts.primary_keys,
        run_opts.order_column,
        run_opts.write_parquet,
        run_opts.write_csv,
        run_opts.parquet_compression,
        run_opts.artifact_names,
        run_opts.partition_columns,
        {},
        model,
    )
    chunk_count = sum(len(paths) for paths in outputs.values())
    join_keys = normalize_join_keys(output_cfg.get("join_keys"))
    metadata = {
        "silver_model": model.value,
        "requested_model": output_cfg.get("model"),
        "formats": {"parquet": run_opts.write_parquet, "csv": run_opts.write_csv},
        "join_type": output_cfg.get("join_type"),
        "join_keys": join_keys,
        "chunk_size": output_cfg.get("chunk_size"),
        "projection": output_cfg.get("select_columns") or output_cfg.get("projection"),
        "lineage": [meta.get("silver_path") or "<missing>" for meta in metadata_list],
        "joined_sources": source_paths,
        "schema": build_schema_snapshot(df),
    }
    write_batch_metadata(
        base_dir,
        record_count=len(df),
        chunk_count=chunk_count,
        extra_metadata=metadata,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Join two Silver assets into a curated third asset")
    parser.add_argument("--config", required=True, help="Silver join configuration YAML")
    parser.add_argument(
        "--storage-scope",
        choices=["any", "onprem"],
        default="any",
        help="Enforce storage classification policy (onprem rejects cloud backends).",
    )
    parser.add_argument(
        "--onprem-only",
        dest="storage_scope",
        action="store_const",
        const="onprem",
        help="Alias for `--storage-scope onprem`",
    )
    args = parser.parse_args()

    full_config, join_config = parse_config(Path(args.config))
    platform_cfg = full_config.get("platform", {})
    validate_storage_metadata(platform_cfg)
    enforce_storage_scope(platform_cfg, args.storage_scope)

    left_config = join_config["left"]
    right_config = join_config["right"]
    output_cfg = join_config.get("output")
    if output_cfg is None:
        raise ValueError("silver_join.output must be configured")

    metadata_list: List[Dict[str, Any]] = []
    source_paths: List[str] = [left_config["path"], right_config["path"]]
    joined_df: pd.DataFrame | None = None
    with tempfile.TemporaryDirectory(prefix="silver_join_") as workspace_dir:
        workspace = Path(workspace_dir)
        left_path = fetch_asset_local(left_config, workspace, platform_cfg, args.storage_scope)
        right_path = fetch_asset_local(right_config, workspace, platform_cfg, args.storage_scope)
        left_meta = read_metadata(left_path)
        right_meta = read_metadata(right_path)
        metadata_list = [left_meta, right_meta]
        left_df = read_silver_data(left_path)
        right_df = read_silver_data(right_path)
        joined_df = perform_join(left_df, right_df, output_cfg)

    if not metadata_list or joined_df is None:
        raise RuntimeError("Join could not be executed due to missing inputs")

    requested_model = output_cfg.get("model") or SilverModel.default_for_load_pattern(LoadPattern.FULL).value
    model = select_model(requested_model, metadata_list)
    run_opts = build_run_options(output_cfg, metadata_list)
    write_output(joined_df, Path(output_cfg["path"]), model, run_opts, metadata_list, output_cfg, source_paths)
    logger.info("Joined silver asset written to %s (model=%s)", output_cfg["path"], model.value)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
