"""Join two Silver assets into a curated third output asset."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import yaml

from core.io import write_batch_metadata
from core.patterns import LoadPattern
from core.storage_policy import enforce_storage_scope
from core.silver_models import SilverModel, resolve_profile
from core.run_options import RunOptions
from silver_extract import write_silver_outputs

logger = logging.getLogger(__name__)


def parse_config(path: Path) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    data = yaml.safe_load(path.read_text())
    if "silver_join" not in data:
        raise ValueError("Config must include a 'silver_join' section")
    return data, data["silver_join"]


def read_metadata(silver_path: Path) -> Dict[str, Any]:
    metadata_path = silver_path / "_metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"No metadata found at {metadata_path}")
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def read_silver_data(silver_path: Path) -> pd.DataFrame:
    parquet_paths = sorted(silver_path.glob("**/*.parquet"))
    csv_paths = sorted(silver_path.glob("**/*.csv"))
    frames: List[pd.DataFrame] = []
    for path in parquet_paths:
        frames.append(pd.read_parquet(path))
    for path in csv_paths:
        frames.append(pd.read_csv(path))
    if not frames:
        raise FileNotFoundError(f"No data files found in {silver_path}")
    return pd.concat(frames, ignore_index=True)


def determine_model(requested: str, metadata: Dict[str, Any]) -> SilverModel:
    profile = resolve_profile(requested)
    if profile:
        return profile
    return SilverModel.normalize(requested)


def select_model(requested_model: str, metadata_list: List[Dict[str, Any]]) -> SilverModel:
    fallback_order = [
        SilverModel.SCD_TYPE_2,
        SilverModel.FULL_MERGE_DEDUPE,
        SilverModel.SCD_TYPE_1,
        SilverModel.PERIODIC_SNAPSHOT,
        SilverModel.INCREMENTAL_MERGE,
    ]
    desired = determine_model(requested_model, metadata_list[0])
    if _model_supported(desired, metadata_list):
        return desired
    for candidate in fallback_order:
        if candidate == desired:
            continue
        if _model_supported(candidate, metadata_list):
            logger.warning("Falling back from %s to %s due to metadata constraints", desired, candidate)
            return candidate
    raise ValueError("No suitable Silver model could be satisfied by the inputs")


def _model_supported(model: SilverModel, metadata_list: List[Dict[str, Any]]) -> bool:
    if model.requires_dedupe:
        for meta in metadata_list:
            if not meta.get("primary_keys") or not meta.get("order_column"):
                return False
    return True


def build_run_options(output: Dict[str, Any], metadata_list: List[Dict[str, Any]]) -> RunOptions:
    primary_keys = metadata_list[0].get("primary_keys", []) or output.get("primary_keys", [])
    order_column = metadata_list[0].get("order_column") or output.get("order_column")
    formats = output.get("formats", ["parquet"])
    return RunOptions(
        load_pattern=LoadPattern.FULL,
        require_checksum=False,
        write_parquet="parquet" in formats,
        write_csv="csv" in formats,
        parquet_compression=output.get("parquet_compression", "snappy"),
        primary_keys=primary_keys,
        order_column=order_column,
        partition_columns=output.get("partition_columns", []),
        artifact_names=RunOptions.default_artifacts(),
    )


def write_output(df: pd.DataFrame, base_dir: Path, model: SilverModel, run_opts: RunOptions, metadata_list: List[Dict[str, Any]]) -> None:
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
    metadata = {
        "joined_sources": [meta.get("bronze_path") for meta in metadata_list],
        "silver_model": model.value,
        "formats": {
            "parquet": run_opts.write_parquet,
            "csv": run_opts.write_csv,
        },
    }
    write_batch_metadata(base_dir, record_count=len(df), chunk_count=len(outputs), extra_metadata=metadata)


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

    full_config, config = parse_config(Path(args.config))
    enforce_storage_scope(full_config.get("platform", {}), args.storage_scope)

    left_meta = read_metadata(Path(config["left"]["path"]))
    right_meta = read_metadata(Path(config["right"]["path"]))
    metadata_list = [left_meta, right_meta]
    left_df = read_silver_data(Path(config["left"]["path"]))
    right_df = read_silver_data(Path(config["right"]["path"]))
    combined = pd.concat([left_df, right_df], ignore_index=True)

    model = select_model(config["output"]["model"], metadata_list)
    run_opts = build_run_options(config["output"], metadata_list)
    write_output(combined, Path(config["output"]["path"]), model, run_opts, metadata_list)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
