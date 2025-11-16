"""Join two Silver assets into a curated third output asset."""

from __future__ import annotations

import argparse
import json
import logging
import tempfile
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

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
DEFAULT_SUFFIXES = ("_x", "_y")


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


def _guess_join_pairs_from_metadata(left_df: pd.DataFrame, right_df: pd.DataFrame, metadata_list: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    if len(metadata_list) < 2:
        raise ValueError("Insufficient metadata for auto join key resolution")
    left_pks = metadata_list[0].get("primary_keys", []) or []
    right_pks = metadata_list[1].get("primary_keys", []) or []
    pairs: List[Tuple[str, str]] = []
    for left_pk, right_pk in zip(left_pks, right_pks):
        if left_pk in left_df.columns and right_pk in right_df.columns:
            pairs.append((left_pk, right_pk))
    if pairs:
        return pairs
    common_keys = sorted(set(left_df.columns).intersection(right_df.columns))
    if common_keys:
        return [(key, key) for key in common_keys[: min(3, len(common_keys))]]
    raise ValueError("Auto join key resolution failed; specify explicit join_key_pairs")


def _resolve_join_pairs(
    output_cfg: Dict[str, Any],
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    metadata_list: List[Dict[str, Any]],
) -> List[Tuple[str, str]]:
    raw_mapping = output_cfg.get("join_key_pairs") or output_cfg.get("join_key_mapping")
    if raw_mapping:
        if not isinstance(raw_mapping, (list, tuple)):
            raise ValueError("join_key_pairs must be a list of mappings")
        pairs: List[Tuple[str, str]] = []
        for entry in raw_mapping:
            if isinstance(entry, str):
                pairs.append((entry, entry))
            elif isinstance(entry, dict):
                left_key = entry.get("left") or entry.get("source")
                right_key = entry.get("right") or entry.get("target")
                if not left_key or not right_key:
                    raise ValueError("Each join_key_pairs entry must contain 'left' and 'right'")
                pairs.append((str(left_key), str(right_key)))
            else:
                raise ValueError("join_key_pairs entries must be strings or mappings")
        return pairs

    raw_keys = output_cfg.get("join_keys")
    if isinstance(raw_keys, str) and raw_keys.strip().lower() == "auto":
        return _guess_join_pairs_from_metadata(left_df, right_df, metadata_list)

    normalized = normalize_join_keys(raw_keys)
    if normalized:
        return [(key, key) for key in normalized]

    # Fallback to auto behavior when nothing provided
    return _guess_join_pairs_from_metadata(left_df, right_df, metadata_list)


def _resolve_chunk_size(
    output_cfg: Dict[str, Any],
    left_df: pd.DataFrame,
    metadata_list: List[Dict[str, Any]],
) -> int:
    explicit = output_cfg.get("chunk_size")
    if explicit is not None:
        return max(0, int(explicit))

    profile = (output_cfg.get("performance_profile") or "auto").strip().lower()
    if profile == "single":
        return 0

    estimated = metadata_list[0].get("record_count") if metadata_list else len(left_df)
    estimated = estimated or len(left_df)
    estimated = max(estimated, 1)

    if profile == "chunked":
        return max(1000, min(50000, estimated // 4))

    # auto profile
    if estimated <= 5000:
        return 0
    return max(1000, min(40000, estimated // 6))


def normalize_join_keys(raw_keys: Any) -> List[str]:
    if not raw_keys:
        return []
    if isinstance(raw_keys, (list, tuple)):
        keys = list(raw_keys)
    else:
        keys = [raw_keys]
    return [str(key) for key in keys if key]


def _extract_join_key_set(df: pd.DataFrame, join_keys: List[str]) -> Set[Any]:
    if df.empty or not join_keys:
        return set()
    if len(join_keys) == 1:
        return set(df[join_keys[0]].tolist())
    return {tuple(row) for row in df[join_keys].itertuples(index=False, name=None)}


class RightPartitionCache:
    """Partition the right-hand table by join keys for streaming access."""

    def __init__(self, df: pd.DataFrame, join_keys: List[str]) -> None:
        if not join_keys:
            raise ValueError("Join keys must be configured to partition the right-hand asset.")
        self.df = df
        self.join_keys = join_keys
        self._cache: Dict[Any, pd.DataFrame] = {}
        self._grouped = df.groupby(join_keys, sort=False, dropna=False)
        self._all_keys = set(self._grouped.groups.keys())

    def batch_for_keys(self, keys: Iterable[Any]) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        for key in keys:
            frames.append(self._get_group(key))
        if frames:
            return pd.concat(frames, ignore_index=True)
        return self.df.iloc[0:0]

    def _get_group(self, key: Any) -> pd.DataFrame:
        if key in self._cache:
            return self._cache[key]
        try:
            frame = self._grouped.get_group(key)
        except KeyError:
            frame = self.df.iloc[0:0]
        self._cache[key] = frame
        return frame

    def all_keys(self) -> Set[Any]:
        return set(self._all_keys)


@dataclass
class JoinRunStats:
    chunk_count: int
    matched_right_keys: int
    unmatched_right_keys: int
    right_only_rows: int


class JoinProgressTracker:
    """Persist chunk progress so retries can resume from the latest checkpoint."""

    MAX_RECENT = 6

    def __init__(self, checkpoint_dir: Optional[Path]) -> None:
        self.checkpoint_dir = checkpoint_dir.resolve() if checkpoint_dir else None
        self.progress_file = self.checkpoint_dir / "progress.json" if self.checkpoint_dir else None
        self.chunks_processed = 0
        self.total_records = 0
        self.recent_chunks: List[Dict[str, Any]] = []

    def record_chunk(self, chunk_index: int, record_count: int, sample_keys: Iterable[Any]) -> None:
        self.chunks_processed = chunk_index
        self.total_records += record_count
        entry = {
            "chunk_index": chunk_index,
            "record_count": record_count,
            "sample_keys": [self._format_key(key) for key in islice(sample_keys, self.MAX_RECENT)],
        }
        self.recent_chunks.append(entry)
        if len(self.recent_chunks) > self.MAX_RECENT:
            self.recent_chunks.pop(0)
        self._write_state(entry)

    def finalize(self) -> None:
        if self.progress_file is None:
            return
        last_chunk = self.recent_chunks[-1] if self.recent_chunks else None
        self._write_state(last_chunk)

    def summary(self) -> Dict[str, Any]:
        return {
            "chunks_processed": self.chunks_processed,
            "total_records": self.total_records,
            "recent_chunks": list(self.recent_chunks),
            "checkpoint_file": str(self.progress_file) if self.progress_file else None,
        }

    def _write_state(self, last_chunk: Optional[Dict[str, Any]]) -> None:
        if self.progress_file is None:
            return
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        state: Dict[str, Any] = {
            "chunks_processed": self.chunks_processed,
            "total_records": self.total_records,
            "recent_chunks": list(self.recent_chunks),
        }
        if last_chunk:
            state["last_chunk"] = last_chunk
        self.progress_file.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _format_key(self, key: Any) -> Any:
        if isinstance(key, tuple):
            return [self._format_value(value) for value in key]
        return self._format_value(key)

    @staticmethod
    def _format_value(value: Any) -> Any:
        if pd.isna(value):
            return None
        return value


def _safe_load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.debug("Failed to read JSON from %s: %s", path, exc)
        return None


def build_input_audit(meta: Dict[str, Any]) -> Dict[str, Any]:
    audit: Dict[str, Any] = {
        "silver_path": meta.get("silver_path"),
        "silver_model": meta.get("silver_model"),
        "record_count": meta.get("record_count"),
        "chunk_count": meta.get("chunk_count"),
    }
    for key in (
        "load_pattern",
        "load_partition_name",
        "partition_columns",
        "primary_keys",
        "order_column",
        "domain",
        "entity",
        "version",
    ):
        value = meta.get(key)
        if value is not None:
            audit[key] = value

    bronze_path = meta.get("bronze_path")
    if bronze_path:
        bronze_dir = Path(bronze_path)
        audit["bronze_path"] = bronze_path
        bronze_meta = _safe_load_json(bronze_dir / "_metadata.json")
        if bronze_meta:
            audit["bronze_metadata"] = bronze_meta
        manifest_path = bronze_dir / "_checksums.json"
        manifest_payload = _safe_load_json(manifest_path)
        if isinstance(manifest_payload, dict):
            audit["bronze_checksum_manifest"] = {
                "path": str(manifest_path),
                "file_count": len(manifest_payload.get("files", [])),
                "sample_files": [entry.get("path") for entry in manifest_payload.get("files", [])[:3]],
            }
    return audit


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


def perform_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    join_pairs: List[Tuple[str, str]],
    chunk_size: int,
    output_cfg: Dict[str, Any],
    progress_tracker: Optional[JoinProgressTracker] = None,
) -> Tuple[pd.DataFrame, JoinRunStats]:
    join_type = output_cfg.get("join_type", "inner").strip().lower()
    if join_type not in VALID_JOIN_TYPES:
        raise ValueError(f"join_type must be one of {', '.join(VALID_JOIN_TYPES)}")

    if not join_pairs:
        raise ValueError("At least one join pair must be configured")

    rename_right: Dict[str, str] = {}
    canonical_keys: List[str] = []
    for left_key, right_key in join_pairs:
        canonical_keys.append(left_key)
        if right_key != left_key:
            rename_right[right_key] = left_key

    left_aligned = left.copy()
    right_aligned = right.rename(columns=rename_right).copy()

    cache = RightPartitionCache(right_aligned, canonical_keys)
    matched_right_keys: Set[Any] = set()
    chunk_frames: List[pd.DataFrame] = []
    chunk_count = 0

    for chunk in chunk_dataframe(left_aligned, chunk_size):
        if chunk.empty:
            continue
        chunk_count += 1
        key_set = _extract_join_key_set(chunk, canonical_keys)
        right_subset = cache.batch_for_keys(key_set)
        merged_chunk = pd.merge(chunk, right_subset, how=join_type, on=canonical_keys, suffixes=DEFAULT_SUFFIXES)
        chunk_frames.append(merged_chunk)
        matched_right_keys.update(_extract_join_key_set(right_subset, canonical_keys))
        if progress_tracker:
            progress_tracker.record_chunk(chunk_count, len(merged_chunk), key_set)

    all_right_keys = cache.all_keys()
    unmatched_keys = all_right_keys - matched_right_keys
    right_only_rows = 0
    if join_type in {"right", "outer"} and unmatched_keys:
        right_only_df = cache.batch_for_keys(unmatched_keys)
        left_template = pd.DataFrame(columns=left_aligned.columns)
        right_only = pd.merge(left_template, right_only_df, how="right", on=canonical_keys, suffixes=DEFAULT_SUFFIXES)
        chunk_frames.append(right_only)
        right_only_rows = len(right_only)

    if progress_tracker:
        progress_tracker.finalize()

    if not chunk_frames:
        merged = pd.merge(left_aligned, right_aligned, how=join_type, on=canonical_keys, suffixes=DEFAULT_SUFFIXES)
    else:
        merged = pd.concat(chunk_frames, ignore_index=True)

    stats = JoinRunStats(
        chunk_count=chunk_count,
        matched_right_keys=len(matched_right_keys),
        unmatched_right_keys=len(unmatched_keys),
        right_only_rows=right_only_rows,
    )
    return merged, stats


def _normalize_projection(raw: Any) -> List[Tuple[str, str]]:
    if not raw:
        return []
    entries: List[Tuple[str, str]] = []
    for item in raw:
        if isinstance(item, str):
            entries.append((item, item))
            continue
        if isinstance(item, dict):
            if "source" in item:
                src = item["source"]
                alias = item.get("alias", src)
            elif len(item) == 1:
                src, alias = next(iter(item.items()))
            else:
                raise ValueError("Projection mappings must contain 'source' or a single key/value")
            entries.append((str(src), str(alias)))
            continue
        raise ValueError("Projection entries must be strings or single key/value mappings")
    return entries


def apply_projection(df: pd.DataFrame, output_cfg: Dict[str, Any]) -> pd.DataFrame:
    raw = output_cfg.get("select_columns") or output_cfg.get("projection")
    projection = _normalize_projection(raw)
    if not projection:
        return df
    missing = [src for src, _ in projection if src not in df.columns]
    if missing:
        raise ValueError(f"Projection references missing columns: {missing}")
    rename_map: Dict[str, str] = {}
    selected_columns: List[str] = []
    for src, alias in projection:
        selected_columns.append(src)
        if alias != src:
            rename_map[src] = alias
    result = df[selected_columns]
    if rename_map:
        result = result.rename(columns=rename_map)
    return result


def write_output(
    df: pd.DataFrame,
    base_dir: Path,
    model: SilverModel,
    run_opts: RunOptions,
    metadata_list: List[Dict[str, Any]],
    output_cfg: Dict[str, Any],
    source_paths: List[str],
    source_audits: List[Dict[str, Any]],
    progress_summary: Dict[str, Any],
    join_stats: JoinRunStats,
    join_pairs: List[Tuple[str, str]],
    chunk_size: int,
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
    metadata = {
        "joined_sources": source_paths,
        "silver_model": model.value,
        "requested_model": output_cfg.get("model"),
        "formats": {"parquet": run_opts.write_parquet, "csv": run_opts.write_csv},
        "join_type": output_cfg.get("join_type"),
        "projection": output_cfg.get("select_columns") or output_cfg.get("projection"),
        "lineage": [meta.get("silver_path") or "<missing>" for meta in metadata_list],
        "inputs": source_audits,
        "progress": progress_summary,
        "join_stats": {
            "chunk_count": join_stats.chunk_count,
            "matched_right_keys": join_stats.matched_right_keys,
            "unmatched_right_keys": join_stats.unmatched_right_keys,
            "right_only_rows": join_stats.right_only_rows,
        },
        "join_key_pairs": [{"left": left_key, "right": right_key} for left_key, right_key in join_pairs],
        "chunk_size": chunk_size,
        "output_columns": list(df.columns),
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

    output_cfg = join_config.get("output")
    if output_cfg is None:
        raise ValueError("silver_join.output must be configured")

    checkpoint_dir = output_cfg.get("checkpoint_dir") or Path(output_cfg["path"]) / ".join_progress"
    progress_tracker = JoinProgressTracker(Path(checkpoint_dir))

    metadata_list: List[Dict[str, Any]] = []
    source_paths: List[str] = [join_config["left"]["path"], join_config["right"]["path"]]

    with tempfile.TemporaryDirectory(prefix="silver_join_") as workspace_dir:
        workspace = Path(workspace_dir)
        left_path = fetch_asset_local(join_config["left"], workspace, platform_cfg, args.storage_scope)
        right_path = fetch_asset_local(join_config["right"], workspace, platform_cfg, args.storage_scope)
        left_meta = read_metadata(left_path)
        right_meta = read_metadata(right_path)
        metadata_list = [left_meta, right_meta]
        left_df = read_silver_data(left_path)
        right_df = read_silver_data(right_path)
        join_pairs = _resolve_join_pairs(output_cfg, left_df, right_df, metadata_list)
        chunk_size = _resolve_chunk_size(output_cfg, left_df, metadata_list)
        joined_df, join_stats = perform_join(
            left_df,
            right_df,
            join_pairs,
            chunk_size,
            output_cfg,
            progress_tracker,
        )
        joined_df = apply_projection(joined_df, output_cfg)

    source_audits = [build_input_audit(meta) for meta in metadata_list]
    requested_model = output_cfg.get("model") or SilverModel.default_for_load_pattern(LoadPattern.FULL).value
    model = select_model(requested_model, metadata_list)
    run_opts = build_run_options(output_cfg, metadata_list)
    progress_summary = progress_tracker.summary()
    write_output(
        joined_df,
        Path(output_cfg["path"]),
        model,
        run_opts,
        metadata_list,
        output_cfg,
        source_paths,
        source_audits,
        progress_summary,
        join_stats,
        join_pairs,
        chunk_size,
    )
    logger.info("Joined silver asset written to %s (model=%s)", output_cfg["path"], model.value)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
