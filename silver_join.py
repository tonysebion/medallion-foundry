"""Join two Silver assets into a curated third output asset."""

from __future__ import annotations

import argparse
import json
import logging
import tempfile
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast
import time

import pandas as pd
import yaml

from pandas.api.types import is_datetime64_any_dtype

from core.bronze.io import write_batch_metadata
from core.context import RunContext, load_run_context
from core.patterns import LoadPattern
from core.run_options import RunOptions
from core.silver.models import SilverModel, resolve_profile
from core.storage import get_storage_backend
from core.storage.policy import enforce_storage_scope, validate_storage_metadata
from core.silver.writer import get_silver_writer

logger = logging.getLogger(__name__)

VALID_JOIN_TYPES = {"inner", "left", "right", "outer"}
MODEL_FALLBACK_ORDER = [
    SilverModel.SCD_TYPE_2,
    SilverModel.FULL_MERGE_DEDUPE,
    SilverModel.SCD_TYPE_1,
    SilverModel.PERIODIC_SNAPSHOT,
    SilverModel.INCREMENTAL_MERGE,
]
DEFAULT_SUFFIXES = ("", "_right")


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


def merge_platform_configs(
    base: Dict[str, Any] | None, override: Dict[str, Any] | None
) -> Dict[str, Any]:
    base_norm = normalize_platform_config(base)
    override_norm = normalize_platform_config(override)
    merged = dict(base_norm)
    merged["bronze"] = {
        **base_norm.get("bronze", {}),
        **override_norm.get("bronze", {}),
    }
    for key, value in override_norm.items():
        if key == "bronze":
            continue
        merged[key] = value
    return merged


def fetch_asset_local(
    entry: Dict[str, Any],
    tmp_dir: Path,
    global_platform: Dict[str, Any],
    storage_scope: str,
) -> Path:
    platform_cfg = merge_platform_configs(global_platform, entry.get("platform", {}))
    validate_storage_metadata(platform_cfg)
    enforce_storage_scope(platform_cfg, storage_scope)

    backend = get_storage_backend(platform_cfg)
    requested_path = Path(entry["path"])
    if requested_path.exists():
        return requested_path

    prefix = entry["path"].rstrip("/\\")
    if not prefix:
        raise ValueError(
            "Remote path must be provided when the local path does not exist"
        )

    name_value = entry.get("name")
    if isinstance(name_value, str) and name_value:
        dir_name = name_value
    else:
        dir_name = prefix.replace("/", "_").replace("\\", "_")
    target_dir = tmp_dir / dir_name
    target_dir.mkdir(parents=True, exist_ok=True)

    files = backend.list_files(prefix)
    if not files:
        raise FileNotFoundError(f"No remote files found under {prefix}")

    for remote in files:
        if remote.startswith(prefix):
            rel_path = Path(remote).relative_to(prefix)
        else:
            rel_path = Path(Path(remote).name)
        local_path = target_dir / rel_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        backend.download_file(remote, str(local_path))

    return target_dir


def read_metadata(silver_path: Path) -> Dict[str, Any]:
    metadata_path = silver_path / "_metadata.json"
    if metadata_path.exists():
        payload = cast(Dict[str, Any], json.loads(metadata_path.read_text(encoding="utf-8")))
    else:
        logger.warning(
            "No metadata found for %s; falling back to minimal defaults", silver_path
        )
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


def _guess_join_pairs_from_metadata(
    left_df: pd.DataFrame, right_df: pd.DataFrame, metadata_list: List[Dict[str, Any]]
) -> List[Tuple[str, str]]:
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
                    raise ValueError(
                        "Each join_key_pairs entry must contain 'left' and 'right'"
                    )
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
    join_strategy: str,
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
    split = max(1000, min(40000, estimated // 6))
    if join_strategy == "broadcast":
        return 0
    if join_strategy == "partitioned":
        return max(1000, split)
    if join_strategy == "hash":
        return max(1000, min(20000, split // 2))
    return split


def _align_datetime_columns(left: pd.DataFrame, right: pd.DataFrame) -> None:
    for col in set(left.columns).intersection(right.columns):
        if not (
            is_datetime64_any_dtype(left[col]) and is_datetime64_any_dtype(right[col])
        ):
            continue
        left_tz = left[col].dt.tz
        right_tz = right[col].dt.tz
        if left_tz and right_tz:
            if left_tz != right_tz:
                right[col] = right[col].dt.tz_convert(left_tz)
            continue
        if left_tz and not right_tz:
            right[col] = right[col].dt.tz_localize(left_tz)
            continue
        if right_tz and not left_tz:
            left[col] = left[col].dt.tz_localize(right_tz)


def _build_column_origin(
    merged_columns: Iterable[str],
    left_cols: Iterable[str],
    right_cols: Iterable[str],
    canonical_keys: Iterable[str],
) -> Dict[str, str]:
    left_set = set(left_cols)
    right_set = set(right_cols)
    canonical_set = set(canonical_keys)
    suffix = "_right"
    origin: Dict[str, str] = {}
    for col in merged_columns:
        if col in canonical_set:
            origin[col] = "both"
            continue
        if col.endswith(suffix) and col[: -len(suffix)] in right_set:
            origin[col] = "right"
            continue
        if col in left_set:
            origin[col] = "left"
            continue
        base = col
        if col.endswith(suffix):
            base = col[: -len(suffix)]
        if base in right_set:
            origin[col] = "right"
            continue
        origin[col] = "unknown"
    return origin


class QualityGuardError(Exception):
    def __init__(self, results: List[Dict[str, Any]]) -> None:
        self.results = results
        failures = [result for result in results if result["status"] != "pass"]
        names = ", ".join(result["name"] for result in failures)
        super().__init__(f"Quality guard failure(s): {names}")


def _evaluate_row_count(df: pd.DataFrame, cfg: Dict[str, Any]) -> Dict[str, Any]:
    name = "row_count_range"
    count = len(df)
    min_expected = cfg.get("min")
    max_expected = cfg.get("max")
    passed = True
    details = {"actual": count}
    if min_expected is not None:
        details["min"] = min_expected
        if count < min_expected:
            passed = False
    if max_expected is not None:
        details["max"] = max_expected
        if count > max_expected:
            passed = False
    return {"name": name, "status": "pass" if passed else "fail", "details": details}


def _evaluate_null_ratio(df: pd.DataFrame, cfg: Dict[str, Any]) -> Dict[str, Any]:
    name = f"null_ratio:{cfg.get('column')}"
    column = cfg["column"]
    if column not in df.columns:
        raise ValueError(f"Null ratio guard references unknown column '{column}'")
    threshold = cfg.get("max_ratio")
    ratio = df[column].isna().mean()
    details = {"column": column, "actual": ratio, "max_ratio": threshold}
    status = "pass"
    if threshold is not None and ratio > float(threshold):
        status = "fail"
    return {"name": name, "status": status, "details": details}


def _evaluate_unique_keys(df: pd.DataFrame, cfg: Dict[str, Any]) -> Dict[str, Any]:
    columns = cfg.get("columns") or cfg.get("keys") or []
    if not columns:
        raise ValueError("Unique key guard requires at least one column")
    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Unique guard references unknown column '{column}'")
    has_duplicates = df.duplicated(subset=columns).any()
    details = {"columns": columns, "duplicates": int(has_duplicates)}
    status = "fail" if has_duplicates else "pass"
    return {
        "name": f"unique_keys:{'+'.join(columns)}",
        "status": status,
        "details": details,
    }


def run_quality_guards(
    df: pd.DataFrame, guard_cfg: Dict[str, Any]
) -> List[Dict[str, Any]]:
    if not guard_cfg:
        return []
    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    row_cfg = guard_cfg.get("row_count")
    if row_cfg:
        result = _evaluate_row_count(df, row_cfg)
        results.append(result)
        if result["status"] != "pass":
            failures.append(result)
    for entry in guard_cfg.get("null_ratio", []):
        result = _evaluate_null_ratio(df, entry)
        results.append(result)
        if result["status"] != "pass":
            failures.append(result)
    for entry in guard_cfg.get("unique_keys", []):
        result = _evaluate_unique_keys(df, entry)
        results.append(result)
        if result["status"] != "pass":
            failures.append(result)
    if failures:
        raise QualityGuardError(results)
    return results


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
            raise ValueError(
                "Join keys must be configured to partition the right-hand asset."
            )
        self.df = df
        self.join_keys = join_keys
        self._cache: Dict[Tuple[Any, ...], pd.DataFrame] = {}
        self._lookup: Dict[Tuple[Any, ...], List[int]] = {}
        for idx, row in df.iterrows():
            key = self._build_key(row)
            self._lookup.setdefault(key, []).append(idx)
        self._all_keys = set(self._lookup.keys())

    def _build_key(self, row: pd.Series) -> Tuple[Any, ...]:
        return tuple(self._normalize_value(row[col]) for col in self.join_keys)

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        return "__silver_join_null__" if pd.isna(value) else value

    def batch_for_keys(self, keys: Iterable[Tuple[Any, ...]]) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        for key in keys:
            frames.append(self._get_group(key))
        if frames:
            return pd.concat(frames, ignore_index=True)
        return self.df.iloc[0:0]

    def _get_group(self, key: Tuple[Any, ...]) -> pd.DataFrame:
        if key in self._cache:
            return self._cache[key]
        idxs = self._lookup.get(key) or []
        frame = self.df.loc[idxs] if idxs else self.df.iloc[0:0]
        self._cache[key] = frame
        return frame

    def all_keys(self) -> Set[Tuple[Any, ...]]:
        return set(self._all_keys)

    def normalize_query_keys(self, keys: Iterable[Any]) -> Set[Tuple[Any, ...]]:
        normalized: Set[Tuple[Any, ...]] = set()
        for key in keys:
            if isinstance(key, tuple):
                normalized.add(tuple(self._normalize_value(value) for value in key))
            else:
                normalized.add((self._normalize_value(key),))
        return normalized


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
        self.progress_file = (
            self.checkpoint_dir / "progress.json" if self.checkpoint_dir else None
        )
        self.chunks_processed = 0
        self.total_records = 0
        self.recent_chunks: List[Dict[str, Any]] = []

    def record_chunk(
        self,
        chunk_index: int,
        record_count: int,
        sample_keys: Iterable[Any],
        duration_seconds: float,
    ) -> None:
        self.chunks_processed = chunk_index
        self.total_records += record_count
        entry = {
            "chunk_index": chunk_index,
            "record_count": record_count,
            "duration_seconds": duration_seconds,
            "sample_keys": [
                self._format_key(key) for key in islice(sample_keys, self.MAX_RECENT)
            ],
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
        return cast(Dict[str, Any], json.loads(path.read_text(encoding="utf-8")))
    except Exception as exc:
        logger.debug("Failed to read JSON from %s: %s", path, exc)
        return None


def build_input_audit(meta: Dict[str, Any]) -> Dict[str, Any]:
    audit: Dict[str, Any] = {
        "silver_path": meta.get("silver_path"),
        "silver_model": meta.get("silver_model"),
        "record_count": meta.get("record_count"),
        "chunk_count": meta.get("chunk_count"),
        "reference_mode": meta.get("reference_mode"),
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
                "sample_files": [
                    entry.get("path") for entry in manifest_payload.get("files", [])[:3]
                ],
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


def select_model(
    requested_model: str | None, metadata_list: List[Dict[str, Any]]
) -> SilverModel:
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


def _normalize_join_strategy(raw: str | None) -> str:
    strategy = (raw or "auto").strip().lower()
    if strategy not in {"auto", "broadcast", "partitioned", "hash"}:
        raise ValueError("join_strategy must be auto, broadcast, partitioned, or hash")
    return strategy


def _order_inputs_by_reference(
    inputs: List[Tuple[Dict[str, Any], pd.DataFrame, Dict[str, Any], str]],
) -> List[Tuple[Dict[str, Any], pd.DataFrame, Dict[str, Any], str]]:
    delta_run_dates = {
        meta.get("run_date")
        for _, _, meta, _ in inputs
        if (meta.get("reference_mode") or {}).get("role") == "delta"
    }

    def score(entry: Tuple[Any, Any, Dict[str, Any], Any]) -> int:
        meta = entry[2]
        reference = (meta.get("reference_mode") or {}).get("role")
        if reference == "reference":
            run_date = meta.get("run_date")
            if run_date in delta_run_dates:
                return 3
            return 0
        if reference == "delta":
            return 2
        return 1

    return sorted(inputs, key=score)


def build_run_options(
    output_cfg: Dict[str, Any], metadata_list: List[Dict[str, Any]]
) -> RunOptions:
    formats = {fmt.strip().lower() for fmt in output_cfg.get("formats", ["parquet"])}
    write_parquet = "parquet" in formats
    write_csv = "csv" in formats
    if not (write_parquet or write_csv):
        raise ValueError(
            "silver_join.output.formats must include at least one of 'parquet' or 'csv'"
        )

    primary_keys = (
        metadata_list[0].get("primary_keys") or output_cfg.get("primary_keys") or []
    )
    order_column = metadata_list[0].get("order_column") or output_cfg.get(
        "order_column"
    )

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


def _execute_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    join_pairs: List[Tuple[str, str]],
    chunk_size: int,
    output_cfg: Dict[str, Any],
    progress_tracker: Optional[JoinProgressTracker] = None,
    spill_dir: Optional[Path] = None,
) -> Tuple[pd.DataFrame, JoinRunStats, Dict[str, str], List[Dict[str, Any]]]:
    _align_datetime_columns(left, right)
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
    matched_right_keys: Set[Tuple[Any, ...]] = set()
    chunk_frames: List[pd.DataFrame] = []
    chunk_count = 0

    join_metrics: List[Dict[str, Any]] = []
    for chunk in chunk_dataframe(left_aligned, chunk_size):
        if chunk.empty:
            continue
        chunk_count += 1
        start = time.perf_counter()
        key_set = _extract_join_key_set(chunk, canonical_keys)
        normalized_keys = cache.normalize_query_keys(key_set)
        right_subset = cache.batch_for_keys(normalized_keys)
        if spill_dir:
            spill_file = spill_dir / f"chunk_{chunk_count:04d}.parquet"
            spill_dir.mkdir(parents=True, exist_ok=True)
            right_subset.to_parquet(spill_file, index=False)
        merged_chunk = pd.merge(
            chunk,
            right_subset,
            how=join_type,
            on=canonical_keys,
            suffixes=DEFAULT_SUFFIXES,
        )
        duration = time.perf_counter() - start
        join_metrics.append(
            {
                "chunk_index": chunk_count,
                "record_count": len(merged_chunk),
                "right_rows": len(right_subset),
                "duration_seconds": duration,
            }
        )
        chunk_frames.append(merged_chunk)
        matched_right_keys.update(
            cache.normalize_query_keys(
                _extract_join_key_set(right_subset, canonical_keys)
            )
        )
        if progress_tracker:
            progress_tracker.record_chunk(
                chunk_count, len(merged_chunk), key_set, duration
            )

    all_right_keys = cache.all_keys()
    unmatched_keys = all_right_keys - matched_right_keys
    right_only_rows = 0
    if join_type in {"right", "outer"} and unmatched_keys:
        right_only_df = cache.batch_for_keys(unmatched_keys)
        left_template = pd.DataFrame(columns=left_aligned.columns)
        right_only = pd.merge(
            left_template,
            right_only_df,
            how="right",
            on=canonical_keys,
            suffixes=DEFAULT_SUFFIXES,
        )
        chunk_frames.append(right_only)
        right_only_rows = len(right_only)

    if progress_tracker:
        progress_tracker.finalize()

    if not chunk_frames:
        merged = pd.merge(
            left_aligned,
            right_aligned,
            how=join_type,
            on=canonical_keys,
            suffixes=DEFAULT_SUFFIXES,
        )
    else:
        merged = pd.concat(chunk_frames, ignore_index=True)

    column_origin = _build_column_origin(
        merged.columns, left_aligned.columns, right_aligned.columns, canonical_keys
    )

    stats = JoinRunStats(
        chunk_count=chunk_count,
        matched_right_keys=len(matched_right_keys),
        unmatched_right_keys=len(unmatched_keys),
        right_only_rows=right_only_rows,
    )
    return merged, stats, column_origin, join_metrics


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
                raise ValueError(
                    "Projection mappings must contain 'source' or a single key/value"
                )
            entries.append((str(src), str(alias)))
            continue
        raise ValueError(
            "Projection entries must be strings or single key/value mappings"
        )
    return entries


def _resolve_projection_source(source: str, df: pd.DataFrame) -> str:
    candidate = source
    qualifier = None
    if "." in source:
        qualifier, candidate = source.split(".", 1)
        qualifier = qualifier.lower()
    suffix = "_right"

    def right_name(name: str) -> str:
        return f"{name}{suffix}"

    if qualifier in {"right", "target"}:
        name = right_name(candidate)
        if name in df.columns:
            return name
        raise ValueError(f"Column '{source}' not found in joined output")
    if qualifier in {"left", "source"}:
        if candidate in df.columns:
            return candidate
        raise ValueError(f"Column '{source}' not found in joined output")

    if candidate in df.columns:
        return candidate
    right_candidate = right_name(candidate)
    if right_candidate in df.columns:
        return right_candidate
    raise ValueError(f"Column '{source}' not found in joined output")


def apply_projection(
    df: pd.DataFrame,
    output_cfg: Dict[str, Any],
    column_origin: Dict[str, str],
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    raw = output_cfg.get("select_columns") or output_cfg.get("projection")
    projection = _normalize_projection(raw)
    if not projection:
        lineage = [
            {
                "column": col,
                "source": column_origin.get(col, "unknown"),
                "original": col,
                "alias": None,
            }
            for col in df.columns
        ]
        return df, lineage
    resolved: List[Tuple[str, str]] = []
    missing: List[str] = []
    for src, alias in projection:
        try:
            resolved_src = _resolve_projection_source(src, df)
        except ValueError:
            missing.append(str(src))
            continue
        resolved.append((resolved_src, alias))
    if missing:
        raise ValueError(f"Projection references missing columns: {missing}")
    rename_map: Dict[str, str] = {}
    selected_columns: List[str] = []
    resolved_lineage: List[Dict[str, Any]] = []
    for src, alias in resolved:
        selected_columns.append(src)
        if alias != src:
            rename_map[src] = alias
        column_name = alias if alias else src
        resolved_lineage.append(
            {
                "column": column_name,
                "source": column_origin.get(src, "unknown"),
                "original": src,
                "alias": alias if alias != src else None,
            }
        )
    result = df[selected_columns]
    if rename_map:
        result = result.rename(columns=rename_map)
    return result, resolved_lineage


def _persist_join_output(
    df: pd.DataFrame,
    base_dir: Path,
    model: SilverModel | None,
    run_opts: RunOptions,
    metadata_list: List[Dict[str, Any]],
    output_cfg: Dict[str, Any],
    source_paths: List[str],
    source_audits: List[Dict[str, Any]],
    progress_summary: Dict[str, Any],
    join_stats: JoinRunStats,
    join_pairs: List[Tuple[str, str]],
    chunk_size: int,
    column_lineage: List[Dict[str, Any]],
    quality_guards: List[Dict[str, Any]],
    join_metrics: List[Dict[str, Any]],
    run_context: RunContext | None = None,
) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    # Fallback for legacy callers that passed None for model
    if model is None:
        try:
            from core.silver.models import SilverModel as _SM

            model = _SM.PERIODIC_SNAPSHOT
            logger.debug(
                "Defaulted Silver model to PERIODIC_SNAPSHOT for legacy join output"
            )
        except Exception:  # pragma: no cover - defensive
            pass

    effective_model: SilverModel = model or SilverModel.PERIODIC_SNAPSHOT

    writer = get_silver_writer(run_opts.artifact_writer_kind)
    outputs = writer.write(
        df,
        primary_keys=run_opts.primary_keys,
        order_column=run_opts.order_column,
        write_parquet=run_opts.write_parquet,
        write_csv=run_opts.write_csv,
        parquet_compression=run_opts.parquet_compression,
        artifact_names=run_opts.artifact_names,
        partition_columns=run_opts.partition_columns,
        error_cfg={},  # error handling config
        silver_model=effective_model,
        output_dir=base_dir,
    )
    chunk_count = sum(len(paths) for paths in outputs.values())
    metadata = {
        "joined_sources": source_paths,
        "silver_model": effective_model.value,
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
        "join_key_pairs": [
            {"left": left_key, "right": right_key} for left_key, right_key in join_pairs
        ],
        "chunk_size": chunk_size,
        "output_columns": list(df.columns),
        "column_lineage": column_lineage,
        "quality_guards": quality_guards,
        "join_metrics": join_metrics,
    }
    if run_context:
        metadata.update(
            {
                "dataset_id": run_context.dataset_id,
                "config_name": run_context.config_name,
                "run_date": run_context.run_date.isoformat(),
                "relative_path": run_context.relative_path,
            }
        )
    write_batch_metadata(
        base_dir,
        record_count=len(df),
        chunk_count=chunk_count,
        extra_metadata=metadata,
    )


@dataclass
class AssetFetchResult:
    left_df: pd.DataFrame
    right_df: pd.DataFrame
    metadata_list: List[Dict[str, Any]]
    source_paths: List[str]


@dataclass
class JoinExecutionResult:
    joined_df: pd.DataFrame
    join_stats: JoinRunStats
    column_lineage: List[Dict[str, Any]]
    quality_guards: List[Dict[str, Any]]
    join_metrics: List[Dict[str, Any]]
    join_pairs: List[Tuple[str, str]]
    chunk_size: int


class SilverJoinRunner:
    def __init__(
        self,
        config_path: Path | None,
        run_context_path: Path | None,
        storage_scope: str,
    ) -> None:
        self.config_path = Path(config_path) if config_path else None
        self.run_context_path = Path(run_context_path) if run_context_path else None
        self.storage_scope = storage_scope
        self.run_context: RunContext | None = None
        self.full_config: Dict[str, Any] = {}
        self.join_config: Dict[str, Any] = {}
        self.platform_cfg: Dict[str, Any] = {}
        self.output_cfg: Dict[str, Any] = {}

    def load_configuration(self) -> None:
        if self.run_context_path:
            self.run_context = load_run_context(self.run_context_path)
            full_config = self.run_context.cfg
            if "silver_join" not in full_config:
                raise ValueError("RunContext cfg must include a 'silver_join' section")
            self.full_config = full_config
            self.join_config = full_config["silver_join"]
        elif self.config_path:
            full_config, join_config = parse_config(self.config_path)
            self.full_config = full_config
            self.join_config = join_config
        else:
            raise ValueError("Either config_path or run_context_path must be provided")

        self.platform_cfg = self.full_config.get("platform", {})
        output_cfg = self.join_config.get("output")
        if output_cfg is None:
            raise ValueError("silver_join.output must be configured")
        self.output_cfg = cast(Dict[str, Any], output_cfg)

    def fetch_assets(self, workspace: Path) -> AssetFetchResult:
        left_entry = self.join_config["left"]
        right_entry = self.join_config["right"]
        left_path = fetch_asset_local(
            left_entry, workspace, self.platform_cfg, self.storage_scope
        )
        right_path = fetch_asset_local(
            right_entry, workspace, self.platform_cfg, self.storage_scope
        )
        left_meta = read_metadata(left_path)
        right_meta = read_metadata(right_path)
        left_df = read_silver_data(left_path)
        right_df = read_silver_data(right_path)
        entries = [
            (left_entry, left_df, left_meta, left_entry["path"]),
            (right_entry, right_df, right_meta, right_entry["path"]),
        ]
        ordered = _order_inputs_by_reference(entries)
        (
            (left_cfg, left_df, left_meta, left_path_str),
            (right_cfg, right_df, right_meta, right_path_str),
        ) = ordered
        self.join_config["left"], self.join_config["right"] = left_cfg, right_cfg
        metadata_list = [left_meta, right_meta]
        source_paths = [left_path_str, right_path_str]
        return AssetFetchResult(
            left_df=left_df,
            right_df=right_df,
            metadata_list=metadata_list,
            source_paths=source_paths,
        )

    def perform_join(
        self,
        assets: AssetFetchResult,
        progress_tracker: JoinProgressTracker,
        spill_dir: Optional[Path],
        join_strategy: str,
    ) -> JoinExecutionResult:
        join_pairs = _resolve_join_pairs(
            self.output_cfg, assets.left_df, assets.right_df, assets.metadata_list
        )
        chunk_size = _resolve_chunk_size(
            self.output_cfg, assets.left_df, assets.metadata_list, join_strategy
        )
        joined, join_stats, column_origin, join_metrics = _execute_join(
            assets.left_df,
            assets.right_df,
            join_pairs,
            chunk_size,
            self.output_cfg,
            progress_tracker,
            spill_dir,
        )
        projected, column_lineage = apply_projection(joined, self.output_cfg, column_origin)
        guards = run_quality_guards(projected, self.output_cfg.get("quality_guards", {}))
        return JoinExecutionResult(
            joined_df=projected,
            join_stats=join_stats,
            column_lineage=column_lineage,
            quality_guards=guards,
            join_metrics=join_metrics,
            join_pairs=join_pairs,
            chunk_size=chunk_size,
        )

    def write_output(
        self,
        assets: AssetFetchResult,
        result: JoinExecutionResult,
        progress_tracker: JoinProgressTracker,
    ) -> None:
        source_audits = [build_input_audit(meta) for meta in assets.metadata_list]
        requested_model = (
            self.output_cfg.get("model")
            or SilverModel.default_for_load_pattern(LoadPattern.FULL).value
        )
        model = select_model(requested_model, assets.metadata_list)
        run_opts = build_run_options(self.output_cfg, assets.metadata_list)
        progress_summary = progress_tracker.summary()
        _persist_join_output(
            result.joined_df,
            Path(self.output_cfg["path"]),
            model,
            run_opts,
            assets.metadata_list,
            self.output_cfg,
            assets.source_paths,
            source_audits,
            progress_summary,
            result.join_stats,
            result.join_pairs,
            result.chunk_size,
            result.column_lineage,
            result.quality_guards,
            result.join_metrics,
            run_context=self.run_context,
        )
        logger.info(
            "Joined silver asset written to %s (model=%s)",
            self.output_cfg["path"],
            model.value,
        )

    def run(self) -> int:
        self.load_configuration()
        validate_storage_metadata(self.platform_cfg)
        enforce_storage_scope(self.platform_cfg, self.storage_scope)
        output_path = Path(self.output_cfg["path"])
        checkpoint_dir = (
            self.output_cfg.get("checkpoint_dir") or output_path / ".join_progress"
        )
        progress_tracker = JoinProgressTracker(Path(checkpoint_dir))
        join_strategy = _normalize_join_strategy(self.output_cfg.get("join_strategy"))
        spill_path = self.output_cfg.get("spill_dir")
        spill_dir = Path(spill_path) if spill_path else None
        with tempfile.TemporaryDirectory(prefix="silver_join_") as workspace_dir:
            workspace = Path(workspace_dir)
            assets = self.fetch_assets(workspace)
            result = self.perform_join(
                assets, progress_tracker, spill_dir, join_strategy
            )
        self.write_output(assets, result, progress_tracker)
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Join two Silver assets into a curated third asset"
    )
    parser.add_argument("--config", help="Silver join configuration YAML")
    parser.add_argument(
        "--run-context",
        help="Path to a RunContext JSON payload describing the join output dataset",
    )
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
    if not args.config and not args.run_context:
        parser.error("Either --config or --run-context must be provided")

    runner = SilverJoinRunner(
        Path(args.config) if args.config else None,
        Path(args.run_context) if args.run_context else None,
        args.storage_scope,
    )
    return runner.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
