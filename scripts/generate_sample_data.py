"""Generate realistic Bronze sample datasets for testing load patterns.

Usage examples:
        # Default: small dataset for dev/tests
        python scripts/generate_sample_data.py

        # Large dataset: 250K starting rows and 60 days coverage
        python scripts/generate_sample_data.py --large --days 60 --full-row-count 250000 --cdc-row-count 250000 --linear-growth 2500 --enable-updates

This script supports fine-grained arguments for controlling the size and behavior of each pattern:
    --days, --start-date, --full-row-count, --cdc-row-count, --current-rows, --history-rows,
    --linear-growth, --enable-updates, --large
"""

from __future__ import annotations

import csv
import sys
from datetime import datetime, date, timedelta
import argparse
from pathlib import Path
from random import Random
from typing import Iterable, List, Dict, Any
import shutil

import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.bronze.io import write_batch_metadata, write_checksum_manifest  # noqa: E402

CONFIG_DIR = REPO_ROOT / "docs" / "examples" / "configs" / "patterns"
BASE_DIR = REPO_ROOT / "sampledata" / "source_samples"
SAMPLE_BRONZE_SAMPLES = REPO_ROOT / "sampledata" / "bronze_samples"
# Mirror the generated source_samples into sampledata/bronze_samples for quick lookups

SAMPLE_START_DATE = date(2025, 11, 13)
DAILY_DAYS = 60


def load_pattern_configs() -> Dict[str, Any]:
    """Load all pattern YAML config files."""
    configs = {}
    for yaml_file in CONFIG_DIR.glob("pattern*.yaml"):
        cfg = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
        pattern_id = cfg.get("pattern_id")
        if pattern_id:
            configs[pattern_id] = cfg
    return configs


# Load pattern configs once at module level
PATTERN_CONFIGS = load_pattern_configs()


def _get_path_keys(pattern_id: str) -> Dict[str, str]:
    """Extract path_structure keys from pattern config with defaults."""
    config = PATTERN_CONFIGS.get(pattern_id, {})
    path_struct = config.get("path_structure", {})
    return {
        "sample_key": path_struct.get("sample_key", "sample"),
        "system_key": path_struct.get("system_key", "system"),
        "entity_key": path_struct.get("entity_key", "table"),
        "date_key": path_struct.get("date_key", "dt"),
    }


def _get_runtime_values(pattern_id: str) -> Dict[str, Any]:
    """Extract runtime values (system, entity, pattern_folder) from pattern config."""
    config = PATTERN_CONFIGS.get(pattern_id, {})
    bronze = config.get("bronze", {})
    options = bronze.get("options", {}) or {}
    raw_formats = options.get("output_formats") or options.get("formats") or []
    if isinstance(raw_formats, str):
        raw_formats_list = [raw_formats]
    else:
        raw_formats_list = list(raw_formats)
    normalized_formats: List[str] = [
        fmt.strip().lower()
        for fmt in raw_formats_list
        if isinstance(fmt, str) and fmt.strip()
    ]
    if not normalized_formats:
        normalized_formats = ["csv", "parquet"]

    return {
        "system": config.get("system", "retail_demo"),
        "entity": config.get("entity", "orders"),
        "pattern_folder": options.get("pattern_folder", pattern_id),
        "load_pattern": options.get("load_pattern", "full"),
        "output_formats": normalized_formats,
    }


def _daily_schedule(start: date, days: int) -> List[str]:
    return [(start + timedelta(days=day)).isoformat() for day in range(days)]


FULL_DATES = _daily_schedule(SAMPLE_START_DATE, DAILY_DAYS)
CDC_DATES = _daily_schedule(SAMPLE_START_DATE, DAILY_DAYS)
CURRENT_HISTORY_DATES = _daily_schedule(SAMPLE_START_DATE, DAILY_DAYS)
FULL_ROW_COUNT = 250_000
CDC_ROW_COUNT = 250_000
CURRENT_HISTORY_CURRENT = 250_000
CURRENT_HISTORY_HISTORY = 100_000
HYBRID_REFERENCE_ROWS = 150
HYBRID_DELTA_ROWS_PER_DAY = 50
LARGE_DEFAULT_ROW_COUNT = 250_000
LARGE_DAYS = 60
DEFAULT_LINEAR_GROWTH = 2500
DEFAULT_ENABLE_UPDATES = True
PATTERN_DIRS = {
    "full": "pattern1_full_events",
    "cdc": "pattern2_cdc_events",
    "current_history": "pattern3_scd_state",
    "hybrid_cdc_point": "pattern4_hybrid_cdc_point",
    "hybrid_cdc_cumulative": "pattern5_hybrid_cdc_cumulative",
    "hybrid_incremental_point": "pattern6_hybrid_incremental_point",
    "hybrid_incremental_cumulative": "pattern7_hybrid_incremental_cumulative",
}
PATTERN_DESC = {
    "pattern1_full_events": "Pattern 1 – Full events: daily rewrites of the entire dataset.",
    "pattern2_cdc_events": "Pattern 2 – CDC events: append-only changelog with change_type metadata.",
    "pattern3_scd_state": "Pattern 3 – SCD state: current + history rows with effective dates.",
    "pattern4_hybrid_cdc_point": "Pattern 4 – Hybrid CDC point: reference metadata plus delta files.",
    "pattern5_hybrid_cdc_cumulative": "Pattern 5 – Hybrid CDC cumulative: cumulative deltas layered over reference.",
    "pattern6_hybrid_incremental_point": "Pattern 6 – Hybrid incremental point: incremental merges for point-in-time state.",
    "pattern7_hybrid_incremental_cumulative": "Pattern 7 – Hybrid incremental cumulative: cumulative merges with history retention.",
}
PATTERN_DETAILS = {
    "pattern1_full_events": [
        "Each day writes a full-part-0001.csv snapshot; midweek schema changes optionally add extra full-part-0002.csv files.",
        "Use this to confirm Bronze drops and rebuilds the partition daily before Silver reads a clean snapshot.",
    ],
    "pattern2_cdc_events": [
        "Daily delta chunks (cdc-part-0001.csv) arrive with change_type/changed_at timestamps; some days may add extra delta-part-0002.csv files.",
        "Bronze must honor change_type values so Silver merges can apply inserts/updates/deletes safely.",
    ],
    "pattern3_scd_state": [
        "Each dt folder contains current-history-part-0001.csv plus optional extra history files when state transitions spike.",
        "Silver builds SCD Type 2 timelines by reading effective_start/effective_end and current_flag columns.",
    ],
    "pattern4_hybrid_cdc_point": [
        "Reference folders plus point-in-time delta folders appear together; some days show both to illustrate derived_event needs.",
        "Bronze keeps the reference metadata aligned before Silver consumes the deltas.",
    ],
    "pattern5_hybrid_cdc_cumulative": [
        "Daily folders include cumulative delta files that grow over time (delta-part-0001.csv, delta-part-0002.csv... ) to show retention of history.",
        "This pattern demonstrates combining a reference snapshot with cumulative deltas for Silver-derived state.",
    ],
    "pattern6_hybrid_incremental_point": [
        "Each day writes incremental merge snapshots plus auxiliary delta files that illustrate point-in-time processing.",
        "Bronze merges these files; Silver uses the incremental context to build current views.",
    ],
    "pattern7_hybrid_incremental_cumulative": [
        "Daily cumulative delta folders accumulate history while keeping previous deltas (delta-part-000X.csv) for backfills.",
        "Silver merges these cumulative deltas while preserving lineage to Bronze increments.",
    ],
}


def _pattern_dir(pattern: str) -> str:
    return PATTERN_DIRS.get(pattern, pattern)


HYBRID_REFERENCE_INITIAL = datetime(2025, 11, 13).date()
HYBRID_REFERENCE_SWITCH_DAY = 9
HYBRID_REFERENCE_SECOND = HYBRID_REFERENCE_INITIAL + timedelta(
    days=HYBRID_REFERENCE_SWITCH_DAY
)
HYBRID_DELTA_DAYS = 11
HYBRID_COMBOS = [
    ("hybrid_cdc_point", "cdc", "point_in_time"),
    ("hybrid_cdc_cumulative", "cdc", "cumulative"),
    ("hybrid_incremental_point", "incremental_merge", "point_in_time"),
    ("hybrid_incremental_cumulative", "incremental_merge", "cumulative"),
]


def _write_chunk_files(
    path: Path, rows: Iterable[Dict[str, object]], formats: Iterable[str] | None = None
) -> List[Path]:
    """Write CSV/parquet chunk copies according to the requested formats."""
    rows_list = list(rows)
    if not rows_list:
        return []
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt_set = {fmt.strip().lower() for fmt in (formats or []) if isinstance(fmt, str) and fmt.strip()}
    if not fmt_set:
        fmt_set = {"csv", "parquet"}

    files: List[Path] = []

    if "csv" in fmt_set:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows_list[0].keys())
            writer.writeheader()
            writer.writerows(rows_list)
        files.append(path)

    if "parquet" in fmt_set:
        df = pd.DataFrame(rows_list)
        parquet_path = path.with_suffix(".parquet")
        df.to_parquet(parquet_path, index=False)
        files.append(parquet_path)

    return files


def _build_hybrid_reference_rows(
    run_date: str, rng: Random, row_count: int, order_id_offset: int
) -> List[Dict[str, object]]:
    """Produce a deterministic set of reference rows for hybrid patterns."""
    start = datetime.fromisoformat(f"{run_date}T00:00:00")
    interval_seconds = max(1, 86400 // max(row_count, 1))
    statuses = ["new", "processing", "shipped", "delivered", "returned"]
    rows: List[Dict[str, object]] = []
    for idx in range(row_count):
        event_time = start + timedelta(seconds=idx * interval_seconds)
        rows.append(
            {
                "order_id": f"HYB-{order_id_offset + idx + 1:08d}",
                "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                "status": rng.choice(statuses),
                "order_total": round(rng.uniform(10.0, 300.0), 2),
                "changed_at": event_time.isoformat() + "Z",
                "updated_at": event_time.isoformat() + "Z",
                "run_date": run_date,
            }
        )
    return rows


def generate_full_snapshot(
    seed: int = 42,
    row_count: int = FULL_ROW_COUNT,
    linear_growth: int = DEFAULT_LINEAR_GROWTH,
    enable_updates: bool = DEFAULT_ENABLE_UPDATES,
) -> None:
    pattern_id = _pattern_dir("full")
    runtime = _get_runtime_values(pattern_id)
    formats = runtime["output_formats"]
    path_keys = _get_path_keys(pattern_id)

    prev_day_order_ids: List[str] = []
    for day_offset, date_str in enumerate(FULL_DATES):
        rng = Random(seed + day_offset)
        base_dir = (
            BASE_DIR
            / f"{path_keys['sample_key']}={runtime['pattern_folder']}"
            / f"{path_keys['system_key']}={runtime['system']}"
            / f"{path_keys['entity_key']}={runtime['entity']}"
            / f"{path_keys['date_key']}={date_str}"
        )
        rows: List[Dict[str, object]] = []
        start = datetime.fromisoformat(f"{date_str}T00:00:00")
        total_rows = row_count + day_offset * linear_growth
        # If updates are enabled, keep some previous day order_ids and mutate a portion
        # so the snapshot simulates updates across days. Also keep unique ids by adding new.
        if enable_updates and prev_day_order_ids:
            base_ids = prev_day_order_ids.copy()
            keep_count = max(int(len(base_ids) * 0.9), 0)
            keep_ids = rng.sample(base_ids, k=keep_count) if base_ids else []
        else:
            keep_ids = []
        new_count = total_rows - len(keep_ids)
        base_order_start = 1 + day_offset * (linear_growth + 1)
        # Add kept orders first
        for idx, oid in enumerate(keep_ids, start=1):
            order_time = start + timedelta(hours=idx)
            rows.append(
                {
                    "order_id": oid,
                    "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                    "status": rng.choice(
                        ["new", "processing", "shipped", "delivered", "returned"]
                    ),
                    "order_total": round(rng.uniform(25.0, 500.0), 2),
                    "updated_at": order_time.isoformat() + "Z",
                    "run_date": date_str,
                }
            )

        for order_id in range(1, new_count + 1):
            order_time = start + timedelta(hours=order_id)
            oid_num = base_order_start + order_id
            oid = f"ORD-{oid_num:05d}"
            rows.append(
                {
                    "order_id": oid,
                    "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                    "status": rng.choice(
                        ["new", "processing", "shipped", "delivered", "returned"]
                    ),
                    "order_total": round(rng.uniform(25.0, 500.0), 2),
                    "updated_at": order_time.isoformat() + "Z",
                    "run_date": date_str,
                }
            )
        # Append sentinel malformed row for validation tests
        rows.append(
            {
                "order_id": None,
                "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                "status": "processing",
                "order_total": round(rng.uniform(25.0, 500.0), 2),
                "updated_at": (start + timedelta(days=total_rows + 1)).isoformat()
                + "Z",
                "run_date": date_str,
            }
        )

        chunk_path = base_dir / "full-part-0001.csv"
        chunk_files = _write_chunk_files(chunk_path, rows, formats=formats)
        total_records = len(rows)
        chunk_count = 1

        if day_offset == 1:
            schema_rows: List[Dict[str, object]] = []
            for idx in range(30):
                schema_rows.append(
                    {
                        "order_id": f"ORD-SC-{idx:05d}",
                        "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                        "status": rng.choice(["high-priority", "standard"]),
                        "order_total": round(rng.uniform(10.0, 999.0), 2),
                        "updated_at": (start + timedelta(hours=idx)).isoformat() + "Z",
                        "run_date": date_str,
                        "campaign_id": f"CAM-{rng.randint(100, 999)}",
                        "priority": rng.choice(["high", "normal", "low"]),
                    }
                )
            schema_chunk = base_dir / "full-part-0002.csv"
            chunk_files.extend(_write_chunk_files(schema_chunk, schema_rows, formats=formats))
            total_records += len(schema_rows)
            chunk_count += 1

        # Write metadata files for Bronze layer
        write_batch_metadata(
            out_dir=base_dir,
            record_count=total_records,
            chunk_count=chunk_count,
        )
        write_checksum_manifest(
            out_dir=base_dir,
            files=chunk_files,
            load_pattern=runtime["load_pattern"],
        )


def _write_hybrid_reference(
    base_dir: Path,
    date_str: str,
    seed: int,
    delta_patterns: List[str],
    delta_mode: str,
    formats: Iterable[str],
    row_count: int,
    order_id_offset: int,
) -> None:
    rng = Random(seed)
    rows = _build_hybrid_reference_rows(date_str, rng, row_count, order_id_offset)
    chunk_path = base_dir / "reference-part-0001.csv"
    _write_chunk_files(chunk_path, rows, formats=formats)
    _write_reference_metadata(base_dir, date_str, rows, delta_mode, delta_patterns)


def _write_hybrid_delta(
    base_dir: Path,
    date_str: str,
    delta_pattern: str,
    seed: int,
    role: str,
    rows: List[Dict[str, object]],
    delta_mode: str,
    reference_run_date: date,
    formats: Iterable[str],
) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    chunk_path = base_dir / "delta-part-0001.csv"
    _write_chunk_files(chunk_path, rows, formats=formats)
    _write_delta_metadata(
        base_dir, date_str, rows, delta_mode, [delta_pattern], reference_run_date
    )


def _build_delta_rows(
    delta_pattern: str,
    date_str: str,
    seed: int,
    row_count: int,
    order_id_start: int,
) -> List[Dict[str, object]]:
    rng = Random(seed)
    start = datetime.fromisoformat(f"{date_str}T08:00:00")
    interval_seconds = max(1, 86400 // max(row_count, 1))
    rows: List[Dict[str, object]] = []
    change_types = ["insert", "update"]
    for idx in range(row_count):
        change_time = start + timedelta(seconds=idx * interval_seconds)
        rows.append(
            {
                "order_id": f"HYB-{order_id_start + idx:08d}",
                "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                "status": rng.choice(["processing", "shipped", "cancelled"]),
                "change_type": rng.choices(change_types, weights=[0.7, 0.3])[0],
                "changed_at": change_time.isoformat() + "Z",
                "updated_at": change_time.isoformat() + "Z",
                "order_total": round(rng.uniform(15.0, 300.0), 2),
                "delta_tag": f"{delta_pattern}-{date_str}",
                "run_date": date_str,
            }
        )
    return rows


def _write_reference_metadata(
    base_dir: Path,
    run_date: str,
    rows: List[Dict[str, object]],
    delta_mode: str,
    delta_patterns: List[str],
) -> None:
    if not rows:
        return
    metadata = {
        "run_date": run_date,
        "reference_mode": {
            "role": "reference",
            "reference_run_date": run_date,
            "delta_mode": delta_mode,
            "delta_patterns": delta_patterns,
        },
    }
    write_batch_metadata(
        base_dir, record_count=len(rows), chunk_count=1, extra_metadata=metadata
    )


def _write_delta_metadata(
    base_dir: Path,
    run_date: str,
    rows: List[Dict[str, object]],
    delta_mode: str,
    delta_patterns: List[str],
    reference_run_date: date,
) -> None:
    if not rows:
        return
    metadata = {
        "run_date": run_date,
        "reference_mode": {
            "role": "delta",
            "reference_run_date": reference_run_date.isoformat(),
            "delta_mode": delta_mode,
            "delta_patterns": delta_patterns,
        },
    }
    write_batch_metadata(
        base_dir, record_count=len(rows), chunk_count=1, extra_metadata=metadata
    )


def generate_cdc(seed: int = 99, row_count: int = CDC_ROW_COUNT) -> None:
    pattern_id = _pattern_dir("cdc")
    runtime = _get_runtime_values(pattern_id)
    formats = runtime["output_formats"]
    path_keys = _get_path_keys(pattern_id)
    change_types = ["insert", "update", "delete"]

    for day_offset, date_str in enumerate(CDC_DATES):
        rng = Random(seed + day_offset)
        base_dir = (
            BASE_DIR
            / f"{path_keys['sample_key']}={runtime['pattern_folder']}"
            / f"{path_keys['system_key']}={runtime['system']}"
            / f"{path_keys['entity_key']}={runtime['entity']}"
            / f"{path_keys['date_key']}={date_str}"
        )
        rows: List[Dict[str, object]] = []
        start = datetime.fromisoformat(f"{date_str}T08:00:00")

        total_rows = row_count + day_offset * 60
        # We deliberately weight deletes to a smaller fraction than inserts
        # to simulate large datasets where deletes are a minority.
        for idx in range(1, total_rows + 1):
            change_time = start + timedelta(minutes=idx * 3)
            rows.append(
                {
                    # We pick random orders from a larger range for variation
                    "order_id": f"ORD-{rng.randint(1, max(800, total_rows)) :05d}",
                    "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                    "change_type": rng.choices(change_types, weights=[0.7, 0.25, 0.05])[0],
                    "changed_at": change_time.isoformat() + "Z",
                    "status": rng.choice(["processing", "shipped", "cancelled"]),
                    "order_total": round(rng.uniform(10.0, 800.0), 2),
                    "run_date": date_str,
                }
            )

        rows.append(
            {
                "order_id": None,
                "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                "change_type": "insert",
                "changed_at": (
                    start + timedelta(minutes=total_rows * 3 + 5)
                ).isoformat()
                + "Z",
                "status": "processing",
                "order_total": round(rng.uniform(10.0, 800.0), 2),
                "run_date": date_str,
            }
        )

        chunk_path = base_dir / "cdc-part-0001.csv"
        chunk_files = _write_chunk_files(chunk_path, rows, formats=formats)
        total_records = len(rows)
        chunk_count = 1

        if day_offset == 1:
            schema_rows: List[Dict[str, object]] = []
            for idx in range(20):
                note_time = start + timedelta(minutes=(idx + 1) * 2)
                schema_rows.append(
                    {
                        "order_id": f"ORD-{rng.randint(1, 1200):05d}",
                        "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                        "change_type": rng.choice(["update", "insert"]),
                        "changed_at": note_time.isoformat() + "Z",
                        "status": "delta",
                        "order_total": round(rng.uniform(5.0, 1200.0), 2),
                        "run_date": date_str,
                        "note": f"schema-change-{idx}",
                    }
                )
            schema_chunk = base_dir / "cdc-part-0002.csv"
            chunk_files.extend(_write_chunk_files(schema_chunk, schema_rows, formats=formats))
            total_records += len(schema_rows)
            chunk_count += 1

        # Write metadata files for Bronze layer
        write_batch_metadata(
            out_dir=base_dir,
            record_count=total_records,
            chunk_count=chunk_count,
        )
        write_checksum_manifest(
            out_dir=base_dir,
            files=chunk_files,
            load_pattern=runtime["load_pattern"],
        )


def generate_current_history(
    seed: int = 7,
    current_rows: int = CURRENT_HISTORY_CURRENT,
    history_rows: int = CURRENT_HISTORY_HISTORY,
) -> None:
    pattern_id = _pattern_dir("current_history")
    runtime = _get_runtime_values(pattern_id)
    formats = runtime["output_formats"]
    path_keys = _get_path_keys(pattern_id)

    for day_offset, date_str in enumerate(CURRENT_HISTORY_DATES):
        rng = Random(seed + day_offset)
        base_dir = (
            BASE_DIR
            / f"{path_keys['sample_key']}={runtime['pattern_folder']}"
            / f"{path_keys['system_key']}={runtime['system']}"
            / f"{path_keys['entity_key']}={runtime['entity']}"
            / f"{path_keys['date_key']}={date_str}"
        )

        def build_history_rows() -> List[Dict[str, object]]:
            rows: List[Dict[str, object]] = []
            base_time = datetime(2024, 1, 1) + timedelta(days=day_offset * 30)
            total_rows = history_rows + day_offset * 80
            for idx in range(1, total_rows + 1):
                start_ts = base_time + timedelta(days=rng.randint(0, 365))
                end_ts = start_ts + timedelta(days=rng.randint(5, 120))
                rows.append(
                    {
                        "order_id": f"ORD-{rng.randint(1, 1200):05d}",
                        "customer_id": f"CUST-{rng.randint(1000, 9999)}",
                        "status": rng.choice(["active", "expired", "suspended"]),
                        "effective_start": start_ts.isoformat() + "Z",
                        "effective_end": end_ts.isoformat() + "Z",
                        "current_flag": 0,
                        "updated_at": (end_ts + timedelta(hours=2)).isoformat() + "Z",
                        "run_date": date_str,
                    }
                )
            return rows

        def build_current_rows() -> List[Dict[str, object]]:
            rows: List[Dict[str, object]] = []
            start_time = datetime.fromisoformat(f"{date_str}T00:00:00")
            total_rows = current_rows + day_offset * 40
            for idx in range(1, total_rows + 1):
                rows.append(
                    {
                        "order_id": f"ORD-{idx + 900 + day_offset * 500:05d}",
                        "customer_id": f"CUST-{rng.randint(2000, 9999)}",
                        "status": rng.choice(["active", "pending", "suspended"]),
                        "effective_start": "",
                        "effective_end": "",
                        "current_flag": 1,
                        "updated_at": (start_time + timedelta(hours=idx)).isoformat()
                        + "Z",
                        "run_date": date_str,
                    }
                )
            return rows

        history_data = build_history_rows()
        current_data = build_current_rows()
        combined_rows = history_data + current_data
        combined_rows.append(
            {
                "order_id": None,
                "customer_id": None,
                "status": "unknown",
                "effective_start": None,
                "effective_end": None,
                "current_flag": None,
                "updated_at": datetime.fromisoformat(f"{date_str}T00:00:00").isoformat()
                + "Z",
                "run_date": date_str,
            }
        )
        total_records = len(combined_rows)
        chunk_count = 1
        chunk_path = base_dir / "current-history-part-0001.csv"
        chunk_files = _write_chunk_files(chunk_path, combined_rows, formats=formats)

        if day_offset == 1:
            skew_rows: List[Dict[str, object]] = []
            for idx in range(150):
                skew_rows.append(
                    {
                        "order_id": f"ORD-SK-{idx:05d}",
                        "customer_id": f"CUST-{rng.randint(2000, 9999)}",
                        "status": "active",
                        "effective_start": (
                            datetime.fromisoformat(f"{date_str}T00:00:00")
                            - timedelta(days=idx % 5)
                        ).isoformat()
                        + "Z",
                        "effective_end": None,
                        "current_flag": 1,
                        "updated_at": (
                            datetime.fromisoformat(f"{date_str}T12:00:00")
                            + timedelta(minutes=idx)
                        ).isoformat()
                        + "Z",
                        "run_date": date_str,
                        "revision_notes": f"skewed-{idx % 3}",
                    }
                )
            skew_chunk = base_dir / "current-history-part-0002.csv"
            chunk_files.extend(_write_chunk_files(skew_chunk, skew_rows, formats=formats))
            total_records += len(skew_rows)
            chunk_count += 1

        # Write metadata files for Bronze layer
        write_batch_metadata(
            out_dir=base_dir,
            record_count=total_records,
            chunk_count=chunk_count,
        )
        write_checksum_manifest(
            out_dir=base_dir,
            files=chunk_files,
            load_pattern=runtime["load_pattern"],
        )


def generate_hybrid_combinations(seed: int = 123) -> None:
    for combo_name, delta_pattern, delta_mode in HYBRID_COMBOS:
        pattern_id = _pattern_dir(combo_name)
        runtime = _get_runtime_values(pattern_id)
        formats = runtime["output_formats"]
        path_keys = _get_path_keys(pattern_id)

        base_pattern_dir = (
            BASE_DIR
            / f"{path_keys['sample_key']}={runtime['pattern_folder']}"
            / f"{path_keys['system_key']}={runtime['system']}"
            / f"{path_keys['entity_key']}={runtime['entity']}"
        )
        reference_dates = (HYBRID_REFERENCE_INITIAL, HYBRID_REFERENCE_SECOND)
        reference_total_rows = len(reference_dates) * HYBRID_REFERENCE_ROWS

        for ref_idx, ref_date in enumerate(reference_dates):
            ref_dir = (
                base_pattern_dir
                / f"{path_keys['date_key']}={ref_date.isoformat()}"
                / "reference"
            )
            _write_hybrid_reference(
                ref_dir,
                ref_date.isoformat(),
                seed + ref_idx,
                [delta_pattern],
                delta_mode,
                formats=formats,
                row_count=HYBRID_REFERENCE_ROWS,
                order_id_offset=ref_idx * HYBRID_REFERENCE_ROWS,
            )

        cumulative_rows: List[Dict[str, object]] = []
        for offset in range(1, HYBRID_DELTA_DAYS + 1):
            delta_date = HYBRID_REFERENCE_INITIAL + timedelta(days=offset)
            order_id_start = reference_total_rows + (offset - 1) * HYBRID_DELTA_ROWS_PER_DAY + 1
            rows = _build_delta_rows(
                delta_pattern,
                delta_date.isoformat(),
                seed + offset * 10,
                row_count=HYBRID_DELTA_ROWS_PER_DAY,
                order_id_start=order_id_start,
            )
            if delta_mode == "cumulative":
                cumulative_rows.extend(rows)
                rows_to_write = cumulative_rows.copy()
            else:
                rows_to_write = rows
            delta_dir = (
                base_pattern_dir
                / f"{path_keys['date_key']}={delta_date.isoformat()}"
                / "delta"
            )
            if delta_date == HYBRID_REFERENCE_SECOND:
                reference_run_date = HYBRID_REFERENCE_INITIAL
            elif delta_date > HYBRID_REFERENCE_SECOND:
                reference_run_date = HYBRID_REFERENCE_SECOND
            else:
                reference_run_date = HYBRID_REFERENCE_INITIAL
            _write_hybrid_delta(
                delta_dir,
                delta_date.isoformat(),
                delta_pattern,
                seed + offset * 10,
                "delta",
                rows_to_write,
                delta_mode,
                reference_run_date,
                formats=formats,
            )


def main() -> None:
    global DAILY_DAYS, SAMPLE_START_DATE, FULL_DATES, CDC_DATES, CURRENT_HISTORY_DATES, HYBRID_REFERENCE_ROWS, HYBRID_DELTA_ROWS_PER_DAY
    parser = argparse.ArgumentParser(
        description="Generate Bronze source sample datasets with configurable scale and time ranges."
    )
    parser.add_argument("--days", type=int, default=None, help="Number of days to generate for each pattern.")
    parser.add_argument(
        "--start-date",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument("--full-row-count", type=int, default=None, help="Initial full snapshot row count.")
    parser.add_argument("--cdc-row-count", type=int, default=None, help="Initial CDC row count.")
    parser.add_argument(
        "--current-rows",
        type=int,
        default=None,
        help="Initial current rows for current-history pattern.",
    )
    parser.add_argument(
        "--history-rows", type=int, default=None, help="Initial history rows for current-history pattern."
    )
    parser.add_argument(
        "--hybrid-reference-rows",
        type=int,
        default=None,
        help="Row count for each hybrid reference snapshot (defaults to --full-row-count).",
    )
    parser.add_argument(
        "--hybrid-delta-rows",
        type=int,
        default=None,
        help=(
            "Row count for each hybrid delta partition (defaults to --cdc-row-count "
            f"/ {HYBRID_DELTA_DAYS})."
        ),
    )
    parser.add_argument(
        "--linear-growth",
        type=int,
        default=DEFAULT_LINEAR_GROWTH,
        help="Linear daily increment to simulate growth.",
    )
    parser.add_argument(
        "--enable-updates",
        dest="enable_updates",
        action="store_true",
        default=DEFAULT_ENABLE_UPDATES,
        help="Enable updates simulation for full snapshot pattern (mutate existing order_ids across days).",
    )
    parser.add_argument(
        "--disable-updates",
        dest="enable_updates",
        action="store_false",
        help="Disable the update simulation even though the default dataset enables it.",
    )
    parser.add_argument(
        "--large",
        action="store_true",
        help=(
            "Create a large dataset set (default 250k rows and 60 days). "
            "This overrides per-row defaults unless explicitly passed."
        ),
    )
    args = parser.parse_args()

    global DAILY_DAYS, SAMPLE_START_DATE, FULL_DATES, CDC_DATES, CURRENT_HISTORY_DATES
    # If --large flag is set, pre-fill only unspecified values with large defaults
    if args.large:
        if args.full_row_count is None:
            args.full_row_count = LARGE_DEFAULT_ROW_COUNT
        if args.cdc_row_count is None:
            args.cdc_row_count = LARGE_DEFAULT_ROW_COUNT
        if args.days is None:
            args.days = LARGE_DAYS
    # Use defaults for any values that are still omitted on the command line
    if args.days is None:
        args.days = DAILY_DAYS
    if args.start_date is None:
        args.start_date = SAMPLE_START_DATE
    if args.full_row_count is None:
        args.full_row_count = FULL_ROW_COUNT
    if args.cdc_row_count is None:
        args.cdc_row_count = CDC_ROW_COUNT
    if args.current_rows is None:
        args.current_rows = CURRENT_HISTORY_CURRENT
    if args.history_rows is None:
        args.history_rows = CURRENT_HISTORY_HISTORY
    if args.hybrid_reference_rows is None:
        args.hybrid_reference_rows = args.full_row_count
    if args.hybrid_delta_rows is None:
        args.hybrid_delta_rows = max(1, args.cdc_row_count // max(HYBRID_DELTA_DAYS, 1))
    args.hybrid_reference_rows = max(1, args.hybrid_reference_rows)
    args.hybrid_delta_rows = max(1, args.hybrid_delta_rows)
    HYBRID_REFERENCE_ROWS = args.hybrid_reference_rows
    HYBRID_DELTA_ROWS_PER_DAY = args.hybrid_delta_rows
    # Set runtime variables
    DAILY_DAYS = args.days
    SAMPLE_START_DATE = args.start_date
    FULL_DATES = _daily_schedule(SAMPLE_START_DATE, DAILY_DAYS)
    CDC_DATES = _daily_schedule(SAMPLE_START_DATE, DAILY_DAYS)
    CURRENT_HISTORY_DATES = _daily_schedule(SAMPLE_START_DATE, DAILY_DAYS)

    print(f"Generating samples for {DAILY_DAYS} day(s) starting {SAMPLE_START_DATE}. Large={args.large}")
    generate_full_snapshot(seed=42, row_count=args.full_row_count, linear_growth=args.linear_growth, enable_updates=args.enable_updates)
    generate_cdc(seed=99, row_count=args.cdc_row_count)
    generate_current_history(seed=7, current_rows=args.current_rows, history_rows=args.history_rows)
    generate_hybrid_combinations(seed=123)
    print(f"Sample datasets written under {BASE_DIR}")
    _write_pattern_readmes()
    _sync_sampledata_bronze()
    print(f"Bronze sample mirror available under {SAMPLE_BRONZE_SAMPLES}")


def _write_pattern_readmes() -> None:
    for pattern_key, desc in PATTERN_DESC.items():
        # Get the pattern_folder from YAML config
        runtime = _get_runtime_values(pattern_key)
        path_keys = _get_path_keys(pattern_key)
        pattern_dir = BASE_DIR / f"{path_keys['sample_key']}={runtime['pattern_folder']}"
        pattern_dir.mkdir(parents=True, exist_ok=True)
        readme = pattern_dir / "README.md"
        lines = [
            f"# {pattern_key}",
            "",
            desc,
            "",
            "Each `dt=YYYY-MM-DD` directory contains the source files emitted on that day.",
        ]
        details = PATTERN_DETAILS.get(pattern_key, [])
        for detail in details:
            lines.append("")
            lines.append(detail)
        lines.append("")
        lines.append(
            "These source files drive the Bronze/Silver behavior described in `docs/usage/patterns/pattern_matrix.md`."
        )
        readme.write_text("\n".join(lines), encoding="utf-8")


def _sync_sampledata_bronze() -> None:
    if SAMPLE_BRONZE_SAMPLES.exists():
        shutil.rmtree(SAMPLE_BRONZE_SAMPLES)
    SAMPLE_BRONZE_SAMPLES.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(BASE_DIR, SAMPLE_BRONZE_SAMPLES)


if __name__ == "__main__":
    main()
