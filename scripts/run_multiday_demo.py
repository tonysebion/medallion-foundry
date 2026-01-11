#!/usr/bin/env python3
"""
Run multi-day pattern demo that writes persistent data to MinIO.

This script runs Bronze and Silver pipelines for all pattern combinations
over 3 days of evolution, leaving the data in MinIO for inspection.

Usage:
    python scripts/run_multiday_demo.py

Data will be written to:
    s3://mdf/demo/bronze/system=demo/entity=<pattern>/dt=<date>/
    s3://mdf/demo/silver/system=demo/entity=<pattern>/dt=<date>/
"""

import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode, DeleteMode

# MinIO configuration - adjust if needed
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.environ.get("MINIO_REGION", "us-east-1")
BUCKET = "mdf"
PREFIX = "demo"

# Test dates
DAY1 = "2025-01-10"
DAY2 = "2025-01-11"
DAY3 = "2025-01-12"


def get_storage_options():
    """Get S3/MinIO storage options."""
    return {
        "endpoint_url": MINIO_ENDPOINT,
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "region": MINIO_REGION,
        "addressing_style": "path",
    }


# =============================================================================
# Test Data Generators
# =============================================================================


def generate_snapshot_day1() -> pd.DataFrame:
    """Day 1: Initial 5 records."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "David", "Eve"],
            "value": [100, 200, 300, 400, 500],
            "ts": [datetime(2025, 1, 10, 10, 0, 0)] * 5,
        }
    )


def generate_snapshot_day2() -> pd.DataFrame:
    """Day 2: Updated values for IDs 1, 3; new ID 6."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Carol", "David", "Eve", "Frank"],
            "value": [110, 200, 330, 400, 500, 600],  # ID 1: 100->110, ID 3: 300->330
            "ts": [
                datetime(2025, 1, 11, 10, 0, 0),  # ID 1 updated
                datetime(2025, 1, 10, 10, 0, 0),  # ID 2 unchanged
                datetime(2025, 1, 11, 10, 0, 0),  # ID 3 updated
                datetime(2025, 1, 10, 10, 0, 0),  # ID 4 unchanged
                datetime(2025, 1, 10, 10, 0, 0),  # ID 5 unchanged
                datetime(2025, 1, 11, 10, 0, 0),  # ID 6 new
            ],
        }
    )


def generate_snapshot_day3() -> pd.DataFrame:
    """Day 3: ID 2 removed (snapshot doesn't include it), ID 4 updated."""
    return pd.DataFrame(
        {
            "id": [1, 3, 4, 5, 6],  # ID 2 missing = deleted
            "name": ["Alice", "Carol", "David", "Eve", "Frank"],
            "value": [110, 330, 440, 500, 600],  # ID 4: 400->440
            "ts": [
                datetime(2025, 1, 11, 10, 0, 0),  # ID 1 unchanged from day 2
                datetime(2025, 1, 11, 10, 0, 0),  # ID 3 unchanged from day 2
                datetime(2025, 1, 12, 10, 0, 0),  # ID 4 updated
                datetime(2025, 1, 10, 10, 0, 0),  # ID 5 unchanged
                datetime(2025, 1, 11, 10, 0, 0),  # ID 6 unchanged from day 2
            ],
        }
    )


def generate_cdc_day1() -> pd.DataFrame:
    """Day 1 CDC: All inserts."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "David", "Eve"],
            "value": [100, 200, 300, 400, 500],
            "ts": [datetime(2025, 1, 10, 10, 0, 0)] * 5,
            "op": ["I", "I", "I", "I", "I"],
        }
    )


def generate_cdc_day2() -> pd.DataFrame:
    """Day 2 CDC: Updates for IDs 1, 3; Insert for ID 6."""
    return pd.DataFrame(
        {
            "id": [1, 3, 6],
            "name": ["Alice", "Carol", "Frank"],
            "value": [110, 330, 600],
            "ts": [
                datetime(2025, 1, 11, 10, 0, 0),
                datetime(2025, 1, 11, 10, 0, 0),
                datetime(2025, 1, 11, 10, 0, 0),
            ],
            "op": ["U", "U", "I"],
        }
    )


def generate_cdc_day3() -> pd.DataFrame:
    """Day 3 CDC: Delete for ID 2, Update for ID 4."""
    return pd.DataFrame(
        {
            "id": [2, 4],
            "name": ["Bob", "David"],
            "value": [200, 440],
            "ts": [
                datetime(2025, 1, 12, 10, 0, 0),
                datetime(2025, 1, 12, 10, 0, 0),
            ],
            "op": ["D", "U"],
        }
    )


# =============================================================================
# Pipeline Runners
# =============================================================================


def run_bronze(
    csv_path: str, run_date: str, entity: str, load_pattern: str, cdc_col: str = None
):
    """Run Bronze pipeline."""
    opts = get_storage_options()
    if cdc_col:
        opts["cdc_operation_column"] = cdc_col

    watermark = "ts" if load_pattern == "incremental_append" else None

    bronze = BronzeSource(
        system="demo",
        entity=entity,
        source_type=SourceType.FILE_CSV,
        source_path=csv_path,
        target_path=f"s3://{BUCKET}/{PREFIX}/bronze/system=demo/entity={entity}/dt={{run_date}}/",
        load_pattern=LoadPattern(load_pattern),
        watermark_column=watermark,
        partition_by=[],
        options=opts,
    )
    return bronze.run(run_date)


def run_silver(
    run_date: str,
    entity: str,
    entity_kind: EntityKind,
    history_mode: HistoryMode,
    delete_mode: DeleteMode = DeleteMode.IGNORE,
    cdc_col: str = None,
):
    """Run Silver pipeline."""
    cdc_opts = None
    if cdc_col:
        cdc_opts = {"operation_column": cdc_col}

    silver = SilverEntity(
        source_path=f"s3://{BUCKET}/{PREFIX}/bronze/system=demo/entity={entity}/dt=*/*.parquet",
        target_path=f"s3://{BUCKET}/{PREFIX}/silver/system=demo/entity={entity}/dt={{run_date}}/",
        unique_columns=["id"],
        last_updated_column="ts",
        entity_kind=entity_kind,
        history_mode=history_mode,
        delete_mode=delete_mode,
        cdc_options=cdc_opts,
        storage_options=get_storage_options(),
    )
    return silver.run(run_date)


def save_csv(df: pd.DataFrame, tmpdir: Path, name: str) -> str:
    """Save DataFrame to CSV and return path."""
    path = tmpdir / f"{name}.csv"
    df.to_csv(path, index=False)
    return str(path)


# =============================================================================
# Pattern Demos
# =============================================================================


def run_snapshot_scd1_demo(tmpdir: Path):
    """Snapshot -> State (SCD1) over 3 days."""
    entity = "snapshot_scd1"
    print(f"\n{'=' * 60}")
    print(f"Pattern: {entity}")
    print(f"{'=' * 60}")

    for day, (date, gen_fn) in enumerate(
        [
            (DAY1, generate_snapshot_day1),
            (DAY2, generate_snapshot_day2),
            (DAY3, generate_snapshot_day3),
        ],
        1,
    ):
        print(f"\n  Day {day} ({date}):")
        df = gen_fn()
        csv_path = save_csv(df, tmpdir, f"{entity}_day{day}")

        bronze_result = run_bronze(csv_path, date, entity, "full_snapshot")
        print(f"    Bronze: {bronze_result.get('row_count', 0)} rows")

        silver_result = run_silver(
            date, entity, EntityKind.STATE, HistoryMode.CURRENT_ONLY
        )
        print(f"    Silver: {silver_result.get('row_count', 0)} rows")


def run_snapshot_scd2_demo(tmpdir: Path):
    """Snapshot -> State (SCD2) over 3 days."""
    entity = "snapshot_scd2"
    print(f"\n{'=' * 60}")
    print(f"Pattern: {entity}")
    print(f"{'=' * 60}")

    for day, (date, gen_fn) in enumerate(
        [
            (DAY1, generate_snapshot_day1),
            (DAY2, generate_snapshot_day2),
            (DAY3, generate_snapshot_day3),
        ],
        1,
    ):
        print(f"\n  Day {day} ({date}):")
        df = gen_fn()
        csv_path = save_csv(df, tmpdir, f"{entity}_day{day}")

        bronze_result = run_bronze(csv_path, date, entity, "full_snapshot")
        print(f"    Bronze: {bronze_result.get('row_count', 0)} rows")

        silver_result = run_silver(
            date, entity, EntityKind.STATE, HistoryMode.FULL_HISTORY
        )
        print(f"    Silver: {silver_result.get('row_count', 0)} rows")


def run_cdc_scd1_ignore_demo(tmpdir: Path):
    """CDC -> State (SCD1) with delete_mode=ignore over 3 days."""
    entity = "cdc_scd1_ignore"
    print(f"\n{'=' * 60}")
    print(f"Pattern: {entity}")
    print(f"{'=' * 60}")

    for day, (date, gen_fn) in enumerate(
        [
            (DAY1, generate_cdc_day1),
            (DAY2, generate_cdc_day2),
            (DAY3, generate_cdc_day3),
        ],
        1,
    ):
        print(f"\n  Day {day} ({date}):")
        df = gen_fn()
        csv_path = save_csv(df, tmpdir, f"{entity}_day{day}")

        bronze_result = run_bronze(csv_path, date, entity, "cdc", cdc_col="op")
        print(f"    Bronze: {bronze_result.get('row_count', 0)} rows")

        silver_result = run_silver(
            date,
            entity,
            EntityKind.STATE,
            HistoryMode.CURRENT_ONLY,
            delete_mode=DeleteMode.IGNORE,
            cdc_col="op",
        )
        print(f"    Silver: {silver_result.get('row_count', 0)} rows")


def run_cdc_scd1_tombstone_demo(tmpdir: Path):
    """CDC -> State (SCD1) with delete_mode=tombstone over 3 days."""
    entity = "cdc_scd1_tombstone"
    print(f"\n{'=' * 60}")
    print(f"Pattern: {entity}")
    print(f"{'=' * 60}")

    for day, (date, gen_fn) in enumerate(
        [
            (DAY1, generate_cdc_day1),
            (DAY2, generate_cdc_day2),
            (DAY3, generate_cdc_day3),
        ],
        1,
    ):
        print(f"\n  Day {day} ({date}):")
        df = gen_fn()
        csv_path = save_csv(df, tmpdir, f"{entity}_day{day}")

        bronze_result = run_bronze(csv_path, date, entity, "cdc", cdc_col="op")
        print(f"    Bronze: {bronze_result.get('row_count', 0)} rows")

        silver_result = run_silver(
            date,
            entity,
            EntityKind.STATE,
            HistoryMode.CURRENT_ONLY,
            delete_mode=DeleteMode.TOMBSTONE,
            cdc_col="op",
        )
        print(f"    Silver: {silver_result.get('row_count', 0)} rows")


def run_cdc_scd1_hard_delete_demo(tmpdir: Path):
    """CDC -> State (SCD1) with delete_mode=hard_delete over 3 days."""
    entity = "cdc_scd1_hard"
    print(f"\n{'=' * 60}")
    print(f"Pattern: {entity}")
    print(f"{'=' * 60}")

    for day, (date, gen_fn) in enumerate(
        [
            (DAY1, generate_cdc_day1),
            (DAY2, generate_cdc_day2),
            (DAY3, generate_cdc_day3),
        ],
        1,
    ):
        print(f"\n  Day {day} ({date}):")
        df = gen_fn()
        csv_path = save_csv(df, tmpdir, f"{entity}_day{day}")

        bronze_result = run_bronze(csv_path, date, entity, "cdc", cdc_col="op")
        print(f"    Bronze: {bronze_result.get('row_count', 0)} rows")

        silver_result = run_silver(
            date,
            entity,
            EntityKind.STATE,
            HistoryMode.CURRENT_ONLY,
            delete_mode=DeleteMode.HARD_DELETE,
            cdc_col="op",
        )
        print(f"    Silver: {silver_result.get('row_count', 0)} rows")


def run_cdc_scd2_tombstone_demo(tmpdir: Path):
    """CDC -> State (SCD2) with delete_mode=tombstone over 3 days."""
    entity = "cdc_scd2_tombstone"
    print(f"\n{'=' * 60}")
    print(f"Pattern: {entity}")
    print(f"{'=' * 60}")

    for day, (date, gen_fn) in enumerate(
        [
            (DAY1, generate_cdc_day1),
            (DAY2, generate_cdc_day2),
            (DAY3, generate_cdc_day3),
        ],
        1,
    ):
        print(f"\n  Day {day} ({date}):")
        df = gen_fn()
        csv_path = save_csv(df, tmpdir, f"{entity}_day{day}")

        bronze_result = run_bronze(csv_path, date, entity, "cdc", cdc_col="op")
        print(f"    Bronze: {bronze_result.get('row_count', 0)} rows")

        silver_result = run_silver(
            date,
            entity,
            EntityKind.STATE,
            HistoryMode.FULL_HISTORY,
            delete_mode=DeleteMode.TOMBSTONE,
            cdc_col="op",
        )
        print(f"    Silver: {silver_result.get('row_count', 0)} rows")


def run_cdc_event_demo(tmpdir: Path):
    """CDC -> Event entity over 3 days."""
    entity = "cdc_event"
    print(f"\n{'=' * 60}")
    print(f"Pattern: {entity}")
    print(f"{'=' * 60}")

    for day, (date, gen_fn) in enumerate(
        [
            (DAY1, generate_cdc_day1),
            (DAY2, generate_cdc_day2),
            (DAY3, generate_cdc_day3),
        ],
        1,
    ):
        print(f"\n  Day {day} ({date}):")
        df = gen_fn()
        csv_path = save_csv(df, tmpdir, f"{entity}_day{day}")

        bronze_result = run_bronze(csv_path, date, entity, "cdc", cdc_col="op")
        print(f"    Bronze: {bronze_result.get('row_count', 0)} rows")

        silver_result = run_silver(
            date, entity, EntityKind.EVENT, HistoryMode.CURRENT_ONLY, cdc_col="op"
        )
        print(f"    Silver: {silver_result.get('row_count', 0)} rows")


def main():
    """Run all pattern demos."""
    print("=" * 60)
    print("Multi-Day Pattern Demo")
    print("=" * 60)
    print(f"\nMinIO Endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket: {BUCKET}")
    print(f"Prefix: {PREFIX}")
    print(f"\nData will be written to: s3://{BUCKET}/{PREFIX}/")
    print("\nThis data will PERSIST in MinIO for inspection.")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Snapshot patterns
        run_snapshot_scd1_demo(tmpdir)
        run_snapshot_scd2_demo(tmpdir)

        # CDC patterns with different delete modes
        run_cdc_scd1_ignore_demo(tmpdir)
        run_cdc_scd1_tombstone_demo(tmpdir)
        run_cdc_scd1_hard_delete_demo(tmpdir)
        run_cdc_scd2_tombstone_demo(tmpdir)

        # CDC event pattern
        run_cdc_event_demo(tmpdir)

    print("\n" + "=" * 60)
    print("Demo Complete!")
    print("=" * 60)
    print(
        f"\nBrowse results at: {MINIO_ENDPOINT.replace('http://', 'http://').replace(':9000', ':9001')}/browser/{BUCKET}"
    )
    print(f"Or use: aws s3 ls s3://{BUCKET}/{PREFIX}/ --endpoint-url {MINIO_ENDPOINT}")


if __name__ == "__main__":
    main()
