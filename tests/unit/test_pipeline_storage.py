"""Tests for pipeline storage helpers (Silver I/O and partition utilities)."""

import json
from pathlib import Path

import ibis
import pandas as pd

from pipelines.lib.checksum import verify_checksum_manifest
from pipelines.lib.io import (
    get_latest_partition,
    list_partitions,
    write_partitioned,
    write_silver_with_artifacts,
)


def test_write_silver_with_artifacts_creates_metadata_and_checksums(tmp_path: Path):
    df = pd.DataFrame(
        [
            {"order_id": 1, "amount": 10.5, "updated_at": "2025-01-01T00:00:00"},
            {"order_id": 2, "amount": 20.75, "updated_at": "2025-01-01T01:00:00"},
        ]
    )
    target = tmp_path / "silver/orders"

    metadata = write_silver_with_artifacts(
        ibis.memtable(df),
        str(target),
        entity_kind="state",
        history_mode="current_only",
        natural_keys=["order_id"],
        change_timestamp="updated_at",
        source_path="s3://bronze/system=tests",
        pipeline_name="tests.orders",
        run_date="2025-01-15",
    )

    assert metadata.row_count == 2
    assert metadata.entity_kind == "state"
    assert metadata.history_mode == "current_only"

    assert (target / "_metadata.json").exists()
    assert (target / "_checksums.json").exists()

    checksum_result = verify_checksum_manifest(target)
    assert checksum_result.valid
    assert "data.parquet" in checksum_result.verified_files


def test_list_partitions_and_latest(tmp_path: Path):
    base = tmp_path / "silver/orders"
    (base / "dt=2025-01-15").mkdir(parents=True)
    (base / "dt=2025-01-16").mkdir(parents=True)

    partitions = list_partitions(str(base), ["dt"])
    assert len(partitions) == 2
    assert {"dt": "2025-01-15"} in partitions

    latest = get_latest_partition(str(base), "dt")
    assert latest == "2025-01-16"


def test_write_partitioned_creates_subdirs(tmp_path: Path):
    df = pd.DataFrame(
        [
            {"order_id": 1, "region": "us", "amount": 100},
            {"order_id": 2, "region": "eu", "amount": 200},
            {"order_id": 3, "region": "us", "amount": 150},
        ]
    )
    target = tmp_path / "silver/region_orders"

    partitions = write_partitioned(
        ibis.memtable(df),
        str(target),
        partition_by=["region"],
        format="parquet",
    )

    assert any("region=us" in path for path in partitions)
    assert (target / "region=us" / "_metadata.json").exists()
    assert (target / "region=us" / "_metadata.json").exists()
