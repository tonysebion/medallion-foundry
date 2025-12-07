"""Tests for Bronze/Silver path helpers."""

from datetime import date
from pathlib import Path

import pytest

from core.runtime import paths
from core.runtime.paths import (
    BronzePartition,
    SilverPartition,
    build_bronze_partition,
    build_bronze_relative_path,
    build_silver_partition_path,
)
from core.foundation.primitives.patterns import LoadPattern


class FixedDatetime:
    def __init__(self, formatted: str):
        self.formatted = formatted

    def now(self):
        class Dummy:
            def __init__(self, value: str):
                self.value = value

            def strftime(self, fmt: str) -> str:
                return self.value

        return Dummy(self.formatted)


def test_bronze_partition_relative_path_respects_keys() -> None:
    partition = BronzePartition(
        system="sys",
        table="tbl",
        pattern="full",
        run_date=date(2025, 1, 1),
        system_key="sys_key",
        entity_key="tbl_key",
        pattern_key="pat_key",
        date_key="dt_key",
    )

    assert partition.relative_path().as_posix() == (
        "sys_key=sys/tbl_key=tbl/pat_key=full/dt_key=2025-01-01"
    )


def test_build_bronze_partition_uses_config_overrides() -> None:
    cfg = {
        "source": {
            "system": "payments",
            "table": "claims",
            "run": {"load_pattern": "cdc", "pattern_folder": "run_folder"},
        },
        "platform": {
            "bronze": {
                "options": {"pattern_folder": "opt_folder"},
            }
        },
        "path_structure": {
            "bronze": {
                "system_key": "sys",
                "entity_key": "table",
                "pattern_key": "pattern",
                "date_key": "dt",
            }
        },
    }

    partition = build_bronze_partition(cfg, run_date=date(2025, 1, 2))
    assert partition.pattern == "opt_folder"
    assert partition.system == "payments"
    assert partition.run_date == date(2025, 1, 2)


def _make_bronze_cfg(partitioning: dict | None = None, run_overrides: dict | None = None) -> dict:
    cfg = {
        "source": {
            "system": "sys",
            "table": "table",
            "run": {"load_pattern": LoadPattern.SNAPSHOT.value},
        },
        "platform": {"bronze": {"partitioning": partitioning or {}}},
        "path_structure": {"bronze": {}},
    }
    if run_overrides:
        cfg["source"]["run"].update(run_overrides)
    return cfg


def test_build_bronze_relative_path_various_partitions(monkeypatch) -> None:
    # Default date partition
    cfg = _make_bronze_cfg()
    path = build_bronze_relative_path(cfg, run_date=date(2025, 1, 1))
    assert path == "system=sys/table=table/pattern=snapshot/dt=2025-01-01/"

    # Hourly partitioning uses current hour
    cfg = _make_bronze_cfg(partitioning={"partition_strategy": "hourly"})
    monkeypatch.setattr(paths, "datetime", FixedDatetime("08"))
    hourly = build_bronze_relative_path(cfg, run_date=date(2025, 1, 1))
    assert hourly.endswith("/hour=08/")

    # Timestamp partitioning
    cfg = _make_bronze_cfg(partitioning={"partition_strategy": "timestamp"})
    monkeypatch.setattr(paths, "datetime", FixedDatetime("20250101_1234"))
    timestamp_path = build_bronze_relative_path(cfg, run_date=date(2025, 1, 1))
    assert timestamp_path.endswith("/batch=20250101_1234/")

    # Batch id partitioning with user-supplied batch_id
    cfg = _make_bronze_cfg(
        partitioning={"partition_strategy": "batch_id"},
        run_overrides={"batch_id": "batch-001"},
    )
    batch_path = build_bronze_relative_path(cfg, run_date=date(2025, 1, 1))
    assert batch_path.endswith("/batch_id=batch-001/")

    # When use_dt_partition is False, only the base path is returned
    cfg = _make_bronze_cfg(partitioning={"use_dt_partition": False})
    nodt_path = build_bronze_relative_path(cfg, run_date=date(2025, 1, 1))
    assert nodt_path == "system=sys/table=table/pattern=snapshot/"


def test_silver_partition_base_path_with_and_without_pattern() -> None:
    partition = SilverPartition(
        domain="dom",
        entity="ent",
        version=2,
        load_partition_name="load",
        run_date=date(2025, 1, 1),
        include_pattern_folder=True,
        pattern="pattern_value",
    )
    assert partition.base_path().as_posix() == (
        "/domain=dom/entity=ent/v2/pattern=pattern_value/load_date=2025-01-01"
    )

    partition_no_pattern = SilverPartition(
        domain="dom",
        entity="ent",
        version=2,
        load_partition_name="load",
        run_date=date(2025, 1, 1),
        include_pattern_folder=False,
    )
    assert partition_no_pattern.base_path().as_posix() == (
        "/domain=dom/entity=ent/v2/load_date=2025-01-01"
    )


def test_build_silver_partition_path_respects_overrides() -> None:
    silver_base = Path("/data")
    path = build_silver_partition_path(
        silver_base=silver_base,
        domain="domain",
        entity="entity",
        version=3,
        load_partition_name="load",
        include_pattern_folder=True,
        load_pattern=LoadPattern.INCREMENTAL_APPEND,
        run_date=date(2025, 1, 1),
        path_structure={
            "silver": {
                "domain_key": "d",
                "entity_key": "e",
                "version_key": "ver",
                "pattern_key": "pat",
                "load_date_key": "ld",
            }
        },
        pattern_folder="custom",
    )

    assert path.as_posix() == (
        "/data/d=domain/e=entity/ver3/pat=custom/ld=2025-01-01"
    )

    path_no_pattern = build_silver_partition_path(
        silver_base=silver_base,
        domain="domain",
        entity="entity",
        version=3,
        load_partition_name="load",
        include_pattern_folder=False,
        load_pattern=LoadPattern.SNAPSHOT,
        run_date=date(2025, 1, 1),
    )
    assert path_no_pattern.as_posix() == "/data/domain=domain/entity=entity/v3/load_date=2025-01-01"
