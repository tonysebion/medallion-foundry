"""Tests for the new SilverProcessor pattern engine."""

from datetime import date
from pathlib import Path

import pandas as pd

from core.config.dataset import DatasetConfig
from core.silver.processor import SilverProcessor


def _write_bronze(tmp_path: Path, rows: list[dict]) -> Path:
    bronze_path = tmp_path / "bronze_partition"
    bronze_path.mkdir()
    df = pd.DataFrame(rows)
    df.to_csv(bronze_path / "part-0001.csv", index=False)
    return bronze_path


def test_event_append_log_processor(tmp_path):
    dataset = DatasetConfig.from_dict(
        {
            "system": "crm",
            "entity": "orders",
            "bronze": {
                "enabled": True,
                "source_type": "file",
                "path_pattern": "./docs/examples/data/orders.csv",
            },
            "silver": {
                "enabled": True,
                "entity_kind": "event",
                "input_mode": "append_log",
                "natural_keys": ["order_id"],
                "event_ts_column": "event_ts",
                "change_ts_column": "event_ts",
                "attributes": ["status", "amount"],
                "partition_by": ["event_ts_dt"],
            },
        }
    )
    bronze_path = _write_bronze(
        tmp_path,
        [
            {"order_id": 1, "event_ts": "2024-01-01T10:00:00Z", "status": "new", "amount": 10},
            {"order_id": 2, "event_ts": "2024-01-02T08:30:00Z", "status": "pending", "amount": 25},
        ],
    )
    silver_partition = tmp_path / "silver" / "event"
    processor = SilverProcessor(dataset, bronze_path, silver_partition, date(2024, 1, 3))
    result = processor.run()
    assert result.metrics.rows_written == 2
    assert "events" in result.outputs
    stored = pd.concat(pd.read_parquet(path) for path in result.outputs["events"])
    assert set(stored["order_id"]) == {1, 2}
    assert "load_batch_id" in stored.columns
    assert "event_ts_dt" in stored.columns


def test_state_scd2_processor(tmp_path):
    dataset = DatasetConfig.from_dict(
        {
            "system": "hr",
            "entity": "employees",
            "bronze": {
                "enabled": True,
                "source_type": "file",
                "path_pattern": "./docs/examples/data/employees.csv",
            },
            "silver": {
                "enabled": True,
                "entity_kind": "state",
                "history_mode": "scd2",
                "natural_keys": ["employee_id"],
                "change_ts_column": "changed_at",
                "attributes": ["status"],
            },
        }
    )
    bronze_path = _write_bronze(
        tmp_path,
        [
            {"employee_id": "E1", "changed_at": "2024-01-01T00:00:00Z", "status": "active"},
            {"employee_id": "E1", "changed_at": "2024-02-01T00:00:00Z", "status": "inactive"},
        ],
    )
    silver_partition = tmp_path / "silver" / "state"
    processor = SilverProcessor(dataset, bronze_path, silver_partition, date(2024, 2, 2))
    result = processor.run()
    history_df = pd.concat(
        pd.read_parquet(path) for path in result.outputs["state_history"]
    )
    assert set(history_df.columns) >= {
        "employee_id",
        "status",
        "effective_from",
        "effective_to",
        "is_current",
    }
    assert history_df.loc[history_df["is_current"] == 1, "status"].tolist() == ["inactive"]


def test_state_latest_only_processor(tmp_path):
    dataset = DatasetConfig.from_dict(
        {
            "system": "finance",
            "entity": "rates",
            "bronze": {
                "enabled": True,
                "source_type": "file",
                "path_pattern": "./docs/examples/data/rates.csv",
            },
            "silver": {
                "enabled": True,
                "entity_kind": "state",
                "history_mode": "latest_only",
                "natural_keys": ["rate_code"],
                "change_ts_column": "as_of_ts",
                "attributes": ["value"],
            },
        }
    )
    bronze_path = _write_bronze(
        tmp_path,
        [
            {"rate_code": "R1", "as_of_ts": "2024-01-01T00:00:00Z", "value": 1.2},
            {"rate_code": "R1", "as_of_ts": "2024-01-05T00:00:00Z", "value": 1.3},
        ],
    )
    silver_partition = tmp_path / "silver" / "latest"
    processor = SilverProcessor(dataset, bronze_path, silver_partition, date(2024, 1, 5))
    result = processor.run()
    df = pd.concat(
        pd.read_parquet(path) for path in result.outputs["state_current"]
    )
    assert df.shape[0] == 1
    assert df["value"].iloc[0] == 1.3


def test_derived_state_processor(tmp_path):
    dataset = DatasetConfig.from_dict(
        {
            "system": "ops",
            "entity": "tickets",
            "bronze": {
                "enabled": True,
                "source_type": "file",
                "path_pattern": "./docs/examples/data/tickets.csv",
            },
            "silver": {
                "enabled": True,
                "entity_kind": "derived_state",
                "history_mode": "scd2",
                "natural_keys": ["ticket_id"],
                "change_ts_column": "event_ts",
                "event_ts_column": "event_ts",
                "attributes": ["status"],
            },
        }
    )
    bronze_path = _write_bronze(
        tmp_path,
        [
            {"ticket_id": "T1", "event_ts": "2024-03-01T00:00:00Z", "status": "new"},
            {"ticket_id": "T1", "event_ts": "2024-03-02T00:00:00Z", "status": "assigned"},
        ],
    )
    silver_partition = tmp_path / "silver" / "derived_state"
    processor = SilverProcessor(dataset, bronze_path, silver_partition, date(2024, 3, 2))
    result = processor.run()
    assert "state_history" in result.outputs
    history_df = pd.concat(
        pd.read_parquet(path) for path in result.outputs["state_history"]
    )
    assert history_df["is_current"].sum() == 1


def test_derived_event_processor(tmp_path):
    dataset = DatasetConfig.from_dict(
        {
            "system": "erp",
            "entity": "project_changes",
            "bronze": {
                "enabled": True,
                "source_type": "file",
                "path_pattern": "./docs/examples/data/projects.csv",
            },
            "silver": {
                "enabled": True,
                "entity_kind": "derived_event",
                "input_mode": "append_log",
                "natural_keys": ["project_id"],
                "event_ts_column": "change_ts",
                "change_ts_column": "change_ts",
                "attributes": ["status"],
                "delete_mode": "tombstone_event",
            },
        }
    )
    bronze_path = _write_bronze(
        tmp_path,
        [
            {"project_id": "P1", "change_ts": "2024-04-01T00:00:00Z", "status": "planned"},
            {
                "project_id": "P1",
                "change_ts": "2024-04-02T00:00:00Z",
                "status": "active",
            },
        ],
    )
    silver_partition = tmp_path / "silver" / "derived_events"
    processor = SilverProcessor(dataset, bronze_path, silver_partition, date(2024, 4, 2))
    result = processor.run()
    events_df = pd.concat(
        pd.read_parquet(path) for path in result.outputs["derived_events"]
    )
    assert events_df.shape[0] == 2
    assert set(events_df["change_type"]) == {"upsert", "update"}
