"""Tests ensuring Silver models work across bronze load patterns."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pandas as pd
import pytest

from core.patterns import LoadPattern
from core.silver.models import SilverModel
from silver_extract import SilverModelPlanner
from core.silver.writer import DefaultSilverArtifactWriter


PRIMARY_KEYS = ["order_id"]
ORDER_COLUMN = "updated_at"


def _build_sample_df() -> pd.DataFrame:
    rows = [
        {"order_id": "ORD-001", "status": "new", "updated_at": "2025-01-01T08:00:00"},
        {
            "order_id": "ORD-001",
            "status": "shipped",
            "updated_at": "2025-01-02T09:00:00",
        },
        {"order_id": "ORD-002", "status": "new", "updated_at": "2025-01-01T12:00:00"},
    ]
    frame = pd.DataFrame(rows)
    frame["updated_at"] = pd.to_datetime(frame["updated_at"])
    return frame


class TrackingWriter:
    """Stub writer that records dataset names and dataframes."""

    def __init__(self) -> None:
        self.written: dict[str, list[pd.DataFrame]] = defaultdict(list)

    def write_dataset(self, dataset_name: str, dataset_df: pd.DataFrame) -> list:
        self.written[dataset_name].append(dataset_df.copy())
        return []


MODEL_LABELS = [
    (SilverModel.SCD_TYPE_1, {"current"}),
    (SilverModel.SCD_TYPE_2, {"history", "current"}),
    (SilverModel.INCREMENTAL_MERGE, {"cdc"}),
    (SilverModel.FULL_MERGE_DEDUPE, {"full_snapshot"}),
    (SilverModel.PERIODIC_SNAPSHOT, {"full_snapshot"}),
]


@pytest.mark.parametrize("silver_model,expected_labels", MODEL_LABELS)
@pytest.mark.parametrize("bronze_pattern", list(LoadPattern))
def test_silver_model_planner_handles_all_combinations(
    bronze_pattern: LoadPattern,
    silver_model: SilverModel,
    expected_labels: set[str],
) -> None:
    artifact_names = {
        "full_snapshot": f"full_{bronze_pattern.value}",
        "cdc": f"cdc_{bronze_pattern.value}",
        "history": f"history_{bronze_pattern.value}",
        "current": f"current_{bronze_pattern.value}",
    }
    writer = TrackingWriter()
    planner = SilverModelPlanner(
        writer, PRIMARY_KEYS, ORDER_COLUMN, artifact_names, silver_model
    )

    planner.render(_build_sample_df())
    actual_labels = set(writer.written.keys())
    expected_full_names = {artifact_names[label] for label in expected_labels}
    assert actual_labels == expected_full_names

    if silver_model in {
        SilverModel.SCD_TYPE_1,
        SilverModel.SCD_TYPE_2,
        SilverModel.FULL_MERGE_DEDUPE,
    }:
        target_label = (
            "current"
            if silver_model != SilverModel.FULL_MERGE_DEDUPE
            else "full_snapshot"
        )
        target_name = artifact_names[target_label]
        assert target_name in writer.written
        dedup_df = writer.written[target_name][0]
        assert len(dedup_df["order_id"].unique()) == len(dedup_df)

    if silver_model == SilverModel.SCD_TYPE_2:
        history_name = artifact_names["history"]
        history_df = writer.written[history_name][0]
        assert "is_current" in history_df.columns
        assert history_df["is_current"].sum() == len(history_df["order_id"].unique())


@pytest.mark.parametrize("silver_model,expected_labels", MODEL_LABELS)
@pytest.mark.parametrize("bronze_pattern", list(LoadPattern))
def test_silver_output_files_saved_to_sample_structure(
    tmp_path: Path,
    bronze_pattern: LoadPattern,
    silver_model: SilverModel,
    expected_labels: set[str],
) -> None:
    sample_df = _build_sample_df()
    base_dir = (
        tmp_path
        / "silver_samples"
        / bronze_pattern.value
        / "domain=test"
        / "entity=orders"
        / "v1"
        / "load_date=2025-11-14"
    )

    artifact_names = {
        "full_snapshot": f"full_snapshot_{bronze_pattern.value}",
        "cdc": f"cdc_{bronze_pattern.value}",
        "history": f"history_{bronze_pattern.value}",
        "current": f"current_{bronze_pattern.value}",
    }

    primary_keys = PRIMARY_KEYS if silver_model.requires_dedupe else []
    order_column = ORDER_COLUMN if silver_model.requires_dedupe else None

    writer = DefaultSilverArtifactWriter()
    outputs = writer.write(
        df=sample_df,
        primary_keys=primary_keys,
        order_column=order_column,
        write_parquet=False,
        write_csv=True,
        parquet_compression="snappy",
        artifact_names=artifact_names,
        partition_columns=[],
        error_cfg={"enabled": False, "max_bad_records": 0, "max_bad_percent": 0.0},
        silver_model=silver_model,
        output_dir=base_dir,
    )

    assert base_dir.exists(), "Sample structure should be created"
    expected_file_names = {artifact_names[label] for label in expected_labels}
    assert set(outputs.keys()) == expected_file_names

    for dataset_name, paths in outputs.items():
        assert paths, f"{dataset_name} should have at least one output file"
        for path in paths:
            assert path.exists()
            assert path.suffix == ".csv"
            assert path.parent == base_dir
            contents = path.read_text(encoding="utf-8")
            assert "order_id" in contents


def test_silver_model_defaults_match_load_pattern() -> None:
    assert (
        SilverModel.default_for_load_pattern(LoadPattern.FULL)
        == SilverModel.PERIODIC_SNAPSHOT
    )
    assert (
        SilverModel.default_for_load_pattern(LoadPattern.CDC)
        == SilverModel.INCREMENTAL_MERGE
    )
    assert (
        SilverModel.default_for_load_pattern(LoadPattern.CURRENT_HISTORY)
        == SilverModel.SCD_TYPE_2
    )
