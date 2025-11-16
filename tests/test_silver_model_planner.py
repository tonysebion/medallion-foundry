from __future__ import annotations


import pandas as pd
import pytest

from core.silver.artifacts import SilverModelPlanner
from core.silver.models import SilverModel


def _make_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 1, 2],
            "seq": [1, 2, 1],
            "value": ["a", "b", "c"],
        }
    )


class DummyWriter:
    def __init__(self):
        self.written: list[str] = []

    def write_dataset(self, name: str, df: pd.DataFrame):
        self.written.append(name)
        return []


def test_silver_model_planner_periodic():
    planner = SilverModelPlanner(
        writer=DummyWriter(),
        primary_keys=[],
        order_column=None,
        artifact_names={},
        silver_model=SilverModel.PERIODIC_SNAPSHOT,
    )
    artifacts = planner.prepare_artifacts(_make_df())
    assert set(artifacts.keys()) == {"full_snapshot"}


def test_silver_model_planner_scd2_history():
    planner = SilverModelPlanner(
        writer=DummyWriter(),
        primary_keys=["id"],
        order_column="seq",
        artifact_names={},
        silver_model=SilverModel.SCD_TYPE_2,
    )
    artifacts = planner.prepare_artifacts(_make_df())
    assert set(artifacts.keys()) == {"history", "current"}
    assert not artifacts["current"].empty


def test_silver_model_planner_missing_order_column_raises():
    planner = SilverModelPlanner(
        writer=DummyWriter(),
        primary_keys=["id"],
        order_column=None,
        artifact_names={},
        silver_model=SilverModel.SCD_TYPE_1,
    )
    with pytest.raises(ValueError):
        planner.prepare_artifacts(_make_df())
