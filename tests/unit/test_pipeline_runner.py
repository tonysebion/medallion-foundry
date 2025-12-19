"""Tests for pipelines runner utilities."""

import logging
from unittest.mock import Mock

import pytest

from pipelines.lib.runner import (
    PipelineResult,
    pipeline,
    run_bronze_only,
    run_pipeline,
    run_silver_only,
)


class DummyBronze:
    def __init__(self):
        self.system = "tests"
        self.entity = "orders"
        self.calls = 0

    def run(self, run_date, *, dry_run=False, **kwargs):
        self.calls += 1
        if dry_run:
            return {"dry_run": True, "row_count": 0}
        return {"row_count": 5, "target": f"/tmp/{run_date}"}


class DummySilver:
    def __init__(self):
        self.calls = 0

    def run(self, run_date, *, dry_run=False, **kwargs):
        self.calls += 1
        if dry_run:
            return {"dry_run": True, "row_count": 0}
        return {"row_count": 3, "target": f"/silver/{run_date}"}


def test_pipeline_decorator_adds_metadata(caplog):
    caplog.set_level(logging.INFO)

    @pipeline("tests.orders", log_level=logging.INFO)
    def run_bridge(run_date: str) -> dict:
        return {"run_date": run_date}

    result = run_bridge("2025-01-15")
    assert result["_pipeline"] == "tests.orders"
    assert result["_elapsed_seconds"] >= 0
    assert result["run_date"] == "2025-01-15"

    dry_result = run_bridge("2025-01-16", dry_run=True)
    assert dry_result["dry_run"] is True
    assert "DRY RUN" in caplog.text


def test_run_pipeline_success():
    bronze = DummyBronze()
    silver = DummySilver()
    result = run_pipeline(bronze, silver, "2025-01-15")
    assert isinstance(result, PipelineResult)
    assert result.success
    assert result.total_rows == 8
    assert result.pipeline_name == "tests.orders"
    assert bronze.calls == 1
    assert silver.calls == 1

def test_run_pipeline_failure(monkeypatch):
    bronze = DummyBronze()
    silver = DummySilver()

    def fail_run(*args, **kwargs):
        raise RuntimeError("boom")

    bronze.run = fail_run
    result = run_pipeline(bronze, silver, "2025-01-15")
    assert not result.success
    assert isinstance(result.error, RuntimeError)
    assert silver.calls == 0

def test_run_bronze_only():
    bronze = DummyBronze()
    result = run_bronze_only(bronze, "2025-01-17")
    assert result.success
    assert result.pipeline_name == "tests.orders:bronze"
    assert result.bronze["row_count"] == 5

def test_run_silver_only():
    silver = DummySilver()
    result = run_silver_only(silver, "2025-01-18")
    assert result.success
    assert result.pipeline_name == "silver"
    assert result.silver["row_count"] == 3
