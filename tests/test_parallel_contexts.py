from __future__ import annotations

from types import SimpleNamespace
from typing import List

import pytest

from core.parallel import _safe_run_extract, run_parallel_extracts


def _build_context(name: str) -> SimpleNamespace:
    return SimpleNamespace(config_name=name)


def test_run_parallel_extracts_reports_status(monkeypatch) -> None:
    contexts = [
        _build_context("success"),
        _build_context("failure"),
    ]

    def fake_safe(context):
        if context.config_name == "success":
            return 0, None
        return -1, RuntimeError("boom")

    monkeypatch.setattr("core.parallel._safe_run_extract", fake_safe)
    results = run_parallel_extracts(contexts, max_workers=2)

    assert len(results) == len(contexts)
    assert any(config_name == "success" and status == 0 for config_name, status, _ in results)
    assert any(config_name == "failure" and status == -1 for config_name, status, _ in results)


def test_safe_run_extract_records_exception(monkeypatch) -> None:
    context = _build_context("retry")
    call_count = {"count": 0}

    def fake_run(context_arg) -> int:
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise RuntimeError("temporary failure")
        return 0

    monkeypatch.setattr("core.parallel.run_extract", fake_run)
    status, error = _safe_run_extract(context)

    assert status == -1
    assert isinstance(error, RuntimeError)
