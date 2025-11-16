from __future__ import annotations

from types import SimpleNamespace
from typing import List


from core.parallel import _safe_run_extract, run_parallel_extracts


class DummyContext(SimpleNamespace):
    pass


def test_safe_run_extract_success(monkeypatch):
    monkeypatch.setattr("core.parallel.run_extract", lambda context: 0)
    context = DummyContext(config_name="success")
    status, error = _safe_run_extract(context)
    assert status == 0
    assert error is None


def test_safe_run_extract_failure(monkeypatch):
    def raise_error(context):
        raise RuntimeError("boom")

    monkeypatch.setattr("core.parallel.run_extract", raise_error)
    context = DummyContext(config_name="fail")
    status, error = _safe_run_extract(context)
    assert status == -1
    assert isinstance(error, RuntimeError)


def test_run_parallel_extracts_reports_statuses(monkeypatch):
    contexts: List[DummyContext] = [
        DummyContext(config_name="success"),
        DummyContext(config_name="partial"),
    ]

    def fake_safe(context):
        if context.config_name == "success":
            return 0, None
        return -1, RuntimeError("failed")

    monkeypatch.setattr("core.parallel._safe_run_extract", fake_safe)
    results = run_parallel_extracts(contexts, max_workers=2)

    assert len(results) == len(contexts)
    assert any(status == -1 for _, status, _ in results)
    assert any(status == 0 for _, status, _ in results)
