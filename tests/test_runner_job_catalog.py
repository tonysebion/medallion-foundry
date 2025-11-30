from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pytest

from core.context import RunContext, build_run_context
from core.runner.job import ExtractJob


def _build_context(tmp_path: Path) -> RunContext:
    cfg = {
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_root": str(tmp_path / "remote"),
                "output_defaults": {
                    "allow_csv": True,
                    "allow_parquet": False,
                    "default_parquet_compression": "snappy",
                },
            }
        },
        "source": {
            "system": "catalog",
            "table": "items",
            "run": {
                "write_csv": True,
                "write_parquet": False,
                "storage_enabled": False,
            },
        },
    }
    return build_run_context(cfg, date(2025, 11, 15), local_output_override=tmp_path)


def test_run_extract_emits_catalog_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _build_context(tmp_path)
    job = ExtractJob(context)
    job.schema_snapshot = [{"name": "id", "dtype": "int"}]
    job.load_pattern = context.load_pattern
    job.output_formats = {"csv": True, "parquet": False}
    # create chunk file so checksum manifest includes something
    chunk_file = job._out_dir / "part-0001.csv"
    chunk_file.parent.mkdir(parents=True, exist_ok=True)
    chunk_file.write_text("id")
    job.created_files = [chunk_file]

    captured: Dict[str, List[Any]] = {"schema": [], "quality": [], "run": []}
    monkeypatch.setattr(
        "core.runner.job.report_schema_snapshot",
        lambda dataset_id, schema: captured["schema"].append(
            (dataset_id, list(schema))
        ),
    )
    monkeypatch.setattr(
        "core.runner.job.report_quality_snapshot",
        lambda dataset_id, metrics: captured["quality"].append((dataset_id, metrics)),
    )
    monkeypatch.setattr(
        "core.runner.job.report_run_metadata",
        lambda dataset_id, metadata: captured["run"].append((dataset_id, metadata)),
    )

    job._emit_metadata(record_count=1, chunk_count=1, cursor="1")
    assert captured["schema"]
    assert captured["quality"][0][1]["record_count"] == 1
    assert captured["run"][0][1]["status"] == "success"


def test_cleanup_on_failure_removes_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from core.runner.job import ExtractJob

    context = _build_context(tmp_path)
    job = ExtractJob(context)
    file_path = tmp_path / "temp.csv"
    file_path.write_text("x")
    job.created_files = [file_path]

    class DummyExtractor:
        def fetch_records(self, cfg, run_date):
            return [{"id": 1}], None

    monkeypatch.setattr("core.runner.job.build_extractor", lambda cfg: DummyExtractor())

    def fail_process_chunks(records):
        raise RuntimeError("boom")

    monkeypatch.setattr(job, "_process_chunks", fail_process_chunks)

    with pytest.raises(RuntimeError):
        job.run()

    assert not file_path.exists()
