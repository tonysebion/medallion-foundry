from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict

import pytest

from core.context import RunContext, build_run_context
from core.runner.job import ExtractJob, run_extract


def _build_context(
    tmp_path: Path,
    *,
    storage_enabled: bool = False,
    cleanup_on_failure: bool = True,
) -> "RunContext":
    cfg: Dict[str, Any] = {
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
            "system": "testing",
            "table": "items",
            "run": {
                "write_csv": True,
                "write_parquet": False,
                "storage_enabled": storage_enabled,
                "cleanup_on_failure": cleanup_on_failure,
                "parallel_workers": 1,
            },
        },
    }

    return build_run_context(
        cfg,
        date(2025, 11, 15),
        local_output_override=tmp_path,
    )


class DummyExtractor:
    def fetch_records(self, cfg: Dict[str, Any], run_date: date):
        return [{"id": 1}, {"id": 2}], "2"


class RecordingBackend:
    def __init__(self) -> None:
        self.uploads: list[str] = []

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        self.uploads.append(remote_path)
        return True

    def get_backend_type(self) -> str:
        return "record"


def test_run_extract_emits_metadata_and_uploads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _build_context(tmp_path, storage_enabled=True)
    backend = RecordingBackend()

    monkeypatch.setattr("core.runner.job.build_extractor", lambda cfg: DummyExtractor())
    monkeypatch.setattr("core.runner.job.get_storage_backend", lambda cfg: backend)

    result = run_extract(context)
    assert result == 0

    bronze_dir = context.bronze_path
    assert any(bronze_dir.glob("*.csv"))

    metadata = json.loads((bronze_dir / "_metadata.json").read_text())
    assert metadata["record_count"] == 2
    assert metadata["chunk_count"] == 1

    manifest = json.loads((bronze_dir / "_checksums.json").read_text())
    assert manifest["load_pattern"] == context.load_pattern.value
    assert any(remote.startswith(context.relative_path) for remote in backend.uploads)


def test_cleanup_on_failure_removes_files(tmp_path: Path) -> None:
    context = _build_context(tmp_path, cleanup_on_failure=True)
    job = ExtractJob(context)

    file_a = tmp_path / "temp-a.csv"
    file_b = tmp_path / "temp-b.csv"
    file_a.write_text("a")
    file_b.write_text("b")
    job.created_files = [file_a, file_b]

    job._cleanup_on_failure()
    assert not file_a.exists()
    assert not file_b.exists()


def test_cleanup_on_failure_skips_when_disabled(tmp_path: Path) -> None:
    context = _build_context(tmp_path, cleanup_on_failure=False)
    job = ExtractJob(context)

    file_a = tmp_path / "temp-a.csv"
    file_a.write_text("a")
    job.created_files = [file_a]

    job._cleanup_on_failure()
    assert file_a.exists()
