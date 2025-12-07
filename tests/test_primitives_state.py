from __future__ import annotations

from datetime import date
from pathlib import Path

import hashlib
import pytest

from core.foundation.state import manifest as manifest_module
from core.foundation.state.manifest import (
    FileEntry,
    FileManifest,
    ManifestTracker,
    compute_file_checksum,
    _discover_local_files,
)
from core.foundation.state.watermark import (
    Watermark,
    WatermarkStore,
    WatermarkType,
    build_watermark_store,
    compute_max_watermark,
)

MOCK_TIMESTAMP = "2025-01-01T00:00:00Z"


def test_file_entry_to_dict_and_from_dict() -> None:
    entry = FileEntry(
        path="data/file.csv",
        size_bytes=100,
        checksum="sha256:abc",
        discovered_at=MOCK_TIMESTAMP,
        processed_at=MOCK_TIMESTAMP,
        run_id="run-1",
    )

    payload = entry.to_dict()
    assert payload["checksum"] == "sha256:abc"
    assert payload["discovered_at"] == MOCK_TIMESTAMP

    restored = FileEntry.from_dict(payload)
    assert restored.path == entry.path


def test_manifest_add_and_mark(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(manifest_module, "_utc_isoformat", lambda: MOCK_TIMESTAMP)
    manifest = FileManifest(manifest_path="state.json")
    assert manifest.add_discovered_file("file.csv", size_bytes=10)
    assert not manifest.add_discovered_file("file.csv")

    manifest.mark_processed("file.csv", run_id="run", checksum="sha256:123")
    assert manifest.processed_files
    assert manifest.processed_files[0].processed_at == MOCK_TIMESTAMP


def test_discover_local_files(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "good.csv").write_text("x")
    (data_dir / "_skip.csv").write_text("y")
    (data_dir / "folder").mkdir()

    found = _discover_local_files(str(data_dir / "*.csv"))
    assert all(Path(path).name == "good.csv" for path, _ in found)


def test_manifest_tracker_save_and_discover(tmp_path: Path) -> None:
    tracker = ManifestTracker(str(tmp_path / "state.json"))

    data_dir = tmp_path / "files"
    data_dir.mkdir()
    (data_dir / "a.csv").write_text("one")
    (data_dir / "b.csv").write_text("two")

    discovered = tracker.discover_files(str(data_dir / "*.csv"))
    assert len(discovered) == 2
    assert tracker.load().pending_files

    tracker.save()
    new_tracker = ManifestTracker(str(tmp_path / "state.json"))
    assert new_tracker.load().pending_files


def test_compute_file_checksum(tmp_path: Path) -> None:
    target = tmp_path / "file.bin"
    target.write_bytes(b"hello")
    expected = "sha256:" + hashlib.sha256(b"hello").hexdigest()
    assert compute_file_checksum(str(target)) == expected


def test_watermark_update_and_compare(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("core.foundation.state.watermark._utc_isoformat", lambda: MOCK_TIMESTAMP)
    watermark = Watermark(
        source_key="sys.tbl",
        watermark_column="ts",
        watermark_value="100",
        watermark_type=WatermarkType.INTEGER,
    )
    watermark.update("200", run_id="run", run_date=date(2025, 1, 2), record_count=5)
    assert watermark.watermark_value == "200"
    assert watermark.compare("150") == -1
    assert watermark.compare("200") == 0
    assert watermark.compare("250") == 1


def test_watermark_store_local(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("core.foundation.state.watermark._utc_isoformat", lambda: MOCK_TIMESTAMP)
    store = WatermarkStore(storage_backend="local", local_path=tmp_path / "wm")
    wm = store.get("sys", "table", "ts")
    wm.update("abc", run_id="run", run_date=date(2025, 1, 3), record_count=10)
    store.save(wm)

    reloaded = store.get("sys", "table", "ts")
    assert reloaded.watermark_value == "abc"
    assert store.list_watermarks()
    assert store.delete("sys", "table")
    assert not store.delete("sys", "table")


def test_build_watermark_store_local_path() -> None:
    cfg = {"platform": {"bronze": {"storage_backend": "local", "local_path": "custom"}}}
    store = build_watermark_store(cfg)
    assert store.local_path.name == "_watermarks"
    assert store.storage_backend == "local"


def test_compute_max_watermark() -> None:
    records = [{"ts": "2025-01-01"}, {"ts": "2025-02-01"}]
    assert compute_max_watermark(records, "ts") == "2025-02-01"

    assert compute_max_watermark([], "ts", current_watermark="base") == "base"
