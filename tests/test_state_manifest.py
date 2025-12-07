"""Unit tests for manifest state helpers."""

from unittest.mock import patch

import pytest

from core.foundation.state.manifest import FileEntry, FileManifest


MOCK_TIMESTAMP = "2025-01-01T00:00:00Z"


def _build_manifest_with_entries() -> FileManifest:
    manifest = FileManifest(manifest_path="state.json")
    manifest.processed_files.append(FileEntry(path="processed.csv"))
    manifest.pending_files.append(FileEntry(path="pending.csv"))
    return manifest


def test_add_discovered_file_records_discovery_time() -> None:
    manifest = FileManifest(manifest_path="state.json")

    with patch("core.foundation.state.manifest._utc_isoformat", return_value=MOCK_TIMESTAMP):
        added = manifest.add_discovered_file("file.csv", size_bytes=123)

    assert added is True
    assert manifest.pending_files[0].discovered_at == MOCK_TIMESTAMP
    # Re-adding the same path should be idempotent.
    assert manifest.add_discovered_file("file.csv", size_bytes=456) is False


def test_mark_processed_moves_entry_to_processed() -> None:
    manifest = FileManifest(manifest_path="state.json")
    manifest.pending_files.append(FileEntry(path="batch.csv"))

    with patch("core.foundation.state.manifest._utc_isoformat", return_value=MOCK_TIMESTAMP):
        manifest.mark_processed("batch.csv", run_id="run-uuid", checksum="sha256:123")

    assert manifest.pending_files == []
    assert manifest.processed_files[0].processed_at == MOCK_TIMESTAMP
    assert manifest.processed_files[0].run_id == "run-uuid"
    assert manifest.processed_files[0].checksum == "sha256:123"


def test_to_dict_includes_last_updated_and_entries() -> None:
    manifest = _build_manifest_with_entries()

    with patch("core.foundation.state.manifest._utc_isoformat", return_value=MOCK_TIMESTAMP):
        payload = manifest.to_dict()

    assert payload["last_updated"] == MOCK_TIMESTAMP
    assert payload["manifest_path"] == "state.json"
    assert len(payload["processed_files"]) == 1
    assert len(payload["pending_files"]) == 1


def test_from_dict_rebuilds_file_manifest() -> None:
    data = {
        "manifest_path": "state.json",
        "last_updated": "ignored",
        "processed_files": [
            {
                "path": "processed.csv",
                "size_bytes": 10,
                "checksum": "sha256:abc",
                "processed_at": MOCK_TIMESTAMP,
                "run_id": "run-1",
            }
        ],
        "pending_files": [
            {
                "path": "pending.csv",
                "size_bytes": 20,
                "discovered_at": MOCK_TIMESTAMP,
            }
        ],
    }

    manifest = FileManifest.from_dict(data)
    assert manifest.manifest_path == "state.json"
    assert manifest.processed_files[0].path == "processed.csv"
    assert manifest.pending_files[0].path == "pending.csv"
