"""Tests that cover Bronze IO metadata/checksum helpers."""

import json
from unittest.mock import patch

from core.services.pipelines.bronze.io import write_batch_metadata, write_checksum_manifest

MOCK_TIMESTAMP = "2025-01-01T00:00:00Z"


def test_write_batch_metadata_records_timestamp_and_fields(tmp_path) -> None:
    with patch("core.services.pipelines.bronze.io._utc_isoformat", return_value=MOCK_TIMESTAMP):
        metadata_path = write_batch_metadata(
            out_dir=tmp_path,
            record_count=11,
            chunk_count=2,
            cursor="cursor-value",
            performance_metrics={"throughput": 100},
            quality_metrics={"score": "good"},
            extra_metadata={"source": "unit-test"},
        )

    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert payload["timestamp"] == MOCK_TIMESTAMP
    assert payload["record_count"] == 11
    assert payload["chunk_count"] == 2
    assert payload["cursor"] == "cursor-value"
    assert payload["performance"]["throughput"] == 100
    assert payload["quality"]["score"] == "good"
    assert payload["source"] == "unit-test"


def test_write_checksum_manifest_includes_file_entries(tmp_path) -> None:
    file_path = tmp_path / "sample.txt"
    file_path.write_text("payload")

    # Patch at io.storage level since write_checksum_manifest delegates there
    with patch(
        "core.io.storage.checksum.utc_isoformat",
        return_value=MOCK_TIMESTAMP,
    ):
        manifest_path = write_checksum_manifest(
            out_dir=tmp_path,
            files=[file_path],
            load_pattern="snapshot",
            extra_metadata={"tag": "unit"},
        )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["timestamp"] == MOCK_TIMESTAMP
    assert manifest["load_pattern"] == "snapshot"
    assert manifest["tag"] == "unit"
    assert manifest["files"][0]["path"] == "sample.txt"
    assert manifest["files"][0]["size_bytes"] == len(file_path.read_bytes())
    assert manifest["files"][0]["sha256"]
