from datetime import date
import os
import time
from pathlib import Path

from core.config import DatasetConfig
from core.infrastructure.io.storage.checksum import write_checksum_manifest
from core.services.pipelines.silver.processor import SilverProcessor


def _build_event_dataset(require_checksum: bool = False) -> DatasetConfig:
    dataset = {
        "system": "payments",
        "entity": "transactions",
        "bronze": {
            "source_type": "file",
            "path_pattern": "./data",
        },
        "silver": {
            "entity_kind": "event",
            "natural_keys": ["tx_id"],
            "event_ts_column": "processed_at",
            "attributes": ["tx_id", "processed_at", "amount"],
            "partition_by": [],
            "require_checksum": require_checksum,
        },
    }
    return DatasetConfig.from_dict(dataset)


def test_verify_bronze_checksums_returns_result(tmp_path: Path) -> None:
    dataset = _build_event_dataset(require_checksum=True)
    processor = SilverProcessor(
        dataset,
        bronze_path=tmp_path,
        silver_partition=tmp_path / "silver",
        run_date=date.today(),
    )

    good_file = tmp_path / "chunk_0001.parquet"
    good_file.write_bytes(b"good data")
    write_checksum_manifest(tmp_path, [good_file], "snapshot")

    result, quarantine_result = processor._verify_bronze_checksums()

    assert result.valid
    assert "chunk_0001.parquet" in result.verified_files
    assert len(result.verified_files) == 1
    assert quarantine_result is None


def test_should_verify_checksum_skips_fresh_manifest(tmp_path: Path) -> None:
    dataset = _build_event_dataset(require_checksum=True)
    processor = SilverProcessor(
        dataset,
        bronze_path=tmp_path,
        silver_partition=tmp_path / "silver",
        run_date=date.today(),
        skip_verification_if_fresh=True,
        freshness_threshold_seconds=60,
    )

    manifest_path = tmp_path / "_checksums.json"
    manifest_path.write_text("{}")

    assert not processor._should_verify_checksum()


def test_should_verify_checksum_when_manifest_old(tmp_path: Path) -> None:
    dataset = _build_event_dataset(require_checksum=True)
    processor = SilverProcessor(
        dataset,
        bronze_path=tmp_path,
        silver_partition=tmp_path / "silver",
        run_date=date.today(),
        skip_verification_if_fresh=True,
        freshness_threshold_seconds=1,
    )

    manifest_path = tmp_path / "_checksums.json"
    manifest_path.write_text("{}")
    old_time = time.time() - 10
    os.utime(manifest_path, (old_time, old_time))

    assert processor._should_verify_checksum()
