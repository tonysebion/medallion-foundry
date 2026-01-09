"""Tests for destination-based watermark functions in pipelines/lib/state.py."""

import json
import pytest

from pipelines.lib.state import (
    WatermarkSource,
    get_watermark,
    get_watermark_from_destination,
    get_watermark_with_source,
    save_watermark,
    _find_latest_partition,
)
from pipelines.lib.storage import LocalStorage


class TestWatermarkSourceEnum:
    """Tests for WatermarkSource enum."""

    def test_enum_values(self):
        """WatermarkSource enum has expected values."""
        assert WatermarkSource.DESTINATION.value == "destination"
        assert WatermarkSource.LOCAL.value == "local"
        assert WatermarkSource.AUTO.value == "auto"

    def test_all_values(self):
        """All enum values are strings."""
        for member in WatermarkSource:
            assert isinstance(member.value, str)


class TestFindLatestPartition:
    """Tests for _find_latest_partition function."""

    def test_finds_latest_partition(self, tmp_path):
        """Returns the latest partition by lexicographic sort."""
        # Create partitions
        (tmp_path / "dt=2025-01-10").mkdir()
        (tmp_path / "dt=2025-01-15").mkdir()
        (tmp_path / "dt=2025-01-12").mkdir()

        result = _find_latest_partition(str(tmp_path))

        assert result is not None
        assert "dt=2025-01-15" in result

    def test_returns_none_when_no_partitions(self, tmp_path):
        """Returns None when no partitions exist."""
        result = _find_latest_partition(str(tmp_path))

        assert result is None

    def test_ignores_non_partition_directories(self, tmp_path):
        """Ignores directories that don't match partition prefix."""
        (tmp_path / "dt=2025-01-10").mkdir()
        (tmp_path / "other_dir").mkdir()
        (tmp_path / "data.parquet").write_bytes(b"data")

        result = _find_latest_partition(str(tmp_path))

        assert result is not None
        assert "dt=2025-01-10" in result

    def test_custom_partition_prefix(self, tmp_path):
        """Supports custom partition prefix."""
        (tmp_path / "date=2025-01-10").mkdir()
        (tmp_path / "date=2025-01-15").mkdir()

        result = _find_latest_partition(str(tmp_path), partition_prefix="date=")

        assert result is not None
        assert "date=2025-01-15" in result


class TestGetWatermarkFromDestination:
    """Tests for get_watermark_from_destination function."""

    def test_reads_watermark_from_metadata(self, tmp_path):
        """Reads watermark from _metadata.json in latest partition."""
        # Create partition with metadata
        partition = tmp_path / "dt=2025-01-15"
        partition.mkdir()
        metadata = {
            "row_count": 100,
            "extra": {
                "system": "test",
                "entity": "orders",
                "last_watermark": "2025-01-15T10:30:00",
            },
        }
        (partition / "_metadata.json").write_text(json.dumps(metadata))

        result = get_watermark_from_destination(str(tmp_path / "dt={run_date}"))

        assert result == "2025-01-15T10:30:00"

    def test_reads_watermark_from_top_level_metadata(self, tmp_path):
        """Reads watermark from top-level last_watermark field."""
        partition = tmp_path / "dt=2025-01-15"
        partition.mkdir()
        metadata = {
            "row_count": 100,
            "last_watermark": "2025-01-15T10:30:00",
        }
        (partition / "_metadata.json").write_text(json.dumps(metadata))

        result = get_watermark_from_destination(str(tmp_path / "dt={run_date}"))

        assert result == "2025-01-15T10:30:00"

    def test_returns_none_when_no_partitions(self, tmp_path):
        """Returns None when no partitions exist."""
        result = get_watermark_from_destination(str(tmp_path / "dt={run_date}"))

        assert result is None

    def test_returns_none_when_no_watermark_in_metadata(self, tmp_path):
        """Returns None when metadata has no watermark."""
        partition = tmp_path / "dt=2025-01-15"
        partition.mkdir()
        metadata = {"row_count": 100, "extra": {"system": "test"}}
        (partition / "_metadata.json").write_text(json.dumps(metadata))

        result = get_watermark_from_destination(str(tmp_path / "dt={run_date}"))

        assert result is None

    def test_handles_missing_metadata_file(self, tmp_path):
        """Returns None when _metadata.json is missing."""
        partition = tmp_path / "dt=2025-01-15"
        partition.mkdir()
        # No metadata file created

        result = get_watermark_from_destination(str(tmp_path / "dt={run_date}"))

        assert result is None

    def test_handles_corrupted_metadata(self, tmp_path):
        """Returns None when metadata is corrupted."""
        partition = tmp_path / "dt=2025-01-15"
        partition.mkdir()
        (partition / "_metadata.json").write_text("not valid json")

        result = get_watermark_from_destination(str(tmp_path / "dt={run_date}"))

        assert result is None

    def test_strips_dt_template_from_path(self, tmp_path):
        """Correctly strips dt={run_date} template from target path."""
        partition = tmp_path / "dt=2025-01-15"
        partition.mkdir()
        metadata = {"extra": {"last_watermark": "2025-01-15T10:30:00"}}
        (partition / "_metadata.json").write_text(json.dumps(metadata))

        # Different path formats should all work
        paths = [
            str(tmp_path / "dt={run_date}"),
            str(tmp_path / "dt={run_date}/"),
            str(tmp_path) + "/dt={run_date}",
        ]

        for path in paths:
            result = get_watermark_from_destination(path)
            assert result == "2025-01-15T10:30:00", f"Failed for path: {path}"


class TestGetWatermarkWithSource:
    """Tests for get_watermark_with_source function."""

    def test_local_source_uses_local_watermark(self, tmp_path, monkeypatch):
        """LOCAL source reads from local .state/ directory."""
        # Set up local state directory
        state_dir = tmp_path / ".state"
        state_dir.mkdir()
        watermark_file = state_dir / "test_orders_watermark.json"
        watermark_file.write_text(json.dumps({
            "system": "test",
            "entity": "orders",
            "last_value": "2025-01-10T08:00:00",
        }))
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(state_dir))

        result = get_watermark_with_source(
            system="test",
            entity="orders",
            source=WatermarkSource.LOCAL,
        )

        assert result == "2025-01-10T08:00:00"

    def test_destination_source_uses_destination_watermark(self, tmp_path):
        """DESTINATION source reads from destination metadata."""
        # Create destination with metadata
        partition = tmp_path / "bronze" / "dt=2025-01-15"
        partition.mkdir(parents=True)
        metadata = {"extra": {"last_watermark": "2025-01-15T10:30:00"}}
        (partition / "_metadata.json").write_text(json.dumps(metadata))

        result = get_watermark_with_source(
            system="test",
            entity="orders",
            source=WatermarkSource.DESTINATION,
            target_path=str(tmp_path / "bronze" / "dt={run_date}"),
        )

        assert result == "2025-01-15T10:30:00"

    def test_destination_source_without_target_path_falls_back_to_local(
        self, tmp_path, monkeypatch
    ):
        """DESTINATION without target_path falls back to local."""
        state_dir = tmp_path / ".state"
        state_dir.mkdir()
        watermark_file = state_dir / "test_orders_watermark.json"
        watermark_file.write_text(json.dumps({
            "system": "test",
            "entity": "orders",
            "last_value": "2025-01-10T08:00:00",
        }))
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(state_dir))

        result = get_watermark_with_source(
            system="test",
            entity="orders",
            source=WatermarkSource.DESTINATION,
            target_path=None,  # No target path
        )

        assert result == "2025-01-10T08:00:00"

    def test_auto_source_prefers_destination(self, tmp_path, monkeypatch):
        """AUTO source tries destination first."""
        # Set up both local and destination watermarks
        state_dir = tmp_path / ".state"
        state_dir.mkdir()
        watermark_file = state_dir / "test_orders_watermark.json"
        watermark_file.write_text(json.dumps({
            "system": "test",
            "entity": "orders",
            "last_value": "2025-01-10T08:00:00",  # Older
        }))
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(state_dir))

        partition = tmp_path / "bronze" / "dt=2025-01-15"
        partition.mkdir(parents=True)
        metadata = {"extra": {"last_watermark": "2025-01-15T10:30:00"}}  # Newer
        (partition / "_metadata.json").write_text(json.dumps(metadata))

        result = get_watermark_with_source(
            system="test",
            entity="orders",
            source=WatermarkSource.AUTO,
            target_path=str(tmp_path / "bronze" / "dt={run_date}"),
        )

        # Should prefer destination (newer)
        assert result == "2025-01-15T10:30:00"

    def test_auto_source_falls_back_to_local(self, tmp_path, monkeypatch):
        """AUTO source falls back to local when destination is empty."""
        # Set up only local watermark
        state_dir = tmp_path / ".state"
        state_dir.mkdir()
        watermark_file = state_dir / "test_orders_watermark.json"
        watermark_file.write_text(json.dumps({
            "system": "test",
            "entity": "orders",
            "last_value": "2025-01-10T08:00:00",
        }))
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(state_dir))

        # Empty destination (no partitions)
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()

        result = get_watermark_with_source(
            system="test",
            entity="orders",
            source=WatermarkSource.AUTO,
            target_path=str(tmp_path / "bronze" / "dt={run_date}"),
        )

        # Should fall back to local
        assert result == "2025-01-10T08:00:00"

    def test_returns_none_when_no_watermark_anywhere(self, tmp_path, monkeypatch):
        """Returns None when no watermark exists anywhere."""
        state_dir = tmp_path / ".state"
        state_dir.mkdir()
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(state_dir))

        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()

        result = get_watermark_with_source(
            system="test",
            entity="orders",
            source=WatermarkSource.AUTO,
            target_path=str(tmp_path / "bronze" / "dt={run_date}"),
        )

        assert result is None
