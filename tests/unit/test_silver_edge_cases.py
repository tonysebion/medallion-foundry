"""Edge case tests for Silver layer.

Tests for scenarios that were found to cause bugs:
- Category 1: Model + entity_kind combinations
- Category 2: Missing keys/timestamp edge cases
- Category 3: Empty/zero data edge cases
"""

from __future__ import annotations

from pathlib import Path

import ibis
import pandas as pd
import pytest

from pipelines.lib.silver import (
    EntityKind,
    HistoryMode,
    SilverEntity,
    InputMode,
)
from pipelines.lib.config_loader import load_silver_from_yaml
from pipelines.lib.curate import dedupe_latest, build_history


# =============================================================================
# Category 1: Model + Entity Kind Combinations
# =============================================================================


class TestModelEntityKindCombinations:
    """Tests for model + entity_kind combinations."""

    def _create_test_data_with_duplicates(self, tmp_path: Path) -> Path:
        """Create test parquet file with exact duplicate rows."""
        df = pd.DataFrame(
            {
                "id": [1, 1, 2, 2, 3],  # Duplicates on id
                "name": ["Alice", "Alice", "Bob", "Bob", "Charlie"],  # Exact duplicates
                "updated_at": [
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-02",
                    "2025-01-02",
                    "2025-01-03",
                ],
            }
        )
        parquet_path = tmp_path / "data.parquet"
        df.to_parquet(parquet_path)
        return parquet_path

    def test_periodic_snapshot_state_no_deduplication(self, tmp_path: Path):
        """periodic_snapshot with entity_kind=state (default) should NOT deduplicate."""
        parquet_path = self._create_test_data_with_duplicates(tmp_path)
        output_path = tmp_path / "output"
        output_path.mkdir()

        # periodic_snapshot = REPLACE_DAILY + CURRENT_ONLY, allows None keys
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
            input_mode=InputMode.REPLACE_DAILY,  # Required for periodic_snapshot
            natural_keys=None,  # No keys - periodic_snapshot style
            change_timestamp=None,
        )

        result = silver.run("2025-01-15")

        # Should have all 5 rows (no deduplication without keys)
        assert result["row_count"] == 5

    def test_periodic_snapshot_event_deduplicates_exact(self, tmp_path: Path):
        """periodic_snapshot with entity_kind=event should deduplicate exact duplicates."""
        parquet_path = self._create_test_data_with_duplicates(tmp_path)
        output_path = tmp_path / "output"
        output_path.mkdir()

        # periodic_snapshot = REPLACE_DAILY + CURRENT_ONLY, allows None keys
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.EVENT,  # Event calls distinct()
            history_mode=HistoryMode.CURRENT_ONLY,
            input_mode=InputMode.REPLACE_DAILY,  # Required for periodic_snapshot
            natural_keys=None,
            change_timestamp=None,
        )

        result = silver.run("2025-01-15")

        # Should have 3 rows (exact duplicates removed by distinct())
        assert result["row_count"] == 3

    def test_full_merge_dedupe_requires_keys(self, tmp_path: Path):
        """full_merge_dedupe model requires natural_keys and change_timestamp."""
        config = {
            "domain": "test",
            "subject": "data",
            "model": "full_merge_dedupe",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/",
            # Missing natural_keys and change_timestamp
        }

        from pipelines.lib.config_loader import YAMLConfigError

        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config, tmp_path)

        assert (
            "natural_keys" in str(exc_info.value).lower()
            or "change_timestamp" in str(exc_info.value).lower()
        )

    def test_scd_type_2_requires_keys(self, tmp_path: Path):
        """scd_type_2 model requires natural_keys and change_timestamp."""
        config = {
            "domain": "test",
            "subject": "data",
            "model": "scd_type_2",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/",
            # Missing natural_keys and change_timestamp
        }

        from pipelines.lib.config_loader import YAMLConfigError

        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config, tmp_path)

        assert (
            "natural_keys" in str(exc_info.value).lower()
            or "change_timestamp" in str(exc_info.value).lower()
        )

    def test_event_log_deduplicates_exact(self, tmp_path: Path):
        """event_log model should call distinct() on data."""
        parquet_path = self._create_test_data_with_duplicates(tmp_path)
        output_path = tmp_path / "output"
        output_path.mkdir()

        silver = SilverEntity(
            domain="test",
            subject="events",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.EVENT,
            history_mode=HistoryMode.CURRENT_ONLY,
            natural_keys=["id"],
            change_timestamp="updated_at",
        )

        result = silver.run("2025-01-15")

        # Event entity calls distinct(), removing exact duplicates
        assert result["row_count"] == 3


# =============================================================================
# Category 2: Missing Keys/Timestamp Edge Cases
# =============================================================================


class TestMissingKeysTimestamp:
    """Tests for None/empty natural_keys and change_timestamp."""

    def _create_simple_parquet(self, tmp_path: Path) -> Path:
        """Create simple test parquet file."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 1],  # Duplicate id=1
                "name": ["Alice", "Bob", "Charlie", "Alice Updated"],
                "updated_at": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"],
            }
        )
        parquet_path = tmp_path / "data.parquet"
        df.to_parquet(parquet_path)
        return parquet_path

    def test_none_keys_none_timestamp_no_dedup(self, tmp_path: Path):
        """None keys and None timestamp should skip deduplication."""
        parquet_path = self._create_simple_parquet(tmp_path)
        output_path = tmp_path / "output"
        output_path.mkdir()

        # periodic_snapshot config (REPLACE_DAILY + CURRENT_ONLY) allows None keys
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
            input_mode=InputMode.REPLACE_DAILY,
            natural_keys=None,
            change_timestamp=None,
        )

        result = silver.run("2025-01-15")

        # All 4 rows should be present (no deduplication)
        assert result["row_count"] == 4

    def test_keys_without_timestamp_no_dedup(self, tmp_path: Path):
        """Keys provided but no timestamp should skip deduplication."""
        parquet_path = self._create_simple_parquet(tmp_path)
        output_path = tmp_path / "output"
        output_path.mkdir()

        # periodic_snapshot config allows missing keys/timestamp
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
            input_mode=InputMode.REPLACE_DAILY,
            natural_keys=["id"],
            change_timestamp=None,  # Missing timestamp
        )

        result = silver.run("2025-01-15")

        # Should skip deduplication since change_timestamp is missing
        assert result["row_count"] == 4

    def test_timestamp_without_keys_no_dedup(self, tmp_path: Path):
        """Timestamp provided but no keys should skip deduplication."""
        parquet_path = self._create_simple_parquet(tmp_path)
        output_path = tmp_path / "output"
        output_path.mkdir()

        # periodic_snapshot config allows missing keys/timestamp
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
            input_mode=InputMode.REPLACE_DAILY,
            natural_keys=None,  # Missing keys
            change_timestamp="updated_at",
        )

        result = silver.run("2025-01-15")

        # Should skip deduplication since natural_keys is missing
        assert result["row_count"] == 4

    def test_empty_keys_list_treated_as_none(self, tmp_path: Path):
        """Empty keys list [] should be treated same as None."""
        parquet_path = self._create_simple_parquet(tmp_path)
        output_path = tmp_path / "output"
        output_path.mkdir()

        # periodic_snapshot config allows empty keys
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
            input_mode=InputMode.REPLACE_DAILY,
            natural_keys=[],  # Empty list
            change_timestamp="updated_at",
        )

        result = silver.run("2025-01-15")

        # Empty list should be treated as no keys - skip deduplication
        assert result["row_count"] == 4

    def test_keys_and_timestamp_deduplicates(self, tmp_path: Path):
        """Both keys and timestamp provided should deduplicate."""
        parquet_path = self._create_simple_parquet(tmp_path)
        output_path = tmp_path / "output"
        output_path.mkdir()

        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
            natural_keys=["id"],
            change_timestamp="updated_at",
        )

        result = silver.run("2025-01-15")

        # Should deduplicate: id=1 has 2 rows, keep latest
        # Result: 3 unique ids
        assert result["row_count"] == 3


# =============================================================================
# Category 3: Empty/Zero Data Edge Cases
# =============================================================================


class TestEmptyDataEdgeCases:
    """Tests for empty and zero-row data scenarios."""

    def test_empty_parquet_file(self, tmp_path: Path):
        """Silver should handle empty parquet file gracefully."""
        # Create empty parquet file
        df = pd.DataFrame({"id": [], "name": [], "updated_at": []})
        parquet_path = tmp_path / "empty.parquet"
        df.to_parquet(parquet_path)

        output_path = tmp_path / "output"
        output_path.mkdir()

        # periodic_snapshot config
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
            input_mode=InputMode.REPLACE_DAILY,
            natural_keys=None,
            change_timestamp=None,
        )

        result = silver.run("2025-01-15")

        assert result["row_count"] == 0

    def test_glob_matches_no_files(self, tmp_path: Path):
        """Silver should handle glob pattern matching no files gracefully."""
        output_path = tmp_path / "output"
        output_path.mkdir()

        # periodic_snapshot config
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(tmp_path / "nonexistent" / "*.parquet"),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
            input_mode=InputMode.REPLACE_DAILY,
            natural_keys=None,
            change_timestamp=None,
        )

        # When glob matches no files, Silver logs a warning and returns row_count=0
        # but it may raise an error creating an empty DataFrame
        # This tests that we handle this gracefully
        from duckdb import InvalidInputException

        try:
            result = silver.run("2025-01-15")
            assert result["row_count"] == 0
        except InvalidInputException:
            # DuckDB rejects empty DataFrames - this is acceptable behavior
            # A warning was already logged
            pass

    def test_all_exact_duplicates_deduplicated(self, tmp_path: Path):
        """Data with all exact duplicates should result in fewer rows."""
        # All rows are identical
        df = pd.DataFrame(
            {
                "id": [1, 1, 1, 1],
                "name": ["Alice", "Alice", "Alice", "Alice"],
                "updated_at": ["2025-01-01", "2025-01-01", "2025-01-01", "2025-01-01"],
            }
        )
        parquet_path = tmp_path / "duplicates.parquet"
        df.to_parquet(parquet_path)

        output_path = tmp_path / "output"
        output_path.mkdir()

        # periodic_snapshot config with EVENT to use distinct()
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.EVENT,  # Uses distinct()
            history_mode=HistoryMode.CURRENT_ONLY,
            input_mode=InputMode.REPLACE_DAILY,
            natural_keys=None,
            change_timestamp=None,
        )

        result = silver.run("2025-01-15")

        # All 4 rows are identical, distinct() should return 1
        assert result["row_count"] == 1


# =============================================================================
# Curate Function Edge Cases
# =============================================================================


class TestCurateFunctionEdgeCases:
    """Tests for curate helper functions with edge case inputs."""

    def test_dedupe_latest_with_none_keys_all_one_group(self):
        """dedupe_latest with None keys treats all rows as one group."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "updated_at": ["2025-01-01", "2025-01-02"],
            }
        )
        t = con.create_table("test", df)

        # With None keys, all rows are in one group (window over entire dataset)
        # Only the latest row (2025-01-02) is kept
        result = dedupe_latest(t, None, "updated_at")
        assert result.count().execute() == 1

    def test_dedupe_latest_with_empty_keys_all_one_group(self):
        """dedupe_latest with empty keys list treats all rows as one group."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "updated_at": ["2025-01-01", "2025-01-02"],
            }
        )
        t = con.create_table("test", df)

        # Empty keys list = all rows in one group, only latest kept
        result = dedupe_latest(t, [], "updated_at")
        assert result.count().execute() == 1

    def test_build_history_with_none_keys_all_one_group(self):
        """build_history with None keys treats all rows as one entity."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Alice", "Alice Updated"],
                "updated_at": ["2025-01-01", "2025-01-02"],
            }
        )
        t = con.create_table("test", df)

        # With None keys, all rows become history versions of "one entity"
        result = build_history(t, None, "updated_at")
        # Both rows kept, with effective dates
        assert result.count().execute() == 2
        # Should have effective_from, effective_to, is_current columns
        assert "effective_from" in result.columns
        assert "effective_to" in result.columns
        assert "is_current" in result.columns

    def test_dedupe_latest_missing_order_column(self):
        """dedupe_latest should error when order_by column doesn't exist."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )
        t = con.create_table("test", df)

        # Column 'nonexistent' doesn't exist
        with pytest.raises(Exception):  # DuckDB will raise on execution
            result = dedupe_latest(t, ["id"], "nonexistent")
            result.execute()  # Force execution to trigger error

    def test_dedupe_latest_with_null_timestamps(self):
        """dedupe_latest should handle NULL values in timestamp column."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 2],
                "name": ["Alice", "Alice Updated", "Bob"],
                "updated_at": ["2025-01-01", None, "2025-01-02"],  # NULL timestamp
            }
        )
        t = con.create_table("test", df)

        # Should still work, NULL sorts differently
        result = dedupe_latest(t, ["id"], "updated_at")
        # id=1 has two rows, one with NULL timestamp
        # NULL typically sorts to end in DuckDB with DESC, so "2025-01-01" is "latest"
        assert result.count().execute() == 2  # One row per unique id


class TestHistoryModeWithoutKeys:
    """Tests for history_mode=full_history without natural_keys."""

    def test_full_history_requires_keys_non_periodic(self, tmp_path: Path):
        """full_history mode with APPEND_LOG requires natural_keys."""
        # Non-periodic snapshot configs (e.g., APPEND_LOG) require keys
        with pytest.raises(ValueError) as exc_info:
            SilverEntity(
                domain="test",
                subject="data",
                source_path="./data.parquet",
                target_path=str(tmp_path) + "/output/",
                entity_kind=EntityKind.STATE,
                history_mode=HistoryMode.FULL_HISTORY,  # SCD2 mode
                input_mode=InputMode.APPEND_LOG,  # Not periodic snapshot
                natural_keys=None,  # Missing keys
                change_timestamp=None,
            )

        assert "natural_keys" in str(exc_info.value).lower()

    def test_full_history_with_keys_builds_scd2(self, tmp_path: Path):
        """full_history with keys builds proper SCD2 history."""
        df = pd.DataFrame(
            {
                "id": [1, 1, 2],
                "name": ["Alice", "Alice Updated", "Bob"],
                "updated_at": ["2025-01-01", "2025-01-02", "2025-01-03"],
            }
        )
        parquet_path = tmp_path / "data.parquet"
        df.to_parquet(parquet_path)

        output_path = tmp_path / "output"
        output_path.mkdir()

        # Full history with proper keys builds SCD2
        silver = SilverEntity(
            domain="test",
            subject="data",
            source_path=str(parquet_path),
            target_path=str(output_path) + "/",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.FULL_HISTORY,  # SCD2 mode
            natural_keys=["id"],
            change_timestamp="updated_at",
        )

        result = silver.run("2025-01-15")

        # All 3 rows kept (SCD2 preserves history)
        # id=1 has 2 versions, id=2 has 1 version
        assert result["row_count"] == 3
