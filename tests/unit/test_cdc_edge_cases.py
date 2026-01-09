"""Edge case tests for CDC (Change Data Capture) processing.

Tests for scenarios that may cause bugs in CDC handling:
- Category 5a: Operation codes lowercase ('i','u','d')
- Category 5b: Missing operation column
- Category 5c: delete_mode without CDC load_pattern
- Category 5d: Empty/invalid CDC options
"""

from __future__ import annotations

import pandas as pd
import pytest
import ibis

from pipelines.lib.curate import apply_cdc
from pipelines.lib.config_loader import load_silver_from_yaml, YAMLConfigError
from pipelines.lib.silver import DeleteMode


# =============================================================================
# Category 5a: Operation Code Case Sensitivity
# =============================================================================


class TestCDCOperationCodes:
    """Tests for CDC operation code handling."""

    def _create_cdc_table(self, ops: list[str]):
        """Create a test CDC table with given operation codes."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1, 1, 2, 3],
            "name": ["Alice", "Alice Updated", "Bob", "Charlie"],
            "op": ops,
            "updated_at": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"],
        })
        return con.create_table("test", df)

    def test_uppercase_operation_codes(self):
        """Standard uppercase I/U/D codes work correctly."""
        t = self._create_cdc_table(["I", "U", "I", "D"])
        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        # id=1 has latest op=U (kept), id=2 has I (kept), id=3 has D (filtered)
        assert result.count().execute() == 2

    def test_lowercase_operation_codes_custom(self):
        """Lowercase operation codes work when configured."""
        t = self._create_cdc_table(["i", "u", "i", "d"])
        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={
                "operation_column": "op",
                "insert_code": "i",
                "update_code": "u",
                "delete_code": "d",
            },
        )
        assert result.count().execute() == 2

    def test_mixed_case_does_not_match(self):
        """Mixed case codes don't match uppercase defaults."""
        t = self._create_cdc_table(["i", "u", "i", "d"])  # lowercase
        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},  # Defaults to uppercase I/U/D
        )
        # None match the uppercase defaults, so all filtered out
        assert result.count().execute() == 0

    def test_numeric_operation_codes(self):
        """Numeric operation codes work when configured."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "op": ["1", "2", "3"],  # 1=insert, 2=update, 3=delete
            "updated_at": ["2025-01-01", "2025-01-02", "2025-01-03"],
        })
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={
                "operation_column": "op",
                "insert_code": "1",
                "update_code": "2",
                "delete_code": "3",
            },
        )
        # id=1 (insert), id=2 (update) kept; id=3 (delete) filtered
        assert result.count().execute() == 2


# =============================================================================
# Category 5b: Missing Operation Column
# =============================================================================


class TestMissingOperationColumn:
    """Tests for missing or invalid operation column."""

    def test_missing_operation_column_raises(self):
        """apply_cdc raises clear error when operation_column missing from data."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "updated_at": ["2025-01-01", "2025-01-02"],
            # No 'op' column!
        })
        t = con.create_table("test", df)

        with pytest.raises(ValueError) as exc_info:
            apply_cdc(
                t,
                keys=["id"],
                order_by="updated_at",
                delete_mode="ignore",
                cdc_options={"operation_column": "op"},
            )

        assert "op" in str(exc_info.value)
        assert "not found" in str(exc_info.value).lower()

    def test_missing_cdc_options_raises(self):
        """apply_cdc raises when cdc_options is None."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "op": ["I", "U"],
            "updated_at": ["2025-01-01", "2025-01-02"],
        })
        t = con.create_table("test", df)

        with pytest.raises(ValueError) as exc_info:
            apply_cdc(
                t,
                keys=["id"],
                order_by="updated_at",
                delete_mode="ignore",
                cdc_options=None,
            )

        assert "operation_column" in str(exc_info.value)

    def test_empty_cdc_options_raises(self):
        """apply_cdc raises when cdc_options is empty dict."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "op": ["I", "U"],
            "updated_at": ["2025-01-01", "2025-01-02"],
        })
        t = con.create_table("test", df)

        with pytest.raises(ValueError) as exc_info:
            apply_cdc(
                t,
                keys=["id"],
                order_by="updated_at",
                delete_mode="ignore",
                cdc_options={},
            )

        assert "operation_column" in str(exc_info.value)


# =============================================================================
# Category 5c: Delete Mode Without CDC
# =============================================================================


class TestDeleteModeWithoutCDC:
    """Tests for delete_mode configuration without proper CDC setup."""

    def test_tombstone_without_cdc_warns(self, tmp_path):
        """delete_mode=tombstone without CDC load_pattern should warn or error."""
        config = {
            "domain": "test",
            "subject": "data",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "delete_mode": "tombstone",
            # No cdc_operation_column - this is a misconfiguration
        }

        # This should raise YAMLConfigError about CDC configuration
        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config, tmp_path)

        assert "cdc" in str(exc_info.value).lower()

    def test_hard_delete_without_cdc_warns(self, tmp_path):
        """delete_mode=hard_delete without CDC load_pattern should warn or error."""
        config = {
            "domain": "test",
            "subject": "data",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "delete_mode": "hard_delete",
            # No cdc_operation_column
        }

        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config, tmp_path)

        assert "cdc" in str(exc_info.value).lower()

    def test_ignore_delete_mode_works_without_cdc(self, tmp_path):
        """delete_mode=ignore (default) works without CDC configuration."""
        config = {
            "domain": "test",
            "subject": "data",
            "source_path": str(tmp_path / "bronze" / "*.parquet"),
            "target_path": str(tmp_path / "silver") + "/",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            # delete_mode defaults to "ignore" which is fine without CDC
        }

        # Create a test parquet file so the config is valid
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        df = pd.DataFrame({"id": [1], "updated_at": ["2025-01-01"]})
        df.to_parquet(bronze_dir / "data.parquet")

        silver = load_silver_from_yaml(config, tmp_path)
        assert silver is not None
        assert silver.delete_mode == DeleteMode.IGNORE


# =============================================================================
# Category 5d: Invalid Delete Mode Values
# =============================================================================


class TestInvalidDeleteMode:
    """Tests for invalid delete_mode values."""

    def test_invalid_delete_mode_in_apply_cdc(self):
        """apply_cdc raises for invalid delete_mode."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "op": ["I", "U"],
            "updated_at": ["2025-01-01", "2025-01-02"],
        })
        t = con.create_table("test", df)

        with pytest.raises(ValueError) as exc_info:
            apply_cdc(
                t,
                keys=["id"],
                order_by="updated_at",
                delete_mode="invalid_mode",
                cdc_options={"operation_column": "op"},
            )

        assert "invalid_mode" in str(exc_info.value).lower() or "unknown" in str(exc_info.value).lower()

    def test_invalid_delete_mode_in_yaml(self, tmp_path):
        """YAML loader rejects invalid delete_mode."""
        config = {
            "domain": "test",
            "subject": "data",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver/",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "delete_mode": "soft_delete",  # Invalid - should be "tombstone"
            "cdc_operation_column": "op",
        }

        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config, tmp_path)

        assert "delete_mode" in str(exc_info.value).lower()


# =============================================================================
# Category 5e: CDC Delete Mode Behaviors
# =============================================================================


class TestCDCDeleteModes:
    """Tests for different CDC delete mode behaviors."""

    def _create_cdc_table_with_deletes(self):
        """Create test table with I/U/D records."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1, 1, 2, 3, 3],
            "name": ["Alice", "Alice Updated", "Bob", "Charlie", "Charlie Deleted"],
            "op": ["I", "U", "I", "I", "D"],
            "updated_at": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
        })
        return con.create_table("test", df)

    def test_delete_mode_ignore(self):
        """delete_mode=ignore filters out delete records."""
        t = self._create_cdc_table_with_deletes()
        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        # id=1 (latest=U), id=2 (I), id=3 filtered (latest=D)
        assert result.count().execute() == 2

    def test_delete_mode_tombstone(self):
        """delete_mode=tombstone adds _deleted flag."""
        t = self._create_cdc_table_with_deletes()
        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        # All 3 unique ids kept, with _deleted flag
        assert result.count().execute() == 3
        assert "_deleted" in result.columns

        # Check that id=3 has _deleted=True
        df = result.execute()
        charlie_row = df[df["id"] == 3]
        assert bool(charlie_row["_deleted"].iloc[0]) is True

        # Check that id=1 has _deleted=False
        alice_row = df[df["id"] == 1]
        assert bool(alice_row["_deleted"].iloc[0]) is False

    def test_delete_mode_hard_delete(self):
        """delete_mode=hard_delete filters out delete records."""
        t = self._create_cdc_table_with_deletes()
        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="hard_delete",
            cdc_options={"operation_column": "op"},
        )
        # Same as ignore - deletes filtered out
        assert result.count().execute() == 2


# =============================================================================
# Category 5f: CDC with Empty/Zero Data
# =============================================================================


class TestCDCEmptyData:
    """Tests for CDC processing with edge case data."""

    def test_all_deletes_results_in_empty(self):
        """When all records are deletes with ignore mode, result is empty."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "op": ["D", "D", "D"],  # All deletes
            "updated_at": ["2025-01-01", "2025-01-02", "2025-01-03"],
        })
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        assert result.count().execute() == 0

    def test_all_deletes_with_tombstone_preserves_all(self):
        """When all records are deletes with tombstone mode, all marked deleted."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "op": ["D", "D", "D"],
            "updated_at": ["2025-01-01", "2025-01-02", "2025-01-03"],
        })
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        assert result.count().execute() == 3
        df_result = result.execute()
        assert all(df_result["_deleted"])  # All marked as deleted

    def test_single_row_insert(self):
        """CDC handles single insert record."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            "id": [1],
            "name": ["Alice"],
            "op": ["I"],
            "updated_at": ["2025-01-01"],
        })
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        assert result.count().execute() == 1
