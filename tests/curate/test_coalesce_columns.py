"""Tests for coalesce_columns function with data validation.

coalesce_columns creates a coalesced column from multiple source columns,
useful for handling schema evolution where columns have different names.
"""

from __future__ import annotations

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import coalesce_columns


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestCoalesceColumnsBasic:
    """Basic tests for coalesce_columns functionality."""

    def test_primary_column_used_when_not_null(self, con):
        """Primary column value used when not null."""
        df = pd.DataFrame([
            {"id": 1, "new_name": "Alice", "old_name": "OldAlice"},
            {"id": 2, "new_name": "Bob", "old_name": "OldBob"},
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "new_name", "old_name")
        result_df = result.execute()

        # new_name values used (not null)
        assert result_df[result_df["id"] == 1]["new_name"].iloc[0] == "Alice"
        assert result_df[result_df["id"] == 2]["new_name"].iloc[0] == "Bob"

    def test_fallback_used_when_primary_null(self, con):
        """Fallback column used when primary is null."""
        df = pd.DataFrame([
            {"id": 1, "new_name": None, "old_name": "FallbackName"},
            {"id": 2, "new_name": "DirectName", "old_name": "Ignored"},
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "new_name", "old_name")
        result_df = result.execute()

        # Row 1: fallback used
        assert result_df[result_df["id"] == 1]["new_name"].iloc[0] == "FallbackName"
        # Row 2: primary used
        assert result_df[result_df["id"] == 2]["new_name"].iloc[0] == "DirectName"

    def test_multiple_fallbacks(self, con):
        """Multiple fallback columns checked in order."""
        df = pd.DataFrame([
            {"id": 1, "col_a": None, "col_b": None, "col_c": "Third"},
            {"id": 2, "col_a": None, "col_b": "Second", "col_c": "Third"},
            {"id": 3, "col_a": "First", "col_b": "Second", "col_c": "Third"},
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "col_a", "col_b", "col_c")
        result_df = result.execute()

        # Row 1: uses col_c (third fallback)
        assert result_df[result_df["id"] == 1]["col_a"].iloc[0] == "Third"
        # Row 2: uses col_b (first non-null fallback)
        assert result_df[result_df["id"] == 2]["col_a"].iloc[0] == "Second"
        # Row 3: uses col_a (primary)
        assert result_df[result_df["id"] == 3]["col_a"].iloc[0] == "First"


class TestCoalesceColumnsSchemaEvolution:
    """Test coalesce_columns for schema evolution scenarios."""

    def test_renamed_column_migration(self, con):
        """Handle column rename across schema versions."""
        # V1 data has "customer_name", V2 has "cust_name"
        df = pd.DataFrame([
            {"id": 1, "customer_name": None, "cust_name": "LegacyCustomer"},  # V1 record
            {"id": 2, "customer_name": "NewCustomer", "cust_name": None},     # V2 record
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "customer_name", "cust_name")
        result_df = result.execute()

        assert result_df[result_df["id"] == 1]["customer_name"].iloc[0] == "LegacyCustomer"
        assert result_df[result_df["id"] == 2]["customer_name"].iloc[0] == "NewCustomer"

    def test_missing_fallback_column_ignored(self, con):
        """Fallback columns that don't exist are ignored."""
        df = pd.DataFrame([
            {"id": 1, "col_a": "value"},  # col_b doesn't exist
        ])

        t = ibis.memtable(df)
        # col_b doesn't exist in the table - should not error
        result = coalesce_columns(t, "col_a", "col_b")
        result_df = result.execute()

        # col_a value is preserved
        assert result_df.iloc[0]["col_a"] == "value"


class TestCoalesceColumnsDataTypes:
    """Test coalesce_columns with various data types."""

    def test_integer_columns(self, con):
        """Coalesce integer columns."""
        df = pd.DataFrame({
            "id": [1, 2],
            "new_value": pd.array([None, 200], dtype="Int64"),
            "old_value": pd.array([100, 150], dtype="Int64"),
        })

        t = ibis.memtable(df)
        result = coalesce_columns(t, "new_value", "old_value")
        result_df = result.execute()

        assert result_df[result_df["id"] == 1]["new_value"].iloc[0] == 100
        assert result_df[result_df["id"] == 2]["new_value"].iloc[0] == 200

    def test_float_columns(self, con):
        """Coalesce float columns."""
        df = pd.DataFrame([
            {"id": 1, "new_score": None, "old_score": 1.5},
            {"id": 2, "new_score": 2.5, "old_score": 1.0},
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "new_score", "old_score")
        result_df = result.execute()

        assert result_df[result_df["id"] == 1]["new_score"].iloc[0] == 1.5
        assert result_df[result_df["id"] == 2]["new_score"].iloc[0] == 2.5


class TestCoalesceColumnsEdgeCases:
    """Edge case tests for coalesce_columns."""

    def test_all_nulls(self, con):
        """All columns null results in null."""
        df = pd.DataFrame({
            "id": [1],
            "col_a": pd.array([None], dtype="string"),
            "col_b": pd.array([None], dtype="string"),
        })

        t = ibis.memtable(df)
        result = coalesce_columns(t, "col_a", "col_b")
        result_df = result.execute()

        assert pd.isna(result_df.iloc[0]["col_a"])

    def test_empty_table(self, con):
        """Empty table returns empty result."""
        df = pd.DataFrame({
            "id": pd.array([], dtype="int64"),
            "col_a": pd.array([], dtype="string"),
            "col_b": pd.array([], dtype="string")
        })

        t = ibis.memtable(df)
        result = coalesce_columns(t, "col_a", "col_b")
        result_df = result.execute()

        assert len(result_df) == 0

    def test_preserves_other_columns(self, con):
        """Other columns preserved unchanged."""
        df = pd.DataFrame([
            {"id": 1, "keep_me": "preserved", "col_a": None, "col_b": "value"},
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "col_a", "col_b")
        result_df = result.execute()

        assert result_df.iloc[0]["keep_me"] == "preserved"
        assert result_df.iloc[0]["id"] == 1

    def test_no_fallbacks(self, con):
        """Primary column only (no fallbacks) works."""
        df = pd.DataFrame([
            {"id": 1, "col_a": "value"},
            {"id": 2, "col_a": None},
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "col_a")
        result_df = result.execute()

        assert result_df[result_df["id"] == 1]["col_a"].iloc[0] == "value"
        assert pd.isna(result_df[result_df["id"] == 2]["col_a"].iloc[0])

    def test_empty_string_not_null(self, con):
        """Empty string is not null, should be used."""
        df = pd.DataFrame([
            {"id": 1, "col_a": "", "col_b": "fallback"},
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "col_a", "col_b")
        result_df = result.execute()

        # Empty string is not null, so primary used
        assert result_df.iloc[0]["col_a"] == ""
