"""Tests for schema evolution handling.

Tests scenarios where schema changes between batches.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_latest, coalesce_columns, dedupe_exact


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestColumnRename:
    """Test column rename scenarios with coalesce_columns."""

    def test_simple_column_rename(self, con):
        """Handle simple column rename with coalesce."""
        # V1 schema had "customer_name", V2 has "cust_name"
        df = pd.DataFrame([
            {"id": 1, "customer_name": None, "cust_name": "Old Alice"},  # V1 record
            {"id": 2, "customer_name": "New Bob", "cust_name": None},    # V2 record
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "customer_name", "cust_name")
        result_df = result.execute()

        # Both records have customer_name filled
        assert result_df[result_df["id"] == 1]["customer_name"].iloc[0] == "Old Alice"
        assert result_df[result_df["id"] == 2]["customer_name"].iloc[0] == "New Bob"

    def test_multiple_column_renames(self, con):
        """Handle multiple columns renamed at different times."""
        df = pd.DataFrame([
            {"id": 1, "new_name": None, "old_name": "A", "oldest_name": None},
            {"id": 2, "new_name": None, "old_name": None, "oldest_name": "B"},
            {"id": 3, "new_name": "C", "old_name": None, "oldest_name": None},
        ])

        t = ibis.memtable(df)
        result = coalesce_columns(t, "new_name", "old_name", "oldest_name")
        result_df = result.execute()

        assert result_df[result_df["id"] == 1]["new_name"].iloc[0] == "A"
        assert result_df[result_df["id"] == 2]["new_name"].iloc[0] == "B"
        assert result_df[result_df["id"] == 3]["new_name"].iloc[0] == "C"

    def test_fallback_column_not_exist(self, con):
        """Missing fallback column is gracefully ignored."""
        df = pd.DataFrame([
            {"id": 1, "name": "Alice"},  # No fallback column exists
        ])

        t = ibis.memtable(df)
        # "old_name" doesn't exist but shouldn't error
        result = coalesce_columns(t, "name", "old_name")
        result_df = result.execute()

        assert result_df.iloc[0]["name"] == "Alice"


class TestColumnAddition:
    """Test scenarios where new columns are added."""

    def test_new_column_with_defaults(self, con):
        """New column added with NULL for old records."""
        # Simulate union of old and new schema
        old_schema = pd.DataFrame([
            {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
        ])

        # Add new column to simulate evolved schema
        old_schema["new_column"] = None

        new_schema = pd.DataFrame([
            {"id": 2, "name": "Bob", "ts": datetime(2025, 1, 15), "new_column": "has_value"},
        ])

        combined = pd.concat([old_schema, new_schema], ignore_index=True)
        t = ibis.memtable(combined)

        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 2

        # Old record has NULL for new column
        old_rec = result_df[result_df["id"] == 1].iloc[0]
        assert pd.isna(old_rec["new_column"])

        # New record has value
        new_rec = result_df[result_df["id"] == 2].iloc[0]
        assert new_rec["new_column"] == "has_value"


class TestColumnTypeChanges:
    """Test column type compatibility scenarios."""

    def test_integer_to_float_compatible(self, con):
        """Integer to float type change is compatible."""
        df = pd.DataFrame([
            {"id": 1, "amount": 100, "ts": datetime(2025, 1, 10)},     # Integer
            {"id": 1, "amount": 100.50, "ts": datetime(2025, 1, 15)},  # Float (new version)
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["amount"] == 100.50

    def test_string_to_numeric_mixed(self, con):
        """Mixed string/numeric values in same column."""
        # In real scenarios, this would need type casting
        df = pd.DataFrame([
            {"id": 1, "value": "100", "ts": datetime(2025, 1, 10)},
            {"id": 2, "value": "200", "ts": datetime(2025, 1, 15)},
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 2


class TestSchemaEvolutionWithDedup:
    """Test deduplication with schema evolution."""

    def test_dedupe_across_schema_versions(self, con):
        """Dedupe works across schema versions."""
        # V1 schema
        v1_data = pd.DataFrame([
            {"id": 1, "name": "Alice V1", "ts": datetime(2025, 1, 10)},
        ])
        v1_data["new_field"] = None  # Add for union

        # V2 schema (has new_field)
        v2_data = pd.DataFrame([
            {"id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 15), "new_field": "added"},
        ])

        combined = pd.concat([v1_data, v2_data], ignore_index=True)
        t = ibis.memtable(combined)

        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Alice V2"
        assert result_df.iloc[0]["new_field"] == "added"

    def test_exact_dedupe_different_nullability(self, con):
        """Exact dedupe with NULL vs empty string."""
        df = pd.DataFrame([
            {"id": 1, "optional": None},
            {"id": 2, "optional": ""},
            {"id": 1, "optional": None},  # Exact duplicate
        ])

        t = ibis.memtable(df)
        result = dedupe_exact(t)
        result_df = result.execute()

        # NULL duplicate removed, empty string is different
        assert len(result_df) == 2


class TestMultiVersionSchemaEvolution:
    """Test evolution across multiple schema versions."""

    def test_three_schema_versions(self, con):
        """Handle data from 3 different schema versions."""
        # V1: Basic schema
        v1 = pd.DataFrame([{"id": 1, "name": "Old", "ts": datetime(2025, 1, 1)}])
        v1["email"] = None
        v1["phone"] = None

        # V2: Added email
        v2 = pd.DataFrame([{"id": 2, "name": "Mid", "ts": datetime(2025, 1, 5), "email": "test@v2.com"}])
        v2["phone"] = None

        # V3: Added phone
        v3 = pd.DataFrame([
            {"id": 3, "name": "New", "ts": datetime(2025, 1, 10), "email": "test@v3.com", "phone": "555-1234"}
        ])

        combined = pd.concat([v1, v2, v3], ignore_index=True)
        t = ibis.memtable(combined)

        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 3

        # All records have all columns
        assert "email" in result_df.columns
        assert "phone" in result_df.columns

        # V3 record has all fields
        v3_rec = result_df[result_df["id"] == 3].iloc[0]
        assert v3_rec["email"] == "test@v3.com"
        assert v3_rec["phone"] == "555-1234"

    def test_update_to_new_schema(self, con):
        """Update record from old schema to new schema."""
        # Same record, evolving schema
        old_version = pd.DataFrame([
            {"id": 1, "name": "Original", "ts": datetime(2025, 1, 1)}
        ])
        old_version["new_field"] = None

        new_version = pd.DataFrame([
            {"id": 1, "name": "Updated", "ts": datetime(2025, 1, 10), "new_field": "now_populated"}
        ])

        combined = pd.concat([old_version, new_version], ignore_index=True)
        t = ibis.memtable(combined)

        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Updated"
        assert result_df.iloc[0]["new_field"] == "now_populated"
