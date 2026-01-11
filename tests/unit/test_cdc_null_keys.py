"""Tests for CDC with NULL values in natural key columns.

Tests behavior when unique_columns contain NULL values:
- Single NULL key
- Multiple records with NULL key
- NULL in composite key

NULL handling in keys is important because:
- Source systems may have nullable columns
- ETL processes may introduce NULLs
- Behavior should be deterministic and documented
"""

from __future__ import annotations

import pandas as pd
import ibis

from pipelines.lib.curate import apply_cdc


class TestCDCNullKeys:
    """Tests for NULL values in key columns."""

    def test_single_null_key_value(self):
        """Record with NULL in key column.

        NULL keys are a valid (though unusual) case that should
        be handled without errors.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, None, 3],
                "name": ["Alice", "NullKey", "Charlie"],
                "op": ["I", "I", "I"],
                "updated_at": ["2025-01-10", "2025-01-10", "2025-01-10"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # All three records should be present (NULL is a distinct key)
        assert len(result_df) == 3, "NULL should be treated as a distinct key"

    def test_multiple_records_with_null_key(self):
        """Multiple records with NULL key - should dedupe.

        If multiple records have NULL as the key, they should
        be deduped just like any other key value.
        """
        con = ibis.duckdb.connect()
        # Use explicit nullable int type to avoid DuckDB type inference issue
        df = pd.DataFrame(
            {
                "id": pd.array([None, None, None], dtype=pd.Int64Dtype()),
                "name": ["V1", "V2", "V3"],
                "op": ["I", "U", "U"],
                "updated_at": ["2025-01-10", "2025-01-11", "2025-01-12"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # All NULL keys should dedupe to one record
        assert len(result_df) == 1, "NULL keys should dedupe"
        assert result_df.iloc[0]["name"] == "V3", "Latest version should win"

    def test_null_key_with_delete(self):
        """NULL key can be deleted."""
        con = ibis.duckdb.connect()
        # Use explicit nullable int type to avoid DuckDB type inference issue
        df = pd.DataFrame(
            {
                "id": pd.array([None, None], dtype=pd.Int64Dtype()),
                "name": ["NullRecord", "NullRecord"],
                "op": ["I", "D"],
                "updated_at": ["2025-01-10", "2025-01-11"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Delete should filter out the NULL key record
        assert len(result_df) == 0, "NULL key should be deleted"

    def test_null_key_tombstone_mode(self):
        """NULL key with tombstone mode."""
        con = ibis.duckdb.connect()
        # Use explicit nullable int type to avoid DuckDB type inference issue
        df = pd.DataFrame(
            {
                "id": pd.array([None, None], dtype=pd.Int64Dtype()),
                "name": ["NullRecord", "NullRecord"],
                "op": ["I", "D"],
                "updated_at": ["2025-01-10", "2025-01-11"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Should have one tombstoned record
        assert len(result_df) == 1
        assert bool(result_df.iloc[0]["_deleted"]) is True


class TestCDCNullInCompositeKey:
    """Tests for NULL in composite key columns."""

    def test_null_in_first_key_column(self):
        """NULL in first column of composite key."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": [None, "US", None],
                "customer_id": [1, 1, 1],
                "name": ["NullRegion", "USCustomer", "NullRegion2"],
                "op": ["I", "I", "U"],
                "updated_at": ["2025-01-10", "2025-01-10", "2025-01-11"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["region", "customer_id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # (NULL, 1) and (US, 1) are different composite keys
        # (NULL, 1) has I -> U, so 1 record
        # (US, 1) has I only, so 1 record
        assert len(result_df) == 2, "NULL in composite key makes it distinct"

    def test_null_in_second_key_column(self):
        """NULL in second column of composite key."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": ["US", "US"],
                "customer_id": [None, 1],
                "name": ["NullCustomer", "Customer1"],
                "op": ["I", "I"],
                "updated_at": ["2025-01-10", "2025-01-10"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["region", "customer_id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # (US, NULL) and (US, 1) are different composite keys
        assert len(result_df) == 2

    def test_null_in_both_key_columns(self):
        """NULL in both columns of composite key."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": [None, None, "US"],
                "customer_id": [None, None, 1],
                "name": ["V1", "V2", "USCustomer"],
                "op": ["I", "U", "I"],
                "updated_at": ["2025-01-10", "2025-01-11", "2025-01-10"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["region", "customer_id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # (NULL, NULL) has I -> U = V2
        # (US, 1) has I = USCustomer
        assert len(result_df) == 2

    def test_null_composite_key_delete(self):
        """Delete record with NULL in composite key."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": [None, "US", None],
                "customer_id": [1, 1, 1],
                "name": ["NullRegion", "USCustomer", "NullRegion"],
                "op": ["I", "I", "D"],
                "updated_at": ["2025-01-10", "2025-01-10", "2025-01-11"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["region", "customer_id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # (NULL, 1) deleted, (US, 1) remains
        assert len(result_df) == 1
        assert result_df.iloc[0]["region"] == "US"


class TestCDCNullKeyEdgeCases:
    """Edge cases for NULL key handling."""

    def test_all_null_keys(self):
        """All records have NULL keys."""
        con = ibis.duckdb.connect()
        # Use explicit nullable int type to avoid DuckDB type inference issue
        df = pd.DataFrame(
            {
                "id": pd.array([None, None, None], dtype=pd.Int64Dtype()),
                "name": ["A", "B", "C"],
                "op": ["I", "I", "I"],
                "updated_at": ["2025-01-10", "2025-01-11", "2025-01-12"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # All NULL keys dedupe to one record (latest)
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "C"

    def test_null_key_mixed_with_valid_keys(self):
        """Mix of NULL and valid keys."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, None, 2, None, 3],
                "name": ["A", "Null1", "B", "Null2", "C"],
                "op": ["I", "I", "I", "U", "I"],
                "updated_at": [
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-10",
                    "2025-01-11",
                    "2025-01-10",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Keys 1, 2, 3 each have 1 record
        # NULL key has I -> U = Null2
        assert len(result_df) == 4

    def test_null_string_vs_null_value(self):
        """String 'NULL' should be different from actual NULL.

        The string 'NULL' is a valid non-null value.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": ["NULL", None],
                "name": ["StringNull", "ActualNull"],
                "op": ["I", "I"],
                "updated_at": ["2025-01-10", "2025-01-10"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # 'NULL' (string) and NULL (actual) are different keys
        assert len(result_df) == 2

    def test_integer_key_with_none(self):
        """Integer key column with None values."""
        con = ibis.duckdb.connect()
        # Use numpy array with explicit dtype
        df = pd.DataFrame(
            {
                "id": pd.array([1, None, 3], dtype=pd.Int64Dtype()),
                "name": ["One", "Null", "Three"],
                "op": ["I", "I", "I"],
                "updated_at": ["2025-01-10", "2025-01-10", "2025-01-10"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # All three should be present (1, NULL, 3)
        assert len(result_df) == 3
