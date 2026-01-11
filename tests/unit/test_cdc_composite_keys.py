"""Tests for CDC with composite (multi-column) natural keys.

Many production tables have composite primary keys like:
- (order_id, line_item_id)
- (region, customer_id)
- (year, month, account_id)

These tests verify that CDC operations correctly handle
deduplication and delete modes with composite keys.
"""

from __future__ import annotations

import pandas as pd
import ibis

from pipelines.lib.curate import apply_cdc


class TestCDCCompositeKeys:
    """Tests for CDC with composite natural keys."""

    def test_two_column_composite_key_insert_update(self):
        """Two-column composite key with insert and update.

        Insert (A, 1) then update (A, 1) should dedupe to latest.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": ["A", "A"],
                "customer_id": [1, 1],
                "name": ["Original", "Updated"],
                "op": ["I", "U"],
                "updated_at": ["2025-01-10 10:00:00", "2025-01-11 10:00:00"],
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

        assert len(result_df) == 1, "Should dedupe to one record"
        assert result_df.iloc[0]["name"] == "Updated"
        assert result_df.iloc[0]["region"] == "A"
        assert result_df.iloc[0]["customer_id"] == 1

    def test_composite_key_with_deletes(self):
        """Delete should only affect matching composite key.

        Delete (A, 1) should not affect (A, 2) or (B, 1).
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": ["A", "A", "B", "A"],
                "customer_id": [1, 2, 1, 1],
                "name": ["A1", "A2", "B1", "A1 Deleted"],
                "op": ["I", "I", "I", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                ],
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
        result_df = result.execute().sort_values(["region", "customer_id"]).reset_index(drop=True)

        # (A, 1) should be filtered out (delete)
        # (A, 2) and (B, 1) should remain
        assert len(result_df) == 2, "Should have 2 records after delete"

        # Verify correct records remain
        keys = [(r["region"], r["customer_id"]) for _, r in result_df.iterrows()]
        assert ("A", 2) in keys, "(A, 2) should remain"
        assert ("B", 1) in keys, "(B, 1) should remain"
        assert ("A", 1) not in keys, "(A, 1) should be deleted"

    def test_three_column_composite_key(self):
        """Three-column composite key should work correctly."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "year": [2025, 2025, 2025, 2025],
                "month": [1, 1, 2, 1],
                "account_id": [100, 100, 100, 100],
                "balance": [1000, 1500, 2000, 1800],
                "op": ["I", "U", "I", "U"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-15 10:00:00",
                    "2025-02-01 10:00:00",
                    "2025-01-20 10:00:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["year", "month", "account_id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values(["year", "month"]).reset_index(drop=True)

        # (2025, 1, 100) has I then two U's - latest U should win
        # (2025, 2, 100) has I only
        assert len(result_df) == 2, "Should have 2 records (different months)"

        jan_record = result_df[result_df["month"] == 1].iloc[0]
        feb_record = result_df[result_df["month"] == 2].iloc[0]

        assert jan_record["balance"] == 1800, "January should have latest update"
        assert feb_record["balance"] == 2000, "February should have its value"

    def test_partial_match_does_not_dedupe(self):
        """(A, 1) and (A, 2) are distinct keys and should not dedupe.

        Partial match on first component should NOT cause deduplication.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": ["A", "A"],
                "customer_id": [1, 2],
                "name": ["Customer 1", "Customer 2"],
                "op": ["I", "I"],
                "updated_at": ["2025-01-10 10:00:00", "2025-01-10 10:00:00"],
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

        assert len(result_df) == 2, "Both records should exist (different composite keys)"

    def test_composite_key_tombstone_mode(self):
        """Tombstone mode with composite key."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": ["A", "A", "A"],
                "customer_id": [1, 2, 1],
                "name": ["Cust1", "Cust2", "Cust1"],
                "op": ["I", "I", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["region", "customer_id"],
            order_by="updated_at",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values(["region", "customer_id"]).reset_index(drop=True)

        assert len(result_df) == 2, "Both keys should be present with tombstone"

        cust1 = result_df[(result_df["region"] == "A") & (result_df["customer_id"] == 1)].iloc[0]
        cust2 = result_df[(result_df["region"] == "A") & (result_df["customer_id"] == 2)].iloc[0]

        assert bool(cust1["_deleted"]) is True, "(A, 1) should be tombstoned"
        assert bool(cust2["_deleted"]) is False, "(A, 2) should not be tombstoned"

    def test_composite_key_update_affects_only_matching(self):
        """Update to (A, 1) should not affect (B, 1)."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": ["A", "B", "A"],
                "customer_id": [1, 1, 1],
                "name": ["A-Original", "B-Original", "A-Updated"],
                "op": ["I", "I", "U"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                ],
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
        result_df = result.execute().sort_values(["region"]).reset_index(drop=True)

        assert len(result_df) == 2, "Both composite keys should exist"

        a_record = result_df[result_df["region"] == "A"].iloc[0]
        b_record = result_df[result_df["region"] == "B"].iloc[0]

        assert a_record["name"] == "A-Updated", "(A, 1) should be updated"
        assert b_record["name"] == "B-Original", "(B, 1) should be unchanged"

    def test_composite_key_with_mixed_types(self):
        """Composite key with mixed types (string + integer)."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "category": ["electronics", "electronics", "clothing"],
                "product_id": [101, 101, 101],
                "price": [999, 899, 49],
                "op": ["I", "U", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-10 10:00:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["category", "product_id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values(["category"]).reset_index(drop=True)

        assert len(result_df) == 2, "Should have 2 records (different categories)"

        clothing = result_df[result_df["category"] == "clothing"].iloc[0]
        electronics = result_df[result_df["category"] == "electronics"].iloc[0]

        assert clothing["price"] == 49
        assert electronics["price"] == 899, "Electronics should have updated price"

    def test_composite_key_all_operations(self):
        """Full lifecycle (I -> U -> D -> I) on composite key."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": ["US", "US", "US", "US"],
                "customer_id": [100, 100, 100, 100],
                "name": ["V1", "V2", "V2", "V3"],
                "op": ["I", "U", "D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-12 10:00:00",
                    "2025-01-13 10:00:00",
                ],
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

        assert len(result_df) == 1, "Should have 1 record after re-insert"
        assert result_df.iloc[0]["name"] == "V3", "Should have final version"


class TestCompositeKeyEdgeCases:
    """Edge cases for composite keys."""

    def test_single_column_key_still_works(self):
        """Verify single-column key still works (regression test)."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Original", "Updated"],
                "op": ["I", "U"],
                "updated_at": ["2025-01-10 10:00:00", "2025-01-11 10:00:00"],
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

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Updated"

    def test_composite_key_empty_string_component(self):
        """Composite key with empty string component."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": ["", "US"],
                "customer_id": [1, 1],
                "name": ["NoRegion", "USCustomer"],
                "op": ["I", "I"],
                "updated_at": ["2025-01-10 10:00:00", "2025-01-10 10:00:00"],
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

        # Empty string is a valid distinct value, so both should exist
        assert len(result_df) == 2, "Empty string is a distinct key value"

    def test_composite_key_zero_integer_component(self):
        """Composite key with zero as integer component."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "region": ["A", "A"],
                "customer_id": [0, 1],
                "name": ["Zero", "One"],
                "op": ["I", "I"],
                "updated_at": ["2025-01-10 10:00:00", "2025-01-10 10:00:00"],
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

        # Zero is a valid distinct value
        assert len(result_df) == 2, "Zero is a distinct key value"
