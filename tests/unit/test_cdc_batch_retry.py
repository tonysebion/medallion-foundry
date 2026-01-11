"""Tests for CDC handling of duplicate/retry batch processing.

Tests scenarios where the same batch is processed multiple times:
- Retry logic re-sends failed batches
- At-least-once delivery guarantees in streaming
- Manual re-processing of historical data
- Idempotency requirements

The key requirement is that processing the same data multiple times
should produce the same result as processing it once (idempotency).
"""

from __future__ import annotations

import pandas as pd
import ibis

from pipelines.lib.curate import apply_cdc


class TestCDCBatchDuplication:
    """Tests for processing the same batch multiple times."""

    def test_same_batch_twice_is_idempotent(self):
        """Processing identical batch twice produces same result as once."""
        con = ibis.duckdb.connect()

        single_batch = [
            {"id": 1, "name": "Alice", "value": 100, "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 2, "name": "Bob", "value": 200, "op": "I", "ts": "2025-01-10 10:01"},
            {"id": 3, "name": "Charlie", "value": 300, "op": "I", "ts": "2025-01-10 10:02"},
        ]

        # Process once
        df_single = pd.DataFrame(single_batch)
        t_single = con.create_table("single", df_single)
        result_single = apply_cdc(
            t_single,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        ).execute().sort_values("id").reset_index(drop=True)

        # Process twice (duplicate batch)
        df_double = pd.DataFrame(single_batch + single_batch)
        t_double = con.create_table("double", df_double)
        result_double = apply_cdc(
            t_double,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        ).execute().sort_values("id").reset_index(drop=True)

        # Results should be identical
        assert len(result_single) == len(result_double)
        pd.testing.assert_frame_equal(
            result_single[["id", "name", "value"]],
            result_double[["id", "name", "value"]],
        )

    def test_same_batch_three_times(self):
        """Processing identical batch 3 times still idempotent."""
        con = ibis.duckdb.connect()

        batch = [
            {"id": 1, "name": "Entity_1", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 1, "name": "Entity_1_v2", "op": "U", "ts": "2025-01-10 11:00"},
        ]

        # Triplicate the batch
        triple_batch = batch + batch + batch
        df = pd.DataFrame(triple_batch)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Should have exactly 1 record with the latest value
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Entity_1_v2"

    def test_duplicate_delete_operations(self):
        """Same delete operation processed multiple times."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "Entity", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 1, "name": "Entity", "op": "D", "ts": "2025-01-11 10:00"},
            # Duplicate delete
            {"id": 1, "name": "Entity", "op": "D", "ts": "2025-01-11 10:00"},
            {"id": 1, "name": "Entity", "op": "D", "ts": "2025-01-11 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Record should be deleted regardless of how many D operations
        assert len(result_df) == 0


class TestCDCInterleavedDuplicates:
    """Tests for duplicate batches interleaved with new data."""

    def test_old_batch_reprocessed_after_new_batch(self):
        """Old batch arrives again after newer batch already processed.

        Timeline:
        - Day 1: Batch A arrives (id=1 insert)
        - Day 2: Batch B arrives (id=1 update)
        - Day 3: Batch A arrives again (retry)
        """
        con = ibis.duckdb.connect()

        records = [
            # Day 1: Batch A
            {"id": 1, "name": "v1", "op": "I", "ts": "2025-01-10 10:00"},
            # Day 2: Batch B (newer)
            {"id": 1, "name": "v2", "op": "U", "ts": "2025-01-11 10:00"},
            # Day 3: Batch A again (old duplicate)
            {"id": 1, "name": "v1", "op": "I", "ts": "2025-01-10 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Newer batch (v2) should win based on timestamp
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "v2"

    def test_multiple_keys_with_selective_duplicates(self):
        """Some keys have duplicates, others don't."""
        con = ibis.duckdb.connect()

        records = [
            # Key 1: Has duplicates
            {"id": 1, "name": "K1_v1", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 1, "name": "K1_v1", "op": "I", "ts": "2025-01-10 10:00"},  # Duplicate

            # Key 2: No duplicates
            {"id": 2, "name": "K2_v1", "op": "I", "ts": "2025-01-10 10:01"},

            # Key 3: Has duplicates including update
            {"id": 3, "name": "K3_v1", "op": "I", "ts": "2025-01-10 10:02"},
            {"id": 3, "name": "K3_v2", "op": "U", "ts": "2025-01-11 10:00"},
            {"id": 3, "name": "K3_v2", "op": "U", "ts": "2025-01-11 10:00"},  # Duplicate update
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        assert len(result_df) == 3
        assert result_df.iloc[0]["name"] == "K1_v1"
        assert result_df.iloc[1]["name"] == "K2_v1"
        assert result_df.iloc[2]["name"] == "K3_v2"


class TestCDCBatchRetryWithDeletes:
    """Tests for duplicate batches containing deletes."""

    def test_delete_batch_reprocessed(self):
        """Delete batch processed multiple times."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "Entity", "op": "I", "ts": "2025-01-10 10:00"},
            # Delete batch
            {"id": 1, "name": "Entity", "op": "D", "ts": "2025-01-11 10:00"},
            # Same delete batch again
            {"id": 1, "name": "Entity", "op": "D", "ts": "2025-01-11 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Should have one tombstone record
        assert len(result_df) == 1
        assert bool(result_df.iloc[0]["_deleted"]) is True

    def test_insert_after_duplicate_deletes(self):
        """Re-insert after multiple duplicate deletes."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "v1", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 1, "name": "v1", "op": "D", "ts": "2025-01-11 10:00"},
            {"id": 1, "name": "v1", "op": "D", "ts": "2025-01-11 10:00"},  # Dup
            {"id": 1, "name": "v1", "op": "D", "ts": "2025-01-11 10:00"},  # Dup
            # Re-insert
            {"id": 1, "name": "v2", "op": "I", "ts": "2025-01-20 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Re-insert wins
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "v2"


class TestCDCFullBatchRetry:
    """Tests simulating full batch retry scenarios."""

    def test_full_day_batch_reprocessed(self):
        """Entire day's batch reprocessed (common retry scenario)."""
        con = ibis.duckdb.connect()

        day1_batch = [
            {"id": 1, "name": "A", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 2, "name": "B", "op": "I", "ts": "2025-01-10 10:01"},
            {"id": 3, "name": "C", "op": "I", "ts": "2025-01-10 10:02"},
        ]

        day2_batch = [
            {"id": 1, "name": "A_v2", "op": "U", "ts": "2025-01-11 10:00"},
            {"id": 4, "name": "D", "op": "I", "ts": "2025-01-11 10:01"},
        ]

        # Simulate: Day 1, Day 2, Day 1 retry
        all_records = day1_batch + day2_batch + day1_batch

        df = pd.DataFrame(all_records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        # 4 unique keys, Day 2 updates should win where applicable
        assert len(result_df) == 4
        assert result_df[result_df["id"] == 1].iloc[0]["name"] == "A_v2"
        assert result_df[result_df["id"] == 2].iloc[0]["name"] == "B"
        assert result_df[result_df["id"] == 3].iloc[0]["name"] == "C"
        assert result_df[result_df["id"] == 4].iloc[0]["name"] == "D"

    def test_multiple_day_batches_all_duplicated(self):
        """3 days of batches, each processed twice."""
        con = ibis.duckdb.connect()

        day1 = [{"id": 1, "name": "d1", "op": "I", "ts": "2025-01-10 10:00"}]
        day2 = [{"id": 1, "name": "d2", "op": "U", "ts": "2025-01-11 10:00"}]
        day3 = [{"id": 1, "name": "d3", "op": "U", "ts": "2025-01-12 10:00"}]

        # Each day duplicated
        all_records = day1 + day1 + day2 + day2 + day3 + day3

        df = pd.DataFrame(all_records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Day 3 should win
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "d3"


class TestCDCPartialBatchRetry:
    """Tests for partial batch duplications (subset of records)."""

    def test_partial_batch_duplicate(self):
        """Only some records from a batch are duplicated."""
        con = ibis.duckdb.connect()

        original_batch = [
            {"id": 1, "name": "A", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 2, "name": "B", "op": "I", "ts": "2025-01-10 10:01"},
            {"id": 3, "name": "C", "op": "I", "ts": "2025-01-10 10:02"},
        ]

        # Only id=2 gets duplicated
        partial_dup = [{"id": 2, "name": "B", "op": "I", "ts": "2025-01-10 10:01"}]

        all_records = original_batch + partial_dup

        df = pd.DataFrame(all_records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        assert len(result_df) == 3
        # All records present with correct values
        assert result_df.iloc[0]["name"] == "A"
        assert result_df.iloc[1]["name"] == "B"
        assert result_df.iloc[2]["name"] == "C"


class TestCDCDuplicateWithDifferentOrder:
    """Tests for duplicates arriving in different relative order."""

    def test_duplicates_interleaved_with_other_keys(self):
        """Duplicates for one key interleaved with operations for other keys."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "K1_v1", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 2, "name": "K2_v1", "op": "I", "ts": "2025-01-10 10:01"},
            {"id": 1, "name": "K1_v1", "op": "I", "ts": "2025-01-10 10:00"},  # Dup of K1
            {"id": 3, "name": "K3_v1", "op": "I", "ts": "2025-01-10 10:02"},
            {"id": 2, "name": "K2_v1", "op": "I", "ts": "2025-01-10 10:01"},  # Dup of K2
            {"id": 1, "name": "K1_v2", "op": "U", "ts": "2025-01-11 10:00"},
            {"id": 1, "name": "K1_v1", "op": "I", "ts": "2025-01-10 10:00"},  # Another dup
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        assert len(result_df) == 3
        assert result_df.iloc[0]["name"] == "K1_v2"  # Update wins
        assert result_df.iloc[1]["name"] == "K2_v1"
        assert result_df.iloc[2]["name"] == "K3_v1"


class TestCDCBatchRetryIdempotency:
    """Tests verifying strict idempotency guarantees."""

    def test_exact_same_result_with_duplicates(self):
        """Verify exact same result regardless of duplicate count."""
        con = ibis.duckdb.connect()

        base_batch = [
            {"id": 1, "name": "Entity_1", "value": 100, "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 2, "name": "Entity_2", "value": 200, "op": "I", "ts": "2025-01-10 10:01"},
            {"id": 1, "name": "Entity_1_v2", "value": 150, "op": "U", "ts": "2025-01-11 10:00"},
            {"id": 2, "name": "Entity_2", "value": 200, "op": "D", "ts": "2025-01-11 10:01"},
        ]

        results = []
        for dup_count in [1, 2, 3, 5, 10]:
            duplicated = base_batch * dup_count
            df = pd.DataFrame(duplicated)
            t = con.create_table(f"test_{dup_count}", df)

            result = apply_cdc(
                t,
                keys=["id"],
                order_by="ts",
                delete_mode="ignore",
                cdc_options={"operation_column": "op"},
            )
            result_df = result.execute().sort_values("id").reset_index(drop=True)
            results.append(result_df)

        # All results should be identical
        base_result = results[0]
        for other_result in results[1:]:
            pd.testing.assert_frame_equal(
                base_result[["id", "name", "value"]],
                other_result[["id", "name", "value"]],
            )

    def test_idempotency_with_tombstone_mode(self):
        """Idempotency holds with tombstone delete mode."""
        con = ibis.duckdb.connect()

        batch = [
            {"id": 1, "name": "Active", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 2, "name": "Deleted", "op": "I", "ts": "2025-01-10 10:01"},
            {"id": 2, "name": "Deleted", "op": "D", "ts": "2025-01-11 10:00"},
        ]

        # Process once
        df_once = pd.DataFrame(batch)
        t_once = con.create_table("once", df_once)
        result_once = apply_cdc(
            t_once,
            keys=["id"],
            order_by="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        ).execute().sort_values("id").reset_index(drop=True)

        # Process 5 times
        df_five = pd.DataFrame(batch * 5)
        t_five = con.create_table("five", df_five)
        result_five = apply_cdc(
            t_five,
            keys=["id"],
            order_by="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        ).execute().sort_values("id").reset_index(drop=True)

        # Same result
        assert len(result_once) == len(result_five)
        assert list(result_once["id"]) == list(result_five["id"])
        assert list(result_once["_deleted"]) == list(result_five["_deleted"])
