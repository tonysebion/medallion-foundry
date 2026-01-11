"""Tests for CDC high-volume batch processing.

Tests correctness of CDC processing with large record counts:
- 10K+ records per batch
- Many records per key (100+ updates to same key)
- Mixed operations at scale

These tests validate that the CDC implementation handles
production-scale data correctly without performance issues.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

import pandas as pd
import pytest
import ibis

from pipelines.lib.curate import apply_cdc


# Mark all tests in this module as slow
pytestmark = pytest.mark.slow


class TestCDCHighVolume:
    """Tests for high-volume CDC processing."""

    def test_10k_records_single_batch_correctness(self):
        """10,000 records should process correctly.

        Generate 10K insert records with unique keys and verify
        all are preserved correctly.
        """
        con = ibis.duckdb.connect()

        # Generate 10K unique records
        records = []
        base_time = datetime(2025, 1, 10, 10, 0, 0)
        for i in range(10000):
            records.append(
                {
                    "id": i + 1,
                    "name": f"Record_{i + 1}",
                    "value": i * 10,
                    "op": "I",
                    "updated_at": (base_time + timedelta(seconds=i)).isoformat(),
                }
            )

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # All 10K records should be present
        assert len(result_df) == 10000, f"Expected 10000 records, got {len(result_df)}"

        # Verify all IDs are unique
        assert result_df["id"].is_unique, "All IDs should be unique"

        # Verify ID range
        assert result_df["id"].min() == 1
        assert result_df["id"].max() == 10000

        # Verify op column is dropped
        assert "op" not in result_df.columns

    def test_10k_with_100_keys_deduplication(self):
        """10K records across 100 keys (100 records per key).

        Each key gets 100 updates. Only the latest should survive.
        """
        con = ibis.duckdb.connect()

        records = []
        base_time = datetime(2025, 1, 10, 10, 0, 0)
        record_idx = 0

        for key_id in range(1, 101):  # 100 keys
            for version in range(100):  # 100 versions each
                records.append(
                    {
                        "id": key_id,
                        "name": f"Key_{key_id}_V{version}",
                        "value": version * 10,
                        "op": "I" if version == 0 else "U",
                        "updated_at": (
                            base_time + timedelta(seconds=record_idx)
                        ).isoformat(),
                    }
                )
                record_idx += 1

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Should dedupe to 100 records (one per key)
        assert len(result_df) == 100, f"Expected 100 records, got {len(result_df)}"

        # Each key should have the latest version (V99)
        for _, row in result_df.iterrows():
            assert row["name"].endswith("_V99"), f"Key {row['id']} should have V99"
            assert row["value"] == 990, f"Key {row['id']} should have value 990"

    def test_mixed_operations_at_scale(self):
        """10K mixed I/U/D operations across 1000 keys.

        Realistic mix of operations:
        - 40% inserts
        - 40% updates
        - 20% deletes
        """
        con = ibis.duckdb.connect()

        random.seed(42)  # Deterministic for testing
        records = []
        base_time = datetime(2025, 1, 10, 10, 0, 0)

        key_states = {}  # Track expected final state
        for i in range(10000):
            key_id = random.randint(1, 1000)
            op_rand = random.random()

            if op_rand < 0.4:
                op = "I"
            elif op_rand < 0.8:
                op = "U"
            else:
                op = "D"

            ts = base_time + timedelta(seconds=i)
            records.append(
                {
                    "id": key_id,
                    "name": f"Key_{key_id}",
                    "value": i,
                    "op": op,
                    "updated_at": ts.isoformat(),
                }
            )

            # Track latest operation per key
            key_states[key_id] = (op, ts, i)

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Count expected active records (latest op is not D)
        expected_active = sum(1 for op, _, _ in key_states.values() if op != "D")

        assert len(result_df) == expected_active, (
            f"Expected {expected_active} active records, got {len(result_df)}"
        )

        # Verify each result has correct final value
        for _, row in result_df.iterrows():
            key_id = row["id"]
            expected_op, _, expected_value = key_states[key_id]
            assert expected_op != "D", f"Key {key_id} should not be deleted"
            assert row["value"] == expected_value, (
                f"Key {key_id} should have value {expected_value}"
            )

    def test_high_volume_tombstone_mode(self):
        """High volume with tombstone mode preserves all keys."""
        con = ibis.duckdb.connect()

        records = []
        base_time = datetime(2025, 1, 10, 10, 0, 0)

        # Create 1000 keys, each with I then D
        for key_id in range(1, 1001):
            records.append(
                {
                    "id": key_id,
                    "name": f"Key_{key_id}",
                    "op": "I",
                    "updated_at": (base_time + timedelta(seconds=key_id)).isoformat(),
                }
            )
            records.append(
                {
                    "id": key_id,
                    "name": f"Key_{key_id}",
                    "op": "D",
                    "updated_at": (
                        base_time + timedelta(seconds=key_id + 1000)
                    ).isoformat(),
                }
            )

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # All 1000 keys should be present as tombstones
        assert len(result_df) == 1000, "All keys should be tombstoned"
        assert all(result_df["_deleted"]), "All should be marked deleted"


class TestCDCHighVolumeEdgeCases:
    """Edge cases at high volume."""

    def test_all_same_key_high_volume(self):
        """All 10K records for single key - only latest survives."""
        con = ibis.duckdb.connect()

        records = []
        base_time = datetime(2025, 1, 10, 10, 0, 0)

        for i in range(10000):
            records.append(
                {
                    "id": 1,  # All same key
                    "name": f"Version_{i}",
                    "value": i,
                    "op": "I" if i == 0 else "U",
                    "updated_at": (base_time + timedelta(seconds=i)).isoformat(),
                }
            )

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Should dedupe to 1 record
        assert len(result_df) == 1, "Should have exactly 1 record"
        assert result_df.iloc[0]["value"] == 9999, "Should have latest value"
        assert result_df.iloc[0]["name"] == "Version_9999"

    def test_alternating_delete_insert_high_volume(self):
        """High volume alternating I/D for same key."""
        con = ibis.duckdb.connect()

        records = []
        base_time = datetime(2025, 1, 10, 10, 0, 0)

        # 5000 I/D cycles for same key
        for i in range(5000):
            records.append(
                {
                    "id": 1,
                    "name": f"Insert_{i}",
                    "op": "I",
                    "updated_at": (base_time + timedelta(seconds=i * 2)).isoformat(),
                }
            )
            records.append(
                {
                    "id": 1,
                    "name": f"Delete_{i}",
                    "op": "D",
                    "updated_at": (
                        base_time + timedelta(seconds=i * 2 + 1)
                    ).isoformat(),
                }
            )

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Last operation is D, so record should be filtered out
        assert len(result_df) == 0, "Final delete should result in no records"

    def test_high_volume_composite_key(self):
        """High volume with composite key."""
        con = ibis.duckdb.connect()

        records = []
        base_time = datetime(2025, 1, 10, 10, 0, 0)

        # 100 regions x 100 customers = 10K unique composite keys
        idx = 0
        for region in range(100):
            for customer in range(100):
                records.append(
                    {
                        "region": f"R{region}",
                        "customer_id": customer,
                        "name": f"R{region}_C{customer}",
                        "op": "I",
                        "updated_at": (base_time + timedelta(seconds=idx)).isoformat(),
                    }
                )
                idx += 1

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["region", "customer_id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # All 10K composite keys should be present
        assert len(result_df) == 10000, "All composite keys should be present"
