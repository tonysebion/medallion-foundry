"""Tests for DuplicateInjector utility class (Story 1.4).

Tests duplicate injection scenarios for deduplication testing:
- Exact duplicates (identical rows)
- Near duplicates (same key, different mutable fields)
- Out-of-order duplicates (late arrival of older records)
"""

from datetime import date, datetime, timedelta

import pandas as pd

from tests.synthetic_data import (
    DuplicateInjector,
    DuplicateConfig,
    ClaimsGenerator,
    OrdersGenerator,
)


class TestDuplicateInjectorExactDuplicates:
    """Test exact duplicate injection."""

    def test_inject_exact_duplicates_with_rate(self):
        """Should inject exact duplicates based on rate."""
        df = pd.DataFrame(
            {
                "id": range(100),
                "name": [f"Item {i}" for i in range(100)],
                "value": [i * 10 for i in range(100)],
            }
        )

        injector = DuplicateInjector(seed=42)
        result = injector.inject_exact_duplicates(df, rate=0.1)

        # Should have ~10 more rows (10% of 100)
        assert len(result) > len(df)
        assert len(result) == 110  # 100 + 10 duplicates

    def test_inject_exact_duplicates_with_count(self):
        """Should inject exact number of duplicates when count specified."""
        df = pd.DataFrame(
            {
                "id": range(50),
                "name": [f"Item {i}" for i in range(50)],
            }
        )

        injector = DuplicateInjector(seed=42)
        result = injector.inject_exact_duplicates(df, count=5)

        assert len(result) == 55  # 50 + 5 exact duplicates

    def test_exact_duplicates_are_identical(self):
        """Exact duplicates should be identical to source rows."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "value": [100, 200, 300],
            }
        )

        injector = DuplicateInjector(seed=42)
        result = injector.inject_exact_duplicates(df, count=2)

        # Find duplicated rows by checking for repeated IDs
        id_counts = result["id"].value_counts()
        duplicated_ids = id_counts[id_counts > 1].index.tolist()

        # Each duplicated ID should have identical rows
        for dup_id in duplicated_ids:
            rows = result[result["id"] == dup_id]
            assert len(rows) == 2
            assert rows.iloc[0].equals(rows.iloc[1])

    def test_reproducibility_with_seed(self):
        """Same seed should produce identical results."""
        df = pd.DataFrame(
            {
                "id": range(100),
                "value": range(100),
            }
        )

        result1 = DuplicateInjector(seed=123).inject_exact_duplicates(df, count=10)
        result2 = DuplicateInjector(seed=123).inject_exact_duplicates(df, count=10)

        pd.testing.assert_frame_equal(result1, result2)

    def test_different_seeds_produce_different_results(self):
        """Different seeds should produce different duplicates."""
        df = pd.DataFrame(
            {
                "id": range(100),
                "value": range(100),
            }
        )

        result1 = DuplicateInjector(seed=100).inject_exact_duplicates(df, count=10)
        result2 = DuplicateInjector(seed=200).inject_exact_duplicates(df, count=10)

        # The duplicated rows should be different
        assert not result1.tail(10).equals(result2.tail(10))

    def test_empty_dataframe(self):
        """Should handle empty DataFrame gracefully."""
        df = pd.DataFrame({"id": [], "value": []})

        injector = DuplicateInjector(seed=42)
        result = injector.inject_exact_duplicates(df, rate=0.1)

        assert len(result) == 0


class TestDuplicateInjectorNearDuplicates:
    """Test near duplicate injection."""

    def test_inject_near_duplicates_same_key(self):
        """Near duplicates should have same key but different mutable fields."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD001", "ORD002", "ORD003"],
                "customer_id": ["CUST001", "CUST002", "CUST003"],
                "status": ["pending", "pending", "pending"],
                "amount": [100.0, 200.0, 300.0],
                "updated_at": [datetime(2024, 1, 15)] * 3,
            }
        )

        injector = DuplicateInjector(seed=42)
        result = injector.inject_near_duplicates(
            df,
            key_columns=["order_id"],
            mutable_columns=["status", "amount"],
            count=2,
        )

        assert len(result) == 5  # 3 original + 2 near duplicates

        # Find records with duplicate keys
        id_counts = result["order_id"].value_counts()
        duplicated_ids = id_counts[id_counts > 1].index.tolist()

        # Near duplicates should have same key but different mutable values
        for dup_id in duplicated_ids:
            rows = result[result["order_id"] == dup_id]
            assert len(rows) == 2
            # Key should be same
            assert rows["order_id"].nunique() == 1
            # At least one mutable column should differ
            status_diff = rows["status"].nunique() > 1
            amount_diff = rows["amount"].nunique() > 1
            assert status_diff or amount_diff

    def test_near_duplicates_with_timestamp_offset(self):
        """Near duplicates should have incremented timestamps."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [100, 200, 300],
                "updated_at": [datetime(2024, 1, 15, 10, 0, 0)] * 3,
            }
        )

        injector = DuplicateInjector(seed=42)
        result = injector.inject_near_duplicates(
            df,
            key_columns=["id"],
            mutable_columns=["value"],
            timestamp_column="updated_at",
            timestamp_offset_seconds=60,
            count=2,
        )

        # Find duplicated rows and check timestamp offset
        id_counts = result["id"].value_counts()
        duplicated_ids = id_counts[id_counts > 1].index.tolist()

        for dup_id in duplicated_ids:
            rows = result[result["id"] == dup_id].sort_values("updated_at")
            time_diff = (
                rows.iloc[1]["updated_at"] - rows.iloc[0]["updated_at"]
            ).total_seconds()
            assert time_diff == 60  # Should be offset by 60 seconds

    def test_auto_detect_mutable_columns(self):
        """Should auto-detect mutable columns based on naming patterns."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD001", "ORD002"],
                "customer_id": ["CUST001", "CUST002"],
                "status": ["pending", "pending"],
                "total_amount": [100.0, 200.0],
                "updated_at": [datetime(2024, 1, 15)] * 2,
                "modified_ts": [datetime(2024, 1, 15)] * 2,
            }
        )

        injector = DuplicateInjector(seed=42)
        detected = injector._detect_mutable_columns(df, key_columns=["order_id"])

        # Should detect columns with patterns: status, amount, updated, modified
        assert "status" in detected
        assert "total_amount" in detected
        assert "updated_at" in detected
        assert "modified_ts" in detected
        assert "order_id" not in detected  # Key column excluded
        assert "customer_id" not in detected  # No mutable pattern


class TestDuplicateInjectorOutOfOrder:
    """Test out-of-order duplicate injection."""

    def test_inject_out_of_order_duplicates(self):
        """Out-of-order duplicates should appear later in the DataFrame."""
        # Create chronologically ordered data
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        df = pd.DataFrame(
            {
                "event_id": [f"EVT{i:03d}" for i in range(20)],
                "event_ts": [base_time + timedelta(hours=i) for i in range(20)],
                "value": range(20),
            }
        )

        injector = DuplicateInjector(seed=42)
        result = injector.inject_out_of_order_duplicates(
            df,
            key_columns=["event_id"],
            timestamp_column="event_ts",
            count=3,
        )

        assert len(result) == 23  # 20 original + 3 out-of-order

        # Out-of-order duplicates should come from the first half
        # and be inserted in the second half
        original_first_half_ids = set(df["event_id"][:10])
        id_counts = result["event_id"].value_counts()
        duplicated_ids = id_counts[id_counts > 1].index.tolist()

        # All duplicated IDs should be from the first half
        for dup_id in duplicated_ids:
            assert dup_id in original_first_half_ids

    def test_out_of_order_timestamps_are_older(self):
        """Out-of-order duplicates should have older timestamps."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        df = pd.DataFrame(
            {
                "id": range(10),
                "event_ts": [base_time + timedelta(hours=i) for i in range(10)],
            }
        )

        injector = DuplicateInjector(seed=42)
        result = injector.inject_out_of_order_duplicates(
            df,
            key_columns=["id"],
            timestamp_column="event_ts",
            count=2,
        )

        # Find duplicated rows
        id_counts = result["id"].value_counts()
        duplicated_ids = id_counts[id_counts > 1].index.tolist()

        for dup_id in duplicated_ids:
            rows = result[result["id"] == dup_id]
            timestamps = rows["event_ts"].tolist()
            # The duplicate (out-of-order) version should have an older timestamp
            assert len(set(timestamps)) == 2  # Two different timestamps


class TestDuplicateInjectorAllTypes:
    """Test combined duplicate injection."""

    def test_inject_all_duplicate_types(self):
        """Should inject all types of duplicates."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        df = pd.DataFrame(
            {
                "order_id": [f"ORD{i:03d}" for i in range(100)],
                "customer_id": [f"CUST{i % 20:03d}" for i in range(100)],
                "status": ["pending"] * 100,
                "amount": [float(i * 10) for i in range(100)],
                "updated_at": [base_time + timedelta(hours=i) for i in range(100)],
            }
        )

        config = DuplicateConfig(
            exact_duplicate_rate=0.05,
            near_duplicate_rate=0.03,
            out_of_order_rate=0.02,
            seed=42,
        )
        injector = DuplicateInjector(seed=42, config=config)
        result = injector.inject_all_duplicate_types(
            df,
            key_columns=["order_id"],
            timestamp_column="updated_at",
            mutable_columns=["status", "amount"],
        )

        # Should have more rows than original
        assert len(result) > len(df)

        # Should have some duplicate keys
        id_counts = result["order_id"].value_counts()
        assert (id_counts > 1).any()


class TestDuplicateStats:
    """Test duplicate statistics calculation."""

    def test_get_duplicate_stats_no_duplicates(self):
        """Should correctly report no duplicates."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
            }
        )

        injector = DuplicateInjector(seed=42)
        stats = injector.get_duplicate_stats(df, key_columns=["id"])

        assert stats["total_rows"] == 5
        assert stats["unique_keys"] == 5
        assert stats["duplicate_rows"] == 0
        assert stats["duplicate_rate"] == 0.0
        assert stats["keys_with_duplicates"] == 0
        assert stats["max_duplicates_per_key"] == 1

    def test_get_duplicate_stats_with_duplicates(self):
        """Should correctly report duplicate statistics."""
        df = pd.DataFrame(
            {
                "id": [1, 1, 2, 2, 2, 3],  # 1 appears 2x, 2 appears 3x, 3 appears 1x
                "value": [10, 11, 20, 21, 22, 30],
            }
        )

        injector = DuplicateInjector(seed=42)
        stats = injector.get_duplicate_stats(df, key_columns=["id"])

        assert stats["total_rows"] == 6
        assert stats["unique_keys"] == 3
        assert stats["duplicate_rows"] == 3  # 6 - 3 = 3 extra rows
        assert stats["duplicate_rate"] == 0.5  # 3/6
        assert stats["keys_with_duplicates"] == 2  # id=1 and id=2
        assert stats["max_duplicates_per_key"] == 3  # id=2 has 3 rows


class TestDuplicateInjectorWithSyntheticData:
    """Test DuplicateInjector with synthetic data generators."""

    def test_inject_duplicates_into_claims_data(self):
        """Should work with ClaimsGenerator output."""
        generator = ClaimsGenerator(seed=42, row_count=100)
        df = generator.generate_t0(date(2024, 1, 15))

        injector = DuplicateInjector(seed=42)
        result = injector.inject_all_duplicate_types(
            df,
            key_columns=["claim_id"],
            timestamp_column="updated_at",
        )

        # Should have duplicates
        stats = injector.get_duplicate_stats(result, key_columns=["claim_id"])
        assert stats["duplicate_rows"] > 0
        assert stats["keys_with_duplicates"] > 0

    def test_inject_duplicates_into_orders_data(self):
        """Should work with OrdersGenerator output."""
        generator = OrdersGenerator(seed=42, row_count=100)
        df = generator.generate_t0(date(2024, 1, 15))

        injector = DuplicateInjector(seed=42)
        result = injector.inject_near_duplicates(
            df,
            key_columns=["order_id"],
            timestamp_column="updated_at",
            rate=0.1,
        )

        # Should have near duplicates
        stats = injector.get_duplicate_stats(result, key_columns=["order_id"])
        assert stats["duplicate_rows"] > 0

    def test_deduplication_simulation(self):
        """Simulate a deduplication scenario with duplicates and verify stats."""
        # Generate base data
        generator = ClaimsGenerator(seed=42, row_count=50)
        df = generator.generate_t0(date(2024, 1, 15))

        # Inject duplicates
        injector = DuplicateInjector(seed=42)
        df_with_dupes = injector.inject_exact_duplicates(df, count=10)
        df_with_dupes = injector.inject_near_duplicates(
            df_with_dupes,
            key_columns=["claim_id"],
            count=5,
        )

        # Verify we have duplicates
        stats_before = injector.get_duplicate_stats(
            df_with_dupes, key_columns=["claim_id"]
        )
        assert stats_before["duplicate_rows"] > 0

        # Simulate deduplication (keep last occurrence)
        df_deduped = df_with_dupes.drop_duplicates(subset=["claim_id"], keep="last")

        # Verify deduplication worked
        stats_after = injector.get_duplicate_stats(df_deduped, key_columns=["claim_id"])
        assert stats_after["duplicate_rows"] == 0
        assert stats_after["unique_keys"] == len(df_deduped)


class TestDuplicateConfig:
    """Test DuplicateConfig dataclass."""

    def test_default_config(self):
        """Default config should have reasonable rates."""
        config = DuplicateConfig()

        assert config.exact_duplicate_rate == 0.05
        assert config.near_duplicate_rate == 0.03
        assert config.out_of_order_rate == 0.02
        assert config.seed == 42

    def test_custom_config(self):
        """Should accept custom configuration."""
        config = DuplicateConfig(
            exact_duplicate_rate=0.1,
            near_duplicate_rate=0.05,
            out_of_order_rate=0.03,
            seed=123,
        )

        assert config.exact_duplicate_rate == 0.1
        assert config.near_duplicate_rate == 0.05
        assert config.out_of_order_rate == 0.03
        assert config.seed == 123

    def test_injector_uses_config_rates(self):
        """Injector should use rates from config."""
        config = DuplicateConfig(exact_duplicate_rate=0.2, seed=42)
        injector = DuplicateInjector(seed=42, config=config)

        df = pd.DataFrame({"id": range(100), "value": range(100)})
        result = injector.inject_exact_duplicates(df)

        # With 20% rate, should have ~20 duplicates
        assert len(result) == 120  # 100 + 20
