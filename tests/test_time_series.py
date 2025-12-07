"""Tests for T0/T1/T2 time series scenarios per spec Section 10.

T0: Initial full load
T1: First incremental load (inserts + updates)
T2: Late data / backfill scenarios
"""

from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from tests.synthetic_data import (
    ClaimsGenerator,
    OrdersGenerator,
    TransactionsGenerator,
    StateChangeGenerator,
    generate_time_series_data,
)
from core.platform.resilience import LateDataHandler, LateDataMode, LateDataConfig


class TestSyntheticDataGenerators:
    """Test synthetic data generators produce valid data."""

    def test_claims_generator_t0(self):
        """ClaimsGenerator should produce valid T0 data."""
        generator = ClaimsGenerator(seed=42, row_count=100)
        df = generator.generate_t0(date(2024, 1, 15))

        assert len(df) == 100
        assert "claim_id" in df.columns
        assert "patient_id" in df.columns
        assert "billed_amount" in df.columns
        assert df["claim_id"].is_unique

    def test_claims_generator_t1(self):
        """ClaimsGenerator should produce valid T1 incremental data."""
        generator = ClaimsGenerator(seed=42, row_count=100)
        t0_df = generator.generate_t0(date(2024, 1, 15))
        t1_df = generator.generate_t1(date(2024, 1, 16), t0_df)

        # T1 should have updates and new records
        assert len(t1_df) > 0

        # Some records should be updates (existing claim_ids)
        existing_ids = set(t0_df["claim_id"])
        t1_ids = set(t1_df["claim_id"])
        updates = existing_ids & t1_ids
        inserts = t1_ids - existing_ids

        assert len(updates) > 0, "T1 should include updates"
        assert len(inserts) > 0, "T1 should include new records"

    def test_claims_generator_t2_late(self):
        """ClaimsGenerator should produce valid T2 late data."""
        generator = ClaimsGenerator(seed=42, row_count=100)
        t0_df = generator.generate_t0(date(2024, 1, 15))
        t2_df = generator.generate_t2_late(date(2024, 1, 17), t0_df)

        assert len(t2_df) > 0
        # Late data should have service dates before T0
        assert all(t2_df["service_date"] < date(2024, 1, 15))

    def test_orders_generator_t0(self):
        """OrdersGenerator should produce valid T0 data."""
        generator = OrdersGenerator(seed=42, row_count=100)
        df = generator.generate_t0(date(2024, 1, 15))

        assert len(df) == 100
        assert "order_id" in df.columns
        assert "customer_id" in df.columns
        assert "total_amount" in df.columns

    def test_orders_generator_t1(self):
        """OrdersGenerator should produce valid T1 data."""
        generator = OrdersGenerator(seed=42, row_count=100)
        t0_df = generator.generate_t0(date(2024, 1, 15))
        t1_df = generator.generate_t1(date(2024, 1, 16), t0_df)

        assert len(t1_df) > 0
        # Status updates should progress orders through pipeline
        existing_ids = set(t0_df["order_id"])
        t1_ids = set(t1_df["order_id"])
        updates = existing_ids & t1_ids
        assert len(updates) > 0

    def test_transactions_generator_t0(self):
        """TransactionsGenerator should produce valid T0 data."""
        generator = TransactionsGenerator(seed=42, row_count=100)
        df = generator.generate_t0(date(2024, 1, 15))

        assert len(df) == 100
        assert "transaction_id" in df.columns
        assert "amount" in df.columns
        # Should have both positive (credit) and negative (debit) amounts
        assert (df["amount"] > 0).any()
        assert (df["amount"] < 0).any()

    def test_state_change_generator(self):
        """StateChangeGenerator should produce SCD history data."""
        generator = StateChangeGenerator(seed=42)
        df = generator.generate_scd_history(
            entity_count=10,
            changes_per_entity=3,
            run_date=date(2024, 1, 15),
        )

        # Should have 10 entities * 3 versions = 30 rows
        assert len(df) == 30
        assert "entity_id" in df.columns
        assert "version" in df.columns
        assert "effective_from" in df.columns

        # Each entity should have multiple versions
        version_counts = df.groupby("entity_id")["version"].count()
        assert (version_counts == 3).all()


class TestTimeSeriesDataGeneration:
    """Test the generate_time_series_data helper."""

    def test_generate_claims_time_series(self):
        """Should generate complete claims time series."""
        data = generate_time_series_data("claims", date(2024, 1, 15), row_count=100)

        assert "t0" in data
        assert "t1" in data
        assert "t2_late" in data

        assert len(data["t0"]) == 100
        assert len(data["t1"]) > 0
        assert len(data["t2_late"]) > 0

    def test_generate_orders_time_series(self):
        """Should generate complete orders time series."""
        data = generate_time_series_data("orders", date(2024, 1, 15), row_count=50)

        assert "t0" in data
        assert "t1" in data
        assert len(data["t0"]) == 50

    def test_reproducibility_with_seed(self):
        """Same seed should produce identical data."""
        data1 = generate_time_series_data("claims", date(2024, 1, 15), seed=123)
        data2 = generate_time_series_data("claims", date(2024, 1, 15), seed=123)

        pd.testing.assert_frame_equal(data1["t0"], data2["t0"])

    def test_different_seeds_produce_different_data(self):
        """Different seeds should produce different data."""
        data1 = generate_time_series_data("claims", date(2024, 1, 15), seed=100)
        data2 = generate_time_series_data("claims", date(2024, 1, 15), seed=200)

        # Claim IDs should be same (deterministic naming)
        # But other values should differ
        assert not data1["t0"]["billed_amount"].equals(data2["t0"]["billed_amount"])


class TestLateDataHandling:
    """Test late data handling per spec Section 4."""

    def test_late_data_allow_mode(self):
        """ALLOW mode should include all records."""
        records = [
            {"id": 1, "event_ts": datetime(2024, 1, 1)},  # Late
            {"id": 2, "event_ts": datetime(2024, 1, 10)},  # On time
            {"id": 3, "event_ts": datetime(2024, 1, 14)},  # On time
        ]
        reference_time = datetime(2024, 1, 15)

        config = LateDataConfig(
            mode=LateDataMode.ALLOW,
            threshold_days=7,
        )
        handler = LateDataHandler(config)
        result = handler.process_records(records, reference_time, "event_ts")

        # ALLOW mode keeps all records (on_time + late)
        total_records = len(result.on_time_records) + len(result.late_records)
        assert total_records == 3
        assert result.rejected_count == 0
        assert result.quarantined_count == 0

    def test_late_data_reject_mode(self):
        """REJECT mode should raise exception for late records."""
        records = [
            {"id": 1, "event_ts": datetime(2024, 1, 1)},  # Late (>7 days)
            {"id": 2, "event_ts": datetime(2024, 1, 10)},  # On time
            {"id": 3, "event_ts": datetime(2024, 1, 14)},  # On time
        ]
        reference_time = datetime(2024, 1, 15)

        config = LateDataConfig(
            mode=LateDataMode.REJECT,
            threshold_days=7,
        )
        handler = LateDataHandler(config)

        # REJECT mode raises ValueError when late data is found
        with pytest.raises(ValueError, match="Late data rejected"):
            handler.process_records(records, reference_time, "event_ts")

    def test_late_data_quarantine_mode(self):
        """QUARANTINE mode should separate late records."""
        records = [
            {"id": 1, "event_ts": datetime(2024, 1, 1)},  # Late
            {"id": 2, "event_ts": datetime(2024, 1, 10)},  # On time
        ]
        reference_time = datetime(2024, 1, 15)

        config = LateDataConfig(
            mode=LateDataMode.QUARANTINE,
            threshold_days=7,
        )
        handler = LateDataHandler(config)
        result = handler.process_records(records, reference_time, "event_ts")

        assert len(result.on_time_records) == 1
        assert len(result.late_records) == 1
        assert result.quarantined_count == 1
        assert result.late_records[0]["id"] == 1

    def test_late_data_threshold_boundary(self):
        """Records exactly at threshold should be accepted."""
        reference_time = datetime(2024, 1, 15)
        threshold_boundary = reference_time - timedelta(days=7)

        records = [
            {"id": 1, "event_ts": threshold_boundary},  # Exactly at threshold
            {"id": 2, "event_ts": threshold_boundary - timedelta(seconds=1)},  # Just late
        ]

        config = LateDataConfig(
            mode=LateDataMode.QUARANTINE,  # Use QUARANTINE to see the result
            threshold_days=7,
        )
        handler = LateDataHandler(config)
        result = handler.process_records(records, reference_time, "event_ts")

        # Exactly at threshold should be on-time
        assert len(result.on_time_records) == 1
        assert result.on_time_records[0]["id"] == 1


class TestIncrementalMergeScenarios:
    """Test INCREMENTAL_MERGE behavior across time series."""

    def test_t0_to_t1_merge(self, temp_dir):
        """T1 data should merge correctly with T0."""
        from core.domain.services.pipelines.bronze.io import merge_parquet_records

        # T0 data
        t0_data = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [100, 200, 300],
        })
        t0_path = temp_dir / "data.parquet"
        t0_data.to_parquet(t0_path, index=False)

        # T1 data (updates and inserts)
        t1_records = [
            {"id": 2, "name": "B Updated", "value": 250},  # Update
            {"id": 4, "name": "D", "value": 400},  # Insert
        ]

        count = merge_parquet_records(t0_path, t1_records, primary_keys=["id"])
        result = pd.read_parquet(t0_path)

        assert count == 4
        assert len(result) == 4
        assert result[result["id"] == 2]["name"].values[0] == "B Updated"
        assert 4 in result["id"].values

    def test_late_data_merge(self, temp_dir):
        """Late data (T2) should merge without duplicates."""
        from core.domain.services.pipelines.bronze.io import merge_parquet_records

        # Current state after T0+T1
        current_data = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "value": [100, 250, 300, 400],
        })
        data_path = temp_dir / "data.parquet"
        current_data.to_parquet(data_path, index=False)

        # Late data - record that should have been in T0
        late_records = [
            {"id": 5, "value": 500},  # New late record
            {"id": 3, "value": 350},  # Correction to existing
        ]

        count = merge_parquet_records(data_path, late_records, primary_keys=["id"])
        result = pd.read_parquet(data_path)

        assert count == 5
        assert result[result["id"] == 3]["value"].values[0] == 350
        assert 5 in result["id"].values


class TestSCDHistoryPatterns:
    """Test SCD (Slowly Changing Dimension) history patterns."""

    def test_scd2_history_generation(self):
        """SCD2 should track full history with effective dates."""
        generator = StateChangeGenerator(seed=42)
        df = generator.generate_scd_history(
            entity_count=5,
            changes_per_entity=4,
            run_date=date(2024, 1, 15),
        )

        # Each entity should have 4 versions
        assert len(df) == 20

        # Check effective dates are sequential for each entity
        for entity_id in df["entity_id"].unique():
            entity_df = df[df["entity_id"] == entity_id].sort_values("version")
            dates = entity_df["effective_from"].tolist()
            # Each subsequent date should be >= previous
            for i in range(1, len(dates)):
                assert dates[i] >= dates[i - 1]

    def test_scd2_current_record_identification(self):
        """Should identify current (latest) record for each entity."""
        generator = StateChangeGenerator(seed=42)
        df = generator.generate_scd_history(entity_count=10, changes_per_entity=3)

        # Get current record for each entity (max version)
        current = df.loc[df.groupby("entity_id")["version"].idxmax()]

        assert len(current) == 10
        assert (current["version"] == 3).all()
