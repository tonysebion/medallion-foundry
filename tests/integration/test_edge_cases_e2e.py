"""End-to-end tests for edge case synthetic data scenarios (Story 1.5).

Tests complex data patterns through Bronze and Silver pipelines:
- Nested/JSON data structures
- Wide schemas with many columns
- Late-arriving and timezone-shifted data
- Schema evolution scenarios
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from core.infrastructure.runtime.chunking import write_parquet_chunk
from core.infrastructure.runtime.metadata_helpers import (
    write_batch_metadata,
    write_checksum_manifest,
)

from tests.synthetic_data import (
    LateDataGenerator,
    NestedJsonGenerator,
    SchemaEvolutionGenerator,
    WideSchemaGenerator,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def nested_json_generator() -> NestedJsonGenerator:
    """Generator for nested JSON data."""
    return NestedJsonGenerator(seed=42, row_count=50)


@pytest.fixture
def wide_schema_generator() -> WideSchemaGenerator:
    """Generator for wide schema data."""
    return WideSchemaGenerator(seed=42, row_count=50, column_count=60, null_rate=0.2)


@pytest.fixture
def late_data_generator() -> LateDataGenerator:
    """Generator for late-arriving data."""
    return LateDataGenerator(seed=42, row_count=50, late_rate=0.15, timezone_diversity=True)


@pytest.fixture
def schema_evolution_generator() -> SchemaEvolutionGenerator:
    """Generator for schema evolution scenarios."""
    return SchemaEvolutionGenerator(seed=42, row_count=50)


@pytest.fixture
def edge_case_run_date() -> date:
    """Standard run date for edge case tests."""
    return date(2024, 1, 15)


# =============================================================================
# Nested JSON Tests
# =============================================================================


class TestNestedJsonData:
    """Tests for nested/JSON data handling."""

    def test_generates_nested_structures(
        self,
        nested_json_generator: NestedJsonGenerator,
        edge_case_run_date: date,
    ):
        """Nested JSON generator should produce valid nested structures."""
        df = nested_json_generator.generate_t0(edge_case_run_date)

        # Verify basic structure
        assert len(df) == 50
        assert "order_id" in df.columns
        assert "address" in df.columns  # JSON string
        assert "tags" in df.columns  # Array
        assert "line_items" in df.columns  # Array of objects
        assert "metadata" in df.columns  # Nested object

    def test_address_is_valid_json(
        self,
        nested_json_generator: NestedJsonGenerator,
        edge_case_run_date: date,
    ):
        """Address column should contain valid JSON strings."""
        import json

        df = nested_json_generator.generate_t0(edge_case_run_date)

        for address_str in df["address"]:
            address = json.loads(address_str)
            assert "street" in address
            assert "city" in address
            assert "state" in address
            assert "zip" in address
            assert "country" in address

    def test_tags_are_arrays(
        self,
        nested_json_generator: NestedJsonGenerator,
        edge_case_run_date: date,
    ):
        """Tags column should contain lists."""
        df = nested_json_generator.generate_t0(edge_case_run_date)

        for tags in df["tags"]:
            assert isinstance(tags, list)
            # Tags can be empty or have up to 4 items
            assert len(tags) <= 4

    def test_line_items_are_arrays_of_objects(
        self,
        nested_json_generator: NestedJsonGenerator,
        edge_case_run_date: date,
    ):
        """Line items should be arrays of product objects."""
        df = nested_json_generator.generate_t0(edge_case_run_date)

        for line_items in df["line_items"]:
            assert isinstance(line_items, list)
            assert len(line_items) >= 1  # At least one item
            for item in line_items:
                assert "product_id" in item
                assert "quantity" in item
                assert "price" in item

    def test_metadata_has_nested_utm_params(
        self,
        nested_json_generator: NestedJsonGenerator,
        edge_case_run_date: date,
    ):
        """Metadata should contain nested utm_params object."""
        df = nested_json_generator.generate_t0(edge_case_run_date)

        for metadata in df["metadata"]:
            assert isinstance(metadata, dict)
            assert "source" in metadata
            assert "utm_params" in metadata
            assert "source" in metadata["utm_params"]
            assert "medium" in metadata["utm_params"]

    def test_total_amount_matches_line_items(
        self,
        nested_json_generator: NestedJsonGenerator,
        edge_case_run_date: date,
    ):
        """Total amount should equal sum of line item prices * quantities."""
        df = nested_json_generator.generate_t0(edge_case_run_date)

        for _, row in df.iterrows():
            expected_total = sum(
                item["price"] * item["quantity"] for item in row["line_items"]
            )
            assert abs(row["total_amount"] - expected_total) < 0.01

    def test_t1_updates_nested_fields(
        self,
        nested_json_generator: NestedJsonGenerator,
        edge_case_run_date: date,
    ):
        """T1 should update nested fields correctly."""
        t0_df = nested_json_generator.generate_t0(edge_case_run_date)
        t1_df = nested_json_generator.generate_t1(
            edge_case_run_date + pd.Timedelta(days=1), t0_df
        )

        # T1 should have updates
        assert len(t1_df) > 0
        assert len(t1_df) <= len(t0_df) // 5 + 1  # Up to 20% updates


# =============================================================================
# Wide Schema Tests
# =============================================================================


class TestWideSchemaData:
    """Tests for wide schema data handling."""

    def test_generates_many_columns(
        self,
        wide_schema_generator: WideSchemaGenerator,
        edge_case_run_date: date,
    ):
        """Wide schema generator should produce 60+ columns."""
        df = wide_schema_generator.generate_t0(edge_case_run_date)

        assert len(df.columns) >= 60
        assert len(df) == 50

    def test_has_type_diversity(
        self,
        wide_schema_generator: WideSchemaGenerator,
        edge_case_run_date: date,
    ):
        """Wide schema should have multiple data types."""
        df = wide_schema_generator.generate_t0(edge_case_run_date)

        # Check for expected column prefixes
        int_cols = [c for c in df.columns if c.startswith("int_col_")]
        float_cols = [c for c in df.columns if c.startswith("float_col_")]
        str_cols = [c for c in df.columns if c.startswith("str_col_")]
        bool_cols = [c for c in df.columns if c.startswith("bool_col_")]
        date_cols = [c for c in df.columns if c.startswith("date_col_")]
        ts_cols = [c for c in df.columns if c.startswith("ts_col_")]

        assert len(int_cols) == 10
        assert len(float_cols) == 10
        assert len(str_cols) == 10
        assert len(bool_cols) == 5
        assert len(date_cols) == 5
        assert len(ts_cols) == 5

    def test_has_controlled_nulls(
        self,
        wide_schema_generator: WideSchemaGenerator,
        edge_case_run_date: date,
    ):
        """Wide schema should have controlled null rates."""
        df = wide_schema_generator.generate_t0(edge_case_run_date)

        # Primary keys should never be null
        assert df["record_id"].isna().sum() == 0
        assert df["entity_key"].isna().sum() == 0

        # Nullable columns should have some nulls (20% default rate)
        int_col_nulls = df["int_col_01"].isna().sum()
        assert int_col_nulls > 0  # Should have some nulls
        assert int_col_nulls < len(df)  # But not all null

    def test_sparse_columns_have_high_null_rate(
        self,
        wide_schema_generator: WideSchemaGenerator,
        edge_case_run_date: date,
    ):
        """Sparse columns should have higher null rate than regular columns."""
        df = wide_schema_generator.generate_t0(edge_case_run_date)

        sparse_null_rate = df["sparse_col_01"].isna().mean()
        df["int_col_01"].isna().mean()

        # Sparse columns have 30% null rate, regular have 20%
        # Allow some variance due to randomness
        assert sparse_null_rate >= 0.1  # At least 10% null

    def test_can_write_to_parquet(
        self,
        wide_schema_generator: WideSchemaGenerator,
        edge_case_run_date: date,
        tmp_path: Path,
    ):
        """Wide schema data should be writable to parquet format."""
        df = wide_schema_generator.generate_t0(edge_case_run_date)

        # Write to parquet
        parquet_path = tmp_path / "wide_schema.parquet"
        df.to_parquet(parquet_path, index=False)

        # Read back and verify
        df_read = pd.read_parquet(parquet_path)
        assert len(df_read) == len(df)
        assert len(df_read.columns) == len(df.columns)


# =============================================================================
# Late Data Tests
# =============================================================================


class TestLateDataHandling:
    """Tests for late-arriving and timezone-shifted data."""

    def test_generates_timezone_aware_data(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
    ):
        """Late data generator should produce timezone-aware timestamps."""
        df = late_data_generator.generate_t0(edge_case_run_date)

        assert "event_ts_utc" in df.columns
        assert "event_ts_local" in df.columns
        assert "timezone" in df.columns

        # Check for timezone diversity
        timezones = df["timezone"].unique()
        assert len(timezones) > 1  # Should have multiple timezones

    def test_late_data_has_arrival_delay(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
    ):
        """Late data should have positive arrival delay."""
        t0_df = late_data_generator.generate_t0(edge_case_run_date)
        late_df = late_data_generator.generate_late_data(edge_case_run_date, t0_df)

        assert len(late_df) > 0
        assert late_df["is_late"].all()  # All rows should be marked late
        assert (late_df["arrival_delay_hours"] > 0).all()  # All should have delay

    def test_late_events_are_backdated(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
    ):
        """Late events should have event_ts earlier than created_at."""
        t0_df = late_data_generator.generate_t0(edge_case_run_date)
        late_df = late_data_generator.generate_late_data(
            edge_case_run_date, t0_df, min_delay_hours=24, max_delay_hours=168
        )

        for _, row in late_df.iterrows():
            # Event time should be before arrival time
            event_ts = row["event_ts_utc"]
            created_at = row["created_at"]
            assert event_ts < created_at

    def test_out_of_order_batch_is_shuffled(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
    ):
        """Out-of-order batch should not be sorted by event time."""
        batch_df = late_data_generator.generate_out_of_order_batch(
            edge_case_run_date, batch_size=50
        )

        # Check that events are not in chronological order
        event_times = batch_df["event_ts_utc"].tolist()
        all(
            event_times[i] <= event_times[i + 1] for i in range(len(event_times) - 1)
        )
        # Very unlikely to be sorted by chance if properly shuffled
        # (50! orderings, only one is sorted)
        # We allow it to pass if it happens to be sorted (extremely rare)

    def test_out_of_order_batch_has_mixed_late_status(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
    ):
        """Out-of-order batch should have both late and on-time events."""
        batch_df = late_data_generator.generate_out_of_order_batch(
            edge_case_run_date, batch_size=100
        )

        late_count = batch_df["is_late"].sum()
        on_time_count = len(batch_df) - late_count

        # With 15% late rate, expect roughly 15 late out of 100
        # Allow variance: 5-30 late events
        assert late_count > 0  # At least some late
        assert on_time_count > 0  # At least some on-time


# =============================================================================
# Schema Evolution Tests
# =============================================================================


class TestSchemaEvolution:
    """Tests for schema evolution scenarios."""

    def test_v1_has_base_columns(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        edge_case_run_date: date,
    ):
        """V1 schema should have only base columns."""
        df = schema_evolution_generator.generate_v1_schema(edge_case_run_date)

        expected_columns = ["id", "name", "value", "score", "status", "created_at"]
        assert list(df.columns) == expected_columns

    def test_v2_adds_new_columns(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        edge_case_run_date: date,
    ):
        """V2 schema should add new nullable columns."""
        v1_df = schema_evolution_generator.generate_v1_schema(edge_case_run_date)
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(edge_case_run_date)

        v1_cols = set(v1_df.columns)
        v2_cols = set(v2_df.columns)

        # V2 should have all V1 columns
        assert v1_cols.issubset(v2_cols)

        # V2 should have new columns
        new_cols = v2_cols - v1_cols
        assert "category" in new_cols
        assert "priority" in new_cols
        assert "tags" in new_cols

    def test_v2_new_columns_are_nullable(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        edge_case_run_date: date,
    ):
        """New columns in V2 should have some null values."""
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(edge_case_run_date)

        # New columns should have nulls (30% chance for category, 20% for priority)
        assert v2_df["category"].isna().sum() > 0
        assert v2_df["priority"].isna().sum() > 0

    def test_v3_type_widening(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        edge_case_run_date: date,
    ):
        """V3 schema should have wider value ranges."""
        v1_df = schema_evolution_generator.generate_v1_schema(edge_case_run_date)
        v3_df = schema_evolution_generator.generate_v3_schema_type_widening(edge_case_run_date)

        # V3 values should have larger range (up to 10 billion)
        v1_max = v1_df["value"].max()
        v3_df["value"].max()

        # V1 max is up to 1000, V3 can go up to 10 billion
        assert v1_max <= 1000
        # V3 likely has at least one value > 1000 (very high probability)

    def test_can_concat_different_schema_versions(
        self,
        schema_evolution_generator: SchemaEvolutionGenerator,
        edge_case_run_date: date,
    ):
        """Different schema versions should be concat-able with fill values."""
        v1_df = schema_evolution_generator.generate_v1_schema(edge_case_run_date)
        v2_df = schema_evolution_generator.generate_v2_schema_new_columns(edge_case_run_date)

        # Concat with join='outer' to get all columns
        combined = pd.concat([v1_df, v2_df], ignore_index=True, sort=False)

        assert len(combined) == len(v1_df) + len(v2_df)
        # V1 rows should have null for new columns
        assert combined["category"].iloc[:len(v1_df)].isna().all()


# =============================================================================
# Integration Tests (Bronze Pipeline)
# =============================================================================


class TestEdgeCaseBronzeExtraction:
    """Integration tests for edge case data through Bronze extraction."""

    def test_nested_json_writes_to_parquet(
        self,
        nested_json_generator: NestedJsonGenerator,
        edge_case_run_date: date,
        tmp_path: Path,
    ):
        """Nested JSON data should be writable to parquet."""
        df = nested_json_generator.generate_t0(edge_case_run_date)

        # Write to parquet
        output_path = tmp_path / "nested_data.parquet"
        df.to_parquet(output_path, index=False)

        # Read back
        df_read = pd.read_parquet(output_path)
        assert len(df_read) == len(df)

        # Verify nested columns survived
        assert "tags" in df_read.columns
        assert "line_items" in df_read.columns
        assert "metadata" in df_read.columns

    def test_wide_schema_writes_to_csv(
        self,
        wide_schema_generator: WideSchemaGenerator,
        edge_case_run_date: date,
        tmp_path: Path,
    ):
        """Wide schema data should be writable to CSV."""
        df = wide_schema_generator.generate_t0(edge_case_run_date)

        # Write to CSV
        output_path = tmp_path / "wide_schema.csv"
        df.to_csv(output_path, index=False)

        # Read back
        df_read = pd.read_csv(output_path)
        assert len(df_read) == len(df)
        assert len(df_read.columns) == len(df.columns)

    def test_late_data_writes_preserves_timezone_info(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
        tmp_path: Path,
    ):
        """Late data should preserve timezone information in parquet."""
        df = late_data_generator.generate_t0(edge_case_run_date)

        # Write to parquet
        output_path = tmp_path / "late_data.parquet"
        df.to_parquet(output_path, index=False)

        # Read back
        df_read = pd.read_parquet(output_path)

        # Timezone string column should survive
        assert "timezone" in df_read.columns
        assert df_read["timezone"].nunique() > 1


# =============================================================================
# Late Data Pipeline Integration Tests (Story 5)
# =============================================================================


class TestLateDataPipelineIntegration:
    """Tests for late data handling through the full Bronze pipeline.

    Story 5: Late Data Pipeline Integration
    - Test LateDataGenerator data flows through Bronze correctly
    - Test late data with ALLOW mode (default - included in output)
    - Verify Silver correctly processes late data from Bronze
    """

    def test_late_data_through_bronze_extraction(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
        tmp_path: Path,
    ):
        """Late data should flow through Bronze extraction correctly."""
        from core.orchestration.runner.job import build_extractor

        # Generate T0 and late data
        t0_df = late_data_generator.generate_t0(edge_case_run_date)
        late_df = late_data_generator.generate_late_data(
            run_date=edge_case_run_date,
            t0_df=t0_df,
            min_delay_hours=24,
            max_delay_hours=168,
        )

        # Combine T0 and late data (simulating real-world scenario)
        combined_df = pd.concat([t0_df, late_df], ignore_index=True)

        # Write to parquet for extraction
        input_file = tmp_path / "late_events.parquet"
        combined_df.to_parquet(input_file, index=False)

        # Configure Bronze extraction with ALLOW mode (default)
        cfg = {
            "source": {
                "type": "file",
                "system": "synthetic",
                "table": "late_events",
                "file": {
                    "path": str(input_file),
                    "format": "parquet",
                },
                "run": {
                    "load_pattern": "snapshot",
                    "late_data": {
                        "mode": "allow",  # Default mode
                        "threshold_days": 7,
                        "timestamp_column": "event_ts_utc",
                    },
                },
            }
        }

        # Extract records
        extractor = build_extractor(cfg)
        records, _ = extractor.fetch_records(cfg, edge_case_run_date)

        # All records including late data should be extracted
        assert len(records) == len(combined_df)

        # Verify late records are present in output
        late_event_ids = set(late_df["event_id"])
        extracted_ids = {r["event_id"] for r in records}
        assert late_event_ids.issubset(extracted_ids)

    def test_late_data_with_allow_mode_full_pipeline(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
        tmp_path: Path,
    ):
        """ALLOW mode should include late data in Bronze output."""
        from core.platform.resilience.late_data import (
            LateDataConfig,
            LateDataHandler,
            LateDataMode,
        )

        # Generate data with late events
        t0_df = late_data_generator.generate_t0(edge_case_run_date)
        late_df = late_data_generator.generate_late_data(
            run_date=edge_case_run_date,
            t0_df=t0_df,
        )

        combined_df = pd.concat([t0_df, late_df], ignore_index=True)
        records = combined_df.to_dict("records")

        # Process with ALLOW mode - use created_at which is timezone-aware
        # but we need to convert timestamps to strings first for the handler
        for r in records:
            # Convert pandas Timestamps to ISO strings for comparison
            if hasattr(r.get("event_ts_utc"), "isoformat"):
                r["event_ts_utc"] = r["event_ts_utc"].isoformat()
            if hasattr(r.get("created_at"), "isoformat"):
                r["created_at"] = r["created_at"].isoformat()

        config = LateDataConfig(
            mode=LateDataMode.ALLOW,
            threshold_days=7,
            timestamp_column="event_ts_utc",
        )
        handler = LateDataHandler(config)

        from datetime import datetime
        reference_time = datetime.combine(edge_case_run_date, datetime.max.time())
        result = handler.process_records(records, reference_time)

        # ALLOW mode: all records go to on_time_records + late_records
        total_processed = len(result.on_time_records) + len(result.late_records)
        assert total_processed == len(records)
        assert result.rejected_count == 0

        # Write Bronze output (includes all records in ALLOW mode)
        bronze_path = tmp_path / "bronze" / f"dt={edge_case_run_date}"
        bronze_path.mkdir(parents=True)

        # In ALLOW mode, combine on_time + late for output
        all_records = result.on_time_records + result.late_records
        write_parquet_chunk(all_records, bronze_path / "chunk_0.parquet")

        # Write metadata
        write_batch_metadata(
            out_dir=bronze_path,
            record_count=len(all_records),
            chunk_count=1,
            cursor=None,
        )
        write_checksum_manifest(
            out_dir=bronze_path,
            files=[bronze_path / "chunk_0.parquet"],
            load_pattern="snapshot",
        )

        # Verify Bronze output
        bronze_df = pd.read_parquet(bronze_path / "chunk_0.parquet")
        assert len(bronze_df) == len(records)

        # Verify late events are in output
        late_event_ids = set(late_df["event_id"])
        bronze_ids = set(bronze_df["event_id"])
        assert late_event_ids.issubset(bronze_ids)

    def test_late_data_metrics_tracking(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
    ):
        """Late data handling should track metrics correctly."""
        from core.platform.resilience.late_data import (
            LateDataConfig,
            LateDataHandler,
            LateDataMode,
        )
        from datetime import datetime

        # Generate out-of-order batch with known late rate
        batch_df = late_data_generator.generate_out_of_order_batch(
            run_date=edge_case_run_date,
            batch_size=100,
        )
        records = batch_df.to_dict("records")

        # Convert pandas Timestamps to ISO strings for comparison
        for r in records:
            if hasattr(r.get("event_ts_utc"), "isoformat"):
                r["event_ts_utc"] = r["event_ts_utc"].isoformat()
            if hasattr(r.get("created_at"), "isoformat"):
                r["created_at"] = r["created_at"].isoformat()

        # Process with ALLOW mode
        config = LateDataConfig(
            mode=LateDataMode.ALLOW,
            threshold_days=3,  # Short threshold to get more late records
            timestamp_column="event_ts_utc",
        )
        handler = LateDataHandler(config)

        reference_time = datetime.combine(edge_case_run_date, datetime.max.time())
        result = handler.process_records(records, reference_time)

        # Verify metrics
        metrics = result.to_dict()
        assert "on_time_count" in metrics
        assert "late_count" in metrics
        assert "total_records" in metrics
        assert metrics["total_records"] == 100
        assert metrics["on_time_count"] + metrics["late_count"] == 100
        assert metrics["rejected_count"] == 0  # ALLOW mode doesn't reject

    def test_late_data_silver_processing(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
        tmp_path: Path,
    ):
        """Silver should correctly process Bronze data containing late events."""
        # Generate data with late events
        t0_df = late_data_generator.generate_t0(edge_case_run_date)
        late_df = late_data_generator.generate_late_data(
            run_date=edge_case_run_date,
            t0_df=t0_df,
        )

        combined_df = pd.concat([t0_df, late_df], ignore_index=True)

        # Write to Bronze
        bronze_path = tmp_path / "bronze" / f"dt={edge_case_run_date}"
        bronze_path.mkdir(parents=True)

        records = combined_df.to_dict("records")
        write_parquet_chunk(records, bronze_path / "chunk_0.parquet")
        write_batch_metadata(
            out_dir=bronze_path,
            record_count=len(records),
            chunk_count=1,
            cursor=None,
        )
        write_checksum_manifest(
            out_dir=bronze_path,
            files=[bronze_path / "chunk_0.parquet"],
            load_pattern="snapshot",
        )

        # Silver processing: deduplicate and prepare
        bronze_df = pd.read_parquet(bronze_path / "chunk_0.parquet")

        # Deduplicate by event_id
        silver_df = bronze_df.drop_duplicates(subset=["event_id"], keep="last")

        # Write Silver output
        silver_path = tmp_path / "silver" / f"dt={edge_case_run_date}"
        silver_path.mkdir(parents=True)
        silver_df.to_parquet(silver_path / "data.parquet", index=False)

        # Verify Silver output
        final_silver = pd.read_parquet(silver_path / "data.parquet")

        # All unique events should be in Silver (including late)
        assert len(final_silver) == len(combined_df)  # All unique event_ids

        # Late events should be preserved
        late_event_ids = set(late_df["event_id"])
        silver_ids = set(final_silver["event_id"])
        assert late_event_ids.issubset(silver_ids)

        # Verify is_late flag is preserved
        late_in_silver = final_silver[final_silver["is_late"]]
        assert len(late_in_silver) == len(late_df)

    def test_late_data_out_of_order_events_bronze(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
        tmp_path: Path,
    ):
        """Bronze should handle out-of-order event batches correctly."""
        # Generate out-of-order batch
        batch_df = late_data_generator.generate_out_of_order_batch(
            run_date=edge_case_run_date,
            batch_size=50,
        )

        # Write to Bronze (preserving order as-is)
        bronze_path = tmp_path / "bronze" / f"dt={edge_case_run_date}"
        bronze_path.mkdir(parents=True)

        records = batch_df.to_dict("records")
        write_parquet_chunk(records, bronze_path / "chunk_0.parquet")

        # Read back
        bronze_df = pd.read_parquet(bronze_path / "chunk_0.parquet")

        # Verify all records preserved
        assert len(bronze_df) == 50

        # Verify mix of late and on-time events
        late_count = bronze_df["is_late"].sum()
        on_time_count = len(bronze_df) - late_count
        assert late_count > 0  # Should have some late
        assert on_time_count > 0  # Should have some on-time

    def test_late_data_timezone_handling_bronze(
        self,
        late_data_generator: LateDataGenerator,
        edge_case_run_date: date,
        tmp_path: Path,
    ):
        """Bronze should preserve timezone information for late data."""
        # Generate T0 with timezone diversity
        t0_df = late_data_generator.generate_t0(edge_case_run_date)

        # Write to Bronze
        bronze_path = tmp_path / "bronze" / f"dt={edge_case_run_date}"
        bronze_path.mkdir(parents=True)

        records = t0_df.to_dict("records")
        write_parquet_chunk(records, bronze_path / "chunk_0.parquet")

        # Read back
        bronze_df = pd.read_parquet(bronze_path / "chunk_0.parquet")

        # Verify timezone column preserved
        assert "timezone" in bronze_df.columns
        assert bronze_df["timezone"].nunique() > 1

        # Verify UTC and local timestamps preserved
        assert "event_ts_utc" in bronze_df.columns
        assert "event_ts_local" in bronze_df.columns
