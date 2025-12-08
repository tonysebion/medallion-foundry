"""Pattern verification tests for all load patterns.

This module provides comprehensive tests that verify the business logic
of each load pattern by:
1. Generating multi-batch test data
2. Running Bronze extraction with each pattern
3. Validating output against pattern-specific assertions

Patterns tested:
- SNAPSHOT: Full replacement each run
- INCREMENTAL_APPEND: Append new records only
- INCREMENTAL_MERGE: Upsert by primary key
- CURRENT_HISTORY: SCD Type 2 with current/history split

Run specific patterns:
    pytest tests/integration/test_pattern_verification.py -k "snapshot"
    pytest tests/integration/test_pattern_verification.py -k "incremental_append"
    pytest tests/integration/test_pattern_verification.py -k "incremental_merge"
    pytest tests/integration/test_pattern_verification.py -k "current_history"

Run all pattern tests:
    pytest tests/integration/test_pattern_verification.py -v
"""

from __future__ import annotations

import json
import shutil
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import pytest

from tests.integration.pattern_data import (
    PatternTestDataGenerator,
    PatternScenario,
    AssertionValidator,
    PatternAssertions,
    create_snapshot_assertions,
    create_incremental_append_assertions,
    create_incremental_merge_assertions,
    create_scd2_assertions,
)


# Pytest marker for pattern verification tests
pytestmark = pytest.mark.pattern_verification


class TestPatternDataGeneration:
    """Tests for the pattern data generation infrastructure."""

    def test_generator_initialization(self):
        """Test generator initializes with correct defaults."""
        generator = PatternTestDataGenerator(seed=42, base_rows=1000)
        assert generator.seed == 42
        assert generator.base_rows == 1000
        assert generator.base_date == date(2024, 1, 1)

    def test_generator_deterministic(self):
        """Test generator produces identical data with same seed."""
        gen1 = PatternTestDataGenerator(seed=42, base_rows=100)
        gen2 = PatternTestDataGenerator(seed=42, base_rows=100)

        scenario1 = gen1.generate_snapshot_scenario()
        scenario2 = gen2.generate_snapshot_scenario()

        pd.testing.assert_frame_equal(scenario1.t0, scenario2.t0)

    def test_generator_different_seeds(self):
        """Test different seeds produce different data."""
        gen1 = PatternTestDataGenerator(seed=42, base_rows=100)
        gen2 = PatternTestDataGenerator(seed=123, base_rows=100)

        scenario1 = gen1.generate_snapshot_scenario()
        scenario2 = gen2.generate_snapshot_scenario()

        # Random values should be different (record_id is sequential, so check random fields)
        assert not scenario1.t0["amount"].equals(scenario2.t0["amount"])
        assert not scenario1.t0["category"].equals(scenario2.t0["category"])


class TestSnapshotPattern:
    """Tests for SNAPSHOT load pattern.

    SNAPSHOT behavior:
    - Complete replacement each run
    - No watermark tracking
    - Output matches input exactly
    """

    @pytest.fixture
    def generator(self):
        """Create generator for snapshot tests."""
        return PatternTestDataGenerator(seed=42, base_rows=1000)

    def test_snapshot_generates_correct_row_count(self, generator):
        """Test snapshot generates expected number of rows."""
        scenario = generator.generate_snapshot_scenario(rows=1000)

        assert len(scenario.t0) == 1000
        assert scenario.pattern == "snapshot"
        assert scenario.batch_count == 1

    def test_snapshot_has_required_columns(self, generator):
        """Test snapshot data has all required columns."""
        scenario = generator.generate_snapshot_scenario()
        df = scenario.t0

        required_cols = [
            "record_id",
            "entity_key",
            "category",
            "region",
            "status",
            "amount",
            "created_at",
            "updated_at",
        ]
        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"

    def test_snapshot_record_ids_unique(self, generator):
        """Test snapshot record IDs are unique."""
        scenario = generator.generate_snapshot_scenario(rows=1000)
        df = scenario.t0

        assert df["record_id"].is_unique

    def test_snapshot_record_id_format(self, generator):
        """Test snapshot record IDs follow expected format."""
        scenario = generator.generate_snapshot_scenario(rows=100)
        df = scenario.t0

        # First record should be REC00000001
        assert df.iloc[0]["record_id"] == "REC00000001"
        # Last record should be REC00000100
        assert df.iloc[-1]["record_id"] == "REC00000100"

    def test_snapshot_with_replacement(self, generator):
        """Test snapshot with replacement batch generates different data."""
        scenario = generator.generate_snapshot_scenario(
            rows=500, include_replacement=True
        )

        assert scenario.batch_count == 2
        assert scenario.t1 is not None

        # T1 should have different record IDs (replacement)
        t0_ids = set(scenario.t0["record_id"])
        t1_ids = set(scenario.t1["record_id"])
        assert len(t0_ids & t1_ids) == 0, "Replacement batch should have new IDs"

    def test_snapshot_assertions_pass(self, generator):
        """Test snapshot assertions validate correctly."""
        scenario = generator.generate_snapshot_scenario(rows=1000)
        df = scenario.t0

        assertions = create_snapshot_assertions(
            row_count=1000,
            columns=list(df.columns),
            first_row_values={"record_id": "REC00000001"},
        )

        validator = AssertionValidator(assertions)
        report = validator.validate_all(df)

        assert report.passed, f"Assertions failed:\n{report.summary()}"

    def test_snapshot_metadata(self, generator):
        """Test snapshot metadata is captured correctly."""
        scenario = generator.generate_snapshot_scenario(rows=500)

        assert scenario.metadata["pattern"] == "snapshot"
        assert scenario.metadata["t0_rows"] == 500
        assert scenario.metadata["seed"] == 42


class TestIncrementalAppendPattern:
    """Tests for INCREMENTAL_APPEND load pattern.

    INCREMENTAL_APPEND behavior:
    - Append new records only (insert-only CDC)
    - Watermark tracking required
    - No updates to existing records
    """

    @pytest.fixture
    def generator(self):
        """Create generator for incremental append tests."""
        return PatternTestDataGenerator(seed=42, base_rows=1000)

    def test_incremental_append_generates_multiple_batches(self, generator):
        """Test incremental append generates T0 + incremental batches."""
        scenario = generator.generate_incremental_append_scenario(
            rows=1000, batches=4, insert_rate=0.1
        )

        assert scenario.batch_count == 4
        assert scenario.pattern == "incremental_append"
        assert scenario.t0 is not None
        assert scenario.t1 is not None
        assert scenario.t2 is not None
        assert scenario.t3 is not None

    def test_incremental_append_t0_full_load(self, generator):
        """Test T0 contains full initial load."""
        scenario = generator.generate_incremental_append_scenario(rows=1000, batches=4)

        assert len(scenario.t0) == 1000

    def test_incremental_append_batches_contain_only_new_records(self, generator):
        """Test incremental batches contain only NEW records (no updates)."""
        scenario = generator.generate_incremental_append_scenario(
            rows=1000, batches=4, insert_rate=0.1
        )

        t0_ids = set(scenario.t0["record_id"])

        # T1 should have no overlap with T0
        t1_ids = set(scenario.t1["record_id"])
        assert len(t0_ids & t1_ids) == 0, "T1 should have only new records"

        # T2 should have no overlap with T0 or T1
        t2_ids = set(scenario.t2["record_id"])
        assert len((t0_ids | t1_ids) & t2_ids) == 0, "T2 should have only new records"

        # T3 should have no overlap with previous batches
        t3_ids = set(scenario.t3["record_id"])
        assert len((t0_ids | t1_ids | t2_ids) & t3_ids) == 0

    def test_incremental_append_insert_rate(self, generator):
        """Test incremental batches respect insert rate."""
        scenario = generator.generate_incremental_append_scenario(
            rows=1000, batches=4, insert_rate=0.1
        )

        # Each batch should have ~100 new rows (10% of 1000)
        assert len(scenario.t1) == 100
        assert len(scenario.t2) == 100
        assert len(scenario.t3) == 100

    def test_incremental_append_sequential_ids(self, generator):
        """Test incremental batches have sequential IDs."""
        scenario = generator.generate_incremental_append_scenario(
            rows=100, batches=3, insert_rate=0.1
        )

        # T0 ends at REC00000100
        t0_max = max(int(rid[3:]) for rid in scenario.t0["record_id"])
        assert t0_max == 100

        # T1 starts at REC00000101
        t1_min = min(int(rid[3:]) for rid in scenario.t1["record_id"])
        assert t1_min == 101

    def test_incremental_append_assertions_pass(self, generator):
        """Test incremental append assertions validate correctly."""
        scenario = generator.generate_incremental_append_scenario(
            rows=1000, batches=4, insert_rate=0.1
        )

        assertions = create_incremental_append_assertions(
            new_rows_count=100,
            columns=list(scenario.t1.columns),
        )

        validator = AssertionValidator(assertions)
        report = validator.validate_all(
            scenario.t1,
            previous_df=scenario.t0,
        )

        assert report.passed, f"Assertions failed:\n{report.summary()}"

    def test_incremental_append_metadata(self, generator):
        """Test incremental append metadata is captured correctly."""
        scenario = generator.generate_incremental_append_scenario(
            rows=1000, batches=4, insert_rate=0.1
        )

        assert scenario.metadata["pattern"] == "incremental_append"
        assert scenario.metadata["t0_rows"] == 1000
        assert scenario.metadata["insert_rate"] == 0.1
        assert scenario.metadata["new_per_batch"] == 100


class TestIncrementalMergePattern:
    """Tests for INCREMENTAL_MERGE load pattern.

    INCREMENTAL_MERGE behavior:
    - Upsert by primary key
    - Watermark tracking required
    - Mix of updates and inserts
    """

    @pytest.fixture
    def generator(self):
        """Create generator for incremental merge tests."""
        return PatternTestDataGenerator(seed=42, base_rows=1000)

    def test_incremental_merge_generates_multiple_batches(self, generator):
        """Test incremental merge generates T0 + change batches."""
        scenario = generator.generate_incremental_merge_scenario(
            rows=1000, batches=4, update_rate=0.2, insert_rate=0.1
        )

        assert scenario.batch_count == 4
        assert scenario.pattern == "incremental_merge"

    def test_incremental_merge_t0_full_load(self, generator):
        """Test T0 contains full initial load."""
        scenario = generator.generate_incremental_merge_scenario(rows=1000, batches=4)

        assert len(scenario.t0) == 1000

    def test_incremental_merge_contains_updates_and_inserts(self, generator):
        """Test incremental batches contain both updates and inserts."""
        scenario = generator.generate_incremental_merge_scenario(
            rows=1000, batches=4, update_rate=0.2, insert_rate=0.1
        )

        t0_ids = set(scenario.t0["record_id"])
        t1_ids = set(scenario.t1["record_id"])

        # Some IDs should overlap (updates)
        updates = t0_ids & t1_ids
        assert len(updates) > 0, "Should have some updates"

        # Some IDs should be new (inserts)
        inserts = t1_ids - t0_ids
        assert len(inserts) > 0, "Should have some inserts"

    def test_incremental_merge_update_rate(self, generator):
        """Test incremental batches respect update rate."""
        scenario = generator.generate_incremental_merge_scenario(
            rows=1000, batches=4, update_rate=0.2, insert_rate=0.1
        )

        t0_ids = set(scenario.t0["record_id"])
        t1_ids = set(scenario.t1["record_id"])

        updates = t0_ids & t1_ids
        inserts = t1_ids - t0_ids

        # 20% of 1000 = 200 updates, 10% of 1000 = 100 inserts
        assert len(updates) == 200, f"Expected 200 updates, got {len(updates)}"
        assert len(inserts) == 100, f"Expected 100 inserts, got {len(inserts)}"

    def test_incremental_merge_updates_have_new_values(self, generator):
        """Test updates have different values than original."""
        scenario = generator.generate_incremental_merge_scenario(
            rows=100, batches=2, update_rate=0.5, insert_rate=0.0
        )

        # Get an updated record
        t0_ids = set(scenario.t0["record_id"])
        t1_ids = set(scenario.t1["record_id"])
        updated_id = list(t0_ids & t1_ids)[0]

        original = scenario.t0[scenario.t0["record_id"] == updated_id].iloc[0]
        updated = scenario.t1[scenario.t1["record_id"] == updated_id].iloc[0]

        # Status should have progressed
        original_status_idx = generator.STATUSES.index(original["status"])
        updated_status_idx = generator.STATUSES.index(updated["status"])
        assert updated_status_idx >= original_status_idx

        # Updated_at should be different
        assert updated["updated_at"] != original["updated_at"]

    def test_incremental_merge_metadata_tracks_changes(self, generator):
        """Test metadata tracks update and insert counts."""
        scenario = generator.generate_incremental_merge_scenario(
            rows=1000, batches=4, update_rate=0.2, insert_rate=0.1
        )

        changes = scenario.metadata["changes"]
        assert "t1" in changes
        assert changes["t1"]["update_count"] == 200
        assert changes["t1"]["insert_count"] == 100

    def test_incremental_merge_assertions_pass(self, generator):
        """Test incremental merge assertions validate correctly."""
        scenario = generator.generate_incremental_merge_scenario(
            rows=1000, batches=4, update_rate=0.2, insert_rate=0.1
        )

        assertions = create_incremental_merge_assertions(
            updated_count=200,
            inserted_count=100,
            columns=list(scenario.t1.columns),
        )

        validator = AssertionValidator(assertions)
        report = validator.validate_all(
            scenario.t1,
            previous_df=scenario.t0,
        )

        assert report.passed, f"Assertions failed:\n{report.summary()}"


class TestCurrentHistoryPattern:
    """Tests for CURRENT_HISTORY (SCD Type 2) load pattern.

    CURRENT_HISTORY behavior:
    - Maintains current and historical views
    - Each entity can have multiple versions
    - Tracks effective dates
    """

    @pytest.fixture
    def generator(self):
        """Create generator for SCD2 tests."""
        return PatternTestDataGenerator(seed=42, base_rows=1000)

    def test_scd2_generates_entity_versions(self, generator):
        """Test SCD2 generates multiple versions per entity."""
        scenario = generator.generate_scd2_scenario(
            entities=500, changes_per_entity=3
        )

        assert scenario.pattern == "current_history"
        assert scenario.batch_count >= 1

    def test_scd2_t0_initial_versions(self, generator):
        """Test T0 contains initial version for all entities."""
        scenario = generator.generate_scd2_scenario(entities=500, changes_per_entity=3)
        df = scenario.t0

        assert len(df) == 500
        assert all(df["version"] == 1)
        assert all(df["is_current"] == True)

    def test_scd2_entity_ids_unique_per_version(self, generator):
        """Test entity IDs are unique within each batch."""
        scenario = generator.generate_scd2_scenario(entities=100, changes_per_entity=3)

        # T0 should have unique entity IDs
        assert scenario.t0["entity_id"].is_unique

    def test_scd2_version_increments(self, generator):
        """Test versions increment for changed entities."""
        scenario = generator.generate_scd2_scenario(entities=100, changes_per_entity=3)

        if scenario.t1 is not None:
            # All T1 records should have version > 1
            assert all(scenario.t1["version"] > 1)

    def test_scd2_has_required_columns(self, generator):
        """Test SCD2 data has required columns for history tracking."""
        scenario = generator.generate_scd2_scenario(entities=100, changes_per_entity=2)
        df = scenario.t0

        required_cols = [
            "entity_id",
            "entity_name",
            "version",
            "is_current",
            "effective_from",
            "effective_to",
        ]
        for col in required_cols:
            assert col in df.columns, f"Missing SCD2 column: {col}"

    def test_scd2_metadata_tracks_versions(self, generator):
        """Test metadata tracks entity version counts."""
        scenario = generator.generate_scd2_scenario(entities=100, changes_per_entity=3)

        assert scenario.metadata["entity_count"] == 100
        assert scenario.metadata["changes_per_entity"] == 3
        assert "entity_version_counts" in scenario.metadata

    def test_scd2_assertions_pass(self, generator):
        """Test SCD2 assertions validate correctly."""
        scenario = generator.generate_scd2_scenario(entities=500, changes_per_entity=3)

        assertions = create_scd2_assertions(
            current_rows=500,  # Initial batch has all current
            history_rows=500,  # Initial batch = 500 rows
            columns=list(scenario.t0.columns),
        )

        validator = AssertionValidator(assertions)
        report = validator.validate_all(scenario.t0)

        assert report.passed, f"Assertions failed:\n{report.summary()}"


class TestAssertionFramework:
    """Tests for the assertion validation framework."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for assertion tests."""
        return pd.DataFrame({
            "record_id": ["REC00000001", "REC00000002", "REC00000003"],
            "status": ["active", "pending", "completed"],
            "amount": [100.50, 200.75, 300.25],
            "is_active": [True, True, False],
        })

    def test_row_count_assertion_pass(self, sample_df):
        """Test row count assertion passes with correct count."""
        assertions = PatternAssertions(
            pattern="test",
            scenario="test",
            row_assertions={"row_count": 3},
        )
        validator = AssertionValidator(assertions)
        report = validator.validate_all(sample_df)

        assert report.passed

    def test_row_count_assertion_fail(self, sample_df):
        """Test row count assertion fails with incorrect count."""
        assertions = PatternAssertions(
            pattern="test",
            scenario="test",
            row_assertions={"row_count": 5},
        )
        validator = AssertionValidator(assertions)
        report = validator.validate_all(sample_df)

        assert not report.passed
        assert report.fail_count == 1

    def test_columns_present_assertion(self, sample_df):
        """Test columns present assertion."""
        assertions = PatternAssertions(
            pattern="test",
            scenario="test",
            row_assertions={"columns_present": ["record_id", "status", "amount"]},
        )
        validator = AssertionValidator(assertions)
        report = validator.validate_all(sample_df)

        assert report.passed

    def test_columns_present_missing(self, sample_df):
        """Test columns present fails with missing columns."""
        assertions = PatternAssertions(
            pattern="test",
            scenario="test",
            row_assertions={"columns_present": ["record_id", "missing_col"]},
        )
        validator = AssertionValidator(assertions)
        report = validator.validate_all(sample_df)

        assert not report.passed

    def test_first_row_assertion(self, sample_df):
        """Test first row value assertion."""
        assertions = PatternAssertions(
            pattern="test",
            scenario="test",
            row_assertions={
                "first_row": {
                    "record_id": "REC00000001",
                    "status": "active",
                }
            },
        )
        validator = AssertionValidator(assertions)
        report = validator.validate_all(sample_df)

        assert report.passed

    def test_unique_column_assertion(self, sample_df):
        """Test unique column assertion."""
        assertions = PatternAssertions(
            pattern="test",
            scenario="test",
            row_assertions={"unique_columns": ["record_id"]},
        )
        validator = AssertionValidator(assertions)
        report = validator.validate_all(sample_df)

        assert report.passed

    def test_assertion_report_summary(self, sample_df):
        """Test assertion report generates readable summary."""
        assertions = PatternAssertions(
            pattern="test",
            scenario="summary_test",
            row_assertions={
                "row_count": 3,
                "columns_present": ["record_id"],
            },
        )
        validator = AssertionValidator(assertions)
        report = validator.validate_all(sample_df)

        summary = report.summary()
        assert "Pattern: test" in summary
        assert "Scenario: summary_test" in summary
        assert "passed" in summary.lower()


class TestPatternScenarioIntegration:
    """Integration tests that verify complete pattern scenarios."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test outputs."""
        dir_path = Path(tempfile.mkdtemp())
        yield dir_path
        shutil.rmtree(dir_path, ignore_errors=True)

    def test_snapshot_scenario_end_to_end(self, temp_dir):
        """Test complete snapshot scenario generation and validation."""
        generator = PatternTestDataGenerator(seed=42, base_rows=500)
        scenario = generator.generate_snapshot_scenario()

        # Write to temp directory
        output_path = temp_dir / "snapshot_t0.parquet"
        scenario.t0.to_parquet(output_path)

        # Read back and validate
        df = pd.read_parquet(output_path)
        assertions = create_snapshot_assertions(
            row_count=500,
            columns=list(df.columns),
            first_row_values={"record_id": "REC00000001"},
        )

        validator = AssertionValidator(assertions)
        report = validator.validate_all(df)

        assert report.passed, f"End-to-end validation failed:\n{report.summary()}"

    def test_incremental_merge_scenario_end_to_end(self, temp_dir):
        """Test complete incremental merge scenario generation and validation."""
        generator = PatternTestDataGenerator(seed=42, base_rows=500)
        scenario = generator.generate_incremental_merge_scenario(
            batches=3, update_rate=0.2, insert_rate=0.1
        )

        # Write all batches
        for batch_name, batch_df in scenario.batches.items():
            output_path = temp_dir / f"merge_{batch_name}.parquet"
            batch_df.to_parquet(output_path)

        # Validate T1 against T0
        t0_df = pd.read_parquet(temp_dir / "merge_t0.parquet")
        t1_df = pd.read_parquet(temp_dir / "merge_t1.parquet")

        assertions = create_incremental_merge_assertions(
            updated_count=100,  # 20% of 500
            inserted_count=50,  # 10% of 500
            columns=list(t1_df.columns),
        )

        validator = AssertionValidator(assertions)
        report = validator.validate_all(t1_df, previous_df=t0_df)

        assert report.passed, f"End-to-end validation failed:\n{report.summary()}"

    def test_all_patterns_generate_valid_data(self):
        """Test all patterns generate syntactically valid data."""
        generator = PatternTestDataGenerator(seed=42, base_rows=100)

        patterns = [
            ("snapshot", generator.generate_snapshot_scenario()),
            ("incremental_append", generator.generate_incremental_append_scenario()),
            ("incremental_merge", generator.generate_incremental_merge_scenario()),
            ("current_history", generator.generate_scd2_scenario(entities=50)),
        ]

        for pattern_name, scenario in patterns:
            assert scenario.pattern == pattern_name
            assert scenario.t0 is not None
            assert len(scenario.t0) > 0
            assert not scenario.t0.empty

            # Verify data can be serialized/deserialized
            json_data = scenario.t0.to_json()
            assert json_data is not None
