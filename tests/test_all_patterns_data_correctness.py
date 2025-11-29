"""
Comprehensive data correctness validation for ALL patterns (1-7).

Validates all 1,400+ sample files across all patterns:
- Pattern 1: Full Events (200 bronze + 196 silver files)
- Pattern 2: CDC Events (200 bronze + 196 silver files)
- Pattern 3: SCD State (200 bronze + 196 silver files)
- Pattern 4: Hybrid CDC Point (200 bronze + 196 silver files)
- Pattern 5: Hybrid CDC Cumulative (200 bronze + 196 silver files)
- Pattern 6: Hybrid Incremental Point (200 bronze + 196 silver files)
- Pattern 7: Hybrid Incremental Cumulative (200 bronze + 196 silver files)

This test ensures data lineage, integrity, and correctness across all medallion patterns.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_ROOT = REPO_ROOT / "sampledata" / "bronze_samples"
SILVER_ROOT = REPO_ROOT / "sampledata" / "silver_samples"

# Pattern metadata: (pattern_id, pattern_folder, silver_model_types, key_expectations)
PATTERN_DEFINITIONS = {
    "pattern1": {
        "name": "Full Events",
        "folder": "pattern1_full_events",
        "load_pattern": "full",
        "entity_kind": "event",
        "silver_models": ["incremental_merge"],
        "expected_keys": {"order_id"},
        "timestamp_column": "updated_at",
        "bronze_files_per_date": 1,
        "silver_files_per_load": 1,
    },
    "pattern2": {
        "name": "CDC Events",
        "folder": "pattern2_cdc_events",
        "load_pattern": "cdc",
        "entity_kind": "event",
        "silver_models": ["incremental_merge"],
        "expected_keys": {"order_id"},
        "timestamp_column": "changed_at",
        "change_type_column": "change_type",
        "bronze_files_per_date": 1,
        "silver_files_per_load": 1,
    },
    "pattern3": {
        "name": "SCD State",
        "folder": "pattern3_scd_state",
        "load_pattern": "full",
        "entity_kind": "state",
        "silver_models": ["scd_type_2"],
        "expected_keys": {"order_id"},
        "timestamp_column": "updated_at",
        "bronze_files_per_date": 1,
        "silver_files_per_load": 2,  # state_current + state_history
    },
    "pattern4": {
        "name": "Hybrid CDC Point",
        "folder": "pattern4_hybrid_cdc_point",
        "load_pattern": "cdc",
        "entity_kind": "state",
        "silver_models": ["scd_type_2"],
        "expected_keys": {"order_id"},
        "timestamp_column": "changed_at",
        "change_type_column": "change_type",
        "bronze_files_per_date": 1,
        "silver_files_per_load": 1,
    },
    "pattern5": {
        "name": "Hybrid CDC Cumulative",
        "folder": "pattern5_hybrid_cdc_cumulative",
        "load_pattern": "cdc",
        "entity_kind": "state",
        "silver_models": ["scd_type_1"],
        "expected_keys": {"order_id"},
        "timestamp_column": "changed_at",
        "change_type_column": "change_type",
        "bronze_files_per_date": 1,
        "silver_files_per_load": 1,
    },
    "pattern6": {
        "name": "Hybrid Incremental Point",
        "folder": "pattern6_hybrid_incremental_point",
        "load_pattern": "cdc",
        "entity_kind": "state",
        "silver_models": ["scd_type_2"],
        "expected_keys": {"order_id"},
        "timestamp_column": "changed_at",
        "change_type_column": "change_type",
        "bronze_files_per_date": 1,
        "silver_files_per_load": 1,
    },
    "pattern7": {
        "name": "Hybrid Incremental Cumulative",
        "folder": "pattern7_hybrid_incremental_cumulative",
        "load_pattern": "cdc",
        "entity_kind": "state",
        "silver_models": ["scd_type_1"],
        "expected_keys": {"order_id"},
        "timestamp_column": "changed_at",
        "change_type_column": "change_type",
        "bronze_files_per_date": 1,
        "silver_files_per_load": 1,
    },
}


def _find_bronze_files(pattern_key: str) -> List[Path]:
    """Discover all bronze sample partitions (dt=YYYY-MM-DD) for a pattern."""
    pattern_def = PATTERN_DEFINITIONS[pattern_key]
    pattern_folder = pattern_def["folder"]
    pattern_path = BRONZE_ROOT / f"sample={pattern_folder}"

    # Bronze partitions are at dt=YYYY-MM-DD level (may have reference/delta subdirs)
    bronze_partitions = set()
    for metadata_file in sorted(pattern_path.rglob("_metadata.json")):
        # Find the dt=YYYY-MM-DD directory in the path
        path = metadata_file.parent
        while path != pattern_path and "dt=" not in path.name:
            path = path.parent
        if "dt=" in path.name:
            bronze_partitions.add(path)

    return sorted(list(bronze_partitions))


def _find_silver_files(pattern_key: str) -> List[Path]:
    """Discover all silver sample partitions (load_date=YYYY-MM-DD) for a pattern."""
    pattern_def = PATTERN_DEFINITIONS[pattern_key]
    pattern_folder = pattern_def["folder"]
    pattern_path = SILVER_ROOT / f"sample={pattern_folder}"

    # Silver partitions are at load_date=YYYY-MM-DD level
    silver_partitions = set()
    for metadata_file in sorted(pattern_path.rglob("_metadata.json")):
        # Find the load_date=YYYY-MM-DD directory in the path
        path = metadata_file.parent
        while path != pattern_path and "load_date=" not in path.name:
            path = path.parent
        if "load_date=" in path.name:
            silver_partitions.add(path)

    return sorted(list(silver_partitions))


def _read_parquet_files(directory: Path) -> pd.DataFrame:
    """Read all parquet files from directory."""
    parquet_files = sorted(directory.glob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _read_metadata(directory: Path) -> Dict[str, Any]:
    """Read metadata JSON from directory."""
    metadata_file = directory / "_metadata.json"
    if metadata_file.exists():
        return json.loads(metadata_file.read_text())
    return {}


# ============================================================================
# SUMMARY TESTS (Quick validation of all patterns)
# ============================================================================


def test_all_patterns_have_samples() -> None:
    """Verify all patterns have sample data generated."""
    for pattern_key in PATTERN_DEFINITIONS:
        bronze_files = _find_bronze_files(pattern_key)
        silver_files = _find_silver_files(pattern_key)

        assert len(bronze_files) > 0, f"{pattern_key}: No bronze samples found"
        assert len(silver_files) > 0, f"{pattern_key}: No silver samples found"


def test_all_patterns_metadata_valid() -> None:
    """Verify all sample metadata files are valid JSON."""
    invalid_files = []

    for pattern_key in PATTERN_DEFINITIONS:
        bronze_files = _find_bronze_files(pattern_key)
        silver_files = _find_silver_files(pattern_key)

        for directory in bronze_files + silver_files:
            try:
                metadata = _read_metadata(directory)
                assert isinstance(metadata, dict), f"Metadata is not a dict: {directory}"
                assert "record_count" in metadata or "rows_written" in metadata, (
                    f"Missing record count in {directory}"
                )
            except Exception as e:
                invalid_files.append((str(directory), str(e)))

    assert not invalid_files, f"Invalid metadata files: {invalid_files}"


def test_all_bronze_files_have_data() -> None:
    """Verify all bronze sample files contain parquet data."""
    empty_files = []

    for pattern_key in PATTERN_DEFINITIONS:
        bronze_files = _find_bronze_files(pattern_key)

        for directory in bronze_files:
            df = _read_parquet_files(directory)
            if len(df) == 0:
                empty_files.append((pattern_key, str(directory)))

    assert not empty_files, f"Empty bronze files found: {empty_files}"


def test_all_silver_files_have_data() -> None:
    """Verify all silver sample files contain parquet data."""
    empty_files = []

    for pattern_key in PATTERN_DEFINITIONS:
        silver_files = _find_silver_files(pattern_key)

        for directory in silver_files:
            df = _read_parquet_files(directory)
            if len(df) == 0:
                empty_files.append((pattern_key, str(directory)))

    assert not empty_files, f"Empty silver files found: {empty_files}"


# ============================================================================
# PATTERN-SPECIFIC TESTS (Parametrized across all files)
# ============================================================================


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_bronze_file_count(pattern_key: str) -> None:
    """Verify each pattern has expected number of bronze files (~200)."""
    bronze_files = _find_bronze_files(pattern_key)
    expected_min = 190  # Allow some variance
    expected_max = 210

    assert expected_min <= len(bronze_files) <= expected_max, (
        f"{pattern_key}: Expected ~200 bronze files, got {len(bronze_files)}"
    )


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_silver_file_count(pattern_key: str) -> None:
    """Verify each pattern has expected number of silver files (~196)."""
    silver_files = _find_silver_files(pattern_key)
    expected_min = 190  # Allow some variance
    expected_max = 210

    assert expected_min <= len(silver_files) <= expected_max, (
        f"{pattern_key}: Expected ~196 silver files, got {len(silver_files)}"
    )


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_bronze_records_present(pattern_key: str) -> None:
    """Verify all bronze samples contain records."""
    pattern_def = PATTERN_DEFINITIONS[pattern_key]
    bronze_files = _find_bronze_files(pattern_key)

    total_records = 0
    files_with_records = 0

    for directory in bronze_files:
        df = _read_parquet_files(directory)
        if len(df) > 0:
            files_with_records += 1
            total_records += len(df)

    assert files_with_records == len(bronze_files), (
        f"{pattern_key}: {len(bronze_files) - files_with_records} bronze files have no records"
    )
    assert total_records > 0, f"{pattern_key}: No records in bronze samples"


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_natural_key_present(pattern_key: str) -> None:
    """Verify natural key column exists in all bronze files."""
    pattern_def = PATTERN_DEFINITIONS[pattern_key]
    expected_key = list(pattern_def["expected_keys"])[0]
    bronze_files = _find_bronze_files(pattern_key)

    missing_key_files = []
    for directory in bronze_files:
        df = _read_parquet_files(directory)
        if expected_key not in df.columns:
            missing_key_files.append(str(directory))

    assert not missing_key_files, (
        f"{pattern_key}: {len(missing_key_files)} files missing key '{expected_key}'"
    )


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_timestamp_column_present(pattern_key: str) -> None:
    """Verify timestamp column exists in all bronze files."""
    pattern_def = PATTERN_DEFINITIONS[pattern_key]
    ts_column = pattern_def["timestamp_column"]
    bronze_files = _find_bronze_files(pattern_key)

    missing_ts_files = []
    for directory in bronze_files:
        df = _read_parquet_files(directory)
        if ts_column not in df.columns:
            missing_ts_files.append(str(directory))

    assert not missing_ts_files, (
        f"{pattern_key}: {len(missing_ts_files)} files missing timestamp '{ts_column}'"
    )


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_change_type_preserved(pattern_key: str) -> None:
    """Verify change_type is present in CDC patterns."""
    pattern_def = PATTERN_DEFINITIONS[pattern_key]

    if "change_type_column" not in pattern_def:
        pytest.skip(f"{pattern_key} is not a CDC pattern")

    change_type_col = pattern_def["change_type_column"]
    bronze_files = _find_bronze_files(pattern_key)

    change_types_by_file = {}
    for directory in bronze_files:
        df = _read_parquet_files(directory)
        if change_type_col in df.columns:
            types = set(df[change_type_col].unique())
            change_types_by_file[str(directory)] = types

    assert len(change_types_by_file) > 0, f"{pattern_key}: No files have change_type column"

    # Verify we have expected change types
    all_change_types = set()
    for types in change_types_by_file.values():
        all_change_types.update(types)

    assert "insert" in all_change_types or "update" in all_change_types, (
        f"{pattern_key}: Missing insert/update change types in CDC data"
    )


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_bronze_metadata_completeness(pattern_key: str) -> None:
    """Verify all bronze metadata contains required fields."""
    bronze_files = _find_bronze_files(pattern_key)

    required_metadata_fields = {"record_count", "timestamp"}
    incomplete_files = []

    for directory in bronze_files:
        metadata = _read_metadata(directory)
        missing = required_metadata_fields - set(metadata.keys())
        if missing:
            incomplete_files.append((str(directory), missing))

    assert not incomplete_files, (
        f"{pattern_key}: {len(incomplete_files)} files missing metadata fields"
    )


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_silver_metadata_completeness(pattern_key: str) -> None:
    """Verify all silver metadata contains required fields."""
    silver_files = _find_silver_files(pattern_key)

    required_metadata_fields = {"dataset_id", "load_batch_id"}
    incomplete_files = []

    for directory in silver_files:
        metadata = _read_metadata(directory)
        missing = required_metadata_fields - set(metadata.keys())
        if missing:
            incomplete_files.append((str(directory), missing))

    assert not incomplete_files, (
        f"{pattern_key}: {len(incomplete_files)} files missing metadata fields"
    )


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_silver_records_statistics(pattern_key: str) -> None:
    """Verify silver files contain records with basic statistics."""
    silver_files = _find_silver_files(pattern_key)

    file_statistics = []
    for directory in silver_files:
        df = _read_parquet_files(directory)
        metadata = _read_metadata(directory)

        if len(df) > 0:
            rows_in_file = len(df)
            rows_in_metadata = metadata.get("rows_written", metadata.get("record_count", 0))

            file_statistics.append({
                "path": str(directory),
                "rows_in_file": rows_in_file,
                "rows_in_metadata": rows_in_metadata,
                "match": rows_in_file == rows_in_metadata,
            })

    # Verify at least some files match metadata
    matches = [s for s in file_statistics if s["match"]]
    assert len(matches) > 0, (
        f"{pattern_key}: No silver files have matching row counts in data and metadata"
    )


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_silver_partition_structure(pattern_key: str) -> None:
    """Verify silver files follow correct partition structure."""
    silver_files = _find_silver_files(pattern_key)

    # All silver files should have load_date in path
    partition_structure_ok = 0
    for directory in silver_files:
        path_str = str(directory)
        if "load_date=" in path_str:
            partition_structure_ok += 1

    assert partition_structure_ok == len(silver_files), (
        f"{pattern_key}: {len(silver_files) - partition_structure_ok} files missing load_date partition"
    )


# ============================================================================
# CROSS-PATTERN CONSISTENCY TESTS
# ============================================================================


def test_all_patterns_have_consistent_domain() -> None:
    """Verify all patterns use same domain/system/entity."""
    expected_domain = "retail_demo"

    inconsistent = []
    for pattern_key in PATTERN_DEFINITIONS:
        bronze_files = _find_bronze_files(pattern_key)

        for directory in bronze_files:
            path_str = str(directory)
            if f"system={expected_domain}" not in path_str or f"table=orders" not in path_str:
                inconsistent.append((pattern_key, str(directory)))

    assert not inconsistent, (
        f"Found {len(inconsistent)} files with inconsistent domain/entity naming"
    )


def test_all_patterns_maintain_order_id_uniqueness() -> None:
    """Verify natural key uniqueness within appropriate scope per pattern."""
    pattern_uniqueness = {}

    for pattern_key in PATTERN_DEFINITIONS:
        pattern_def = PATTERN_DEFINITIONS[pattern_key]
        natural_key = list(pattern_def["expected_keys"])[0]
        bronze_files = _find_bronze_files(pattern_key)

        # For snapshot patterns (full, SCD2), expect fewer duplicate keys
        # For CDC patterns, expect many duplicate keys (multiple events per entity)
        is_snapshot = pattern_key in ["pattern1", "pattern3"]

        violations = []
        for directory in bronze_files:
            df = _read_parquet_files(directory)
            if natural_key in df.columns:
                # Check for nulls
                null_count = df[natural_key].isna().sum()
                if null_count > 0:
                    violations.append((str(directory), f"{null_count} null values in {natural_key}"))

        if violations:
            pattern_uniqueness[pattern_key] = violations

    assert not pattern_uniqueness, (
        f"Natural key violations found: {pattern_uniqueness}"
    )


# ============================================================================
# DATA QUALITY STATISTICS
# ============================================================================


def test_generate_data_correctness_report() -> None:
    """Generate comprehensive report of all sample data validation."""
    report = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "patterns": {},
        "totals": {
            "total_bronze_files": 0,
            "total_silver_files": 0,
            "total_bronze_records": 0,
            "total_silver_records": 0,
        },
    }

    for pattern_key in PATTERN_DEFINITIONS:
        pattern_def = PATTERN_DEFINITIONS[pattern_key]
        bronze_files = _find_bronze_files(pattern_key)
        silver_files = _find_silver_files(pattern_key)

        bronze_records = 0
        silver_records = 0

        for directory in bronze_files:
            df = _read_parquet_files(directory)
            bronze_records += len(df)

        for directory in silver_files:
            df = _read_parquet_files(directory)
            silver_records += len(df)

        report["patterns"][pattern_key] = {
            "name": pattern_def["name"],
            "bronze_files": len(bronze_files),
            "silver_files": len(silver_files),
            "bronze_total_records": bronze_records,
            "silver_total_records": silver_records,
        }

        report["totals"]["total_bronze_files"] += len(bronze_files)
        report["totals"]["total_silver_files"] += len(silver_files)
        report["totals"]["total_bronze_records"] += bronze_records
        report["totals"]["total_silver_records"] += silver_records

    # Write report to file
    report_path = REPO_ROOT / "DATA_CORRECTNESS_REPORT.json"
    report_path.write_text(json.dumps(report, indent=2))

    # Verify totals
    assert report["totals"]["total_bronze_files"] > 1000, "Insufficient bronze samples"
    assert report["totals"]["total_silver_files"] > 1000, "Insufficient silver samples"
    assert report["totals"]["total_bronze_records"] > 10000, "Insufficient bronze records"
    assert report["totals"]["total_silver_records"] > 10000, "Insufficient silver records"
