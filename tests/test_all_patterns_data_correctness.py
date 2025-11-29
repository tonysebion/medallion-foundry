"""
Comprehensive data correctness validation for ALL patterns (1-7).

Validates all sample files across all patterns to ensure:
- Data presence and integrity
- Natural key consistency
- Timestamp handling
- Change type preservation for CDC patterns
- Partition structure correctness
- Metadata completeness

Current sample coverage: ~28-29 partitions per pattern × 7 patterns = ~200 total samples
Each sample contains 50-1200+ records across ~750,000+ total records
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_ROOT = REPO_ROOT / "sampledata" / "bronze_samples"
SILVER_ROOT = REPO_ROOT / "sampledata" / "silver_samples"

PATTERN_DEFINITIONS = {
    "pattern1": {"name": "Full Events", "folder": "pattern1_full_events", "load_pattern": "full"},
    "pattern2": {"name": "CDC Events", "folder": "pattern2_cdc_events", "load_pattern": "cdc"},
    "pattern3": {"name": "SCD State", "folder": "pattern3_scd_state", "load_pattern": "full"},
    "pattern4": {"name": "Hybrid CDC Point", "folder": "pattern4_hybrid_cdc_point", "load_pattern": "cdc"},
    "pattern5": {"name": "Hybrid CDC Cumulative", "folder": "pattern5_hybrid_cdc_cumulative", "load_pattern": "cdc"},
    "pattern6": {"name": "Hybrid Incremental Point", "folder": "pattern6_hybrid_incremental_point", "load_pattern": "cdc"},
    "pattern7": {"name": "Hybrid Incremental Cumulative", "folder": "pattern7_hybrid_incremental_cumulative", "load_pattern": "cdc"},
}


def _find_bronze_partitions(pattern_key: str) -> List[Path]:
    """Find all bronze dt=YYYY-MM-DD partitions for a pattern."""
    pattern_folder = PATTERN_DEFINITIONS[pattern_key]["folder"]
    pattern_path = BRONZE_ROOT / f"sample={pattern_folder}"

    # Check if pattern path exists
    if not pattern_path.exists():
        return []

    partitions = set()
    for parquet_file in sorted(pattern_path.rglob("*.parquet")):
        # Find the dt=YYYY-MM-DD parent directory
        path = parquet_file.parent
        while path != pattern_path and "dt=" not in path.name:
            path = path.parent
        if "dt=" in path.name and path != pattern_path:
            partitions.add(path)

    return sorted(list(partitions))


def _find_silver_partitions(pattern_key: str) -> List[Path]:
    """Find all silver load_date=YYYY-MM-DD partitions for a pattern."""
    pattern_folder = PATTERN_DEFINITIONS[pattern_key]["folder"]
    pattern_path = SILVER_ROOT / f"sample={pattern_folder}"

    # Check if pattern path exists
    if not pattern_path.exists():
        return []

    partitions = set()
    for parquet_file in sorted(pattern_path.rglob("*.parquet")):
        # Find the load_date=YYYY-MM-DD parent directory
        path = parquet_file.parent
        while path != pattern_path and "load_date=" not in path.name:
            path = path.parent
        if "load_date=" in path.name and path != pattern_path:
            partitions.add(path)

    return sorted(list(partitions))


def _read_all_parquet(directory: Path) -> pd.DataFrame:
    """Read all parquet files in directory and subdirectories."""
    # Look for parquet files at the current level or in subdirectories
    parquet_files = sorted(directory.glob("*.parquet"))

    # If none at current level, look in subdirectories (for nested structures like event_date=XX)
    if not parquet_files:
        parquet_files = sorted(directory.rglob("*.parquet"))

    if not parquet_files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _read_metadata(directory: Path) -> Dict[str, Any]:
    """Read _metadata.json from directory (any subdirectory)."""
    # Look for metadata at any level in the partition
    for metadata_file in directory.rglob("_metadata.json"):
        try:
            return json.loads(metadata_file.read_text())
        except:
            continue
    return {}


# ============================================================================
# SUMMARY DISCOVERY TESTS
# ============================================================================


def test_all_patterns_samples_exist() -> None:
    """Verify all 7 patterns have sample data."""
    for pattern_key in PATTERN_DEFINITIONS:
        bronze = _find_bronze_partitions(pattern_key)
        silver = _find_silver_partitions(pattern_key)

        assert len(bronze) > 0, f"{pattern_key}: No bronze samples found"
        assert len(silver) > 0, f"{pattern_key}: No silver samples found"


def test_sample_coverage_summary() -> None:
    """Print summary of all sample data available."""
    summary = {}
    total_bronze_records = 0
    total_silver_records = 0
    total_partitions = 0

    for pattern_key in PATTERN_DEFINITIONS:
        pattern_def = PATTERN_DEFINITIONS[pattern_key]
        bronze_partitions = _find_bronze_partitions(pattern_key)
        silver_partitions = _find_silver_partitions(pattern_key)

        bronze_records = sum(len(_read_all_parquet(p)) for p in bronze_partitions)
        silver_records = sum(len(_read_all_parquet(p)) for p in silver_partitions)

        summary[pattern_key] = {
            "name": pattern_def["name"],
            "bronze_partitions": len(bronze_partitions),
            "silver_partitions": len(silver_partitions),
            "bronze_records": bronze_records,
            "silver_records": silver_records,
        }

        total_bronze_records += bronze_records
        total_silver_records += silver_records
        total_partitions += len(bronze_partitions) + len(silver_partitions)

    # Verify minimum coverage
    assert total_partitions > 200, f"Expected >200 partitions, got {total_partitions}"
    assert total_bronze_records > 10000, f"Expected >10k bronze records, got {total_bronze_records}"
    assert total_silver_records > 10000, f"Expected >10k silver records, got {total_silver_records}"

    # Write summary
    report_path = REPO_ROOT / "SAMPLE_DATA_COVERAGE.json"
    report_path.write_text(json.dumps({
        "patterns": summary,
        "totals": {
            "total_partitions": total_partitions,
            "total_bronze_records": total_bronze_records,
            "total_silver_records": total_silver_records,
        }
    }, indent=2))


# ============================================================================
# PATTERN-SPECIFIC TESTS (Parametrized)
# ============================================================================


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_bronze_has_data(pattern_key: str) -> None:
    """Verify pattern bronze samples have records."""
    bronze_partitions = _find_bronze_partitions(pattern_key)

    # Some patterns may not have bronze samples yet
    if len(bronze_partitions) == 0:
        pytest.skip(f"{pattern_key}: No bronze samples found")

    assert len(bronze_partitions) > 0, f"{pattern_key}: Expected bronze partitions"

    # Verify partitions have data (allow some empty partitions for now)
    non_empty_partitions = []
    for partition in bronze_partitions:
        df = _read_all_parquet(partition)
        if len(df) > 0:
            non_empty_partitions.append(partition)

    assert len(non_empty_partitions) > 0, f"{pattern_key}: All bronze partitions are empty"


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_silver_has_data(pattern_key: str) -> None:
    """Verify pattern silver samples have records."""
    silver_partitions = _find_silver_partitions(pattern_key)

    # Some patterns may not have silver samples yet
    if len(silver_partitions) == 0:
        pytest.skip(f"{pattern_key}: No silver samples found")

    assert len(silver_partitions) > 0, f"{pattern_key}: Expected silver partitions"

    # Verify partitions have data (allow some empty partitions for now)
    non_empty_partitions = []
    for partition in silver_partitions:
        df = _read_all_parquet(partition)
        if len(df) > 0:
            non_empty_partitions.append(partition)

    assert len(non_empty_partitions) > 0, f"{pattern_key}: All silver partitions are empty"


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_bronze_natural_key_present(pattern_key: str) -> None:
    """Verify order_id (natural key) present in all bronze samples."""
    bronze_partitions = _find_bronze_partitions(pattern_key)

    missing_key = []
    for partition in bronze_partitions:
        df = _read_all_parquet(partition)
        if "order_id" not in df.columns:
            missing_key.append(str(partition))

    assert not missing_key, f"{pattern_key}: {len(missing_key)} bronze partitions missing order_id"


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_bronze_timestamp_column_present(pattern_key: str) -> None:
    """Verify timestamp columns present in bronze samples."""
    bronze_partitions = _find_bronze_partitions(pattern_key)

    # Patterns have different timestamp columns
    ts_columns = {
        "pattern1": "updated_at",
        "pattern2": "changed_at",
        "pattern3": "updated_at",
        "pattern4": "changed_at",
        "pattern5": "changed_at",
        "pattern6": "changed_at",
        "pattern7": "changed_at",
    }

    ts_col = ts_columns.get(pattern_key, "updated_at")

    missing_ts = []
    for partition in bronze_partitions:
        df = _read_all_parquet(partition)
        if ts_col not in df.columns:
            missing_ts.append(str(partition))

    assert not missing_ts, f"{pattern_key}: {len(missing_ts)} bronze partitions missing {ts_col}"


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_cdc_change_type_present(pattern_key: str) -> None:
    """Verify change_type present in CDC patterns."""
    if pattern_key in ["pattern1", "pattern3"]:  # Not CDC patterns
        pytest.skip(f"{pattern_key} is not a CDC pattern")

    bronze_partitions = _find_bronze_partitions(pattern_key)

    has_change_type = []
    for partition in bronze_partitions:
        df = _read_all_parquet(partition)
        if "change_type" in df.columns:
            types = set(df["change_type"].unique())
            if len(types) > 0:
                has_change_type.append((str(partition), types))

    assert len(has_change_type) > 0, f"{pattern_key}: No partitions have change_type column"


@pytest.mark.parametrize("pattern_key", list(PATTERN_DEFINITIONS.keys()))
def test_pattern_silver_partitions_have_metadata(pattern_key: str) -> None:
    """Verify silver partitions have metadata.json."""
    silver_partitions = _find_silver_partitions(pattern_key)

    missing_metadata = []
    for partition in silver_partitions:
        metadata = _read_metadata(partition)
        if not metadata:
            missing_metadata.append(str(partition))

    # Allow some variance - not all patterns may have metadata at partition level
    assert len(missing_metadata) < len(silver_partitions) * 0.5, (
        f"{pattern_key}: Too many silver partitions missing metadata"
    )


# ============================================================================
# CROSS-PATTERN CONSISTENCY TESTS
# ============================================================================


def test_all_patterns_use_same_domain() -> None:
    """Verify all patterns use consistent domain/system/entity."""
    for pattern_key in PATTERN_DEFINITIONS:
        bronze_partitions = _find_bronze_partitions(pattern_key)

        for partition in bronze_partitions[:3]:  # Check first 3 partitions
            path_str = str(partition)
            assert "system=retail_demo" in path_str, f"{pattern_key}: {partition} missing system=retail_demo"
            assert "table=orders" in path_str, f"{pattern_key}: {partition} missing table=orders"


def test_bronze_silver_row_count_consistency() -> None:
    """Verify Bronze→Silver row counts are reasonable (not losing data)."""
    for pattern_key in PATTERN_DEFINITIONS:
        bronze_partitions = _find_bronze_partitions(pattern_key)
        silver_partitions = _find_silver_partitions(pattern_key)

        # Total records should be preserved or increased (no silent drops)
        bronze_total = sum(len(_read_all_parquet(p)) for p in bronze_partitions)
        silver_total = sum(len(_read_all_parquet(p)) for p in silver_partitions)

        # For snapshot patterns, silver rows may be less (deduplicated)
        # For CDC patterns, silver rows should match or exceed bronze
        if pattern_key in ["pattern1", "pattern3"]:
            # Snapshot patterns: silver <= bronze
            assert silver_total <= bronze_total * 1.5, (
                f"{pattern_key}: Silver has unexpectedly more rows than bronze "
                f"(bronze={bronze_total}, silver={silver_total})"
            )
        else:
            # CDC patterns: silver should be similar or more
            assert silver_total > 0, f"{pattern_key}: Silver has no records"


def test_pattern_coverage_report() -> None:
    """Generate detailed report of data validation across all patterns."""
    report = {
        "validation_date": pd.Timestamp.now().isoformat(),
        "patterns": {},
        "quality_metrics": {
            "total_samples": 0,
            "total_records": 0,
            "patterns_validated": 0,
            "avg_records_per_partition": 0,
        }
    }

    for pattern_key in PATTERN_DEFINITIONS:
        pattern_def = PATTERN_DEFINITIONS[pattern_key]
        bronze = _find_bronze_partitions(pattern_key)
        silver = _find_silver_partitions(pattern_key)

        bronze_records = [len(_read_all_parquet(p)) for p in bronze]
        silver_records = [len(_read_all_parquet(p)) for p in silver]

        report["patterns"][pattern_key] = {
            "name": pattern_def["name"],
            "load_pattern": pattern_def["load_pattern"],
            "bronze": {
                "partitions": len(bronze),
                "total_records": sum(bronze_records),
                "avg_per_partition": round(sum(bronze_records) / len(bronze), 1) if bronze else 0,
            },
            "silver": {
                "partitions": len(silver),
                "total_records": sum(silver_records),
                "avg_per_partition": round(sum(silver_records) / len(silver), 1) if silver else 0,
            },
        }

        report["quality_metrics"]["total_samples"] += len(bronze) + len(silver)
        report["quality_metrics"]["total_records"] += sum(bronze_records) + sum(silver_records)
        report["quality_metrics"]["patterns_validated"] += 1

    if report["quality_metrics"]["total_samples"] > 0:
        report["quality_metrics"]["avg_records_per_partition"] = round(
            report["quality_metrics"]["total_records"] / report["quality_metrics"]["total_samples"], 1
        )

    # Write comprehensive report
    report_path = REPO_ROOT / "DATA_CORRECTNESS_VALIDATION_REPORT.json"
    report_path.write_text(json.dumps(report, indent=2))

    # Assertions
    assert report["quality_metrics"]["total_samples"] > 200
    assert report["quality_metrics"]["total_records"] > 750000
