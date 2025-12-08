"""Helper functions for full pipeline integration tests.

Provides utilities for:
- Loading and verifying golden files
- Verifying Bronze metadata and checksums
- Comparing DataFrame values against expectations
- Loading test configs with path substitution
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# Path to fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"
CONFIGS_DIR = FIXTURES_DIR / "configs"
GOLDEN_DIR = FIXTURES_DIR / "golden"


def load_golden_file(name: str) -> Dict[str, Any]:
    """Load a golden file from the fixtures/golden directory.

    Args:
        name: Name of the golden file (without .json extension)

    Returns:
        Dictionary containing the golden file data
    """
    path = GOLDEN_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Golden file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_test_config(name: str) -> Dict[str, Any]:
    """Load a test config YAML file.

    Args:
        name: Name of the config file (without .yaml extension)

    Returns:
        Dictionary containing the config data
    """
    import yaml

    path = CONFIGS_DIR / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def verify_dataframe_first_row(
    df: pd.DataFrame,
    golden: Dict[str, Any],
    columns_to_check: Optional[List[str]] = None,
) -> None:
    """Verify first row of DataFrame matches golden file expectations.

    Args:
        df: DataFrame to verify
        golden: Golden file data containing 'first_row' key
        columns_to_check: Optional list of columns to check (default: all in golden)

    Raises:
        AssertionError: If any value doesn't match
    """
    first_row = golden["first_row"]
    columns = columns_to_check or list(first_row.keys())

    for col in columns:
        if col not in first_row:
            continue
        expected = first_row[col]
        actual = df.iloc[0][col]

        # Handle date/datetime comparison
        if isinstance(expected, str) and col.endswith("_date"):
            actual = str(actual)

        # Handle float comparison with tolerance
        if isinstance(expected, float):
            assert abs(actual - expected) < 0.01, (
                f"Column '{col}': expected {expected}, got {actual}"
            )
        else:
            assert actual == expected, (
                f"Column '{col}': expected {expected!r}, got {actual!r}"
            )


def verify_dataframe_schema(
    df: pd.DataFrame,
    golden: Dict[str, Any],
) -> None:
    """Verify DataFrame schema matches golden file expectations.

    Args:
        df: DataFrame to verify
        golden: Golden file data containing 'column_names' and 'columns'

    Raises:
        AssertionError: If schema doesn't match
    """
    expected_columns = set(golden["column_names"])
    actual_columns = set(df.columns)

    # Check column names
    assert actual_columns == expected_columns, (
        f"Column mismatch: missing={expected_columns - actual_columns}, "
        f"extra={actual_columns - expected_columns}"
    )

    # Check column types
    for col_info in golden["columns"]:
        col_name = col_info["name"]
        expected_dtype = col_info["dtype"]
        actual_dtype = str(df[col_name].dtype)

        # Allow some flexibility in dtype matching
        if expected_dtype == "object" and actual_dtype == "object":
            continue
        if expected_dtype == "float64" and actual_dtype == "float64":
            continue
        if "datetime64" in expected_dtype and "datetime64" in actual_dtype:
            continue

        assert actual_dtype == expected_dtype, (
            f"Column '{col_name}' dtype: expected {expected_dtype}, got {actual_dtype}"
        )


def verify_bronze_metadata(bronze_path: Path) -> Dict[str, Any]:
    """Verify Bronze metadata files exist and are valid.

    Args:
        bronze_path: Path to Bronze partition directory

    Returns:
        Dictionary containing metadata and checksums

    Raises:
        AssertionError: If files are missing or invalid
    """
    metadata_path = bronze_path / "_metadata.json"
    checksums_path = bronze_path / "_checksums.json"

    assert metadata_path.exists(), f"Missing _metadata.json at {bronze_path}"
    assert checksums_path.exists(), f"Missing _checksums.json at {bronze_path}"

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    checksums = json.loads(checksums_path.read_text(encoding="utf-8"))

    # Verify required metadata fields
    required_fields = ["run_datetime", "system", "table", "chunk_count", "record_count"]
    for field in required_fields:
        assert field in metadata, f"Missing required metadata field: {field}"

    # Verify checksums structure
    assert "files" in checksums, "Missing 'files' in checksums manifest"
    assert len(checksums["files"]) > 0, "No files in checksums manifest"

    return {"metadata": metadata, "checksums": checksums}


def verify_checksum_integrity(bronze_path: Path) -> bool:
    """Verify SHA256 checksums in manifest match actual file contents.

    Args:
        bronze_path: Path to Bronze partition directory

    Returns:
        True if all checksums are valid

    Raises:
        AssertionError: If any checksum doesn't match
    """
    checksums_path = bronze_path / "_checksums.json"
    checksums = json.loads(checksums_path.read_text(encoding="utf-8"))

    for file_info in checksums["files"]:
        filename = file_info["filename"]
        expected_sha256 = file_info["sha256"]
        file_path = bronze_path / filename

        assert file_path.exists(), f"File in manifest doesn't exist: {filename}"

        # Calculate actual SHA256
        actual_sha256 = hashlib.sha256(file_path.read_bytes()).hexdigest()

        assert actual_sha256 == expected_sha256, (
            f"Checksum mismatch for {filename}: "
            f"expected {expected_sha256}, got {actual_sha256}"
        )

    return True


def build_bronze_config(
    input_path: Path,
    output_path: Path,
    system: str = "synthetic",
    table: str = "claims",
    load_pattern: str = "snapshot",
) -> Dict[str, Any]:
    """Build a complete Bronze configuration for testing.

    Args:
        input_path: Path to input parquet files
        output_path: Path for Bronze output
        system: Source system name
        table: Source table name
        load_pattern: Load pattern (snapshot, incremental_append, incremental_merge)

    Returns:
        Complete Bronze config dictionary
    """
    return {
        "environment": "test",
        "domain": "healthcare",
        "system": system,
        "entity": table,
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "output_defaults": {
                    "parquet": True,
                    "csv": False,
                },
            },
        },
        "source": {
            "system": system,
            "table": table,
            "type": "file",
            "file": {
                "path": str(input_path),
                "format": "parquet",
            },
            "run": {
                "load_pattern": load_pattern,
                "local_output_dir": str(output_path),
                "storage_enabled": False,
                "max_rows_per_file": 0,
                "cleanup_on_failure": True,
            },
        },
    }


def read_bronze_parquet(bronze_path: Path) -> pd.DataFrame:
    """Read all parquet files from a Bronze partition.

    Args:
        bronze_path: Path to Bronze partition directory

    Returns:
        Combined DataFrame from all parquet files
    """
    parquet_files = list(bronze_path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {bronze_path}")

    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
