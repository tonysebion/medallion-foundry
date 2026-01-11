"""Configuration-driven assertion framework for pattern verification.

This module provides YAML-based assertion specifications for validating
load pattern business logic. Assertions can verify:
- Row counts (exact, min, max)
- Column values (first row, any row, all rows)
- Metadata fields (_metadata.json)
- Checksums (_checksums.json)
- Pattern-specific behavior

Example assertion file:
    pattern: snapshot
    scenario: basic_replacement

    row_assertions:
      row_count: 1000
      columns_present: [record_id, status, amount]
      first_row:
        record_id: "REC00000001"

    metadata_assertions:
      load_pattern: snapshot
      row_count_recorded: 1000

    checksum_assertions:
      files_present: true
      checksums_valid: true

    pattern_assertions:
      replaces_previous: true
"""

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import yaml


@dataclass
class AssertionResult:
    """Result of a single assertion check."""

    name: str
    passed: bool
    expected: Any
    actual: Any
    message: str = ""

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        if self.passed:
            return f"[{status}] {self.name}"
        return f"[{status}] {self.name}: expected {self.expected}, got {self.actual}. {self.message}"


@dataclass
class AssertionReport:
    """Complete report of all assertion results."""

    pattern: str
    scenario: str
    results: List[AssertionResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """True if all assertions passed."""
        return all(r.passed for r in self.results)

    @property
    def pass_count(self) -> int:
        """Number of passed assertions."""
        return sum(1 for r in self.results if r.passed)

    @property
    def fail_count(self) -> int:
        """Number of failed assertions."""
        return sum(1 for r in self.results if not r.passed)

    @property
    def failed_assertions(self) -> List[AssertionResult]:
        """List of failed assertions."""
        return [r for r in self.results if not r.passed]

    def add(self, result: AssertionResult) -> None:
        """Add an assertion result."""
        self.results.append(result)

    def summary(self) -> str:
        """Generate a summary of the assertion results."""
        lines = [
            f"Pattern: {self.pattern}",
            f"Scenario: {self.scenario}",
            f"Results: {self.pass_count} passed, {self.fail_count} failed",
            "",
        ]
        for result in self.results:
            lines.append(str(result))

        if self.failed_assertions:
            lines.append("")
            lines.append("FAILURES:")
            for result in self.failed_assertions:
                lines.append(f"  - {result.name}: {result.message}")
                lines.append(f"    Expected: {result.expected}")
                lines.append(f"    Actual: {result.actual}")

        return "\n".join(lines)


@dataclass
class PatternAssertions:
    """Container for pattern-specific assertions loaded from YAML."""

    pattern: str
    scenario: str
    row_assertions: Dict[str, Any] = field(default_factory=dict)
    metadata_assertions: Dict[str, Any] = field(default_factory=dict)
    checksum_assertions: Dict[str, Any] = field(default_factory=dict)
    pattern_assertions: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, yaml_path: Union[str, Path]) -> "PatternAssertions":
        """Load assertions from a YAML file."""
        path = Path(yaml_path)
        with open(path) as f:
            data = yaml.safe_load(f)

        return cls(
            pattern=data.get("pattern", "unknown"),
            scenario=data.get("scenario", "unknown"),
            row_assertions=data.get("row_assertions", {}),
            metadata_assertions=data.get("metadata_assertions", {}),
            checksum_assertions=data.get("checksum_assertions", {}),
            pattern_assertions=data.get("pattern_assertions", {}),
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PatternAssertions":
        """Create assertions from a dictionary."""
        return cls(
            pattern=data.get("pattern", "unknown"),
            scenario=data.get("scenario", "unknown"),
            row_assertions=data.get("row_assertions", {}),
            metadata_assertions=data.get("metadata_assertions", {}),
            checksum_assertions=data.get("checksum_assertions", {}),
            pattern_assertions=data.get("pattern_assertions", {}),
        )

    def to_yaml(self, yaml_path: Union[str, Path]) -> None:
        """Save assertions to a YAML file."""
        data = {
            "pattern": self.pattern,
            "scenario": self.scenario,
            "row_assertions": self.row_assertions,
            "metadata_assertions": self.metadata_assertions,
            "checksum_assertions": self.checksum_assertions,
            "pattern_assertions": self.pattern_assertions,
        }
        path = Path(yaml_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)


class AssertionValidator:
    """Validates data against pattern assertions."""

    def __init__(self, assertions: PatternAssertions):
        """Initialize validator with assertions.

        Args:
            assertions: PatternAssertions to validate against
        """
        self.assertions = assertions
        self.report = AssertionReport(
            pattern=assertions.pattern, scenario=assertions.scenario
        )

    def validate_all(
        self,
        df: pd.DataFrame,
        metadata: Optional[Dict[str, Any]] = None,
        checksums: Optional[Dict[str, Any]] = None,
        previous_df: Optional[pd.DataFrame] = None,
    ) -> AssertionReport:
        """Run all assertions and return report.

        Args:
            df: DataFrame to validate (Bronze output)
            metadata: Contents of _metadata.json
            checksums: Contents of _checksums.json
            previous_df: Previous batch DataFrame (for pattern assertions)

        Returns:
            AssertionReport with all results
        """
        self.validate_row_assertions(df)

        if metadata:
            self.validate_metadata_assertions(metadata)

        if checksums:
            self.validate_checksum_assertions(checksums)

        self.validate_pattern_assertions(df, previous_df)

        return self.report

    def validate_row_assertions(self, df: pd.DataFrame) -> None:
        """Validate row-level assertions."""
        assertions = self.assertions.row_assertions
        if not assertions:
            return

        # Row count assertions
        if "row_count" in assertions:
            expected = assertions["row_count"]
            actual = len(df)
            self.report.add(
                AssertionResult(
                    name="row_count",
                    passed=actual == expected,
                    expected=expected,
                    actual=actual,
                    message="Row count mismatch",
                )
            )

        if "row_count_min" in assertions:
            expected = assertions["row_count_min"]
            actual = len(df)
            self.report.add(
                AssertionResult(
                    name="row_count_min",
                    passed=actual >= expected,
                    expected=f">= {expected}",
                    actual=actual,
                    message="Row count below minimum",
                )
            )

        if "row_count_max" in assertions:
            expected = assertions["row_count_max"]
            actual = len(df)
            self.report.add(
                AssertionResult(
                    name="row_count_max",
                    passed=actual <= expected,
                    expected=f"<= {expected}",
                    actual=actual,
                    message="Row count above maximum",
                )
            )

        # Column presence assertions
        if "columns_present" in assertions:
            expected_cols = set(assertions["columns_present"])
            actual_cols = set(df.columns)
            missing = expected_cols - actual_cols
            self.report.add(
                AssertionResult(
                    name="columns_present",
                    passed=len(missing) == 0,
                    expected=list(expected_cols),
                    actual=list(actual_cols),
                    message=f"Missing columns: {missing}" if missing else "",
                )
            )

        # Column type assertions
        if "column_types" in assertions:
            for col, expected_type in assertions["column_types"].items():
                if col in df.columns:
                    actual_type = str(df[col].dtype)
                    # Normalize type names
                    type_match = self._types_match(expected_type, actual_type)
                    self.report.add(
                        AssertionResult(
                            name=f"column_type.{col}",
                            passed=type_match,
                            expected=expected_type,
                            actual=actual_type,
                            message=f"Type mismatch for column {col}",
                        )
                    )

        # First row assertions
        if "first_row" in assertions and len(df) > 0:
            first_row = df.iloc[0]
            for col, expected_val in assertions["first_row"].items():
                if col in df.columns:
                    actual_val = first_row[col]
                    # Handle float comparison
                    if isinstance(expected_val, float):
                        passed = abs(float(actual_val) - expected_val) < 0.01
                    else:
                        passed = str(actual_val) == str(expected_val)
                    self.report.add(
                        AssertionResult(
                            name=f"first_row.{col}",
                            passed=passed,
                            expected=expected_val,
                            actual=actual_val,
                            message=f"First row value mismatch for {col}",
                        )
                    )

        # Unique key assertions
        if "unique_columns" in assertions:
            for col in assertions["unique_columns"]:
                if col in df.columns:
                    is_unique = df[col].is_unique
                    duplicate_count = len(df) - df[col].nunique()
                    self.report.add(
                        AssertionResult(
                            name=f"unique.{col}",
                            passed=is_unique,
                            expected="all unique",
                            actual=f"{duplicate_count} duplicates",
                            message=f"Column {col} has duplicates",
                        )
                    )

        # Not null assertions
        if "not_null_columns" in assertions:
            for col in assertions["not_null_columns"]:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    self.report.add(
                        AssertionResult(
                            name=f"not_null.{col}",
                            passed=null_count == 0,
                            expected="no nulls",
                            actual=f"{null_count} nulls",
                            message=f"Column {col} has null values",
                        )
                    )

    def validate_metadata_assertions(self, metadata: Dict[str, Any]) -> None:
        """Validate metadata assertions."""
        assertions = self.assertions.metadata_assertions
        if not assertions:
            return

        for key, expected in assertions.items():
            actual = metadata.get(key)
            self.report.add(
                AssertionResult(
                    name=f"metadata.{key}",
                    passed=actual == expected,
                    expected=expected,
                    actual=actual,
                    message="Metadata field mismatch",
                )
            )

    def validate_checksum_assertions(self, checksums: Dict[str, Any]) -> None:
        """Validate checksum assertions."""
        assertions = self.assertions.checksum_assertions
        if not assertions:
            return

        if "files_present" in assertions:
            expected = assertions["files_present"]
            files = checksums.get("files", [])
            files_present = len(files) > 0
            self.report.add(
                AssertionResult(
                    name="checksum.files_present",
                    passed=files_present == expected,
                    expected=expected,
                    actual=files_present,
                    message="Checksum files presence mismatch",
                )
            )

        if "file_count" in assertions:
            expected = assertions["file_count"]
            files = checksums.get("files", [])
            file_count = len(files)
            self.report.add(
                AssertionResult(
                    name="checksum.file_count",
                    passed=file_count == expected,
                    expected=expected,
                    actual=file_count,
                    message="Checksum file count mismatch",
                )
            )

        if "checksums_valid" in assertions and assertions["checksums_valid"]:
            files = checksums.get("files", [])
            all_have_checksums = all(f.get("checksum") for f in files)
            checksum_status = (
                "checksums present" if all_have_checksums else "missing checksums"
            )
            self.report.add(
                AssertionResult(
                    name="checksum.valid",
                    passed=all_have_checksums,
                    expected="all files have checksums",
                    actual=checksum_status,
                    message="Some files missing checksums",
                )
            )

    def validate_pattern_assertions(
        self, df: pd.DataFrame, previous_df: Optional[pd.DataFrame] = None
    ) -> None:
        """Validate pattern-specific assertions."""
        assertions = self.assertions.pattern_assertions
        if not assertions:
            return

        pattern = self.assertions.pattern

        # SNAPSHOT assertions
        if pattern == "snapshot":
            if "replaces_previous" in assertions and previous_df is not None:
                # Verify no overlap in IDs (complete replacement)
                if "record_id" in df.columns and "record_id" in previous_df.columns:
                    current_ids = set(df["record_id"])
                    previous_ids = set(previous_df["record_id"])
                    overlap = current_ids & previous_ids
                    expected = assertions["replaces_previous"]
                    # If replaces_previous=True, we expect no overlap (or complete overlap for same data)
                    is_replacement = len(overlap) == 0 or overlap == current_ids
                    self.report.add(
                        AssertionResult(
                            name="pattern.replaces_previous",
                            passed=is_replacement == expected,
                            expected=expected,
                            actual=is_replacement,
                            message=f"Snapshot replacement check - overlap: {len(overlap)} IDs",
                        )
                    )

        # INCREMENTAL_APPEND assertions
        elif pattern == "incremental_append":
            if "appends_to_existing" in assertions and previous_df is not None:
                # Verify new batch contains only new IDs
                if "record_id" in df.columns and "record_id" in previous_df.columns:
                    current_ids = set(df["record_id"])
                    previous_ids = set(previous_df["record_id"])
                    overlap = current_ids & previous_ids
                    expected = assertions["appends_to_existing"]
                    is_append_only = len(overlap) == 0
                    self.report.add(
                        AssertionResult(
                            name="pattern.appends_to_existing",
                            passed=is_append_only == expected,
                            expected=expected,
                            actual=is_append_only,
                            message=f"Append check - {len(overlap)} overlapping IDs",
                        )
                    )

            if "new_rows_count" in assertions:
                expected = assertions["new_rows_count"]
                actual = len(df)
                self.report.add(
                    AssertionResult(
                        name="pattern.new_rows_count",
                        passed=actual == expected,
                        expected=expected,
                        actual=actual,
                        message="New rows count mismatch",
                    )
                )

        # INCREMENTAL_MERGE assertions
        elif pattern == "incremental_merge":
            if "updated_rows_count" in assertions:
                expected = assertions["updated_rows_count"]
                # Count rows that exist in previous batch
                if previous_df is not None and "record_id" in df.columns:
                    previous_ids = set(previous_df["record_id"])
                    actual = sum(1 for rid in df["record_id"] if rid in previous_ids)
                else:
                    actual = 0
                self.report.add(
                    AssertionResult(
                        name="pattern.updated_rows_count",
                        passed=actual == expected,
                        expected=expected,
                        actual=actual,
                        message="Updated rows count mismatch",
                    )
                )

            if "inserted_rows_count" in assertions:
                expected = assertions["inserted_rows_count"]
                # Count rows that don't exist in previous batch
                if previous_df is not None and "record_id" in df.columns:
                    previous_ids = set(previous_df["record_id"])
                    actual = sum(
                        1 for rid in df["record_id"] if rid not in previous_ids
                    )
                else:
                    actual = len(df)
                self.report.add(
                    AssertionResult(
                        name="pattern.inserted_rows_count",
                        passed=actual == expected,
                        expected=expected,
                        actual=actual,
                        message="Inserted rows count mismatch",
                    )
                )

        # CURRENT_HISTORY (SCD2) assertions
        elif pattern == "current_history":
            if "current_rows" in assertions:
                expected = assertions["current_rows"]
                if "is_current" in df.columns:
                    actual = df["is_current"].sum()
                else:
                    actual = len(df)
                self.report.add(
                    AssertionResult(
                        name="pattern.current_rows",
                        passed=actual == expected,
                        expected=expected,
                        actual=actual,
                        message="Current rows count mismatch",
                    )
                )

            if "history_rows" in assertions:
                expected = assertions["history_rows"]
                actual = len(df)
                self.report.add(
                    AssertionResult(
                        name="pattern.history_rows",
                        passed=actual == expected,
                        expected=expected,
                        actual=actual,
                        message="History rows count mismatch",
                    )
                )

    def _types_match(self, expected: str, actual: str) -> bool:
        """Check if type names match (with normalization)."""
        type_aliases = {
            "string": ["object", "string", "str"],
            "int": ["int64", "int32", "int", "integer"],
            "float": ["float64", "float32", "float"],
            "bool": ["bool", "boolean"],
            "datetime": ["datetime64[ns]", "datetime64", "datetime"],
            "date": ["date", "datetime64[ns]"],
        }

        expected_lower = expected.lower()
        actual_lower = actual.lower()

        # Direct match
        if expected_lower == actual_lower:
            return True

        # Alias match
        for _, aliases in type_aliases.items():
            if expected_lower in aliases and actual_lower in aliases:
                return True

        return False


# Factory functions for creating assertions
def create_snapshot_assertions(
    row_count: int,
    columns: List[str],
    first_row_values: Optional[Dict[str, Any]] = None,
) -> PatternAssertions:
    """Create assertions for SNAPSHOT pattern."""
    return PatternAssertions(
        pattern="snapshot",
        scenario="generated",
        row_assertions={
            "row_count": row_count,
            "columns_present": columns,
            "first_row": first_row_values or {},
            "unique_columns": ["record_id"] if "record_id" in columns else [],
        },
        metadata_assertions={
            "load_pattern": "snapshot",
        },
        checksum_assertions={
            "files_present": True,
        },
        pattern_assertions={
            "replaces_previous": True,
        },
    )


def create_incremental_append_assertions(
    new_rows_count: int,
    columns: List[str],
) -> PatternAssertions:
    """Create assertions for INCREMENTAL_APPEND pattern."""
    return PatternAssertions(
        pattern="incremental_append",
        scenario="generated",
        row_assertions={
            "row_count": new_rows_count,
            "columns_present": columns,
            "unique_columns": ["record_id"] if "record_id" in columns else [],
        },
        metadata_assertions={
            "load_pattern": "incremental_append",
        },
        checksum_assertions={
            "files_present": True,
        },
        pattern_assertions={
            "appends_to_existing": True,
            "new_rows_count": new_rows_count,
        },
    )


def create_incremental_merge_assertions(
    updated_count: int,
    inserted_count: int,
    columns: List[str],
) -> PatternAssertions:
    """Create assertions for INCREMENTAL_MERGE pattern."""
    return PatternAssertions(
        pattern="incremental_merge",
        scenario="generated",
        row_assertions={
            "row_count": updated_count + inserted_count,
            "columns_present": columns,
        },
        metadata_assertions={
            "load_pattern": "incremental_merge",
        },
        checksum_assertions={
            "files_present": True,
        },
        pattern_assertions={
            "updated_rows_count": updated_count,
            "inserted_rows_count": inserted_count,
        },
    )


def create_scd2_assertions(
    current_rows: int,
    history_rows: int,
    columns: List[str],
) -> PatternAssertions:
    """Create assertions for CURRENT_HISTORY (SCD2) pattern."""
    return PatternAssertions(
        pattern="current_history",
        scenario="generated",
        row_assertions={
            "row_count": history_rows,
            "columns_present": columns,
        },
        metadata_assertions={
            "load_pattern": "current_history",
        },
        checksum_assertions={
            "files_present": True,
        },
        pattern_assertions={
            "current_rows": current_rows,
            "history_rows": history_rows,
        },
    )
