"""CDC-specific assertion helpers for test validation.

Provides reusable assertions for validating CDC processing results:
- Key uniqueness after processing
- Tombstone flag correctness
- Operation column removal
- CDC vs snapshot equivalence
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Set, Any, Optional

import pandas as pd


@dataclass
class AssertionResult:
    """Result of an assertion check."""

    passed: bool
    message: str
    details: Optional[dict] = None


class CDCAssertions:
    """Assertion helpers for CDC-specific validation."""

    @staticmethod
    def assert_no_duplicate_keys(
        df: pd.DataFrame,
        keys: List[str],
    ) -> AssertionResult:
        """Verify each natural key appears exactly once.

        After CDC processing, each unique key combination should
        appear exactly once in the result.

        Args:
            df: DataFrame to validate
            keys: List of key column names

        Returns:
            AssertionResult indicating pass/fail
        """
        if df.empty:
            return AssertionResult(
                passed=True,
                message="Empty DataFrame has no duplicates",
            )

        # Check for missing key columns
        missing_cols = [k for k in keys if k not in df.columns]
        if missing_cols:
            return AssertionResult(
                passed=False,
                message=f"Missing key columns: {missing_cols}",
            )

        # Check for duplicates
        duplicates = df[df.duplicated(subset=keys, keep=False)]
        if len(duplicates) > 0:
            dup_keys = duplicates[keys].drop_duplicates().to_dict("records")
            return AssertionResult(
                passed=False,
                message=f"Found {len(duplicates)} duplicate key rows",
                details={"duplicate_keys": dup_keys[:10]},  # Limit to first 10
            )

        return AssertionResult(
            passed=True,
            message=f"All {len(df)} rows have unique keys",
        )

    @staticmethod
    def assert_tombstone_flag_correct(
        df: pd.DataFrame,
        expected_deleted_keys: Set[Any],
        key_column: str = "id",
    ) -> AssertionResult:
        """Verify _deleted flag matches expected deleted keys.

        Args:
            df: DataFrame with _deleted column
            expected_deleted_keys: Set of key values that should be deleted
            key_column: Name of the key column

        Returns:
            AssertionResult indicating pass/fail
        """
        if "_deleted" not in df.columns:
            return AssertionResult(
                passed=False,
                message="_deleted column not found",
            )

        if key_column not in df.columns:
            return AssertionResult(
                passed=False,
                message=f"Key column '{key_column}' not found",
            )

        errors = []

        for _, row in df.iterrows():
            key_val = row[key_column]
            is_deleted = bool(row["_deleted"])
            should_be_deleted = key_val in expected_deleted_keys

            if is_deleted != should_be_deleted:
                errors.append(
                    {
                        "key": key_val,
                        "actual_deleted": is_deleted,
                        "expected_deleted": should_be_deleted,
                    }
                )

        if errors:
            return AssertionResult(
                passed=False,
                message=f"Found {len(errors)} tombstone flag mismatches",
                details={"mismatches": errors[:10]},
            )

        return AssertionResult(
            passed=True,
            message="All tombstone flags correct",
        )

    @staticmethod
    def assert_operation_column_removed(
        df: pd.DataFrame,
        op_column: str = "op",
    ) -> AssertionResult:
        """Verify operation column is not in final output.

        After CDC processing, the operation column should be dropped.

        Args:
            df: DataFrame to validate
            op_column: Name of the operation column

        Returns:
            AssertionResult indicating pass/fail
        """
        if op_column in df.columns:
            return AssertionResult(
                passed=False,
                message=f"Operation column '{op_column}' should be removed",
            )

        return AssertionResult(
            passed=True,
            message="Operation column correctly removed",
        )

    @staticmethod
    def assert_resurrect_cleared_tombstone(
        df: pd.DataFrame,
        resurrected_keys: Set[Any],
        key_column: str = "id",
    ) -> AssertionResult:
        """Verify re-inserted keys have _deleted=False.

        Keys that were deleted and then re-inserted should have
        _deleted=False in the final state.

        Args:
            df: DataFrame with _deleted column
            resurrected_keys: Set of keys that were deleted then re-inserted
            key_column: Name of the key column

        Returns:
            AssertionResult indicating pass/fail
        """
        if "_deleted" not in df.columns:
            return AssertionResult(
                passed=False,
                message="_deleted column not found",
            )

        errors = []
        for key in resurrected_keys:
            rows = df[df[key_column] == key]
            if len(rows) == 0:
                errors.append({"key": key, "error": "Key not found"})
            elif bool(rows.iloc[0]["_deleted"]):
                errors.append({"key": key, "error": "_deleted is True, should be False"})

        if errors:
            return AssertionResult(
                passed=False,
                message=f"Found {len(errors)} resurrection errors",
                details={"errors": errors},
            )

        return AssertionResult(
            passed=True,
            message="All resurrected keys have _deleted=False",
        )

    @staticmethod
    def assert_cdc_equals_snapshot(
        cdc_df: pd.DataFrame,
        snapshot_df: pd.DataFrame,
        keys: List[str],
        ignore_columns: Optional[List[str]] = None,
    ) -> AssertionResult:
        """Verify CDC accumulation matches snapshot state.

        The final state from CDC processing should match what a
        snapshot at the same point in time would show.

        Args:
            cdc_df: DataFrame from CDC processing
            snapshot_df: DataFrame from snapshot
            keys: List of key column names for matching
            ignore_columns: Columns to ignore in comparison (e.g., metadata)

        Returns:
            AssertionResult indicating pass/fail
        """
        ignore_columns = ignore_columns or []

        # Check record counts
        if len(cdc_df) != len(snapshot_df):
            return AssertionResult(
                passed=False,
                message=f"Record count mismatch: CDC={len(cdc_df)}, Snapshot={len(snapshot_df)}",
            )

        # Get comparable columns
        cdc_cols = set(cdc_df.columns) - set(ignore_columns) - {"_deleted", "op"}
        snap_cols = set(snapshot_df.columns) - set(ignore_columns)
        common_cols = cdc_cols & snap_cols

        if not common_cols:
            return AssertionResult(
                passed=False,
                message="No common columns to compare",
            )

        # Sort both by keys for comparison
        cdc_sorted = cdc_df.sort_values(keys).reset_index(drop=True)
        snap_sorted = snapshot_df.sort_values(keys).reset_index(drop=True)

        # Compare row by row
        mismatches = []
        for idx in range(len(cdc_sorted)):
            cdc_row = cdc_sorted.iloc[idx]
            snap_row = snap_sorted.iloc[idx]

            for col in common_cols:
                if col in keys:
                    continue  # Keys already matched by sort
                cdc_val = cdc_row.get(col)
                snap_val = snap_row.get(col)
                if cdc_val != snap_val:
                    mismatches.append(
                        {
                            "row": idx,
                            "column": col,
                            "cdc_value": cdc_val,
                            "snapshot_value": snap_val,
                        }
                    )

        if mismatches:
            return AssertionResult(
                passed=False,
                message=f"Found {len(mismatches)} value mismatches",
                details={"mismatches": mismatches[:10]},
            )

        return AssertionResult(
            passed=True,
            message="CDC and snapshot are equivalent",
        )

    @staticmethod
    def assert_latest_values(
        df: pd.DataFrame,
        keys: List[str],
        expected_values: dict,
    ) -> AssertionResult:
        """Verify specific keys have expected values.

        Args:
            df: DataFrame to validate
            keys: List of key column names
            expected_values: Dict mapping key values to expected column values
                Example: {1: {"name": "Alice", "value": 100}}

        Returns:
            AssertionResult indicating pass/fail
        """
        errors = []

        for key_val, expected in expected_values.items():
            if len(keys) == 1:
                rows = df[df[keys[0]] == key_val]
            else:
                # For composite keys, key_val should be a tuple
                mask = True
                for i, k in enumerate(keys):
                    mask = mask & (df[k] == key_val[i])
                rows = df[mask]

            if len(rows) == 0:
                errors.append({"key": key_val, "error": "Key not found"})
                continue

            row = rows.iloc[0]
            for col, exp_val in expected.items():
                actual_val = row.get(col)
                if actual_val != exp_val:
                    errors.append(
                        {
                            "key": key_val,
                            "column": col,
                            "expected": exp_val,
                            "actual": actual_val,
                        }
                    )

        if errors:
            return AssertionResult(
                passed=False,
                message=f"Found {len(errors)} value mismatches",
                details={"errors": errors},
            )

        return AssertionResult(
            passed=True,
            message="All expected values match",
        )

    @staticmethod
    def assert_all_deleted(df: pd.DataFrame) -> AssertionResult:
        """Verify all records are marked as deleted (tombstone mode).

        Args:
            df: DataFrame with _deleted column

        Returns:
            AssertionResult indicating pass/fail
        """
        if "_deleted" not in df.columns:
            return AssertionResult(
                passed=False,
                message="_deleted column not found",
            )

        not_deleted = df[~df["_deleted"]]
        if len(not_deleted) > 0:
            return AssertionResult(
                passed=False,
                message=f"Found {len(not_deleted)} records not marked as deleted",
            )

        return AssertionResult(
            passed=True,
            message=f"All {len(df)} records are marked as deleted",
        )

    @staticmethod
    def assert_none_deleted(df: pd.DataFrame) -> AssertionResult:
        """Verify no records are marked as deleted.

        Args:
            df: DataFrame with _deleted column

        Returns:
            AssertionResult indicating pass/fail
        """
        if "_deleted" not in df.columns:
            # No _deleted column means none are deleted
            return AssertionResult(
                passed=True,
                message="No _deleted column present",
            )

        deleted = df[df["_deleted"]]
        if len(deleted) > 0:
            return AssertionResult(
                passed=False,
                message=f"Found {len(deleted)} records marked as deleted",
            )

        return AssertionResult(
            passed=True,
            message=f"None of {len(df)} records are marked as deleted",
        )
