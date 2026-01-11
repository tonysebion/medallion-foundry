"""Reusable assertion helpers for validating Silver layer data transformations.

These classes provide standard assertions for SCD1, SCD2, and EVENT patterns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd


@dataclass
class ValidationResult:
    """Result of a validation check."""

    passed: bool
    message: str
    details: Optional[dict] = None


class SCD1Assertions:
    """Assertion helpers for SCD Type 1 (CURRENT_ONLY) data.

    SCD1 characteristics:
    - Natural keys are unique (no duplicates)
    - Only the latest version of each record is kept
    - No temporal columns (effective_from/to, is_current)
    """

    @staticmethod
    def assert_unique_keys(df: pd.DataFrame, keys: List[str]) -> ValidationResult:
        """Assert that natural keys are unique.

        Args:
            df: DataFrame to validate
            keys: List of natural key columns

        Returns:
            ValidationResult with pass/fail and details
        """
        if df.empty:
            return ValidationResult(True, "Empty DataFrame - keys trivially unique")

        # Check for duplicates
        duplicates = df[df.duplicated(subset=keys, keep=False)]
        if len(duplicates) == 0:
            return ValidationResult(True, f"All {len(df)} records have unique keys")

        dup_count = len(duplicates)
        dup_keys = duplicates[keys].drop_duplicates().to_dict("records")[:5]
        return ValidationResult(
            False,
            f"Found {dup_count} records with duplicate keys",
            {"duplicate_keys": dup_keys, "total_duplicates": dup_count},
        )

    @staticmethod
    def assert_no_scd2_columns(
        df: pd.DataFrame,
        effective_from: str = "effective_from",
        effective_to: str = "effective_to",
        is_current: str = "is_current",
    ) -> ValidationResult:
        """Assert that SCD2 temporal columns are NOT present.

        Args:
            df: DataFrame to validate
            effective_from: Name of effective_from column to check absence
            effective_to: Name of effective_to column to check absence
            is_current: Name of is_current column to check absence

        Returns:
            ValidationResult with pass/fail
        """
        scd2_cols = {effective_from, effective_to, is_current}
        present_cols = scd2_cols.intersection(set(df.columns))

        if not present_cols:
            return ValidationResult(True, "No SCD2 columns present (correct for SCD1)")

        return ValidationResult(
            False,
            f"SCD2 columns found in SCD1 data: {present_cols}",
            {"unexpected_columns": list(present_cols)},
        )

    @staticmethod
    def assert_latest_values(
        df: pd.DataFrame,
        keys: List[str],
        order_by: str,
        expected_values: dict,
    ) -> ValidationResult:
        """Assert that specific keys have their expected latest values.

        Args:
            df: DataFrame to validate
            keys: List of natural key columns
            order_by: Column used for ordering (to verify "latest")
            expected_values: Dict mapping key tuples to expected column values

        Returns:
            ValidationResult with pass/fail
        """
        failures = []
        for key_tuple, expected in expected_values.items():
            if len(keys) == 1:
                mask = df[keys[0]] == key_tuple
            else:
                mask = pd.Series([True] * len(df))
                for i, k in enumerate(keys):
                    mask &= df[k] == key_tuple[i]

            rows = df[mask]
            if len(rows) == 0:
                failures.append(f"Key {key_tuple} not found")
                continue

            if len(rows) > 1:
                failures.append(f"Key {key_tuple} has {len(rows)} rows (expected 1)")
                continue

            row = rows.iloc[0]
            for col, exp_val in expected.items():
                if row[col] != exp_val:
                    failures.append(
                        f"Key {key_tuple}: {col}={row[col]} (expected {exp_val})"
                    )

        if not failures:
            return ValidationResult(True, "All expected values match")

        return ValidationResult(
            False, f"{len(failures)} value mismatches found", {"failures": failures}
        )


class SCD2Assertions:
    """Assertion helpers for SCD Type 2 (FULL_HISTORY) data.

    SCD2 characteristics:
    - Multiple versions per natural key (history preserved)
    - effective_from equals the change timestamp
    - effective_to equals the next version's effective_from (NULL for current)
    - is_current = 1 for only the latest version per key
    - Current view should have unique keys
    """

    @staticmethod
    def assert_has_scd2_columns(
        df: pd.DataFrame,
        effective_from: str = "effective_from",
        effective_to: str = "effective_to",
        is_current: str = "is_current",
    ) -> ValidationResult:
        """Assert that all required SCD2 columns are present.

        Returns:
            ValidationResult with pass/fail
        """
        required = {effective_from, effective_to, is_current}
        present = set(df.columns)
        missing = required - present

        if not missing:
            return ValidationResult(True, "All SCD2 columns present")

        return ValidationResult(
            False,
            f"Missing SCD2 columns: {missing}",
            {"missing_columns": list(missing)},
        )

    @staticmethod
    def assert_effective_from_equals_ts(
        df: pd.DataFrame, ts_col: str, effective_from: str = "effective_from"
    ) -> ValidationResult:
        """Assert effective_from equals the timestamp column for all rows.

        Returns:
            ValidationResult with pass/fail
        """
        if df.empty:
            return ValidationResult(True, "Empty DataFrame - trivially valid")

        mismatches = df[df[effective_from] != df[ts_col]]
        if len(mismatches) == 0:
            return ValidationResult(
                True, "effective_from equals timestamp for all rows"
            )

        return ValidationResult(
            False,
            f"{len(mismatches)} rows have effective_from != {ts_col}",
            {"mismatch_count": len(mismatches)},
        )

    @staticmethod
    def assert_current_view_unique(
        df: pd.DataFrame, keys: List[str], is_current: str = "is_current"
    ) -> ValidationResult:
        """Assert current records (is_current=1) have unique natural keys.

        Returns:
            ValidationResult with pass/fail
        """
        current = df[df[is_current] == 1]

        if current.empty:
            return ValidationResult(True, "No current records - trivially unique")

        duplicates = current[current.duplicated(subset=keys, keep=False)]
        if len(duplicates) == 0:
            return ValidationResult(
                True, f"All {len(current)} current records have unique keys"
            )

        dup_keys = duplicates[keys].drop_duplicates().to_dict("records")[:5]
        return ValidationResult(
            False,
            f"{len(duplicates)} current records have duplicate keys",
            {"duplicate_keys": dup_keys},
        )

    @staticmethod
    def assert_one_current_per_key(
        df: pd.DataFrame, keys: List[str], is_current: str = "is_current"
    ) -> ValidationResult:
        """Assert exactly one is_current=1 per natural key.

        Returns:
            ValidationResult with pass/fail
        """
        if df.empty:
            return ValidationResult(True, "Empty DataFrame - trivially valid")

        # Group by keys and sum is_current
        grouped = df.groupby(keys)[is_current].sum().reset_index()
        invalid = grouped[grouped[is_current] != 1]

        if len(invalid) == 0:
            return ValidationResult(
                True, f"All {len(grouped)} keys have exactly one current record"
            )

        # Keys with 0 or multiple current records
        zero_current = grouped[grouped[is_current] == 0]
        multiple_current = grouped[grouped[is_current] > 1]

        return ValidationResult(
            False,
            f"{len(invalid)} keys have invalid current count",
            {
                "zero_current": len(zero_current),
                "multiple_current": len(multiple_current),
                "sample_invalid": invalid[keys].head().to_dict("records"),
            },
        )

    @staticmethod
    def assert_effective_dates_contiguous(
        df: pd.DataFrame,
        keys: List[str],
        effective_from: str = "effective_from",
        effective_to: str = "effective_to",
    ) -> ValidationResult:
        """Assert effective_to equals next effective_from (no gaps).

        Returns:
            ValidationResult with pass/fail
        """
        if df.empty or len(df) <= 1:
            return ValidationResult(True, "Too few records to check contiguity")

        gaps = []

        for key_vals, group in df.groupby(keys):
            sorted_group = group.sort_values(effective_from)

            for i in range(len(sorted_group) - 1):
                current = sorted_group.iloc[i]
                next_ver = sorted_group.iloc[i + 1]

                # Skip if current is NULL (current version)
                if pd.isna(current[effective_to]):
                    continue

                if current[effective_to] != next_ver[effective_from]:
                    gaps.append(
                        {
                            "key": key_vals,
                            "gap_from": str(current[effective_to]),
                            "gap_to": str(next_ver[effective_from]),
                        }
                    )

        if not gaps:
            return ValidationResult(True, "Effective dates are contiguous")

        return ValidationResult(
            False,
            f"Found {len(gaps)} gaps in effective date ranges",
            {"gaps": gaps[:5]},  # First 5 gaps
        )

    @staticmethod
    def assert_current_has_null_effective_to(
        df: pd.DataFrame,
        effective_to: str = "effective_to",
        is_current: str = "is_current",
    ) -> ValidationResult:
        """Assert current records have NULL effective_to.

        Returns:
            ValidationResult with pass/fail
        """
        current = df[df[is_current] == 1]
        if current.empty:
            return ValidationResult(True, "No current records to check")

        non_null = current[~current[effective_to].isna()]
        if len(non_null) == 0:
            return ValidationResult(
                True, f"All {len(current)} current records have NULL effective_to"
            )

        return ValidationResult(
            False,
            f"{len(non_null)} current records have non-NULL effective_to",
            {"non_null_count": len(non_null)},
        )

    @staticmethod
    def assert_history_preserved(
        df: pd.DataFrame, keys: List[str], expected_counts: dict
    ) -> ValidationResult:
        """Assert expected number of versions per key.

        Args:
            expected_counts: Dict mapping key tuple to expected version count

        Returns:
            ValidationResult with pass/fail
        """
        failures = []

        for key_tuple, expected_count in expected_counts.items():
            if len(keys) == 1:
                mask = df[keys[0]] == key_tuple
            else:
                mask = pd.Series([True] * len(df))
                for i, k in enumerate(keys):
                    mask &= df[k] == key_tuple[i]

            actual_count = len(df[mask])
            if actual_count != expected_count:
                failures.append(
                    f"Key {key_tuple}: {actual_count} versions (expected {expected_count})"
                )

        if not failures:
            return ValidationResult(True, "All keys have expected version counts")

        return ValidationResult(
            False,
            f"{len(failures)} keys have wrong version counts",
            {"failures": failures},
        )


class EventAssertions:
    """Assertion helpers for EVENT entity data.

    EVENT characteristics:
    - Records are immutable (once written, never changed)
    - Exact duplicates only can be removed
    - No SCD2 temporal columns needed
    - Append-only behavior between batches
    """

    @staticmethod
    def assert_no_scd2_columns(
        df: pd.DataFrame,
        effective_from: str = "effective_from",
        effective_to: str = "effective_to",
        is_current: str = "is_current",
    ) -> ValidationResult:
        """Assert that SCD2 temporal columns are NOT present.

        Returns:
            ValidationResult with pass/fail
        """
        return SCD1Assertions.assert_no_scd2_columns(
            df, effective_from, effective_to, is_current
        )

    @staticmethod
    def assert_records_immutable(
        before_df: pd.DataFrame, after_df: pd.DataFrame, event_key: str
    ) -> ValidationResult:
        """Assert existing records are unchanged between batches.

        Args:
            before_df: DataFrame before new batch
            after_df: DataFrame after new batch
            event_key: Unique event identifier column

        Returns:
            ValidationResult with pass/fail
        """
        if before_df.empty:
            return ValidationResult(True, "No prior records to check immutability")

        # Get common keys
        before_keys = set(before_df[event_key])
        after_keys = set(after_df[event_key])
        common_keys = before_keys & after_keys

        if not common_keys:
            return ValidationResult(True, "No common keys - append only")

        # Compare records with common keys
        before_common = before_df[before_df[event_key].isin(common_keys)].sort_values(
            event_key
        )
        after_common = after_df[after_df[event_key].isin(common_keys)].sort_values(
            event_key
        )

        # Reset index for comparison
        before_common = before_common.reset_index(drop=True)
        after_common = after_common.reset_index(drop=True)

        try:
            pd.testing.assert_frame_equal(before_common, after_common)
            return ValidationResult(
                True, f"All {len(common_keys)} existing records unchanged"
            )
        except AssertionError as e:
            return ValidationResult(
                False, "Existing records were modified", {"assertion_error": str(e)}
            )

    @staticmethod
    def assert_append_only(
        before_df: pd.DataFrame, after_df: pd.DataFrame, event_key: str
    ) -> ValidationResult:
        """Assert new batch only adds records, never removes.

        Returns:
            ValidationResult with pass/fail
        """
        before_keys = set(before_df[event_key])
        after_keys = set(after_df[event_key])

        removed = before_keys - after_keys
        if not removed:
            return ValidationResult(
                True, f"Append-only: {len(after_keys - before_keys)} new records added"
            )

        return ValidationResult(
            False,
            f"{len(removed)} records were removed",
            {"removed_keys": list(removed)[:10]},
        )

    @staticmethod
    def assert_no_exact_duplicates(df: pd.DataFrame) -> ValidationResult:
        """Assert no exact duplicate rows exist.

        Returns:
            ValidationResult with pass/fail
        """
        if df.empty:
            return ValidationResult(True, "Empty DataFrame - no duplicates")

        duplicates = df[df.duplicated(keep=False)]
        if len(duplicates) == 0:
            return ValidationResult(True, f"No exact duplicates in {len(df)} rows")

        dup_count = len(duplicates)
        unique_dup_groups = len(duplicates.drop_duplicates())

        return ValidationResult(
            False,
            f"{dup_count} rows are exact duplicates ({unique_dup_groups} unique patterns)",
            {"duplicate_row_count": dup_count},
        )
