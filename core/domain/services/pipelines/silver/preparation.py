"""DataFrame preparation and validation for Silver processing."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Set

import pandas as pd

from core.infrastructure.config import SchemaMode
from core.domain.services.pipelines.silver.handlers.base import ensure_columns_exist

if TYPE_CHECKING:
    from core.infrastructure.config import DatasetConfig

logger = logging.getLogger(__name__)

# Common metadata columns that are always allowed
COMMON_METADATA_COLUMNS = {"run_date"}


class DataFramePreparer:
    """Prepares and validates DataFrames for Silver processing.

    This class handles:
    - Validating required columns are present
    - Filtering to allowed columns based on schema mode
    - Converting timestamp columns to datetime
    - Deduplicating based on natural keys
    """

    def __init__(self, dataset: "DatasetConfig") -> None:
        """Initialize the preparer.

        Args:
            dataset: Dataset configuration with Silver settings.
        """
        self.dataset = dataset

    def get_expected_columns(self) -> Set[str]:
        """Get the set of expected columns based on dataset config."""
        expected = set(self.dataset.silver.natural_keys)

        if (
            self.dataset.silver.entity_kind.is_event_like
            and self.dataset.silver.event_ts_column
        ):
            expected.add(self.dataset.silver.event_ts_column)

        if (
            self.dataset.silver.entity_kind.is_state_like
            and self.dataset.silver.change_ts_column
        ):
            expected.add(self.dataset.silver.change_ts_column)
        expected.update(self.dataset.silver.attributes)
        return expected

    def get_allowed_columns(self, expected: Set[str]) -> Set[str]:
        """Get the set of allowed columns (expected + special columns)."""
        allowed = set(expected)
        if self.dataset.silver.order_column:
            allowed.add(self.dataset.silver.order_column)
        allowed.update({"is_deleted", "deleted_flag"})
        allowed.update(COMMON_METADATA_COLUMNS)
        return allowed

    def validate_columns(self, df: pd.DataFrame) -> None:
        """Validate DataFrame has required columns.

        Args:
            df: DataFrame to validate.

        Raises:
            ValueError: If required columns are missing.
        """
        expected = self.get_expected_columns()
        ensure_columns_exist(
            df,
            list(expected),
            f"Bronze data missing required columns for {self.dataset.dataset_id}",
        )

    def check_extra_columns(self, df: pd.DataFrame) -> None:
        """Check for unexpected columns based on schema mode.

        Args:
            df: DataFrame to check.

        Raises:
            ValueError: If unexpected columns exist in STRICT mode.
        """
        expected = self.get_expected_columns()
        allowed = self.get_allowed_columns(expected)
        # Also allow columns starting with underscore (metadata)
        allowed.update(col for col in df.columns if col.startswith("_"))

        extras = [col for col in df.columns if col not in allowed]
        if extras and self.dataset.silver.schema_mode == SchemaMode.STRICT:
            raise ValueError(
                f"Bronze data contains unexpected columns: {extras}. "
                "Update silver.attributes or switch to schema_mode=allow_new_columns."
            )

    def filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter DataFrame to only allowed columns.

        Args:
            df: DataFrame to filter.

        Returns:
            DataFrame with only allowed columns.
        """
        expected = self.get_expected_columns()
        allowed = self.get_allowed_columns(expected)
        allowed.update(col for col in df.columns if col.startswith("_"))

        selected_cols = [col for col in df.columns if col in allowed]
        return df[selected_cols].copy()

    def convert_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert timestamp columns to datetime type.

        Args:
            df: DataFrame with timestamp columns.

        Returns:
            DataFrame with converted timestamp columns.
        """
        if self.dataset.silver.event_ts_column:
            if self.dataset.silver.event_ts_column in df.columns:
                df[self.dataset.silver.event_ts_column] = pd.to_datetime(
                    df[self.dataset.silver.event_ts_column], errors="coerce"
                )

        if self.dataset.silver.change_ts_column:
            if self.dataset.silver.change_ts_column in df.columns:
                df[self.dataset.silver.change_ts_column] = pd.to_datetime(
                    df[self.dataset.silver.change_ts_column], errors="coerce"
                )

        order_column = self.dataset.silver.order_column
        if order_column and order_column in df.columns:
            df[order_column] = pd.to_datetime(df[order_column], errors="coerce")

        return df

    def _resolve_sort_column(self, df: pd.DataFrame) -> str | None:
        """Resolve the timestamp/order column for sorting during deduplication.

        Priority order:
        1. Explicit order_column if configured and present in DataFrame
        2. event_ts_column for event-like entities if present
        3. change_ts_column if present

        Args:
            df: DataFrame to check for column presence.

        Returns:
            Column name to use for sorting, or None if no valid column found.
        """
        silver = self.dataset.silver

        # Priority 1: Explicit order column
        if silver.order_column and silver.order_column in df.columns:
            return silver.order_column

        # Priority 2: Event timestamp for event-like entities
        if (
            silver.entity_kind.is_event_like
            and silver.event_ts_column
            and silver.event_ts_column in df.columns
        ):
            return silver.event_ts_column

        # Priority 3: Change timestamp as fallback
        if silver.change_ts_column and silver.change_ts_column in df.columns:
            return silver.change_ts_column

        return None

    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate DataFrame based on natural keys and timestamp.

        Args:
            df: DataFrame to deduplicate.

        Returns:
            Deduplicated DataFrame.
        """
        sort_columns = list(self.dataset.silver.natural_keys)

        sort_column = self._resolve_sort_column(df)
        if sort_column and sort_column not in sort_columns:
            sort_columns.append(sort_column)

        df = df.sort_values(sort_columns).drop_duplicates(
            subset=list(self.dataset.silver.natural_keys), keep="last"
        )
        return df.reset_index(drop=True)

    def prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """Full preparation pipeline: validate, filter, convert, dedupe.

        Args:
            df: Raw DataFrame from Bronze.

        Returns:
            Prepared DataFrame ready for pattern processing.

        Raises:
            ValueError: If validation fails.
        """
        self.validate_columns(df)
        self.check_extra_columns(df)
        working = self.filter_columns(df)
        working = self.convert_timestamps(working)
        working = self.deduplicate(working)
        return working
