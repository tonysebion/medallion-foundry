"""Base class for Silver pattern handlers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd

if TYPE_CHECKING:
    from core.infrastructure.config import DatasetConfig


def ensure_column_exists(
    df: pd.DataFrame,
    column: Optional[str],
    column_description: str,
    required: bool = True,
) -> None:
    """Validate that a column exists in a DataFrame.

    Args:
        df: DataFrame to check.
        column: Column name to look for (may be None if not configured).
        column_description: Human-readable description for error messages
            (e.g., "event_ts_column", "change_ts_column").
        required: If True, raises error when column is None.

    Raises:
        ValueError: If column is None (when required=True) or not in DataFrame.
    """
    if not column:
        if required:
            raise ValueError(f"{column_description} is required for this dataset")
        return
    if column not in df.columns:
        raise ValueError(f"{column_description} '{column}' not found in data")


def ensure_columns_exist(
    df: pd.DataFrame,
    columns: List[str],
    context: str,
) -> None:
    """Validate that all specified columns exist in a DataFrame.

    Args:
        df: DataFrame to check.
        columns: List of column names that must exist.
        context: Context description for error messages (e.g., dataset ID).

    Raises:
        ValueError: If any columns are missing.
    """
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"{context}: missing required columns {missing}")


class BasePatternHandler(ABC):
    """Base class for Silver pattern handlers.

    Each handler processes a specific entity kind (EVENT, STATE, DERIVED_EVENT, etc.)
    according to its pattern rules.

    Attributes:
        dataset: The dataset configuration containing Silver processing rules.
    """

    def __init__(self, dataset: "DatasetConfig") -> None:
        """Initialize the handler.

        Args:
            dataset: Dataset configuration with Silver settings.
        """
        self.dataset = dataset

    @abstractmethod
    def process(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Process DataFrame according to pattern rules.

        Args:
            df: Input DataFrame with prepared data.

        Returns:
            Dict mapping output name to processed DataFrame.
            For example: {"events": df} or {"state_current": df, "state_history": df}
        """
        pass

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> None:
        """Validate DataFrame has required columns for this pattern.

        Args:
            df: Input DataFrame to validate.

        Raises:
            ValueError: If required columns are missing.
        """
        pass

    @property
    def natural_keys(self) -> list[str]:
        """Get natural key columns from dataset config."""
        return self.dataset.silver.natural_keys

    @property
    def event_ts_column(self) -> str | None:
        """Get event timestamp column from dataset config."""
        return self.dataset.silver.event_ts_column

    @property
    def change_ts_column(self) -> str | None:
        """Get change timestamp column from dataset config."""
        return self.dataset.silver.change_ts_column

    @property
    def timestamp_column(self) -> str | None:
        """Get the preferred timestamp column for ordering.

        Returns change_ts_column if set, otherwise falls back to event_ts_column.
        This is the standard priority for state and derived_event patterns.
        """
        return self.change_ts_column or self.event_ts_column
