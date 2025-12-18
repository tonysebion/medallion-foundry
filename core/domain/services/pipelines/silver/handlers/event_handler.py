"""Handler for EVENT entity type processing."""

from __future__ import annotations

from typing import Dict

import pandas as pd

from core.infrastructure.config import InputMode, EntityKind
from core.domain.services.pipelines.silver.handlers.base import (
    BasePatternHandler,
    ensure_column_exists,
)
from core.domain.services.pipelines.silver.handlers.registry import register_handler


@register_handler(EntityKind.EVENT)
class EventHandler(BasePatternHandler):
    """Handler for EVENT entity type.

    Events are immutable facts that occurred at a specific point in time.
    This handler processes event data by:
    - Sorting by event timestamp
    - Deduplicating based on natural keys and date (for REPLACE_DAILY mode)
    """

    def validate(self, df: pd.DataFrame) -> None:
        """Validate DataFrame has required event columns.

        Args:
            df: Input DataFrame to validate.

        Raises:
            ValueError: If event_ts_column is not configured or missing from data.
        """
        ensure_column_exists(
            df, self.event_ts_column, "event_ts_column", required=True
        )

    def process(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Process event data.

        Args:
            df: Input DataFrame with event data.

        Returns:
            Dict with single "events" key containing processed DataFrame.
        """
        self.validate(df)

        ts_col = self.event_ts_column
        assert ts_col is not None  # validate() ensures this

        sorted_df = df.sort_values(ts_col)

        if self.dataset.silver.input_mode == InputMode.REPLACE_DAILY:
            date_col = f"{ts_col}_date"
            sorted_df[date_col] = sorted_df[ts_col].dt.date.astype(str)
            subset = self.natural_keys + [date_col]
            sorted_df = sorted_df.drop_duplicates(subset=subset, keep="last")

        return {"events": sorted_df.reset_index(drop=True)}
