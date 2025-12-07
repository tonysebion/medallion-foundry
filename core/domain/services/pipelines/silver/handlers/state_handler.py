"""Handler for STATE and DERIVED_STATE entity type processing."""

from __future__ import annotations

from typing import Dict, TYPE_CHECKING

import pandas as pd

from core.infrastructure.config import HistoryMode, EntityKind
from core.domain.services.pipelines.silver.handlers.base import BasePatternHandler
from core.domain.services.pipelines.silver.handlers.registry import register_handler

if TYPE_CHECKING:
    from core.infrastructure.config import DatasetConfig


# StateHandler handles both STATE and DERIVED_STATE; register both
@register_handler(EntityKind.STATE)
@register_handler(EntityKind.DERIVED_STATE, derived=True)
class StateHandler(BasePatternHandler):
    """Handler for STATE and DERIVED_STATE entity types.

    State entities represent the current state of a business object that changes over time.
    This handler processes state data according to the configured history mode:
    - SCD1/LATEST_ONLY: Keep only the most recent version of each entity
    - SCD2: Track full history with effective_from/effective_to dates
    """

    def __init__(self, dataset: "DatasetConfig", derived: bool = False) -> None:
        """Initialize the handler.

        Args:
            dataset: Dataset configuration with Silver settings.
            derived: If True, processing derived state (from computed sources).
        """
        super().__init__(dataset)
        self.derived = derived

    def validate(self, df: pd.DataFrame) -> None:
        """Validate DataFrame has required state columns.

        Args:
            df: Input DataFrame to validate.

        Raises:
            ValueError: If change_ts_column is not configured or missing from data.
        """
        ts_col = self.change_ts_column or self.event_ts_column
        if not ts_col:
            raise ValueError("change_ts_column is required for state datasets")
        if ts_col not in df.columns:
            raise ValueError(f"Change timestamp column '{ts_col}' not found in data")

    def process(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Process state data according to history mode.

        Args:
            df: Input DataFrame with state data.

        Returns:
            Dict with output DataFrames. For SCD1/LATEST_ONLY: {"state_current": df}.
            For SCD2: {"state_current": df, "state_history": df}.
        """
        self.validate(df)

        history_mode = self.dataset.silver.history_mode or HistoryMode.SCD2
        ts_col = self.change_ts_column or self.event_ts_column
        assert ts_col is not None  # validate() ensures this

        ordered = df.sort_values(self.natural_keys + [ts_col]).copy()

        if history_mode in (HistoryMode.SCD1, HistoryMode.LATEST_ONLY):
            current = ordered.drop_duplicates(
                subset=self.natural_keys, keep="last"
            )
            return {"state_current": current.reset_index(drop=True)}

        # SCD2: Build history with effective dates
        history = ordered.copy()
        history["effective_from"] = history[ts_col]
        history["effective_to"] = history.groupby(self.natural_keys)[
            "effective_from"
        ].shift(-1)
        history["is_current"] = history["effective_to"].isna().astype(int)
        current = history[history["is_current"] == 1].copy()

        return {
            "state_history": history.reset_index(drop=True),
            "state_current": current.reset_index(drop=True),
        }
