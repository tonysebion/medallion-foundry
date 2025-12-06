"""Helpers for cursor state persistence shared by database extractors."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class CursorStateManager:
    """Manage cursor persistence for incremental database extractions."""

    state_dir: Path | None = None

    def __post_init__(self) -> None:
        self._state_dir = (self.state_dir or Path(".state")).resolve()
        self._state_dir.mkdir(parents=True, exist_ok=True)

    def _state_path(self, key: str) -> Path:
        key_safe = key.replace("/", "_").replace("\\", "_")
        return self._state_dir / f"{key_safe}_cursor.json"

    def load_cursor(self, key: str) -> Optional[str]:
        """Load the stored cursor for a key."""
        path = self._state_path(key)
        if not path.exists():
            logger.debug("No cursor file found for %s at %s", key, path)
            return None

        try:
            with path.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
            cursor = state.get("cursor")
            if cursor is not None:
                cursor = str(cursor)
            logger.debug("Loaded cursor for %s: %s", key, cursor)
            return cursor
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load cursor state for %s: %s", key, exc)
            return None

    def save_cursor(self, key: str, cursor: str, run_date: date) -> None:
        """Persist cursor for a key."""
        path = self._state_path(key)
        state = {"cursor": cursor, "last_run": run_date.isoformat()}
        try:
            with path.open("w", encoding="utf-8") as handle:
                json.dump(state, handle, indent=2)
            logger.debug("Saved cursor state for %s to %s", key, path)
        except OSError as exc:
            logger.error("Failed to save cursor state for %s: %s", key, exc)
            raise


def build_incremental_query(
    base_query: str,
    cursor_column: Optional[str],
    last_cursor: Optional[str],
) -> str:
    """Append an incremental WHERE clause to a query if applicable."""
    if not cursor_column or not last_cursor:
        return base_query

    query_upper = base_query.upper()
    connector = "AND" if "WHERE" in query_upper else "WHERE"
    incremental_clause = f"\n{connector} {cursor_column} > ?"
    return base_query + incremental_clause
