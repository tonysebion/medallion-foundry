"""Shared load-pattern utilities for Bronze/Silver workflows.

Load Patterns per Spec Section 4:
- SNAPSHOT: Complete snapshot each run (replaces all data)
- INCREMENTAL_APPEND: Append new/changed records (insert-only)
- INCREMENTAL_MERGE: Merge/upsert with existing data

Legacy pattern names (deprecated):
- full -> SNAPSHOT
- cdc -> INCREMENTAL_APPEND
- current_history -> kept for backward compatibility during transition
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, List

from core.foundation.primitives.base import RichEnumMixin


# Define aliases and descriptions as module constants
# (can't be class attributes due to Enum metaclass behavior)
_LOAD_PATTERN_ALIASES: Dict[str, str] = {
    "full": "snapshot",
    "cdc": "incremental_append",
}

_LOAD_PATTERN_DESCRIPTIONS: Dict[str, str] = {
    "snapshot": "Complete snapshot each run (replaces all data)",
    "incremental_append": "Append new/changed records (insert-only CDC)",
    "incremental_merge": "Merge/upsert with existing data",
    "current_history": "Maintains split current + history tables (SCD Type 2)",
}


class LoadPattern(RichEnumMixin, str, Enum):
    """Supported extraction patterns per spec Section 4."""

    # New spec-compliant names
    SNAPSHOT = "snapshot"
    INCREMENTAL_APPEND = "incremental_append"
    INCREMENTAL_MERGE = "incremental_merge"

    # Legacy compatibility (will be removed in future)
    CURRENT_HISTORY = "current_history"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "LoadPattern":
        """Normalize a pattern value, handling legacy names."""
        if raw is None:
            return cls.SNAPSHOT
        if isinstance(raw, cls):
            return raw

        candidate = raw.strip().lower()

        # Check aliases first
        canonical = _LOAD_PATTERN_ALIASES.get(candidate, candidate)

        for member in cls:
            if member.value == canonical:
                return member

        raise ValueError(
            f"Invalid load pattern '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        return _LOAD_PATTERN_DESCRIPTIONS.get(self.value, self.value)

    @property
    def chunk_prefix(self) -> str:
        """Prefix used for chunk file names."""
        return self.value.replace("_", "-")

    @property
    def folder_name(self) -> str:
        """Return folder name fragment for this pattern."""
        return f"pattern={self.value}"

    @property
    def is_incremental(self) -> bool:
        """Check if this pattern requires incremental/watermark logic."""
        return self in (self.INCREMENTAL_APPEND, self.INCREMENTAL_MERGE)

    @property
    def requires_merge(self) -> bool:
        """Check if this pattern requires merge/upsert logic."""
        return self == self.INCREMENTAL_MERGE
