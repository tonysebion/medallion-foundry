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
from typing import List


class LoadPattern(str, Enum):
    """Supported extraction patterns per spec Section 4."""

    # New spec-compliant names
    SNAPSHOT = "snapshot"
    INCREMENTAL_APPEND = "incremental_append"
    INCREMENTAL_MERGE = "incremental_merge"

    # Legacy compatibility (will be removed in future)
    CURRENT_HISTORY = "current_history"

    @classmethod
    def choices(cls) -> List[str]:
        return [pattern.value for pattern in cls]

    @classmethod
    def normalize(cls, value: str | None) -> "LoadPattern":
        """Normalize a pattern value, handling legacy names."""
        if value is None:
            return cls.SNAPSHOT
        candidate = value.strip().lower()

        # Handle legacy pattern names
        legacy_mapping = {
            "full": cls.SNAPSHOT,
            "cdc": cls.INCREMENTAL_APPEND,
        }
        if candidate in legacy_mapping:
            return legacy_mapping[candidate]

        for pattern in cls:
            if pattern.value == candidate:
                return pattern
        raise ValueError(
            f"Invalid load pattern '{value}'. Valid options: {', '.join(cls.choices())}"
        )

    @property
    def chunk_prefix(self) -> str:
        """Prefix used for chunk file names."""
        return self.value.replace("_", "-")

    @property
    def folder_name(self) -> str:
        """Return folder name fragment for this pattern."""
        return f"pattern={self.value}"

    def describe(self) -> str:
        descriptions = {
            self.SNAPSHOT: "Complete snapshot each run (replaces all data)",
            self.INCREMENTAL_APPEND: "Append new/changed records (insert-only CDC)",
            self.INCREMENTAL_MERGE: "Merge/upsert with existing data",
            self.CURRENT_HISTORY: "Maintains split current + history tables (SCD Type 2)",
        }
        return descriptions.get(self, self.value)

    @property
    def is_incremental(self) -> bool:
        """Check if this pattern requires incremental/watermark logic."""
        return self in (self.INCREMENTAL_APPEND, self.INCREMENTAL_MERGE)

    @property
    def requires_merge(self) -> bool:
        """Check if this pattern requires merge/upsert logic."""
        return self == self.INCREMENTAL_MERGE
