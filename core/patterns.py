"""Shared load-pattern utilities for Bronze/Silver workflows."""

from __future__ import annotations

from enum import Enum
from typing import List


class LoadPattern(str, Enum):
    """Supported extraction patterns."""

    FULL = "full"
    CDC = "cdc"
    CURRENT_HISTORY = "current_history"

    @classmethod
    def choices(cls) -> List[str]:
        return [pattern.value for pattern in cls]

    @classmethod
    def normalize(cls, value: str | None) -> "LoadPattern":
        if value is None:
            return cls.FULL
        candidate = value.strip().lower()
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
            self.FULL: "Complete snapshot each run",
            self.CDC: "Change data capture (expects inserts/updates/deletes)",
            self.CURRENT_HISTORY: "Maintains split current + history tables",
        }
        return descriptions.get(self, self.value)
