"""Core model types for bronze-foundry.

This module contains foundational model enums that are used across layers.
These are pure data structures with minimal dependencies.
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, List

from core.foundation.primitives.base import RichEnumMixin
from core.foundation.primitives.patterns import LoadPattern


# Define aliases and descriptions as module constants
# (can't be class attributes due to Enum metaclass behavior)
_SILVER_MODEL_ALIASES: Dict[str, str] = {
    "scd_type_1": "scd_type_1",
    "scd1": "scd_type_1",
    "scd type 1": "scd_type_1",
    "scd_type_2": "scd_type_2",
    "scd2": "scd_type_2",
    "scd type 2": "scd_type_2",
    "incremental_merge": "incremental_merge",
    "incremental": "incremental_merge",
    "full_merge_dedupe": "full_merge_dedupe",
    "full_merge": "full_merge_dedupe",
    "full merge": "full_merge_dedupe",
    "periodic_snapshot": "periodic_snapshot",
    "periodic": "periodic_snapshot",
}

_SILVER_MODEL_DESCRIPTIONS: Dict[str, str] = {
    "scd_type_1": "Keep only the latest version of each business key (SCD Type 1)",
    "scd_type_2": "Track current + historical rows for each business key (SCD Type 2)",
    "incremental_merge": "Emit incremental changes for merge targets (CDC/timestamp)",
    "full_merge_dedupe": "Emit a deduplicated snapshot suitable for full merges",
    "periodic_snapshot": "Emit the exact Bronze snapshot for periodic refreshes",
}


class SilverModel(RichEnumMixin, str, Enum):
    """Silver transformation model types.

    Defines how Bronze data is transformed into Silver:
    - SCD_TYPE_1: Keep only latest version of each business key
    - SCD_TYPE_2: Track current + historical rows (slowly changing dimension)
    - INCREMENTAL_MERGE: Emit incremental changes for merge targets
    - FULL_MERGE_DEDUPE: Emit deduplicated snapshot for full merges
    - PERIODIC_SNAPSHOT: Emit exact Bronze snapshot for periodic refreshes
    """

    SCD_TYPE_1 = "scd_type_1"
    SCD_TYPE_2 = "scd_type_2"
    INCREMENTAL_MERGE = "incremental_merge"
    FULL_MERGE_DEDUPE = "full_merge_dedupe"
    PERIODIC_SNAPSHOT = "periodic_snapshot"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return super().choices()

    @classmethod
    def normalize(cls, raw: str | None) -> "SilverModel":
        """Normalize a model value, handling aliases and case.

        Args:
            raw: String value, enum instance, or None

        Returns:
            SilverModel member matching the input

        Raises:
            ValueError: If raw is None or doesn't match any member/alias
        """
        if isinstance(raw, cls):
            return raw
        if raw is None:
            raise ValueError("SilverModel value must be provided")

        assert isinstance(raw, str)
        candidate = raw.strip().lower()

        # Check aliases first
        canonical = _SILVER_MODEL_ALIASES.get(candidate, candidate)

        for member in cls:
            if member.value == canonical:
                return member

        raise ValueError(
            f"Invalid SilverModel '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        value_str = str(self.value)
        return _SILVER_MODEL_DESCRIPTIONS.get(value_str, value_str)

    @classmethod
    def default_for_load_pattern(cls, pattern: LoadPattern) -> "SilverModel":
        """Return the default SilverModel for a given LoadPattern."""
        mapping = {
            LoadPattern.SNAPSHOT: cls.PERIODIC_SNAPSHOT,
            LoadPattern.INCREMENTAL_APPEND: cls.INCREMENTAL_MERGE,
            LoadPattern.INCREMENTAL_MERGE: cls.INCREMENTAL_MERGE,
            LoadPattern.CURRENT_HISTORY: cls.SCD_TYPE_2,
        }
        return mapping.get(pattern, cls.PERIODIC_SNAPSHOT)

    @property
    def requires_dedupe(self) -> bool:
        """Check if this model requires deduplication logic."""
        return self in {self.SCD_TYPE_1, self.SCD_TYPE_2, self.FULL_MERGE_DEDUPE}

    @property
    def emits_history(self) -> bool:
        """Check if this model emits history records."""
        return self == self.SCD_TYPE_2


# Backward compatibility alias
SILVER_MODEL_ALIASES = _SILVER_MODEL_ALIASES
