"""Silver asset models and helpers."""

from __future__ import annotations

from enum import Enum
from typing import Dict, List, Mapping

from core.patterns import LoadPattern


class SilverModel(str, Enum):
    SCD_TYPE_1 = "scd_type_1"
    SCD_TYPE_2 = "scd_type_2"
    INCREMENTAL_MERGE = "incremental_merge"
    FULL_MERGE_DEDUPE = "full_merge_dedupe"
    PERIODIC_SNAPSHOT = "periodic_snapshot"

    @classmethod
    def choices(cls) -> List[str]:
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "SilverModel":
        if isinstance(raw, SilverModel):
            return raw
        if not raw:
            raise ValueError("Silver model must be provided")
        candidate = raw.strip().lower()
        canonical = SILVER_MODEL_ALIASES.get(candidate, candidate)
        for member in cls:
            if member.value == canonical:
                return member
        raise ValueError(f"Invalid silver.model '{raw}'. Valid options: {', '.join(cls.choices())}")

    @classmethod
    def default_for_load_pattern(cls, pattern: LoadPattern) -> "SilverModel":
        mapping = {
            LoadPattern.FULL: cls.PERIODIC_SNAPSHOT,
            LoadPattern.CDC: cls.INCREMENTAL_MERGE,
            LoadPattern.CURRENT_HISTORY: cls.SCD_TYPE_2,
        }
        return mapping.get(pattern, cls.PERIODIC_SNAPSHOT)

    @property
    def requires_dedupe(self) -> bool:
        return self in {self.SCD_TYPE_1, self.SCD_TYPE_2, self.FULL_MERGE_DEDUPE}

    @property
    def emits_history(self) -> bool:
        return self == self.SCD_TYPE_2

    def describe(self) -> str:
        descriptions = {
            self.SCD_TYPE_1: "Keep only the latest version of each business key (SCD Type 1)",
            self.SCD_TYPE_2: "Track current + historical rows for each business key (SCD Type 2)",
            self.INCREMENTAL_MERGE: "Emit incremental changes for merge targets (CDC/timestamp)",
            self.FULL_MERGE_DEDUPE: "Emit a deduplicated snapshot suitable for full merges",
            self.PERIODIC_SNAPSHOT: "Emit the exact Bronze snapshot for periodic refreshes",
        }
        return descriptions.get(self, self.value)


MODEL_PROFILES: Dict[str, "SilverModel"] = {
    "analytics": SilverModel.SCD_TYPE_2,
    "operational": SilverModel.SCD_TYPE_1,
    "merge_ready": SilverModel.FULL_MERGE_DEDUPE,
    "cdc_delta": SilverModel.INCREMENTAL_MERGE,
    "snapshot": SilverModel.PERIODIC_SNAPSHOT,
}


def resolve_profile(profile_name: str | None) -> "SilverModel" | None:
    if profile_name is None:
        return None
    key = profile_name.strip().lower()
    return MODEL_PROFILES.get(key)

SILVER_MODEL_ALIASES: Mapping[str, str] = {
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
