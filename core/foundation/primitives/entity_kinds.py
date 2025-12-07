"""Entity kind enums for Bronze and Silver layer processing.

These enums define the semantic intent for entity processing:
- EntityKind: Classification of entities (event, state, derived)
- HistoryMode: History tracking strategy for state entities
- InputMode: Input processing mode for event entities
- DeleteMode: Delete handling behavior
- SchemaMode: Schema evolution policy

These are placed in Foundation (L0) because:
1. They only depend on RichEnumMixin (also in foundation)
2. They need to be imported by all layers (infrastructure, domain, orchestration)
3. This matches the pattern used by LoadPattern and SilverModel
"""

from __future__ import annotations

from enum import Enum

from core.foundation.primitives.base import RichEnumMixin


class EntityKind(RichEnumMixin, str, Enum):
    """Kind of entity for Silver layer processing.

    No default - must be explicitly provided.
    """

    EVENT = "event"
    STATE = "state"
    DERIVED_STATE = "derived_state"
    DERIVED_EVENT = "derived_event"

    @property
    def is_event_like(self) -> bool:
        """Check if entity is event-like (append-only)."""
        return self in {self.EVENT, self.DERIVED_EVENT}

    @property
    def is_state_like(self) -> bool:
        """Check if entity is state-like (mutable)."""
        return self in {self.STATE, self.DERIVED_STATE}


# Class variables for RichEnumMixin (must be set outside class due to Enum metaclass)
# No _default for EntityKind - it's a required field
EntityKind._descriptions = {
    "event": "Immutable event records (append-only)",
    "state": "Mutable state records (current snapshot)",
    "derived_state": "State derived from other entities",
    "derived_event": "Events derived from other entities",
}


class HistoryMode(RichEnumMixin, str, Enum):
    """History tracking mode for state entities."""

    SCD2 = "scd2"
    SCD1 = "scd1"
    LATEST_ONLY = "latest_only"


# Class variables for RichEnumMixin (must be set outside class due to Enum metaclass)
HistoryMode._default = "SCD2"
HistoryMode._descriptions = {
    "scd2": "Type 2 slowly changing dimension - full history with date ranges",
    "scd1": "Type 1 slowly changing dimension - overwrite with latest values",
    "latest_only": "Keep only the most recent version of each record",
}


class InputMode(RichEnumMixin, str, Enum):
    """Input processing mode for event entities."""

    APPEND_LOG = "append_log"
    REPLACE_DAILY = "replace_daily"


# Class variables for RichEnumMixin (must be set outside class due to Enum metaclass)
InputMode._default = "APPEND_LOG"
InputMode._descriptions = {
    "append_log": "Append new records to existing data",
    "replace_daily": "Replace all data for each daily partition",
}


class DeleteMode(RichEnumMixin, str, Enum):
    """Delete handling mode for entity processing."""

    IGNORE = "ignore"
    TOMBSTONE_STATE = "tombstone_state"
    TOMBSTONE_EVENT = "tombstone_event"


# Class variables for RichEnumMixin (must be set outside class due to Enum metaclass)
DeleteMode._default = "IGNORE"
DeleteMode._descriptions = {
    "ignore": "Ignore delete markers in source data",
    "tombstone_state": "Apply tombstone logic for state entities",
    "tombstone_event": "Apply tombstone logic for event entities",
}


class SchemaMode(RichEnumMixin, str, Enum):
    """Schema evolution mode for entity processing."""

    STRICT = "strict"
    ALLOW_NEW_COLUMNS = "allow_new_columns"


# Class variables for RichEnumMixin (must be set outside class due to Enum metaclass)
SchemaMode._default = "STRICT"
SchemaMode._descriptions = {
    "strict": "Reject any schema changes",
    "allow_new_columns": "Allow new columns to be added",
}


__all__ = [
    "EntityKind",
    "HistoryMode",
    "InputMode",
    "DeleteMode",
    "SchemaMode",
]
