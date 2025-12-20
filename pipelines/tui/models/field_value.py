"""Field value with source tracking for inheritance visualization."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class FieldSource(str, Enum):
    """Source of a field's value."""

    DEFAULT = "default"  # From schema default or empty
    PARENT = "parent"  # Inherited from parent config
    LOCAL = "local"  # Set in current config


@dataclass
class FieldValue:
    """Represents a single field's value with provenance tracking.

    This class tracks where a field's value came from (default, parent config,
    or locally set) to enable visual distinction in the TUI.

    Attributes:
        name: The field name (e.g., "system", "source_type")
        value: The current value (can be any type)
        source: Where this value came from
        is_required: Whether this field is required (can be dynamic)
        is_sensitive: Whether this field contains sensitive data
        validation_error: Current validation error, if any
        parent_value: The value from parent config (for reset functionality)
    """

    name: str
    value: Any = ""
    source: FieldSource = FieldSource.DEFAULT
    is_required: bool = False
    is_sensitive: bool = False
    validation_error: str | None = None
    parent_value: Any = field(default=None, repr=False)

    def is_inherited(self) -> bool:
        """Check if this value is inherited from a parent config."""
        return self.source == FieldSource.PARENT

    def is_local(self) -> bool:
        """Check if this value was set locally (not inherited)."""
        return self.source == FieldSource.LOCAL

    def is_default(self) -> bool:
        """Check if this value is the schema default."""
        return self.source == FieldSource.DEFAULT

    def needs_env_var(self) -> bool:
        """Check if this sensitive field should encourage env var usage.

        Returns True if:
        - The field is marked as sensitive AND
        - The value is not empty AND
        - The value doesn't already use ${VAR_NAME} syntax
        """
        if not self.is_sensitive:
            return False
        if not self.value:
            return False
        value_str = str(self.value)
        return not value_str.startswith("${")

    def has_parent_value(self) -> bool:
        """Check if a parent value exists for potential reset."""
        return self.parent_value is not None

    def reset_to_parent(self) -> None:
        """Reset this field to its inherited parent value."""
        if self.parent_value is not None:
            self.value = self.parent_value
            self.source = FieldSource.PARENT
            self.validation_error = None

    def set_local_value(self, value: Any) -> None:
        """Set a local value, overriding any inherited value."""
        self.value = value
        self.source = FieldSource.LOCAL

    def clear(self) -> None:
        """Clear the value and reset to default source."""
        self.value = ""
        self.source = FieldSource.DEFAULT
        self.validation_error = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "value": self.value,
            "source": self.source.value,
            "is_required": self.is_required,
            "is_sensitive": self.is_sensitive,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FieldValue":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            value=data.get("value", ""),
            source=FieldSource(data.get("source", "default")),
            is_required=data.get("is_required", False),
            is_sensitive=data.get("is_sensitive", False),
        )

    def __str__(self) -> str:
        """String representation showing value and source."""
        source_marker = {
            FieldSource.DEFAULT: "(default)",
            FieldSource.PARENT: "(inherited)",
            FieldSource.LOCAL: "",
        }
        marker = source_marker.get(self.source, "")
        return f"{self.name}={self.value!r} {marker}".strip()
