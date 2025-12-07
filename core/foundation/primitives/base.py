"""Base classes for consistent patterns across bronze-foundry.

This module provides foundation classes that enforce consistent patterns:
- RichEnumMixin: Mixin for enums with normalize(), describe(), choices() methods
- SerializableMixin: Mixin for dataclasses with to_dict()/from_dict() methods

All enums and dataclasses in core/ should use these mixins.
"""

from __future__ import annotations

from dataclasses import asdict, fields
from enum import Enum
from typing import Any, ClassVar, Dict, List, Type, TypeVar, cast

T = TypeVar("T", bound="SerializableMixin")


class RichEnumMixin:
    """Mixin that adds utility methods to enums.

    All enums in bronze-foundry should inherit from this mixin AND (str, Enum)
    to ensure they provide:
    - choices(): List of valid string values
    - normalize(): Case-insensitive parsing with alias support and default
    - describe(): Human-readable description

    Class Variables:
        _aliases: Dict mapping alternative names to canonical values
        _descriptions: Dict mapping values to human-readable descriptions
        _default: Optional default member name (e.g., "ALLOW") when raw is None

    Example:
        class MyEnum(RichEnumMixin, str, Enum):
            VALUE_A = "value_a"
            VALUE_B = "value_b"

            _default: ClassVar[str] = "VALUE_A"  # Default when None passed

            _aliases: ClassVar[Dict[str, str]] = {
                "a": "value_a",
                "va": "value_a",
            }

            _descriptions: ClassVar[Dict[str, str]] = {
                "value_a": "First value option",
                "value_b": "Second value option",
            }

    Note:
        Subclasses should NOT override choices(), normalize(), or describe()
        unless they need truly custom behavior. Instead, set the class
        variables above to customize behavior.
    """

    # Subclasses can override these class variables
    _aliases: ClassVar[Dict[str, str]] = {}
    _descriptions: ClassVar[Dict[str, str]] = {}
    _default: ClassVar[str | None] = None  # Member name for default (e.g., "ALLOW")
    value: Any

    @classmethod
    def _member_map(cls) -> Dict[str, "RichEnumMixin"]:
        return cast(Dict[str, "RichEnumMixin"], getattr(cls, "__members__", {}))

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls._member_map().values()]

    @classmethod
    def normalize(cls, raw: RawEnumInput) -> "RichEnumMixin":
        """Parse a string value into this enum, handling aliases and case.

        Args:
            raw: String value, enum instance, or None

        Returns:
            Enum member matching the input, or default if raw is None

        Raises:
            ValueError: If raw is None with no default, or doesn't match any member/alias
        """
        if isinstance(raw, cls):
            return raw

        # Handle None - return default if defined
        if raw is None:
            default_name = getattr(cls, "_default", None)
            members = cls._member_map()
            if default_name is not None and default_name in members:
                return members[default_name]
            raise ValueError(f"{cls.__name__} value must be provided")

        candidate = raw.strip().lower()

        # Check aliases first
        aliases = getattr(cls, "_aliases", {})
        canonical = aliases.get(candidate, candidate)

        # Find matching member
        for member in cls._member_map().values():
            if member.value == canonical:
                return member

        raise ValueError(
            f"Invalid {cls.__name__} '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description of this enum value."""
        descriptions = cast(Dict[str, str], getattr(self.__class__, "_descriptions", {}))
        value_str = str(self.value)
        return descriptions.get(value_str, value_str)


class SerializableMixin:
    """Mixin that adds to_dict() and from_dict() to dataclasses.

    All dataclasses in bronze-foundry should use this mixin to ensure
    consistent serialization behavior.

    Example:
        @dataclass
        class MyConfig(SerializableMixin):
            name: str
            count: int = 0

        cfg = MyConfig(name="test", count=5)
        data = cfg.to_dict()  # {"name": "test", "count": 5}
        cfg2 = MyConfig.from_dict(data)  # MyConfig(name="test", count=5)
    """

    def to_dict(self) -> Dict[str, Any]:
        """Convert this dataclass instance to a dictionary.

        Handles nested dataclasses, enums, and Path objects.
        """
        result = _serialize_value(asdict(cast(Any, self)))
        if not isinstance(result, dict):
            raise TypeError("expected dataclass to serialize to a dict")
        return cast(Dict[str, Any], result)

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """Create an instance from a dictionary.

        Args:
            data: Dictionary with field values

        Returns:
            New instance of this class

        Note:
            This performs simple construction. Subclasses with complex
            nested types may need to override this method.
        """
        # Get field names for this dataclass
        dataclass_fields = fields(cast(Type[Any], cls))
        field_names = {f.name for f in dataclass_fields}

        # Filter to only known fields
        filtered = {k: v for k, v in data.items() if k in field_names}

        return cls(**filtered)


def _serialize_value(value: Any) -> Any:
    """Recursively serialize a value for JSON/dict compatibility."""
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__fspath__"):  # Path-like
        return str(value)
    return value


__all__ = [
    "RichEnumMixin",
    "SerializableMixin",
]
