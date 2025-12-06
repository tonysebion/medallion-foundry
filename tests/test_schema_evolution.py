"""Tests for schema evolution handling.

These tests verify the schema evolution patterns used in the framework:
- SchemaEvolutionMode: Enum with RichEnumMixin pattern
- EvolutionConfig: Configuration with from_dict
- EvolutionChange: Individual change tracking
- EvolutionResult: Aggregated evolution check results
- SchemaEvolution: Evolution checking logic
"""

from __future__ import annotations

import pytest

from core.adapters.schema.evolution import (
    EvolutionChange,
    EvolutionConfig,
    EvolutionResult,
    SchemaEvolution,
    SchemaEvolutionMode,
)
from core.adapters.schema.types import ColumnSpec, DataType, SchemaSpec


class TestSchemaEvolutionMode:
    """Tests for SchemaEvolutionMode enum."""

    def test_choices(self) -> None:
        """choices() should return all valid enum values."""
        choices = SchemaEvolutionMode.choices()
        assert choices == ["strict", "allow_new_nullable", "ignore_unknown"]

    def test_normalize_string(self) -> None:
        """normalize() should convert string to enum member."""
        assert SchemaEvolutionMode.normalize("strict") == SchemaEvolutionMode.STRICT
        assert SchemaEvolutionMode.normalize("allow_new_nullable") == SchemaEvolutionMode.ALLOW_NEW_NULLABLE
        assert SchemaEvolutionMode.normalize("ignore_unknown") == SchemaEvolutionMode.IGNORE_UNKNOWN

    def test_normalize_case_insensitive(self) -> None:
        """normalize() should be case-insensitive."""
        assert SchemaEvolutionMode.normalize("STRICT") == SchemaEvolutionMode.STRICT
        assert SchemaEvolutionMode.normalize("Allow_New_Nullable") == SchemaEvolutionMode.ALLOW_NEW_NULLABLE

    def test_normalize_with_whitespace(self) -> None:
        """normalize() should handle whitespace."""
        assert SchemaEvolutionMode.normalize("  strict  ") == SchemaEvolutionMode.STRICT
        assert SchemaEvolutionMode.normalize("\tignore_unknown\n") == SchemaEvolutionMode.IGNORE_UNKNOWN

    def test_normalize_none_returns_default(self) -> None:
        """normalize(None) should return default (STRICT)."""
        assert SchemaEvolutionMode.normalize(None) == SchemaEvolutionMode.STRICT

    def test_normalize_enum_member(self) -> None:
        """normalize() should pass through enum members."""
        assert SchemaEvolutionMode.normalize(SchemaEvolutionMode.STRICT) == SchemaEvolutionMode.STRICT
        assert SchemaEvolutionMode.normalize(SchemaEvolutionMode.ALLOW_NEW_NULLABLE) == SchemaEvolutionMode.ALLOW_NEW_NULLABLE

    def test_normalize_invalid_raises(self) -> None:
        """normalize() should raise ValueError for invalid values."""
        with pytest.raises(ValueError, match="Invalid SchemaEvolutionMode"):
            SchemaEvolutionMode.normalize("invalid")

    def test_describe(self) -> None:
        """describe() should return human-readable descriptions."""
        assert "Reject" in SchemaEvolutionMode.STRICT.describe()
        assert "nullable" in SchemaEvolutionMode.ALLOW_NEW_NULLABLE.describe().lower()
        assert "Ignore" in SchemaEvolutionMode.IGNORE_UNKNOWN.describe()


class TestEvolutionConfig:
    """Tests for EvolutionConfig dataclass."""

    def test_defaults(self) -> None:
        """Default config should have sensible defaults."""
        config = EvolutionConfig()
        assert config.mode == SchemaEvolutionMode.STRICT
        assert config.allow_type_relaxation is False
        assert config.allow_precision_increase is True
        assert config.protected_columns == []

    def test_custom_values(self) -> None:
        """Custom values should be respected."""
        config = EvolutionConfig(
            mode=SchemaEvolutionMode.ALLOW_NEW_NULLABLE,
            allow_type_relaxation=True,
            allow_precision_increase=False,
            protected_columns=["id", "created_at"],
        )
        assert config.mode == SchemaEvolutionMode.ALLOW_NEW_NULLABLE
        assert config.allow_type_relaxation is True
        assert config.allow_precision_increase is False
        assert config.protected_columns == ["id", "created_at"]

    def test_from_dict_empty(self) -> None:
        """from_dict({}) should return default config."""
        config = EvolutionConfig.from_dict({})
        assert config.mode == SchemaEvolutionMode.STRICT
        assert config.allow_type_relaxation is False

    def test_from_dict_full(self) -> None:
        """from_dict should restore all fields."""
        data = {
            "mode": "ignore_unknown",
            "allow_type_relaxation": True,
            "allow_precision_increase": False,
            "protected_columns": ["pk"],
        }
        config = EvolutionConfig.from_dict(data)
        assert config.mode == SchemaEvolutionMode.IGNORE_UNKNOWN
        assert config.allow_type_relaxation is True
        assert config.allow_precision_increase is False
        assert config.protected_columns == ["pk"]

    def test_from_dict_invalid_mode(self) -> None:
        """from_dict should fallback to STRICT for invalid mode."""
        config = EvolutionConfig.from_dict({"mode": "invalid_mode"})
        assert config.mode == SchemaEvolutionMode.STRICT


class TestEvolutionChange:
    """Tests for EvolutionChange dataclass."""

    def test_creation(self) -> None:
        """EvolutionChange should be created with all fields."""
        change = EvolutionChange(
            change_type="added_column",
            column_name="new_col",
            old_value=None,
            new_value="string",
            allowed=True,
            message="New column allowed",
        )
        assert change.change_type == "added_column"
        assert change.column_name == "new_col"
        assert change.allowed is True

    def test_to_dict(self) -> None:
        """to_dict should serialize all fields."""
        change = EvolutionChange(
            change_type="type_changed",
            column_name="amount",
            old_value="integer",
            new_value="decimal",
            allowed=False,
            message="Type change not allowed",
        )
        data = change.to_dict()
        assert data["change_type"] == "type_changed"
        assert data["column_name"] == "amount"
        assert data["old_value"] == "integer"
        assert data["new_value"] == "decimal"
        assert data["allowed"] is False
        assert data["message"] == "Type change not allowed"


class TestEvolutionResult:
    """Tests for EvolutionResult dataclass."""

    def test_empty_result(self) -> None:
        """Empty result should be compatible."""
        result = EvolutionResult()
        assert result.compatible is True
        assert len(result.changes) == 0
        assert len(result.blocked_changes) == 0
        assert len(result.allowed_changes) == 0

    def test_add_allowed_change(self) -> None:
        """add_change with allowed=True should add to allowed_changes."""
        result = EvolutionResult()
        change = EvolutionChange(
            change_type="added_column",
            column_name="new_col",
            allowed=True,
            message="Allowed",
        )
        result.add_change(change)

        assert result.compatible is True
        assert len(result.changes) == 1
        assert len(result.allowed_changes) == 1
        assert len(result.blocked_changes) == 0

    def test_add_blocked_change(self) -> None:
        """add_change with allowed=False should add to blocked_changes."""
        result = EvolutionResult()
        change = EvolutionChange(
            change_type="removed_column",
            column_name="old_col",
            allowed=False,
            message="Blocked",
        )
        result.add_change(change)

        assert result.compatible is False
        assert len(result.changes) == 1
        assert len(result.blocked_changes) == 1
        assert len(result.allowed_changes) == 0

    def test_to_dict(self) -> None:
        """to_dict should serialize result."""
        result = EvolutionResult(mode=SchemaEvolutionMode.ALLOW_NEW_NULLABLE)
        result.add_change(EvolutionChange(
            change_type="added_column",
            column_name="new_col",
            allowed=True,
            message="OK",
        ))
        result.add_change(EvolutionChange(
            change_type="removed_column",
            column_name="old_col",
            allowed=False,
            message="Not OK",
        ))

        data = result.to_dict()
        assert data["compatible"] is False
        assert data["mode"] == "allow_new_nullable"
        assert data["total_changes"] == 2
        assert len(data["blocked_changes"]) == 1
        assert len(data["allowed_changes"]) == 1


class TestSchemaEvolution:
    """Tests for SchemaEvolution class."""

    def _make_schema(self, columns: list) -> SchemaSpec:
        """Helper to create SchemaSpec from column tuples."""
        col_specs = []
        for col in columns:
            name, dtype = col[0], col[1]
            nullable = col[2] if len(col) > 2 else True
            col_specs.append(ColumnSpec(name=name, type=DataType(dtype), nullable=nullable))
        return SchemaSpec(columns=col_specs)

    def test_no_changes(self) -> None:
        """Identical schemas should be compatible."""
        old = self._make_schema([("id", "integer"), ("name", "string")])
        new = self._make_schema([("id", "integer"), ("name", "string")])

        evolution = SchemaEvolution()
        result = evolution.check_evolution(old, new)

        assert result.compatible is True
        assert len(result.changes) == 0

    def test_strict_mode_rejects_new_column(self) -> None:
        """Strict mode should reject new columns."""
        old = self._make_schema([("id", "integer")])
        new = self._make_schema([("id", "integer"), ("name", "string")])

        config = EvolutionConfig(mode=SchemaEvolutionMode.STRICT)
        evolution = SchemaEvolution(config)
        result = evolution.check_evolution(old, new)

        assert result.compatible is False
        assert len(result.blocked_changes) == 1
        assert result.blocked_changes[0].change_type == "added_column"

    def test_allow_new_nullable_accepts_nullable_column(self) -> None:
        """allow_new_nullable should accept new nullable columns."""
        old = self._make_schema([("id", "integer")])
        new = self._make_schema([("id", "integer"), ("name", "string", True)])

        config = EvolutionConfig(mode=SchemaEvolutionMode.ALLOW_NEW_NULLABLE)
        evolution = SchemaEvolution(config)
        result = evolution.check_evolution(old, new)

        assert result.compatible is True
        assert len(result.allowed_changes) == 1
        assert result.allowed_changes[0].change_type == "added_column"

    def test_allow_new_nullable_rejects_non_nullable_column(self) -> None:
        """allow_new_nullable should reject new non-nullable columns."""
        old = self._make_schema([("id", "integer")])
        new = self._make_schema([("id", "integer"), ("name", "string", False)])

        config = EvolutionConfig(mode=SchemaEvolutionMode.ALLOW_NEW_NULLABLE)
        evolution = SchemaEvolution(config)
        result = evolution.check_evolution(old, new)

        assert result.compatible is False
        assert len(result.blocked_changes) == 1

    def test_ignore_unknown_accepts_new_columns(self) -> None:
        """ignore_unknown should accept new columns."""
        old = self._make_schema([("id", "integer")])
        new = self._make_schema([("id", "integer"), ("name", "string", False)])

        config = EvolutionConfig(mode=SchemaEvolutionMode.IGNORE_UNKNOWN)
        evolution = SchemaEvolution(config)
        result = evolution.check_evolution(old, new)

        assert result.compatible is True
        assert len(result.allowed_changes) == 1

    def test_strict_mode_rejects_removed_column(self) -> None:
        """Strict mode should reject removed columns."""
        old = self._make_schema([("id", "integer"), ("name", "string")])
        new = self._make_schema([("id", "integer")])

        config = EvolutionConfig(mode=SchemaEvolutionMode.STRICT)
        evolution = SchemaEvolution(config)
        result = evolution.check_evolution(old, new)

        assert result.compatible is False
        assert len(result.blocked_changes) == 1
        assert result.blocked_changes[0].change_type == "removed_column"

    def test_protected_column_cannot_be_removed(self) -> None:
        """Protected columns cannot be removed even in ignore_unknown mode."""
        old = self._make_schema([("id", "integer"), ("name", "string")])
        new = self._make_schema([("name", "string")])

        config = EvolutionConfig(
            mode=SchemaEvolutionMode.IGNORE_UNKNOWN,
            protected_columns=["id"],
        )
        evolution = SchemaEvolution(config)
        result = evolution.check_evolution(old, new)

        assert result.compatible is False
        assert "protected" in result.blocked_changes[0].message.lower()

    def test_nullable_relaxation_allowed(self) -> None:
        """Making a column nullable (false->true) should be allowed."""
        old_col = ColumnSpec(name="name", type=DataType.STRING, nullable=False)
        new_col = ColumnSpec(name="name", type=DataType.STRING, nullable=True)
        old = SchemaSpec(columns=[old_col])
        new = SchemaSpec(columns=[new_col])

        evolution = SchemaEvolution()
        result = evolution.check_evolution(old, new)

        assert result.compatible is True
        nullable_changes = [c for c in result.changes if c.change_type == "nullable_changed"]
        assert len(nullable_changes) == 1
        assert nullable_changes[0].allowed is True

    def test_nullable_tightening_blocked(self) -> None:
        """Making a column non-nullable (true->false) should be blocked."""
        old_col = ColumnSpec(name="name", type=DataType.STRING, nullable=True)
        new_col = ColumnSpec(name="name", type=DataType.STRING, nullable=False)
        old = SchemaSpec(columns=[old_col])
        new = SchemaSpec(columns=[new_col])

        evolution = SchemaEvolution()
        result = evolution.check_evolution(old, new)

        assert result.compatible is False
        nullable_changes = [c for c in result.blocked_changes if c.change_type == "nullable_changed"]
        assert len(nullable_changes) == 1

    def test_type_change_blocked_by_default(self) -> None:
        """Type changes should be blocked by default."""
        old = self._make_schema([("amount", "integer")])
        new = self._make_schema([("amount", "string")])

        evolution = SchemaEvolution()
        result = evolution.check_evolution(old, new)

        assert result.compatible is False
        type_changes = [c for c in result.blocked_changes if c.change_type == "type_changed"]
        assert len(type_changes) == 1

    def test_type_widening_allowed_when_enabled(self) -> None:
        """Type widening should be allowed when allow_type_relaxation=True."""
        old_col = ColumnSpec(name="amount", type=DataType.INTEGER, nullable=True)
        new_col = ColumnSpec(name="amount", type=DataType.BIGINT, nullable=True)
        old = SchemaSpec(columns=[old_col])
        new = SchemaSpec(columns=[new_col])

        config = EvolutionConfig(allow_type_relaxation=True)
        evolution = SchemaEvolution(config)
        result = evolution.check_evolution(old, new)

        assert result.compatible is True
        type_changes = [c for c in result.allowed_changes if c.change_type == "type_changed"]
        assert len(type_changes) == 1

    def test_precision_increase_allowed_by_default(self) -> None:
        """Precision increases should be allowed by default."""
        old_col = ColumnSpec(name="amount", type=DataType.DECIMAL, nullable=True, precision=10, scale=2)
        new_col = ColumnSpec(name="amount", type=DataType.DECIMAL, nullable=True, precision=18, scale=2)
        old = SchemaSpec(columns=[old_col])
        new = SchemaSpec(columns=[new_col])

        evolution = SchemaEvolution()
        result = evolution.check_evolution(old, new)

        assert result.compatible is True

    def test_precision_decrease_blocked(self) -> None:
        """Precision decreases should be blocked."""
        old_col = ColumnSpec(name="amount", type=DataType.DECIMAL, nullable=True, precision=18, scale=2)
        new_col = ColumnSpec(name="amount", type=DataType.DECIMAL, nullable=True, precision=10, scale=2)
        old = SchemaSpec(columns=[old_col])
        new = SchemaSpec(columns=[new_col])

        evolution = SchemaEvolution()
        result = evolution.check_evolution(old, new)

        assert result.compatible is False
