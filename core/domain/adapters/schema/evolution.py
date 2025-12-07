"""Schema evolution mode enforcement per spec Section 6.

Implements schema evolution rules:
- strict: Reject any schema differences
- allow_new_nullable: Allow new nullable columns
- ignore_unknown: Ignore unexpected columns

Example config:
```yaml
schema_evolution:
  mode: allow_new_nullable
  allow_type_relaxation: false
```
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd

from core.foundation.primitives.base import RichEnumMixin

from .types import ColumnSpec, DataType, SchemaSpec

logger = logging.getLogger(__name__)


class SchemaEvolutionMode(RichEnumMixin, str, Enum):
    """Schema evolution modes per spec Section 6."""

    STRICT = "strict"  # Reject any schema differences
    ALLOW_NEW_NULLABLE = "allow_new_nullable"  # Allow new nullable columns
    IGNORE_UNKNOWN = "ignore_unknown"  # Ignore unexpected columns


# Class variables for RichEnumMixin (must be set outside class due to Enum metaclass)
SchemaEvolutionMode._default = "STRICT"
SchemaEvolutionMode._descriptions = {
    "strict": "Reject any schema differences",
    "allow_new_nullable": "Allow new nullable columns to be added",
    "ignore_unknown": "Ignore unexpected columns in the data",
}


@dataclass
class EvolutionChange:
    """A single schema change detected."""

    change_type: str  # added_column, removed_column, type_changed, nullable_changed
    column_name: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    allowed: bool = False
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "change_type": self.change_type,
            "column_name": self.column_name,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "allowed": self.allowed,
            "message": self.message,
        }


@dataclass
class EvolutionResult:
    """Result of schema evolution check."""

    compatible: bool = True
    mode: SchemaEvolutionMode = SchemaEvolutionMode.STRICT
    changes: List[EvolutionChange] = field(default_factory=list)
    blocked_changes: List[EvolutionChange] = field(default_factory=list)
    allowed_changes: List[EvolutionChange] = field(default_factory=list)

    def add_change(self, change: EvolutionChange) -> None:
        """Add a change to the result."""
        self.changes.append(change)
        if change.allowed:
            self.allowed_changes.append(change)
        else:
            self.blocked_changes.append(change)
            self.compatible = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "compatible": self.compatible,
            "mode": self.mode.value,
            "total_changes": len(self.changes),
            "blocked_changes": [c.to_dict() for c in self.blocked_changes],
            "allowed_changes": [c.to_dict() for c in self.allowed_changes],
        }


@dataclass
class EvolutionConfig:
    """Configuration for schema evolution handling."""

    mode: SchemaEvolutionMode = SchemaEvolutionMode.STRICT
    allow_type_relaxation: bool = False
    allow_precision_increase: bool = True
    protected_columns: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EvolutionConfig":
        """Create from dictionary."""
        mode_str = data.get("mode", "strict")
        try:
            mode = SchemaEvolutionMode(mode_str)
        except ValueError:
            logger.warning("Unknown evolution mode '%s', using strict", mode_str)
            mode = SchemaEvolutionMode.STRICT

        return cls(
            mode=mode,
            allow_type_relaxation=data.get("allow_type_relaxation", False),
            allow_precision_increase=data.get("allow_precision_increase", True),
            protected_columns=data.get("protected_columns", []),
        )


class SchemaEvolution:
    """Handles schema evolution checking and enforcement."""

    # Type widening rules: source_type -> allowed wider types
    TYPE_WIDENING = {
        DataType.INTEGER: {DataType.BIGINT, DataType.DECIMAL, DataType.FLOAT, DataType.DOUBLE},
        DataType.BIGINT: {DataType.DECIMAL, DataType.DOUBLE},
        DataType.FLOAT: {DataType.DOUBLE},
        DataType.DATE: {DataType.TIMESTAMP, DataType.DATETIME},
    }

    def __init__(self, config: Optional[EvolutionConfig] = None):
        """Initialize evolution handler.

        Args:
            config: Evolution configuration
        """
        self.config = config or EvolutionConfig()

    def check_evolution(
        self,
        old_schema: SchemaSpec,
        new_schema: SchemaSpec,
    ) -> EvolutionResult:
        """Check if schema evolution is allowed.

        Args:
            old_schema: Current schema specification
            new_schema: New schema specification

        Returns:
            EvolutionResult with compatibility and changes
        """
        result = EvolutionResult(mode=self.config.mode)

        old_columns = {c.name: c for c in old_schema.columns}
        new_columns = {c.name: c for c in new_schema.columns}

        old_names = set(old_columns.keys())
        new_names = set(new_columns.keys())

        # Check for added columns
        added = new_names - old_names
        for col_name in added:
            col = new_columns[col_name]
            change = self._check_added_column(col)
            result.add_change(change)

        # Check for removed columns
        removed = old_names - new_names
        for col_name in removed:
            col = old_columns[col_name]
            change = self._check_removed_column(col)
            result.add_change(change)

        # Check for modified columns
        common = old_names & new_names
        for col_name in common:
            old_col = old_columns[col_name]
            new_col = new_columns[col_name]
            changes = self._check_column_changes(old_col, new_col)
            for change in changes:
                result.add_change(change)

        return result

    def _check_added_column(self, column: ColumnSpec) -> EvolutionChange:
        """Check if adding a column is allowed."""
        change = EvolutionChange(
            change_type="added_column",
            column_name=column.name,
            new_value=column.type.value,
        )

        if self.config.mode == SchemaEvolutionMode.STRICT:
            change.allowed = False
            change.message = f"New column '{column.name}' not allowed in strict mode"
        elif self.config.mode == SchemaEvolutionMode.ALLOW_NEW_NULLABLE:
            if column.nullable:
                change.allowed = True
                change.message = f"New nullable column '{column.name}' allowed"
            else:
                change.allowed = False
                change.message = f"New non-nullable column '{column.name}' not allowed"
        elif self.config.mode == SchemaEvolutionMode.IGNORE_UNKNOWN:
            change.allowed = True
            change.message = f"New column '{column.name}' allowed (ignore_unknown mode)"

        return change

    def _check_removed_column(self, column: ColumnSpec) -> EvolutionChange:
        """Check if removing a column is allowed."""
        change = EvolutionChange(
            change_type="removed_column",
            column_name=column.name,
            old_value=column.type.value,
        )

        # Protected columns cannot be removed
        if column.name in self.config.protected_columns:
            change.allowed = False
            change.message = f"Protected column '{column.name}' cannot be removed"
            return change

        # Primary key columns cannot be removed
        if column.primary_key:
            change.allowed = False
            change.message = f"Primary key column '{column.name}' cannot be removed"
            return change

        if self.config.mode == SchemaEvolutionMode.STRICT:
            change.allowed = False
            change.message = f"Column '{column.name}' removal not allowed in strict mode"
        elif self.config.mode == SchemaEvolutionMode.IGNORE_UNKNOWN:
            change.allowed = True
            change.message = f"Column '{column.name}' removal allowed (ignore_unknown mode)"
        else:
            # allow_new_nullable doesn't affect column removal
            change.allowed = False
            change.message = f"Column '{column.name}' removal not allowed"

        return change

    def _check_column_changes(
        self,
        old_col: ColumnSpec,
        new_col: ColumnSpec,
    ) -> List[EvolutionChange]:
        """Check changes to an existing column."""
        changes: List[EvolutionChange] = []

        # Check type change
        if old_col.type != new_col.type:
            change = self._check_type_change(old_col, new_col)
            changes.append(change)

        # Check nullable change
        if old_col.nullable != new_col.nullable:
            change = self._check_nullable_change(old_col, new_col)
            changes.append(change)

        # Check precision/scale changes for decimal types
        if old_col.type == DataType.DECIMAL and new_col.type == DataType.DECIMAL:
            if old_col.precision != new_col.precision:
                change = self._check_precision_change(old_col, new_col)
                changes.append(change)
            if old_col.scale != new_col.scale:
                change = self._check_scale_change(old_col, new_col)
                changes.append(change)

        return changes

    def _check_type_change(
        self,
        old_col: ColumnSpec,
        new_col: ColumnSpec,
    ) -> EvolutionChange:
        """Check if type change is allowed."""
        change = EvolutionChange(
            change_type="type_changed",
            column_name=old_col.name,
            old_value=old_col.type.value,
            new_value=new_col.type.value,
        )

        # Protected columns cannot change type
        if old_col.name in self.config.protected_columns:
            change.allowed = False
            change.message = f"Type change not allowed for protected column '{old_col.name}'"
            return change

        # Check if type relaxation is allowed
        if self.config.allow_type_relaxation:
            allowed_wider = self.TYPE_WIDENING.get(old_col.type, set())
            if new_col.type in allowed_wider:
                change.allowed = True
                change.message = (
                    f"Type widening from '{old_col.type.value}' to "
                    f"'{new_col.type.value}' allowed for '{old_col.name}'"
                )
                return change

        change.allowed = False
        change.message = (
            f"Type change from '{old_col.type.value}' to '{new_col.type.value}' "
            f"not allowed for column '{old_col.name}'"
        )
        return change

    def _check_nullable_change(
        self,
        old_col: ColumnSpec,
        new_col: ColumnSpec,
    ) -> EvolutionChange:
        """Check if nullable change is allowed."""
        change = EvolutionChange(
            change_type="nullable_changed",
            column_name=old_col.name,
            old_value=old_col.nullable,
            new_value=new_col.nullable,
        )

        # Relaxing nullable (false -> true) is generally safe
        if not old_col.nullable and new_col.nullable:
            change.allowed = True
            change.message = f"Column '{old_col.name}' can be made nullable"
            return change

        # Tightening nullable (true -> false) is risky
        change.allowed = False
        change.message = f"Column '{old_col.name}' cannot be made non-nullable (data may have nulls)"
        return change

    def _check_precision_change(
        self,
        old_col: ColumnSpec,
        new_col: ColumnSpec,
    ) -> EvolutionChange:
        """Check if precision change is allowed."""
        old_precision = old_col.precision or 0
        new_precision = new_col.precision or 0

        change = EvolutionChange(
            change_type="precision_changed",
            column_name=old_col.name,
            old_value=old_precision,
            new_value=new_precision,
        )

        # Increasing precision is safe
        if new_precision >= old_precision and self.config.allow_precision_increase:
            change.allowed = True
            change.message = f"Precision increase for '{old_col.name}' allowed"
        else:
            change.allowed = False
            change.message = (
                f"Precision decrease from {old_precision} to {new_precision} "
                f"not allowed for '{old_col.name}'"
            )

        return change

    def _check_scale_change(
        self,
        old_col: ColumnSpec,
        new_col: ColumnSpec,
    ) -> EvolutionChange:
        """Check if scale change is allowed."""
        old_scale = old_col.scale or 0
        new_scale = new_col.scale or 0

        change = EvolutionChange(
            change_type="scale_changed",
            column_name=old_col.name,
            old_value=old_scale,
            new_value=new_scale,
        )

        # Increasing scale is safe (with sufficient precision)
        if new_scale >= old_scale:
            change.allowed = True
            change.message = f"Scale increase for '{old_col.name}' allowed"
        else:
            change.allowed = False
            change.message = (
                f"Scale decrease from {old_scale} to {new_scale} "
                f"not allowed for '{old_col.name}'"
            )

        return change


def apply_evolution_rules(
    df: pd.DataFrame,
    expected_schema: SchemaSpec,
    config: Optional[EvolutionConfig] = None,
) -> pd.DataFrame:
    """Apply schema evolution rules to a DataFrame.

    This function adjusts the DataFrame to match the expected schema
    based on the evolution mode:
    - strict: Raises error on any mismatch
    - allow_new_nullable: Keeps new columns, drops missing expected columns with nulls
    - ignore_unknown: Keeps only expected columns

    Args:
        df: DataFrame to process
        expected_schema: Expected schema specification
        config: Evolution configuration

    Returns:
        Adjusted DataFrame

    Raises:
        ValueError: If schema evolution is not compatible in strict mode
    """
    if config is None:
        config = EvolutionConfig()

    expected_columns = set(expected_schema.column_names)
    actual_columns = set(df.columns)

    # Handle based on mode
    if config.mode == SchemaEvolutionMode.STRICT:
        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns
        if missing or extra:
            raise ValueError(
                f"Schema mismatch in strict mode. "
                f"Missing: {missing or 'none'}, Extra: {extra or 'none'}"
            )
        return df

    elif config.mode == SchemaEvolutionMode.ALLOW_NEW_NULLABLE:
        # Keep extra columns (they're new)
        # Add missing columns with nulls if they're nullable
        result = df.copy()
        for col_name in expected_columns - actual_columns:
            col_spec = expected_schema.get_column(col_name)
            if col_spec and col_spec.nullable:
                result[col_name] = None
                logger.info("Added missing nullable column '%s' with nulls", col_name)
            else:
                raise ValueError(
                    f"Missing non-nullable column '{col_name}' cannot be added"
                )
        return result

    elif config.mode == SchemaEvolutionMode.IGNORE_UNKNOWN:
        # Keep only expected columns that exist
        columns_to_keep = [c for c in df.columns if c in expected_columns]
        result = df[columns_to_keep].copy()

        # Add missing columns with nulls if nullable
        for col_name in expected_columns - actual_columns:
            col_spec = expected_schema.get_column(col_name)
            if col_spec and col_spec.nullable:
                result[col_name] = None
                logger.info("Added missing nullable column '%s' with nulls", col_name)
            elif col_spec and col_spec.default is not None:
                result[col_name] = col_spec.default
                logger.info("Added missing column '%s' with default value", col_name)
            else:
                raise ValueError(
                    f"Missing non-nullable column '{col_name}' with no default"
                )

        return result

    raise ValueError(f"Unsupported schema evolution mode: {config.mode}")


def parse_evolution_config(config: Dict[str, Any]) -> Optional[EvolutionConfig]:
    """Parse evolution configuration from config.

    Args:
        config: Full pipeline config or schema_evolution section

    Returns:
        EvolutionConfig if configured, None otherwise
    """
    evolution_cfg = config.get("schema_evolution", config)

    if not evolution_cfg or not isinstance(evolution_cfg, dict):
        return None

    if "mode" not in evolution_cfg:
        return None

    try:
        return EvolutionConfig.from_dict(evolution_cfg)
    except Exception as e:
        logger.warning("Could not parse evolution config: %s", e)
        return None
