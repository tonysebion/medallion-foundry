"""Schema type definitions per spec Section 6.

Defines the column types and schema specifications used for validation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DataType(str, Enum):
    """Supported data types for schema validation."""

    STRING = "string"
    INTEGER = "integer"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    DATETIME = "datetime"
    BINARY = "binary"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"
    ANY = "any"  # Wildcard type

    @classmethod
    def from_string(cls, value: str) -> "DataType":
        """Convert string to DataType, with aliases."""
        aliases = {
            "str": cls.STRING,
            "text": cls.STRING,
            "varchar": cls.STRING,
            "int": cls.INTEGER,
            "int32": cls.INTEGER,
            "int64": cls.BIGINT,
            "long": cls.BIGINT,
            "number": cls.DECIMAL,
            "numeric": cls.DECIMAL,
            "real": cls.FLOAT,
            "float32": cls.FLOAT,
            "float64": cls.DOUBLE,
            "bool": cls.BOOLEAN,
            "time": cls.TIMESTAMP,
            "bytes": cls.BINARY,
            "list": cls.ARRAY,
            "dict": cls.MAP,
            "object": cls.STRUCT,
        }

        normalized = value.lower().strip()
        if normalized in aliases:
            return aliases[normalized]

        try:
            return cls(normalized)
        except ValueError:
            logger.warning(f"Unknown data type '{value}', using ANY")
            return cls.ANY


@dataclass
class ColumnSpec:
    """Specification for a single column."""

    name: str
    type: DataType = DataType.STRING
    nullable: bool = True
    precision: Optional[int] = None  # For decimal
    scale: Optional[int] = None  # For decimal
    description: Optional[str] = None
    primary_key: bool = False
    default: Optional[Any] = None
    format: Optional[str] = None  # For date/timestamp parsing

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnSpec":
        """Create from dictionary."""
        name = data.get("name")
        if not name:
            raise ValueError("Column spec must have a 'name'")

        type_str = data.get("type", "string")
        data_type = DataType.from_string(type_str)

        return cls(
            name=name,
            type=data_type,
            nullable=data.get("nullable", True),
            precision=data.get("precision"),
            scale=data.get("scale"),
            description=data.get("description"),
            primary_key=data.get("primary_key", False),
            default=data.get("default"),
            format=data.get("format"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "name": self.name,
            "type": self.type.value,
            "nullable": self.nullable,
        }
        if self.precision is not None:
            result["precision"] = self.precision
        if self.scale is not None:
            result["scale"] = self.scale
        if self.description:
            result["description"] = self.description
        if self.primary_key:
            result["primary_key"] = self.primary_key
        if self.default is not None:
            result["default"] = self.default
        if self.format:
            result["format"] = self.format
        return result

    def matches_type(self, actual_type: str) -> bool:
        """Check if actual type matches expected type."""
        if self.type == DataType.ANY:
            return True

        actual_normalized = actual_type.lower().strip()

        # Map pandas/numpy types to our DataType
        type_mapping = {
            "object": [DataType.STRING, DataType.ANY],
            "string": [DataType.STRING],
            "int64": [DataType.INTEGER, DataType.BIGINT],
            "int32": [DataType.INTEGER],
            "float64": [DataType.FLOAT, DataType.DOUBLE, DataType.DECIMAL],
            "float32": [DataType.FLOAT],
            "bool": [DataType.BOOLEAN],
            "boolean": [DataType.BOOLEAN],
            "datetime64[ns]": [DataType.TIMESTAMP, DataType.DATETIME],
            "datetime64[ns, utc]": [DataType.TIMESTAMP, DataType.DATETIME],
            "date": [DataType.DATE],
            "bytes": [DataType.BINARY],
        }

        acceptable = type_mapping.get(actual_normalized, [])
        return self.type in acceptable or self.type.value in actual_normalized


@dataclass
class SchemaSpec:
    """Complete schema specification."""

    columns: List[ColumnSpec] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    partition_columns: List[str] = field(default_factory=list)
    version: int = 1

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchemaSpec":
        """Create from dictionary."""
        columns_raw = data.get("expected_columns") or data.get("columns", [])
        columns = [ColumnSpec.from_dict(c) for c in columns_raw]

        # Extract primary keys from columns or explicit list
        primary_keys = data.get("primary_keys", [])
        if not primary_keys:
            primary_keys = [c.name for c in columns if c.primary_key]

        return cls(
            columns=columns,
            primary_keys=primary_keys,
            partition_columns=data.get("partition_columns", []),
            version=data.get("version", 1),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "columns": [c.to_dict() for c in self.columns],
            "primary_keys": self.primary_keys,
            "partition_columns": self.partition_columns,
            "version": self.version,
        }

    def get_column(self, name: str) -> Optional[ColumnSpec]:
        """Get column spec by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    @property
    def column_names(self) -> List[str]:
        """Get list of column names."""
        return [c.name for c in self.columns]

    @property
    def required_columns(self) -> List[str]:
        """Get list of required (non-nullable) columns."""
        return [c.name for c in self.columns if not c.nullable]


def parse_schema_config(config: Dict[str, Any]) -> Optional[SchemaSpec]:
    """Parse schema configuration from config.

    Args:
        config: Full pipeline config or schema section

    Returns:
        SchemaSpec if configured, None otherwise
    """
    schema_cfg = config.get("schema", config)

    if not schema_cfg:
        return None

    # Check for expected_columns or columns
    if not schema_cfg.get("expected_columns") and not schema_cfg.get("columns"):
        return None

    try:
        return SchemaSpec.from_dict(schema_cfg)
    except Exception as e:
        logger.warning(f"Could not parse schema config: {e}")
        return None
