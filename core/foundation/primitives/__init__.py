"""Foundational modules for bronze-foundry.

This package contains shared primitives with no internal dependencies:
- base: RichEnumMixin, SerializableMixin base classes
- patterns: LoadPattern enum (SNAPSHOT, INCREMENTAL_APPEND, INCREMENTAL_MERGE)
- models: SilverModel enum (SCD_TYPE_1, SCD_TYPE_2, etc.)
- exceptions: Domain exception hierarchy and deprecation utilities
- logging: Logging configuration (JSONFormatter, setup_logging)
"""

from .base import RichEnumMixin, SerializableMixin
from .patterns import LoadPattern
from .models import SilverModel, SILVER_MODEL_ALIASES
from .entity_kinds import (
    EntityKind,
    HistoryMode,
    InputMode,
    DeleteMode,
    SchemaMode,
)
from .exceptions import (
    BronzeFoundryError,
    ConfigValidationError,
    ExtractionError,
    StorageError,
    AuthenticationError,
    PaginationError,
    StateManagementError,
    DataQualityError,
    RetryExhaustedError,
    BronzeFoundryDeprecationWarning,
    BronzeFoundryCompatibilityWarning,
    DeprecationSpec,
    emit_deprecation,
    emit_compat,
)
from .logging import setup_logging

__all__ = [
    # Base classes
    "RichEnumMixin",
    "SerializableMixin",
    # Patterns
    "LoadPattern",
    # Models
    "SilverModel",
    "SILVER_MODEL_ALIASES",
    # Entity kinds
    "EntityKind",
    "HistoryMode",
    "InputMode",
    "DeleteMode",
    "SchemaMode",
    # Exceptions
    "BronzeFoundryError",
    "ConfigValidationError",
    "ExtractionError",
    "StorageError",
    "AuthenticationError",
    "PaginationError",
    "StateManagementError",
    "DataQualityError",
    "RetryExhaustedError",
    # Deprecation
    "BronzeFoundryDeprecationWarning",
    "BronzeFoundryCompatibilityWarning",
    "DeprecationSpec",
    "emit_deprecation",
    "emit_compat",
    # Logging
    "setup_logging",
]
