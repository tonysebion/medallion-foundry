"""Foundational modules for bronze-foundry.

This package contains shared primitives with no internal dependencies:
- patterns: LoadPattern enum (SNAPSHOT, INCREMENTAL_APPEND, INCREMENTAL_MERGE)
- exceptions: Domain exception hierarchy and deprecation utilities
- logging: Logging configuration (JSONFormatter, setup_logging)
"""

from .patterns import LoadPattern
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
    # Patterns
    "LoadPattern",
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
