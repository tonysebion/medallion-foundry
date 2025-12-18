"""Shared utilities for database extractors.

This module provides common functionality used across DbExtractor and
DbMultiExtractor to avoid code duplication.
"""

from __future__ import annotations


def is_retryable_db_error(exc: BaseException) -> bool:
    """Determine if a database error is retryable.

    Retries on transient connection errors but not on query/data errors.
    Supports pyodbc, SQLAlchemy, and generic Python connection exceptions.

    Args:
        exc: The exception to check

    Returns:
        True if the exception represents a transient/retryable error
    """
    exc_type = type(exc).__name__
    exc_module = type(exc).__module__

    # pyodbc and database connection errors
    if "pyodbc" in exc_module:
        # Retry on connection and timeout errors
        if "OperationalError" in exc_type or "InterfaceError" in exc_type:
            return True

    # SQLAlchemy connection errors
    if "sqlalchemy" in exc_module:
        if "OperationalError" in exc_type or "DisconnectionError" in exc_type:
            return True

    # Generic connection-related errors
    if exc_type in ("ConnectionError", "TimeoutError", "BrokenPipeError"):
        return True

    return False


__all__ = ["is_retryable_db_error"]
