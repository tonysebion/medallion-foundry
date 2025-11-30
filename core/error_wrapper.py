"""Helpers to wrap third-party exceptions into domain exceptions.

Provides consistent error handling by mapping boto3, requests, azure, and other
third-party exceptions into medallion-foundry's typed exception hierarchy.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional, TypeVar

from core.exceptions import (
    ExtractionError,
    StorageError,
    AuthenticationError,
    RetryExhaustedError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


def wrap_storage_error(
    func: Callable[..., T],
    backend_type: str,
    operation: str,
    file_path: Optional[str] = None,
    remote_path: Optional[str] = None,
) -> Callable[..., T]:
    """Decorator to wrap storage backend exceptions.

    Args:
        func: Function to wrap
        backend_type: Storage backend type (s3, azure, local)
        operation: Operation being performed (upload, download, list, delete)
        file_path: Local file path (if applicable)
        remote_path: Remote path (if applicable)

    Returns:
        Wrapped function that raises StorageError on failures
    """

    def wrapper(*args: Any, **kwargs: Any) -> T:
        try:
            return func(*args, **kwargs)
        except StorageError:
            # Already a domain exception, re-raise
            raise
        except Exception as exc:
            # Wrap third-party exception
            error_type = type(exc).__name__
            raise StorageError(
                f"Storage operation failed: {error_type}: {exc}",
                backend_type=backend_type,
                operation=operation,
                file_path=file_path,
                remote_path=remote_path,
                original_error=exc,
            ) from exc

    return wrapper


def wrap_extraction_error(
    func: Callable[..., T],
    extractor_type: str,
    system: Optional[str] = None,
    table: Optional[str] = None,
) -> Callable[..., T]:
    """Decorator to wrap extraction exceptions.

    Args:
        func: Function to wrap
        extractor_type: Type of extractor (api, db, file, custom)
        system: System name from config
        table: Table name from config

    Returns:
        Wrapped function that raises ExtractionError on failures
    """

    def wrapper(*args: Any, **kwargs: Any) -> T:
        try:
            return func(*args, **kwargs)
        except (ExtractionError, AuthenticationError, RetryExhaustedError):
            # Already a domain exception, re-raise
            raise
        except Exception as exc:
            # Wrap third-party exception
            error_type = type(exc).__name__
            raise ExtractionError(
                f"Extraction failed: {error_type}: {exc}",
                extractor_type=extractor_type,
                system=system,
                table=table,
                original_error=exc,
            ) from exc

    return wrapper


def wrap_requests_exception(
    exc: Exception, operation: str = "api_request"
) -> ExtractionError:
    """Convert requests library exceptions to ExtractionError.

    Args:
        exc: Original requests exception
        operation: Description of the operation

    Returns:
        ExtractionError with context
    """
    # Try to import requests for type checking
    try:
        import requests

        if isinstance(exc, requests.exceptions.Timeout):
            return ExtractionError(
                f"API request timed out: {operation}",
                extractor_type="api",
                original_error=exc,
            )
        elif isinstance(exc, requests.exceptions.ConnectionError):
            return ExtractionError(
                f"Connection failed: {operation}",
                extractor_type="api",
                original_error=exc,
            )
        elif isinstance(exc, requests.exceptions.HTTPError):
            status_code = getattr(
                getattr(exc, "response", None), "status_code", "unknown"
            )
            if status_code in (401, 403):
                return AuthenticationError(
                    f"Authentication failed (HTTP {status_code})",
                    auth_type="api",
                )
            return ExtractionError(
                f"HTTP error {status_code}: {operation}",
                extractor_type="api",
                original_error=exc,
            )
    except ImportError:
        pass

    # Fallback for unknown exception types
    return ExtractionError(
        f"API request failed: {type(exc).__name__}: {exc}",
        extractor_type="api",
        original_error=exc,
    )


def wrap_boto3_exception(
    exc: Exception, operation: str, bucket: Optional[str] = None
) -> StorageError:
    """Convert boto3/botocore exceptions to StorageError.

    Args:
        exc: Original boto3/botocore exception
        operation: Storage operation (upload, download, list, delete)
        bucket: S3 bucket name

    Returns:
        StorageError with context
    """
    try:
        from botocore.exceptions import ClientError, BotoCoreError

        if isinstance(exc, ClientError):
            error_code = exc.response.get("Error", {}).get("Code", "Unknown")
            status_code = exc.response.get("ResponseMetadata", {}).get(
                "HTTPStatusCode", 0
            )
            return StorageError(
                f"S3 operation failed: {error_code} (HTTP {status_code})",
                backend_type="s3",
                operation=operation,
                remote_path=bucket,
                original_error=exc,
            )
        elif isinstance(exc, BotoCoreError):
            return StorageError(
                f"S3 operation failed: {type(exc).__name__}",
                backend_type="s3",
                operation=operation,
                remote_path=bucket,
                original_error=exc,
            )
    except ImportError:
        pass

    return StorageError(
        f"S3 operation failed: {type(exc).__name__}: {exc}",
        backend_type="s3",
        operation=operation,
        remote_path=bucket,
        original_error=exc,
    )


def wrap_azure_exception(
    exc: Exception, operation: str, container: Optional[str] = None
) -> StorageError:
    """Convert Azure SDK exceptions to StorageError.

    Args:
        exc: Original Azure exception
        operation: Storage operation (upload, download, list, delete)
        container: Azure container name

    Returns:
        StorageError with context
    """
    try:
        from azure.core.exceptions import AzureError, ResourceNotFoundError

        if isinstance(exc, ResourceNotFoundError):
            return StorageError(
                f"Azure resource not found: {operation}",
                backend_type="azure",
                operation=operation,
                remote_path=container,
                original_error=exc,
            )
        elif isinstance(exc, AzureError):
            return StorageError(
                f"Azure operation failed: {type(exc).__name__}",
                backend_type="azure",
                operation=operation,
                remote_path=container,
                original_error=exc,
            )
    except ImportError:
        pass

    return StorageError(
        f"Azure operation failed: {type(exc).__name__}: {exc}",
        backend_type="azure",
        operation=operation,
        remote_path=container,
        original_error=exc,
    )
