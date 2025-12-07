"""Error wrappers and unified error mapping.

This module provides:
- wrap_storage_error: Decorator to wrap storage exceptions
- wrap_extraction_error: Decorator to wrap extraction exceptions
- wrap_requests_exception: Convert requests exceptions
- wrap_boto3_exception: Convert boto3/botocore exceptions
- wrap_azure_exception: Convert Azure SDK exceptions
- register_error_mapper: Decorator to register custom error mappers
- exception_to_domain_error: Unified exception mapper
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, TypeVar

from core.foundation.primitives.exceptions import (
    AuthenticationError,
    ExtractionError,
    RetryExhaustedError,
    StorageError,
)

T = TypeVar("T")

# Type alias for error mapper functions
ErrorMapper = Callable[[Exception, str, Optional[str]], Exception]

# Registry of backend-specific error mappers
_ERROR_MAPPERS: Dict[str, ErrorMapper] = {}


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


def register_error_mapper(backend_type: str) -> Callable[[ErrorMapper], ErrorMapper]:
    """Decorator to register a backend-specific error mapper.

    Usage:
        @register_error_mapper("my_backend")
        def my_mapper(exc, operation, context=None):
            return MyDomainError(...)
    """
    def decorator(mapper: ErrorMapper) -> ErrorMapper:
        _ERROR_MAPPERS[backend_type.lower()] = mapper
        return mapper
    return decorator


def _default_error_mapper(
    exc: Exception, operation: str, context: Optional[str] = None
) -> Exception:
    """Default error mapper for unknown backend types.

    Returns the original exception wrapped in a generic error message.
    """
    if isinstance(exc, (StorageError, ExtractionError, AuthenticationError)):
        return exc

    error_type = type(exc).__name__
    return ExtractionError(
        f"Operation failed: {error_type}: {exc}",
        extractor_type="unknown",
        original_error=exc,
    )


@register_error_mapper("s3")
def _s3_error_mapper(
    exc: Exception, operation: str, context: Optional[str] = None
) -> StorageError:
    """Map S3/boto3 exceptions to StorageError."""
    return wrap_boto3_exception(exc, operation, bucket=context)


@register_error_mapper("azure")
def _azure_error_mapper(
    exc: Exception, operation: str, context: Optional[str] = None
) -> StorageError:
    """Map Azure exceptions to StorageError."""
    return wrap_azure_exception(exc, operation, container=context)


@register_error_mapper("api")
def _api_error_mapper(
    exc: Exception, operation: str, context: Optional[str] = None
) -> ExtractionError:
    """Map API/requests exceptions to ExtractionError."""
    return wrap_requests_exception(exc, operation)


@register_error_mapper("local")
def _local_error_mapper(
    exc: Exception, operation: str, context: Optional[str] = None
) -> StorageError:
    """Map local filesystem exceptions to StorageError."""
    if isinstance(exc, StorageError):
        return exc

    error_type = type(exc).__name__
    return StorageError(
        f"Local storage operation failed: {error_type}: {exc}",
        backend_type="local",
        operation=operation,
        file_path=context,
        original_error=exc,
    )


def exception_to_domain_error(
    exc: Exception,
    backend_type: str,
    operation: str,
    context: Optional[str] = None,
) -> Exception:
    """Unified exception mapper that routes to backend-specific wrappers.

    This is the primary entry point for converting third-party exceptions
    to domain exceptions (StorageError, ExtractionError, etc.).

    Args:
        exc: The original exception to wrap
        backend_type: The backend type (s3, azure, local, api, db, etc.)
        operation: The operation that failed (upload, download, fetch, etc.)
        context: Optional context (bucket name, container, file path, etc.)

    Returns:
        A domain exception (StorageError or ExtractionError)

    Example:
        try:
            s3_client.upload_file(...)
        except Exception as exc:
            raise exception_to_domain_error(exc, "s3", "upload", bucket_name)
    """
    # Already a domain exception - return as-is
    if isinstance(exc, (StorageError, ExtractionError, AuthenticationError, RetryExhaustedError)):
        return exc

    mapper = _ERROR_MAPPERS.get(backend_type.lower(), _default_error_mapper)
    return mapper(exc, operation, context)


def list_error_mappers() -> list[str]:
    """Return all registered error mapper backend types."""
    return sorted(_ERROR_MAPPERS.keys())


__all__ = [
    "wrap_storage_error",
    "wrap_extraction_error",
    "wrap_requests_exception",
    "wrap_boto3_exception",
    "wrap_azure_exception",
    "register_error_mapper",
    "exception_to_domain_error",
    "list_error_mappers",
]
