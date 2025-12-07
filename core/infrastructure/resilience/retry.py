"""Resilience patterns: retry, rate limiting, circuit breaker, and error wrapping.

This module consolidates:
- RetryPolicy, CircuitBreaker, execute_with_retry - exponential backoff with jitter
- RateLimiter - token bucket rate limiting for sync/async
- Error wrappers - map third-party exceptions to domain exceptions
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import time
from dataclasses import dataclass, field, asdict
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from core.primitives.foundations.exceptions import (
    AuthenticationError,
    ExtractionError,
    RetryExhaustedError,
    StorageError,
)
from core.infrastructure.resilience.constants import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_BASE_DELAY,
    DEFAULT_MAX_DELAY,
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_JITTER,
    DEFAULT_FAILURE_THRESHOLD,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_HALF_OPEN_MAX_CALLS,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")
Predicate = Callable[[BaseException], bool]
DelayCallback = Callable[[BaseException, int, float], Optional[float]]


# =============================================================================
# Retry Policy and Circuit Breaker
# =============================================================================


@dataclass
class RetryPolicy:
    """Configuration for retry behavior with exponential backoff."""

    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    base_delay: float = DEFAULT_BASE_DELAY
    max_delay: float = DEFAULT_MAX_DELAY
    backoff_multiplier: float = DEFAULT_BACKOFF_MULTIPLIER
    jitter: float = DEFAULT_JITTER  # fraction of delay as jitter (0.0-1.0)
    retry_on_exceptions: Tuple[Type[BaseException], ...] = field(
        default_factory=lambda: (
            TimeoutError,
            ConnectionError,
            OSError,
        )
    )
    retry_if: Optional[Predicate] = None  # custom predicate
    # Optional callback to compute delay from an exception (e.g., Retry-After).
    # If it returns None, fall back to exponential backoff.
    delay_from_exception: Optional[DelayCallback] = None

    def should_retry(self, exc: BaseException) -> bool:
        if self.retry_if is not None:
            try:
                return bool(self.retry_if(exc))
            except Exception:
                return False
        return isinstance(exc, self.retry_on_exceptions)

    def compute_delay(self, attempt: int) -> float:
        delay = min(
            self.max_delay, self.base_delay * (self.backoff_multiplier ** (attempt - 1))
        )
        if self.jitter > 0:
            span = delay * self.jitter
            delay = max(0.0, random.uniform(delay - span, delay + span))
        return delay

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization.

        Note: Callables (retry_if, delay_from_exception) are not serialized.
        """
        return {
            "max_attempts": self.max_attempts,
            "base_delay": self.base_delay,
            "max_delay": self.max_delay,
            "backoff_multiplier": self.backoff_multiplier,
            "jitter": self.jitter,
            "retry_on_exceptions": [exc.__name__ for exc in self.retry_on_exceptions],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetryPolicy":
        """Create from dictionary.

        Note: retry_on_exceptions are restored as a tuple of built-in exception types.
        Custom exception types and callables are not restored.
        """
        # Map exception names back to types (only for common built-ins)
        exc_name_map = {
            "TimeoutError": TimeoutError,
            "ConnectionError": ConnectionError,
            "OSError": OSError,
        }
        exc_names = data.get("retry_on_exceptions", [])
        exc_types = tuple(exc_name_map.get(name, Exception) for name in exc_names)

        return cls(
            max_attempts=data.get("max_attempts", DEFAULT_MAX_ATTEMPTS),
            base_delay=data.get("base_delay", DEFAULT_BASE_DELAY),
            max_delay=data.get("max_delay", DEFAULT_MAX_DELAY),
            backoff_multiplier=data.get("backoff_multiplier", DEFAULT_BACKOFF_MULTIPLIER),
            jitter=data.get("jitter", DEFAULT_JITTER),
            retry_on_exceptions=exc_types if exc_types else (TimeoutError, ConnectionError, OSError),
        )


class CircuitState:
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker with open/half-open/closed states."""

    failure_threshold: int = DEFAULT_FAILURE_THRESHOLD
    cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS
    half_open_max_calls: int = DEFAULT_HALF_OPEN_MAX_CALLS
    on_state_change: Optional[Callable[[str], None]] = None

    _state: str = field(default=CircuitState.CLOSED, init=False)
    _failures: int = field(default=0, init=False)
    _opened_at: float = field(default=0.0, init=False)
    _half_open_calls: int = field(default=0, init=False)

    def allow(self) -> bool:
        now = time.time()
        if self._state == CircuitState.OPEN:
            if now - self._opened_at >= self.cooldown_seconds:
                self._state = CircuitState.HALF_OPEN
                if self.on_state_change:
                    try:
                        self.on_state_change(self._state)
                    except Exception:
                        pass
                self._half_open_calls = 0
            else:
                return False
        if self._state == CircuitState.HALF_OPEN:
            if self._half_open_calls >= self.half_open_max_calls:
                return False
            self._half_open_calls += 1
        return True

    def record_success(self) -> None:
        prev = self._state
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._half_open_calls = 0
        if prev != self._state and self.on_state_change:
            try:
                self.on_state_change(self._state)
            except Exception:
                pass

    def record_failure(self) -> None:
        self._failures += 1
        if (
            self._state in (CircuitState.CLOSED, CircuitState.HALF_OPEN)
            and self._failures >= self.failure_threshold
        ):
            self._state = CircuitState.OPEN
            self._opened_at = time.time()
            self._half_open_calls = 0
            if self.on_state_change:
                try:
                    self.on_state_change(self._state)
                except Exception:
                    pass

    @property
    def state(self) -> str:
        return self._state

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization.

        Note: on_state_change callback is not serialized.
        """
        return {
            "failure_threshold": self.failure_threshold,
            "cooldown_seconds": self.cooldown_seconds,
            "half_open_max_calls": self.half_open_max_calls,
            "state": self._state,
            "failures": self._failures,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CircuitBreaker":
        """Create from dictionary.

        Note: on_state_change callback is not restored.
        """
        breaker = cls(
            failure_threshold=data.get("failure_threshold", DEFAULT_FAILURE_THRESHOLD),
            cooldown_seconds=data.get("cooldown_seconds", DEFAULT_COOLDOWN_SECONDS),
            half_open_max_calls=data.get("half_open_max_calls", DEFAULT_HALF_OPEN_MAX_CALLS),
        )
        breaker._state = data.get("state", CircuitState.CLOSED)
        breaker._failures = data.get("failures", 0)
        return breaker


def execute_with_retry(
    func: Callable[..., T],
    *args: Any,
    policy: Optional[RetryPolicy] = None,
    breaker: Optional[CircuitBreaker] = None,
    operation_name: Optional[str] = None,
    **kwargs: Any,
) -> T:
    """Execute a function with retry and optional circuit breaker.

    - Skips execution if breaker is open (returns RetryExhaustedError)
    - Exponential backoff with jitter between attempts
    - Resets breaker on success; increments on failure
    """
    policy = policy or RetryPolicy()
    attempts = 0

    while True:
        attempts += 1
        if breaker is not None and not breaker.allow():
            raise RetryExhaustedError(
                f"Circuit open; refusing to execute {operation_name or func.__name__}",
                attempts=attempts - 1,
                operation=operation_name or func.__name__,
            )

        try:
            result = func(*args, **kwargs)
            if breaker is not None:
                breaker.record_success()
            return result
        except BaseException as exc:  # noqa: BLE001
            retryable = policy.should_retry(exc)
            if not retryable or attempts >= policy.max_attempts:
                if breaker is not None:
                    breaker.record_failure()
                raise RetryExhaustedError(
                    f"Operation failed after {attempts} attempt(s): {operation_name or func.__name__}",
                    attempts=attempts,
                    operation=operation_name or func.__name__,
                    last_error=(exc if isinstance(exc, Exception) else None),
                ) from exc
            # allow exception-specific delay override
            delay = policy.compute_delay(attempts)
            if policy.delay_from_exception is not None:
                try:
                    override = policy.delay_from_exception(exc, attempts, delay)
                    if override is not None:
                        delay = max(0.0, float(override))
                except Exception:
                    pass
            time.sleep(delay)


async def execute_with_retry_async(
    func: Callable[..., Awaitable[T]],
    *args: Any,
    policy: Optional[RetryPolicy] = None,
    breaker: Optional[CircuitBreaker] = None,
    operation_name: Optional[str] = None,
    **kwargs: Any,
) -> T:
    """Async variant of execute_with_retry for httpx and async callables.

    - Skips execution if breaker is open (raises RetryExhaustedError)
    - Exponential backoff with jitter between attempts (async sleep)
    - Resets breaker on success; increments on failure
    """
    policy = policy or RetryPolicy()
    attempts = 0

    while True:
        attempts += 1
        if breaker is not None and not breaker.allow():
            raise RetryExhaustedError(
                f"Circuit open; refusing to execute {operation_name or func.__name__}",
                attempts=attempts - 1,
                operation=operation_name or func.__name__,
            )

        try:
            result = await func(*args, **kwargs)
            if breaker is not None:
                breaker.record_success()
            return result
        except BaseException as exc:  # noqa: BLE001
            retryable = policy.should_retry(exc)
            if not retryable or attempts >= policy.max_attempts:
                if breaker is not None:
                    breaker.record_failure()
                raise RetryExhaustedError(
                    f"Operation failed after {attempts} attempt(s): {operation_name or func.__name__}",
                    attempts=attempts,
                    operation=operation_name or func.__name__,
                    last_error=(exc if isinstance(exc, Exception) else None),
                ) from exc
            # allow exception-specific delay override
            delay = policy.compute_delay(attempts)
            if policy.delay_from_exception is not None:
                try:
                    override = policy.delay_from_exception(exc, attempts, delay)
                    if override is not None:
                        delay = max(0.0, float(override))
                except Exception:
                    pass
            await asyncio.sleep(delay)


# =============================================================================
# Rate Limiter
# =============================================================================


class RateLimiter:
    """Token-bucket rate limiter for sync and async paths.

    Configuration:
    - requests_per_second (float): tokens per second
    - burst (int): optional bucket capacity (defaults to ceil(rps))
    - component (str): optional label used when emitting rate-limit metrics
    """

    def __init__(
        self,
        requests_per_second: float,
        burst: Optional[int] = None,
        component: Optional[str] = None,
        emit_metrics: bool = True,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be > 0")
        self.rate = float(requests_per_second)
        self.capacity = burst if burst is not None else max(1, math.ceil(self.rate))
        self.tokens = float(self.capacity)
        self.last_refill = time.monotonic()
        self.component = component
        self._emit_metrics = bool(component) and emit_metrics

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        added = elapsed * self.rate
        if added > 0:
            self.tokens = min(self.capacity, self.tokens + added)
            self.last_refill = now
            self._emit_metric("refill")

    def acquire(self) -> None:
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                self._emit_metric("acquire")
                return
            # sleep until a token is likely available
            deficit = 1.0 - self.tokens
            time.sleep(max(0.0, deficit / self.rate))

    async def async_acquire(self) -> None:
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                self._emit_metric("acquire")
                return
            deficit = 1.0 - self.tokens
            await asyncio.sleep(max(0.0, deficit / self.rate))

    def _emit_metric(self, phase: str) -> None:
        if not self._emit_metrics or not self.component:
            return
        try:
            logger.info(
                "metric=rate_limit component=%s phase=%s tokens=%.2f capacity=%d rate=%.2f",
                self.component,
                phase,
                self.tokens,
                self.capacity,
                self.rate,
            )
        except Exception:
            logger.debug(
                "Failed to emit rate limit metric for %s phase=%s",
                self.component,
                phase,
            )

    @staticmethod
    def from_config(
        extractor_cfg: Optional[Dict[str, Any]],
        run_cfg: Optional[Dict[str, Any]],
        *,
        component: Optional[str] = None,
        env_var: Optional[str] = "BRONZE_API_RPS",
    ) -> Optional["RateLimiter"]:
        """Create RateLimiter from per-extractor or run configuration."""

        rps_value = None
        burst_value = None

        if isinstance(extractor_cfg, dict):
            rl_cfg = extractor_cfg.get("rate_limit")
            if isinstance(rl_cfg, dict):
                rps_value = rl_cfg.get("rps")
                burst_value = rl_cfg.get("burst")

        if rps_value is None and isinstance(run_cfg, dict):
            rps_value = run_cfg.get("rate_limit_rps")

        if rps_value is None and env_var:
            env_val = os.environ.get(env_var)
            if env_val:
                rps_value = env_val

        if not rps_value:
            return None

        try:
            burst = int(burst_value) if burst_value is not None else None
        except (TypeError, ValueError):
            burst = None

        try:
            return RateLimiter(
                float(rps_value), burst=burst, component=component, emit_metrics=bool(component)
            )
        except Exception:
            return None


# =============================================================================
# Error Wrappers
# =============================================================================


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


# =============================================================================
# Unified Error Mapper
# =============================================================================

# Type alias for error mapper functions
ErrorMapper = Callable[[Exception, str, Optional[str]], Exception]

# Registry of backend-specific error mappers
_ERROR_MAPPERS: Dict[str, ErrorMapper] = {}


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
