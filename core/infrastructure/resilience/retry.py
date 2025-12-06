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
import random
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
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

    max_attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 8.0
    backoff_multiplier: float = 2.0
    jitter: float = 0.2  # fraction of delay as jitter (0.0-1.0)
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


class CircuitState:
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker with open/half-open/closed states."""

    failure_threshold: int = 5
    cooldown_seconds: float = 30.0
    half_open_max_calls: int = 1
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
    """

    def __init__(self, requests_per_second: float, burst: Optional[int] = None) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be > 0")
        self.rate = float(requests_per_second)
        self.capacity = burst if burst is not None else max(1, math.ceil(self.rate))
        self.tokens = float(self.capacity)
        self.last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        added = elapsed * self.rate
        if added > 0:
            self.tokens = min(self.capacity, self.tokens + added)
            self.last_refill = now

    def acquire(self) -> None:
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            # sleep until a token is likely available
            deficit = 1.0 - self.tokens
            time.sleep(max(0.0, deficit / self.rate))

    async def async_acquire(self) -> None:
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            deficit = 1.0 - self.tokens
            await asyncio.sleep(max(0.0, deficit / self.rate))

    @staticmethod
    def from_config(api_cfg: dict, run_cfg: dict) -> Optional["RateLimiter"]:
        # Priority: api.rate_limit.rps -> run.rate_limit_rps -> env BRONZE_API_RPS
        rps = None
        rl_cfg = api_cfg.get("rate_limit", {}) if isinstance(api_cfg, dict) else {}
        if isinstance(rl_cfg, dict) and "rps" in rl_cfg:
            rps = rl_cfg.get("rps")
        if rps is None:
            rps = run_cfg.get("rate_limit_rps") if isinstance(run_cfg, dict) else None
        if rps is None:
            import os

            env_val = os.environ.get("BRONZE_API_RPS")
            if env_val:
                try:
                    rps = float(env_val)
                except ValueError:
                    rps = None
        if not rps:
            return None
        try:
            return RateLimiter(float(rps))
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
