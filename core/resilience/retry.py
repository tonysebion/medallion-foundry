"""Retry policy with exponential backoff and jitter.

This module provides:
- RetryPolicy: Configuration for retry behavior
- execute_with_retry: Sync execution with retry
- execute_with_retry_async: Async execution with retry
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
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

from core.primitives.foundations.exceptions import RetryExhaustedError
from core.resilience.constants import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_BASE_DELAY,
    DEFAULT_MAX_DELAY,
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_JITTER,
)

T = TypeVar("T")
Predicate = Callable[[BaseException], bool]
DelayCallback = Callable[[BaseException, int, float], Optional[float]]


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
        """Check if an exception should trigger a retry."""
        if self.retry_if is not None:
            try:
                return bool(self.retry_if(exc))
            except Exception:
                return False
        return isinstance(exc, self.retry_on_exceptions)

    def compute_delay(self, attempt: int) -> float:
        """Compute delay for an attempt with exponential backoff and jitter."""
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


def execute_with_retry(
    func: Callable[..., T],
    *args: Any,
    policy: Optional[RetryPolicy] = None,
    breaker: Optional[Any] = None,  # CircuitBreaker, imported dynamically to avoid circular import
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
    breaker: Optional[Any] = None,  # CircuitBreaker
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


__all__ = [
    "RetryPolicy",
    "execute_with_retry",
    "execute_with_retry_async",
]
