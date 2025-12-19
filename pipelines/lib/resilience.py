"""Resilience utilities for pipelines.

Provides opt-in retry decorator and related utilities for handling
transient failures in database connections, API calls, etc.

Philosophy: Keep BronzeSource and SilverEntity simple. Retry is opt-in
via decorator when sources are known to be flaky.
"""

from __future__ import annotations

import logging
import random
import time
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type, TypeVar

logger = logging.getLogger(__name__)

__all__ = ["with_retry"]

F = TypeVar("F", bound=Callable[..., Any])


def with_retry(
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
    exponential: bool = True,
    jitter: bool = True,
    retry_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
) -> Callable[[F], F]:
    """Opt-in retry decorator for flaky operations.

    Use when connecting to unreliable sources (flaky databases,
    rate-limited APIs, network issues).

    Args:
        max_attempts: Maximum number of attempts (default 3)
        backoff_seconds: Base delay between attempts (default 1.0)
        exponential: Use exponential backoff (default True)
        jitter: Add random jitter to backoff (default True)
        retry_exceptions: Only retry on these exceptions (default: all)

    Example:
        # Simple case - no retry (most sources)
        def run(run_date: str):
            return bronze.run(run_date)

        # Flaky database - add retry
        @with_retry(max_attempts=3)
        def run(run_date: str):
            return bronze.run(run_date)

        # API with rate limiting - longer backoff
        @with_retry(max_attempts=5, backoff_seconds=5.0)
        def run(run_date: str):
            return bronze.run(run_date)
    """

    def decorator(fn: F) -> F:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            fn_logger = logging.getLogger(fn.__module__)
            last_error: Optional[Exception] = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    # Check if we should retry this exception
                    if retry_exceptions and not isinstance(e, retry_exceptions):
                        raise

                    last_error = e

                    if attempt < max_attempts:
                        # Calculate wait time
                        if exponential:
                            wait = backoff_seconds * (2 ** (attempt - 1))
                        else:
                            wait = backoff_seconds

                        # Add jitter (0-50% of wait time)
                        if jitter:
                            wait = wait * (1 + random.random() * 0.5)

                        fn_logger.warning(
                            "Attempt %d/%d failed: %s. Retrying in %.1fs...",
                            attempt,
                            max_attempts,
                            e,
                            wait,
                        )
                        time.sleep(wait)
                    else:
                        fn_logger.error(
                            "All %d attempts failed. Last error: %s",
                            max_attempts,
                            e,
                        )

            # Re-raise the last error
            if last_error:
                raise last_error

        return wrapper  # type: ignore

    return decorator


class RetryConfig:
    """Configuration for retry behavior.

    Use with run methods that accept a retry_config parameter.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
        exponential: bool = True,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.exponential = exponential
        self.jitter = jitter

    @classmethod
    def none(cls) -> "RetryConfig":
        """No retry - fail immediately."""
        return cls(max_attempts=1)

    @classmethod
    def default(cls) -> "RetryConfig":
        """Default retry: 3 attempts with exponential backoff."""
        return cls()

    @classmethod
    def aggressive(cls) -> "RetryConfig":
        """Aggressive retry: 5 attempts with longer backoff."""
        return cls(max_attempts=5, backoff_seconds=5.0)


def retry_operation(
    operation: Callable[[], Any],
    config: RetryConfig,
    operation_name: str = "operation",
) -> Any:
    """Execute an operation with retry logic.

    Alternative to the decorator for one-off retries.

    Args:
        operation: Callable to execute
        config: Retry configuration
        operation_name: Name for logging

    Returns:
        Result of the operation

    Example:
        result = retry_operation(
            lambda: database.execute(query),
            RetryConfig.default(),
            "database query"
        )
    """
    last_error: Optional[Exception] = None

    for attempt in range(1, config.max_attempts + 1):
        try:
            return operation()
        except Exception as e:
            last_error = e

            if attempt < config.max_attempts:
                if config.exponential:
                    wait = config.backoff_seconds * (2 ** (attempt - 1))
                else:
                    wait = config.backoff_seconds

                if config.jitter:
                    wait = wait * (1 + random.random() * 0.5)

                logger.warning(
                    "%s attempt %d/%d failed: %s. Retrying in %.1fs...",
                    operation_name,
                    attempt,
                    config.max_attempts,
                    e,
                    wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "%s failed after %d attempts: %s",
                    operation_name,
                    config.max_attempts,
                    e,
                )

    if last_error:
        raise last_error


class CircuitBreaker:
    """Simple circuit breaker for protecting against cascading failures.

    Use when a downstream service might be completely unavailable,
    and you want to fail fast rather than waiting for timeouts.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failing fast, requests immediately fail
    - HALF_OPEN: Testing if service recovered

    Example:
        breaker = CircuitBreaker(failure_threshold=5, recovery_time=60)

        try:
            with breaker:
                result = api.call()
        except CircuitBreakerOpen:
            # Use cached data or fail gracefully
            pass
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_time: float = 60.0,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "CLOSED"

    def _check_state(self) -> None:
        """Check and possibly transition circuit state."""
        if self.state == "OPEN":
            if self.last_failure_time:
                elapsed = time.time() - self.last_failure_time
                if elapsed >= self.recovery_time:
                    logger.info("Circuit breaker transitioning to HALF_OPEN")
                    self.state = "HALF_OPEN"

    def __enter__(self) -> "CircuitBreaker":
        self._check_state()

        if self.state == "OPEN":
            raise CircuitBreakerOpen("Circuit breaker is OPEN - failing fast")

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_val: Optional[Exception],
        exc_tb: Any,
    ) -> bool:
        if exc_type is not None:
            # Failure occurred
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == "HALF_OPEN":
                # Failed during recovery test
                logger.warning(
                    "Circuit breaker returning to OPEN after HALF_OPEN failure"
                )
                self.state = "OPEN"
            elif self.failure_count >= self.failure_threshold:
                logger.warning(
                    "Circuit breaker opening after %d failures",
                    self.failure_count,
                )
                self.state = "OPEN"
        else:
            # Success
            if self.state == "HALF_OPEN":
                logger.info("Circuit breaker recovered - transitioning to CLOSED")
            self.failure_count = 0
            self.state = "CLOSED"

        return False  # Don't suppress the exception


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open."""

    pass
