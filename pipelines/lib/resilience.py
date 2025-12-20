"""Resilience utilities for pipelines.

Provides opt-in retry decorator and related utilities for handling
transient failures in database connections, API calls, etc.

Philosophy: Keep BronzeSource and SilverEntity simple. Retry is opt-in
via decorator when sources are known to be flaky.

Implementation: Uses tenacity library internally for battle-tested retry logic.
"""

from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Any, Callable, Literal, Optional, Tuple, Type, TypeVar

import tenacity
from tenacity.wait import wait_base

logger = logging.getLogger(__name__)

__all__ = ["with_retry", "RetryConfig", "retry_operation", "CircuitBreaker", "CircuitBreakerOpen"]

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
    # Build wait strategy
    wait_strategy: wait_base
    if exponential:
        # Tenacity exponential: multiplier * 2^(attempt-1)
        # To match our original: backoff_seconds * 2^(attempt-1)
        # We use multiplier=backoff_seconds
        wait_strategy = tenacity.wait_exponential(multiplier=backoff_seconds, min=backoff_seconds)
    else:
        wait_strategy = tenacity.wait_fixed(backoff_seconds)

    if jitter:
        # Add random jitter (0-50% of wait time, matching original behavior)
        wait_strategy = wait_strategy + tenacity.wait_random(0, backoff_seconds * 0.5)

    # Build retry condition
    if retry_exceptions:
        retry_condition = tenacity.retry_if_exception_type(retry_exceptions)
    else:
        retry_condition = tenacity.retry_if_exception_type(Exception)

    def decorator(fn: F) -> F:
        fn_logger = logging.getLogger(fn.__module__)

        def before_sleep_handler(retry_state: tenacity.RetryCallState) -> None:
            """Log retry attempts."""
            exception = retry_state.outcome.exception() if retry_state.outcome else None
            fn_logger.warning(
                "Attempt %d/%d failed: %s. Retrying in %.1fs...",
                retry_state.attempt_number,
                max_attempts,
                exception,
                retry_state.next_action.sleep if retry_state.next_action else 0,
            )

        def after_handler(retry_state: tenacity.RetryCallState) -> None:
            """Log when all retries are exhausted."""
            if retry_state.outcome and retry_state.outcome.failed:
                exception = retry_state.outcome.exception()
                fn_logger.error(
                    "All %d attempts failed. Last error: %s",
                    max_attempts,
                    exception,
                )

        # Create the tenacity retry decorator
        tenacity_decorator = tenacity.retry(
            stop=tenacity.stop_after_attempt(max_attempts),
            wait=wait_strategy,
            retry=retry_condition,
            before_sleep=before_sleep_handler,
            reraise=True,
        )

        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Apply tenacity decorator and call
            retrying_fn = tenacity_decorator(fn)
            return retrying_fn(*args, **kwargs)

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
    # Build wait strategy using tenacity
    wait_strategy: wait_base
    if config.exponential:
        wait_strategy = tenacity.wait_exponential(
            multiplier=config.backoff_seconds, min=config.backoff_seconds
        )
    else:
        wait_strategy = tenacity.wait_fixed(config.backoff_seconds)

    if config.jitter:
        wait_strategy = wait_strategy + tenacity.wait_random(0, config.backoff_seconds * 0.5)

    def before_sleep_handler(retry_state: tenacity.RetryCallState) -> None:
        """Log retry attempts."""
        exception = retry_state.outcome.exception() if retry_state.outcome else None
        logger.warning(
            "%s attempt %d/%d failed: %s. Retrying in %.1fs...",
            operation_name,
            retry_state.attempt_number,
            config.max_attempts,
            exception,
            retry_state.next_action.sleep if retry_state.next_action else 0,
        )

    # Create retryer
    retryer = tenacity.Retrying(
        stop=tenacity.stop_after_attempt(config.max_attempts),
        wait=wait_strategy,
        retry=tenacity.retry_if_exception_type(Exception),
        before_sleep=before_sleep_handler,
        reraise=True,
    )

    try:
        return retryer(operation)
    except tenacity.RetryError:
        # This shouldn't happen with reraise=True, but handle it just in case
        raise
    except Exception:
        logger.error(
            "%s failed after %d attempts",
            operation_name,
            config.max_attempts,
        )
        raise


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
    ) -> Literal[False]:
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
