"""Resilience patterns for extractors.

This module provides a mixin class that extractors can use to gain circuit
breaker and retry capabilities, following the same pattern used by
BaseCloudStorage in the infrastructure layer.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional, TypeVar

from core.platform.resilience import (
    CircuitBreaker,
    RetryPolicy,
    execute_with_retry,
)
from core.platform.resilience.constants import (
    DEFAULT_FAILURE_THRESHOLD,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_HALF_OPEN_MAX_CALLS,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_BASE_DELAY,
    DEFAULT_MAX_DELAY,
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_JITTER,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ResilientExtractorMixin:
    """Mixin providing circuit breaker and retry for extractors.

    This mixin provides the same resilience patterns used by BaseCloudStorage,
    enabling extractors to have consistent error handling and fault tolerance.

    Usage:
        class MyExtractor(BaseExtractor, ResilientExtractorMixin):
            def __init__(self, ...):
                self._init_resilience()  # Initialize circuit breaker

            def fetch_records(self, cfg, run_date):
                return self._execute_with_resilience(
                    lambda: self._do_fetch(cfg, run_date),
                    "my_extractor_fetch",
                    retry_if=self._should_retry,
                )
    """

    _breaker: CircuitBreaker

    def _init_resilience(
        self,
        failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        half_open_max_calls: int = DEFAULT_HALF_OPEN_MAX_CALLS,
        on_state_change: Optional[Callable[[str], None]] = None,
    ) -> None:
        """Initialize circuit breaker for this extractor.

        Call this in the extractor's __init__ method.

        Args:
            failure_threshold: Number of failures before opening circuit
            cooldown_seconds: Seconds to wait before trying again
            half_open_max_calls: Max calls in half-open state
            on_state_change: Optional callback when state changes
        """
        extractor_name = getattr(self, "__class__", type(self)).__name__

        def _emit_state(state: str) -> None:
            logger.info(
                "metric=breaker_state component=%s state=%s",
                extractor_name,
                state,
            )
            if on_state_change:
                on_state_change(state)

        self._breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            cooldown_seconds=cooldown_seconds,
            half_open_max_calls=half_open_max_calls,
            on_state_change=_emit_state,
        )

    def _build_retry_policy(
        self,
        retry_if: Optional[Callable[[BaseException], bool]] = None,
        delay_from_exception: Optional[
            Callable[[BaseException, int, float], Optional[float]]
        ] = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
        backoff_multiplier: float = DEFAULT_BACKOFF_MULTIPLIER,
        jitter: float = DEFAULT_JITTER,
    ) -> RetryPolicy:
        """Build a retry policy for extraction operations.

        Args:
            retry_if: Custom predicate to determine if exception is retryable
            delay_from_exception: Optional callback to extract delay from exception
            max_attempts: Maximum retry attempts
            base_delay: Initial delay between retries
            max_delay: Maximum delay between retries
            backoff_multiplier: Multiplier for exponential backoff
            jitter: Random jitter factor (0.0 to 1.0)

        Returns:
            Configured RetryPolicy instance
        """
        return RetryPolicy(
            max_attempts=max_attempts,
            base_delay=base_delay,
            max_delay=max_delay,
            backoff_multiplier=backoff_multiplier,
            jitter=jitter,
            retry_on_exceptions=(),
            retry_if=retry_if or self._default_retry_if,
            delay_from_exception=delay_from_exception,
        )

    def _default_retry_if(self, exc: BaseException) -> bool:
        """Default retry predicate for extraction errors.

        Override this method in subclasses to customize retry behavior.

        Args:
            exc: The exception that was raised

        Returns:
            True if the operation should be retried
        """
        # Retry on connection errors and timeouts by default
        exc_type = type(exc).__name__
        exc_module = type(exc).__module__

        # requests library errors
        if "requests" in exc_module:
            if exc_type in ("ConnectionError", "Timeout", "ReadTimeout", "ConnectTimeout"):
                return True

        # httpx errors
        if "httpx" in exc_module:
            if "Timeout" in exc_type or "Connect" in exc_type:
                return True

        # Check for HTTP status errors with retryable codes
        if hasattr(exc, "response"):
            response = getattr(exc, "response", None)
            if response is not None:
                status = getattr(response, "status_code", None)
                if status is not None:
                    try:
                        status_int = int(status)
                        # Retry on 429 (rate limit) and 5xx (server errors)
                        return status_int == 429 or 500 <= status_int < 600
                    except (ValueError, TypeError):
                        pass

        return False

    def _execute_with_resilience(
        self,
        operation: Callable[[], T],
        operation_name: str,
        retry_if: Optional[Callable[[BaseException], bool]] = None,
        delay_from_exception: Optional[
            Callable[[BaseException, int, float], Optional[float]]
        ] = None,
        max_attempts: Optional[int] = None,
    ) -> T:
        """Execute an operation with circuit breaker and retry logic.

        Args:
            operation: The operation to execute (should be a callable)
            operation_name: Name for logging/metrics
            retry_if: Optional custom retry predicate
            delay_from_exception: Optional delay extraction callback
            max_attempts: Override default max attempts

        Returns:
            Result of the operation

        Raises:
            Exception: From operation if all retries exhausted or circuit open
        """
        policy_kwargs = {}
        if max_attempts is not None:
            policy_kwargs["max_attempts"] = max_attempts

        policy = self._build_retry_policy(
            retry_if=retry_if,
            delay_from_exception=delay_from_exception,
            **policy_kwargs,
        )

        return execute_with_retry(
            operation,
            policy=policy,
            breaker=self._breaker,
            operation_name=operation_name,
        )

    def get_circuit_state(self) -> str:
        """Get the current circuit breaker state.

        Returns:
            Current state: "closed", "open", or "half_open"
        """
        if hasattr(self, "_breaker"):
            return self._breaker.state.value
        return "unknown"

    def reset_circuit(self) -> None:
        """Reset the circuit breaker to closed state."""
        if hasattr(self, "_breaker"):
            # record_success resets failures and sets state to CLOSED
            self._breaker.record_success()
