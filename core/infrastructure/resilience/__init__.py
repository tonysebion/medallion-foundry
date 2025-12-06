"""Resilience patterns for bronze-foundry.

This package contains failure handling and recovery patterns:
- retry: RetryPolicy, CircuitBreaker, RateLimiter, execute_with_retry
- late_data: Late data detection and handling (allow, reject, quarantine)
- constants: Default values for resilience patterns
"""

from __future__ import annotations

from typing import Callable

from .constants import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_BASE_DELAY,
    DEFAULT_MAX_DELAY,
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_JITTER,
    DEFAULT_FAILURE_THRESHOLD,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_HALF_OPEN_MAX_CALLS,
    DEFAULT_LATE_DATA_THRESHOLD_DAYS,
    DEFAULT_QUARANTINE_PATH,
    DEFAULT_CHUNK_SIZE_BYTES,
    DEFAULT_STORAGE_TIMEOUT_SECONDS,
)
from .retry import (
    RetryPolicy,
    CircuitBreaker,
    CircuitState,
    RateLimiter,
    execute_with_retry,
    execute_with_retry_async,
    wrap_storage_error,
    wrap_extraction_error,
    wrap_requests_exception,
    wrap_boto3_exception,
    wrap_azure_exception,
)
from .late_data import (
    LateDataMode,
    LateDataConfig,
    LateDataResult,
    LateDataHandler,
    BackfillWindow,
    build_late_data_handler,
    parse_backfill_window,
)


def build_resilience_components(
    config: dict | None = None,
    on_state_change: Callable[[str], None] | None = None,
) -> tuple[RetryPolicy, CircuitBreaker]:
    """Build a RetryPolicy and CircuitBreaker pair from configuration.

    This helper reduces boilerplate when initializing resilience patterns
    for storage backends, extractors, and other components.

    Args:
        config: Optional dict with resilience settings. Supported keys:
            - max_attempts: int (default: DEFAULT_MAX_ATTEMPTS)
            - base_delay: float (default: DEFAULT_BASE_DELAY)
            - max_delay: float (default: DEFAULT_MAX_DELAY)
            - backoff_multiplier: float (default: DEFAULT_BACKOFF_MULTIPLIER)
            - jitter: float (default: DEFAULT_JITTER)
            - failure_threshold: int (default: DEFAULT_FAILURE_THRESHOLD)
            - cooldown_seconds: float (default: DEFAULT_COOLDOWN_SECONDS)
            - half_open_max_calls: int (default: DEFAULT_HALF_OPEN_MAX_CALLS)
        on_state_change: Optional callback invoked when circuit breaker state changes

    Returns:
        Tuple of (RetryPolicy, CircuitBreaker) configured from the provided settings

    Example:
        >>> policy, breaker = build_resilience_components(
        ...     {"max_attempts": 3, "failure_threshold": 10},
        ...     on_state_change=lambda s: logger.info("Circuit: %s", s)
        ... )
    """
    cfg = config or {}

    policy = RetryPolicy(
        max_attempts=cfg.get("max_attempts", DEFAULT_MAX_ATTEMPTS),
        base_delay=cfg.get("base_delay", DEFAULT_BASE_DELAY),
        max_delay=cfg.get("max_delay", DEFAULT_MAX_DELAY),
        backoff_multiplier=cfg.get("backoff_multiplier", DEFAULT_BACKOFF_MULTIPLIER),
        jitter=cfg.get("jitter", DEFAULT_JITTER),
    )

    breaker = CircuitBreaker(
        failure_threshold=cfg.get("failure_threshold", DEFAULT_FAILURE_THRESHOLD),
        cooldown_seconds=cfg.get("cooldown_seconds", DEFAULT_COOLDOWN_SECONDS),
        half_open_max_calls=cfg.get("half_open_max_calls", DEFAULT_HALF_OPEN_MAX_CALLS),
        on_state_change=on_state_change,
    )

    return policy, breaker

__all__ = [
    # Helper functions
    "build_resilience_components",
    # Constants
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_BASE_DELAY",
    "DEFAULT_MAX_DELAY",
    "DEFAULT_BACKOFF_MULTIPLIER",
    "DEFAULT_JITTER",
    "DEFAULT_FAILURE_THRESHOLD",
    "DEFAULT_COOLDOWN_SECONDS",
    "DEFAULT_HALF_OPEN_MAX_CALLS",
    "DEFAULT_LATE_DATA_THRESHOLD_DAYS",
    "DEFAULT_QUARANTINE_PATH",
    "DEFAULT_CHUNK_SIZE_BYTES",
    "DEFAULT_STORAGE_TIMEOUT_SECONDS",
    # Retry and circuit breaker
    "RetryPolicy",
    "CircuitBreaker",
    "CircuitState",
    "execute_with_retry",
    "execute_with_retry_async",
    # Rate limiting
    "RateLimiter",
    # Error wrapping
    "wrap_storage_error",
    "wrap_extraction_error",
    "wrap_requests_exception",
    "wrap_boto3_exception",
    "wrap_azure_exception",
    # Late data
    "LateDataMode",
    "LateDataConfig",
    "LateDataResult",
    "LateDataHandler",
    "BackfillWindow",
    "build_late_data_handler",
    "parse_backfill_window",
]
