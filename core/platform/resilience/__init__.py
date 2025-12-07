"""Resilience patterns: retry, rate limiting, circuit breaker, and error handling.

This package provides:
- RetryPolicy, execute_with_retry: Exponential backoff with jitter
- CircuitBreaker, CircuitState: Circuit breaker pattern
- RateLimiter: Token bucket rate limiting
- Error wrappers: Map third-party exceptions to domain exceptions
- Late data handling: Detect and process late-arriving data
"""

from core.platform.resilience.constants import (
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
from core.platform.resilience.retry import (
    RetryPolicy,
    execute_with_retry,
    execute_with_retry_async,
)
from core.platform.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
)
from core.platform.resilience.rate_limiter import RateLimiter
from core.platform.resilience.error_mapping import (
    wrap_storage_error,
    wrap_extraction_error,
    wrap_requests_exception,
    wrap_boto3_exception,
    wrap_azure_exception,
    register_error_mapper,
    exception_to_domain_error,
    list_error_mappers,
)
from core.platform.resilience.late_data import (
    LateDataMode,
    LateDataConfig,
    LateDataResult,
    LateDataHandler,
    BackfillWindow,
    build_late_data_handler,
    parse_backfill_window,
)
from core.platform.resilience.mixins import ResilienceMixin

__all__ = [
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
    # Retry
    "RetryPolicy",
    "execute_with_retry",
    "execute_with_retry_async",
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitState",
    # Rate Limiter
    "RateLimiter",
    # Error Mapping
    "wrap_storage_error",
    "wrap_extraction_error",
    "wrap_requests_exception",
    "wrap_boto3_exception",
    "wrap_azure_exception",
    "register_error_mapper",
    "exception_to_domain_error",
    "list_error_mappers",
    # Late Data
    "LateDataMode",
    "LateDataConfig",
    "LateDataResult",
    "LateDataHandler",
    "BackfillWindow",
    "build_late_data_handler",
    "parse_backfill_window",
    # Mixins
    "ResilienceMixin",
]
