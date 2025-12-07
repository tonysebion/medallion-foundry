"""Resilience patterns for extractors.

This module provides a mixin class that extractors can use to gain circuit
breaker and retry capabilities. It extends the shared ResilienceMixin from
the platform layer with extractor-specific defaults.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional, TYPE_CHECKING

from core.platform.resilience import ResilienceMixin
from core.platform.resilience.constants import (
    DEFAULT_FAILURE_THRESHOLD,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_HALF_OPEN_MAX_CALLS,
)

if TYPE_CHECKING:
    from core.platform.resilience import CircuitBreaker

logger = logging.getLogger(__name__)


class ResilientExtractorMixin(ResilienceMixin):
    """Mixin providing circuit breaker and retry for extractors.

    This mixin extends ResilienceMixin with extractor-specific defaults
    and a convenient _init_resilience() method. It provides the same
    resilience patterns used by BaseCloudStorage, enabling extractors
    to have consistent error handling and fault tolerance.

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

    _breaker: "CircuitBreaker"

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
        self._init_resilience_single(
            component_name=extractor_name,
            failure_threshold=failure_threshold,
            cooldown_seconds=cooldown_seconds,
            half_open_max_calls=half_open_max_calls,
            on_state_change=on_state_change,
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
