"""Rate limiting utilities for API sources.

Provides a simple token-bucket rate limiter for controlling
request rates to external APIs.
"""

from __future__ import annotations

import logging
import threading
import time
from functools import wraps
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

__all__ = ["RateLimiter", "rate_limited"]

F = TypeVar("F", bound=Callable[..., Any])


class RateLimiter:
    """Token-bucket rate limiter.

    Limits the rate of operations to a specified number per second.
    Thread-safe for concurrent usage.

    Example:
        limiter = RateLimiter(requests_per_second=10)

        for item in items:
            limiter.acquire()  # Blocks until allowed
            make_api_call(item)
    """

    def __init__(
        self,
        requests_per_second: float,
        *,
        burst_size: int | None = None,
    ) -> None:
        """Initialize rate limiter.

        Args:
            requests_per_second: Maximum sustained rate
            burst_size: Maximum burst capacity (defaults to 1)
        """
        self.rate = requests_per_second
        self.burst_size = burst_size or 1
        self.tokens = float(self.burst_size)
        self.last_update = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a token, blocking until available.

        Args:
            timeout: Maximum time to wait (None = wait forever)

        Returns:
            True if token acquired, False if timeout expired
        """
        start_time = time.monotonic()

        while True:
            with self._lock:
                self._refill_tokens()

                if self.tokens >= 1:
                    self.tokens -= 1
                    return True

                # Calculate wait time for next token
                wait_time = (1 - self.tokens) / self.rate

            # Check timeout
            if timeout is not None:
                elapsed = time.monotonic() - start_time
                if elapsed + wait_time > timeout:
                    return False

            # Wait and retry
            time.sleep(min(wait_time, 0.1))  # Cap wait at 100ms increments

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_update
        self.tokens = min(
            self.burst_size,
            self.tokens + elapsed * self.rate,
        )
        self.last_update = now

    def try_acquire(self) -> bool:
        """Try to acquire a token without blocking.

        Returns:
            True if token acquired, False if not available
        """
        with self._lock:
            self._refill_tokens()
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False


def rate_limited(
    requests_per_second: float,
    *,
    burst_size: int | None = None,
) -> Callable[[F], F]:
    """Decorator to rate-limit a function.

    Example:
        @rate_limited(10)  # 10 requests per second
        def call_api(item):
            return requests.get(f"https://api.example.com/{item}")

        # Can also be used with burst capacity
        @rate_limited(5, burst_size=10)  # 5 RPS sustained, burst of 10
        def call_api(item):
            ...
    """
    limiter = RateLimiter(requests_per_second, burst_size=burst_size)

    def decorator(fn: F) -> F:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            limiter.acquire()
            return fn(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator
