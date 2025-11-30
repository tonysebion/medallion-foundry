"""Centralized retry and circuit-breaker utilities.

Provides:
- RetryPolicy: configuration for max attempts, delays, jitter, and predicates
- CircuitBreaker: open/half-open/closed states with cooldowns and thresholds
- execute_with_retry: helper to run callables with retry + breaker
- execute_with_retry_async: async variant for httpx/async paths

This module is self-contained and avoids changing existing behavior; adoption
is opt-in from extractors/backends.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Awaitable,
)

from core.exceptions import RetryExhaustedError


Predicate = Callable[[BaseException], bool]
DelayCallback = Callable[[BaseException, int, float], Optional[float]]


@dataclass
class RetryPolicy:
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
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
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


T = TypeVar("T")


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
