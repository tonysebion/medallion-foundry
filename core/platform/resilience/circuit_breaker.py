"""Circuit breaker pattern implementation.

This module provides:
- CircuitState: Enum for circuit states (closed, open, half_open)
- CircuitBreaker: Circuit breaker with configurable thresholds
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional

from core.platform.resilience.constants import (
    DEFAULT_FAILURE_THRESHOLD,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_HALF_OPEN_MAX_CALLS,
)

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker with open/half-open/closed states."""

    failure_threshold: int = DEFAULT_FAILURE_THRESHOLD
    cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS
    half_open_max_calls: int = DEFAULT_HALF_OPEN_MAX_CALLS
    on_state_change: Optional[Callable[[str], None]] = None

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failures: int = field(default=0, init=False)
    _opened_at: float = field(default=0.0, init=False)
    _half_open_calls: int = field(default=0, init=False)

    def allow(self) -> bool:
        """Check if a call is allowed through the circuit."""
        now = time.time()
        if self._state == CircuitState.OPEN:
            if now - self._opened_at >= self.cooldown_seconds:
                self._state = CircuitState.HALF_OPEN
                if self.on_state_change:
                    try:
                        self.on_state_change(self._state)
                    except Exception as e:
                        logger.debug("Callback failed during state change to %s: %s", self._state, e)
                self._half_open_calls = 0
            else:
                return False
        if self._state == CircuitState.HALF_OPEN:
            if self._half_open_calls >= self.half_open_max_calls:
                return False
            self._half_open_calls += 1
        return True

    def record_success(self) -> None:
        """Record a successful call."""
        prev = self._state
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._half_open_calls = 0
        if prev != self._state and self.on_state_change:
            try:
                self.on_state_change(self._state)
            except Exception as e:
                logger.debug("Callback failed during state change to %s: %s", self._state, e)

    def record_failure(self) -> None:
        """Record a failed call."""
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
                except Exception as e:
                    logger.debug("Callback failed during state change to %s: %s", self._state, e)

    @property
    def state(self) -> CircuitState:
        """Get the current circuit state."""
        return self._state

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization.

        Note: on_state_change callback is not serialized.
        """
        return {
            "failure_threshold": self.failure_threshold,
            "cooldown_seconds": self.cooldown_seconds,
            "half_open_max_calls": self.half_open_max_calls,
            "state": self._state,
            "failures": self._failures,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CircuitBreaker":
        """Create from dictionary.

        Note: on_state_change callback is not restored.
        """
        breaker = cls(
            failure_threshold=data.get("failure_threshold", DEFAULT_FAILURE_THRESHOLD),
            cooldown_seconds=data.get("cooldown_seconds", DEFAULT_COOLDOWN_SECONDS),
            half_open_max_calls=data.get("half_open_max_calls", DEFAULT_HALF_OPEN_MAX_CALLS),
        )
        state_value = data.get("state", CircuitState.CLOSED)
        breaker._state = CircuitState(state_value) if isinstance(state_value, str) else state_value
        breaker._failures = data.get("failures", 0)
        return breaker


__all__ = [
    "CircuitState",
    "CircuitBreaker",
]
