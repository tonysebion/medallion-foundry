"""Tests for retry policies and circuit breakers.

These tests verify the resilience patterns used across the framework:
- RetryPolicy: Exponential backoff with jitter
- CircuitBreaker: Failure detection and recovery
"""

from __future__ import annotations

import pytest

from core.infrastructure.resilience.retry import (
    CircuitBreaker,
    CircuitState,
    RetryPolicy,
)


class TestRetryPolicy:
    """Tests for RetryPolicy dataclass."""

    def test_default_policy(self) -> None:
        """Default policy should have sensible defaults."""
        policy = RetryPolicy()
        assert policy.max_attempts == 5
        assert policy.base_delay == 0.5
        assert policy.max_delay == 8.0
        assert policy.backoff_multiplier == 2.0
        assert policy.jitter == 0.2

    def test_custom_policy(self) -> None:
        """Custom policy should respect provided values."""
        policy = RetryPolicy(
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            backoff_multiplier=3.0,
            jitter=0.1,
        )
        assert policy.max_attempts == 3
        assert policy.base_delay == 1.0
        assert policy.max_delay == 10.0
        assert policy.backoff_multiplier == 3.0
        assert policy.jitter == 0.1

    def test_compute_delay_first_attempt(self) -> None:
        """First attempt delay should be base_delay (with jitter)."""
        policy = RetryPolicy(base_delay=1.0, jitter=0.0)
        delay = policy.compute_delay(1)
        assert delay == 1.0

    def test_compute_delay_exponential_backoff(self) -> None:
        """Delay should increase exponentially with attempts."""
        policy = RetryPolicy(base_delay=1.0, backoff_multiplier=2.0, jitter=0.0)
        assert policy.compute_delay(1) == 1.0
        assert policy.compute_delay(2) == 2.0
        assert policy.compute_delay(3) == 4.0
        assert policy.compute_delay(4) == 8.0

    def test_compute_delay_respects_max_delay(self) -> None:
        """Delay should not exceed max_delay."""
        policy = RetryPolicy(base_delay=1.0, max_delay=5.0, backoff_multiplier=10.0, jitter=0.0)
        delay = policy.compute_delay(3)  # Would be 100 without cap
        assert delay == 5.0

    def test_compute_delay_with_jitter(self) -> None:
        """Delay should vary with jitter."""
        policy = RetryPolicy(base_delay=1.0, jitter=0.2)
        delays = [policy.compute_delay(1) for _ in range(10)]
        # With 20% jitter, delays should be between 0.8 and 1.2
        assert all(0.8 <= d <= 1.2 for d in delays)
        # And not all the same (statistically unlikely)
        assert len(set(delays)) > 1

    def test_to_dict(self) -> None:
        """to_dict should serialize all fields."""
        policy = RetryPolicy(max_attempts=3, base_delay=2.0)
        data = policy.to_dict()
        assert data["max_attempts"] == 3
        assert data["base_delay"] == 2.0
        assert "retry_on_exceptions" in data

    def test_from_dict(self) -> None:
        """from_dict should restore policy from dictionary."""
        data = {
            "max_attempts": 3,
            "base_delay": 2.0,
            "max_delay": 16.0,
            "backoff_multiplier": 3.0,
            "jitter": 0.1,
        }
        policy = RetryPolicy.from_dict(data)
        assert policy.max_attempts == 3
        assert policy.base_delay == 2.0
        assert policy.max_delay == 16.0
        assert policy.backoff_multiplier == 3.0
        assert policy.jitter == 0.1

    def test_to_dict_from_dict_roundtrip(self) -> None:
        """to_dict -> from_dict should preserve policy settings."""
        original = RetryPolicy(
            max_attempts=7,
            base_delay=0.25,
            max_delay=32.0,
            backoff_multiplier=1.5,
            jitter=0.3,
        )
        restored = RetryPolicy.from_dict(original.to_dict())
        assert restored.max_attempts == original.max_attempts
        assert restored.base_delay == original.base_delay
        assert restored.max_delay == original.max_delay
        assert restored.backoff_multiplier == original.backoff_multiplier
        assert restored.jitter == original.jitter


class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""

    def test_initial_state_closed(self) -> None:
        """Circuit breaker should start in CLOSED state."""
        breaker = CircuitBreaker()
        assert breaker.state == CircuitState.CLOSED

    def test_default_settings(self) -> None:
        """Default settings should be sensible."""
        breaker = CircuitBreaker()
        assert breaker.failure_threshold == 5
        assert breaker.cooldown_seconds == 30.0
        assert breaker.half_open_max_calls == 1

    def test_custom_settings(self) -> None:
        """Custom settings should be respected."""
        breaker = CircuitBreaker(
            failure_threshold=3,
            cooldown_seconds=60.0,
            half_open_max_calls=2,
        )
        assert breaker.failure_threshold == 3
        assert breaker.cooldown_seconds == 60.0
        assert breaker.half_open_max_calls == 2

    def test_record_failure_increments_count(self) -> None:
        """Recording failures should increment failure count."""
        breaker = CircuitBreaker(failure_threshold=5)
        assert breaker._failures == 0
        breaker.record_failure()
        assert breaker._failures == 1
        breaker.record_failure()
        assert breaker._failures == 2

    def test_opens_after_threshold(self) -> None:
        """Circuit should open after reaching failure threshold."""
        breaker = CircuitBreaker(failure_threshold=2)
        assert breaker.state == CircuitState.CLOSED
        breaker.record_failure()
        assert breaker.state == CircuitState.CLOSED
        breaker.record_failure()
        assert breaker.state == CircuitState.OPEN

    def test_record_success_resets_failures(self) -> None:
        """Recording success should reset failure count."""
        breaker = CircuitBreaker(failure_threshold=5)
        breaker.record_failure()
        breaker.record_failure()
        assert breaker._failures == 2
        breaker.record_success()
        assert breaker._failures == 0

    def test_to_dict(self) -> None:
        """to_dict should serialize breaker state."""
        breaker = CircuitBreaker(failure_threshold=3)
        breaker.record_failure()
        data = breaker.to_dict()
        assert data["failure_threshold"] == 3
        assert data["state"] == CircuitState.CLOSED
        assert data["failures"] == 1

    def test_from_dict(self) -> None:
        """from_dict should restore breaker state."""
        data = {
            "failure_threshold": 3,
            "cooldown_seconds": 60.0,
            "half_open_max_calls": 2,
            "state": CircuitState.OPEN,
            "failures": 3,
        }
        breaker = CircuitBreaker.from_dict(data)
        assert breaker.failure_threshold == 3
        assert breaker.cooldown_seconds == 60.0
        assert breaker.half_open_max_calls == 2
        assert breaker.state == CircuitState.OPEN
        assert breaker._failures == 3

    def test_to_dict_from_dict_roundtrip(self) -> None:
        """to_dict -> from_dict should preserve breaker state."""
        original = CircuitBreaker(failure_threshold=4, cooldown_seconds=45.0)
        original.record_failure()
        original.record_failure()

        restored = CircuitBreaker.from_dict(original.to_dict())
        assert restored.failure_threshold == original.failure_threshold
        assert restored.cooldown_seconds == original.cooldown_seconds
        assert restored.state == original.state
        assert restored._failures == original._failures
