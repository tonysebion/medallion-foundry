"""Tests for pipelines resilience utilities.

Tests retry decorator, RetryConfig, and CircuitBreaker functionality.
"""

import time
from unittest.mock import patch

import pytest

from pipelines.lib.resilience import (
    with_retry,
    RetryConfig,
    retry_operation,
    CircuitBreaker,
    CircuitBreakerOpen,
)


class TestWithRetryDecorator:
    """Tests for the @with_retry decorator."""

    def test_success_on_first_attempt(self):
        """Should return result immediately on success."""
        call_count = 0

        @with_retry(max_attempts=3)
        def successful_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = successful_function()

        assert result == "success"
        assert call_count == 1

    def test_retry_on_failure(self):
        """Should retry on failure."""
        call_count = 0

        @with_retry(max_attempts=3, backoff_seconds=0.01)
        def failing_then_succeeding():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"

        result = failing_then_succeeding()

        assert result == "success"
        assert call_count == 3

    def test_exhausts_all_attempts(self):
        """Should raise after exhausting all attempts."""
        call_count = 0

        @with_retry(max_attempts=3, backoff_seconds=0.01)
        def always_failing():
            nonlocal call_count
            call_count += 1
            raise ValueError("Persistent failure")

        with pytest.raises(ValueError, match="Persistent failure"):
            always_failing()

        assert call_count == 3

    def test_retry_specific_exceptions(self):
        """Should only retry specified exceptions."""
        call_count = 0

        @with_retry(
            max_attempts=3,
            backoff_seconds=0.01,
            retry_exceptions=(ValueError,),
        )
        def fails_with_type_error():
            nonlocal call_count
            call_count += 1
            raise TypeError("Not retried")

        with pytest.raises(TypeError, match="Not retried"):
            fails_with_type_error()

        # Should fail immediately without retry
        assert call_count == 1

    def test_exponential_backoff(self):
        """Should use exponential backoff between attempts."""
        call_count = 0
        sleep_times = []

        @with_retry(
            max_attempts=3,
            backoff_seconds=1.0,
            exponential=True,
            jitter=False,
        )
        def failing_function():
            nonlocal call_count
            call_count += 1
            raise ValueError("Failure")

        with patch("time.sleep") as mock_sleep:
            mock_sleep.side_effect = lambda x: sleep_times.append(x)
            with pytest.raises(ValueError):
                failing_function()

        # First retry: 1.0 sec, Second retry: 2.0 sec
        assert len(sleep_times) == 2
        assert sleep_times[0] == 1.0
        assert sleep_times[1] == 2.0

    def test_linear_backoff(self):
        """Should use linear backoff when exponential=False."""
        sleep_times = []

        @with_retry(
            max_attempts=3,
            backoff_seconds=1.0,
            exponential=False,
            jitter=False,
        )
        def failing_function():
            raise ValueError("Failure")

        with patch("time.sleep") as mock_sleep:
            mock_sleep.side_effect = lambda x: sleep_times.append(x)
            with pytest.raises(ValueError):
                failing_function()

        # Both should be 1.0 sec
        assert len(sleep_times) == 2
        assert sleep_times[0] == 1.0
        assert sleep_times[1] == 1.0

    def test_preserves_function_metadata(self):
        """Decorator should preserve function name and docstring."""

        @with_retry(max_attempts=3)
        def my_function():
            """This is my function."""
            return 42

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "This is my function."


class TestRetryConfig:
    """Tests for RetryConfig class."""

    def test_default_config(self):
        """Default config should have sensible defaults."""
        config = RetryConfig.default()

        assert config.max_attempts == 3
        assert config.backoff_seconds == 1.0
        assert config.exponential is True
        assert config.jitter is True

    def test_none_config(self):
        """None config should have single attempt."""
        config = RetryConfig.none()

        assert config.max_attempts == 1

    def test_aggressive_config(self):
        """Aggressive config should have more attempts and longer backoff."""
        config = RetryConfig.aggressive()

        assert config.max_attempts == 5
        assert config.backoff_seconds == 5.0

    def test_custom_config(self):
        """Should allow custom configuration."""
        config = RetryConfig(
            max_attempts=10,
            backoff_seconds=0.5,
            exponential=False,
            jitter=False,
        )

        assert config.max_attempts == 10
        assert config.backoff_seconds == 0.5
        assert config.exponential is False
        assert config.jitter is False


class TestRetryOperation:
    """Tests for retry_operation function."""

    def test_success(self):
        """Should return result on success."""
        result = retry_operation(
            lambda: "result",
            RetryConfig.default(),
            "test operation",
        )

        assert result == "result"

    def test_retry_and_succeed(self):
        """Should retry and return result when eventually succeeds."""
        call_count = 0

        def sometimes_fails():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Temporary")
            return "success"

        config = RetryConfig(max_attempts=3, backoff_seconds=0.01)
        result = retry_operation(sometimes_fails, config, "test")

        assert result == "success"
        assert call_count == 2

    def test_exhausts_attempts(self):
        """Should raise after exhausting attempts."""
        config = RetryConfig(max_attempts=2, backoff_seconds=0.01)

        with pytest.raises(ValueError):
            retry_operation(
                lambda: (_ for _ in ()).throw(ValueError("Always fails")),
                config,
                "test",
            )


class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""

    def test_closed_state_passes_through(self):
        """Closed circuit should allow operations."""
        breaker = CircuitBreaker(failure_threshold=3)

        with breaker:
            result = "success"

        assert result == "success"
        assert breaker.state == "CLOSED"

    def test_opens_after_threshold(self):
        """Circuit should open after reaching failure threshold."""
        breaker = CircuitBreaker(failure_threshold=3)

        for _ in range(3):
            try:
                with breaker:
                    raise ValueError("Failure")
            except ValueError:
                pass

        assert breaker.state == "OPEN"

    def test_open_state_fails_fast(self):
        """Open circuit should fail fast without executing operation."""
        breaker = CircuitBreaker(failure_threshold=1)

        # Trigger open state
        try:
            with breaker:
                raise ValueError("Failure")
        except ValueError:
            pass

        assert breaker.state == "OPEN"

        # Should fail fast
        with pytest.raises(CircuitBreakerOpen):
            with breaker:
                # This should not execute
                raise ValueError("Should not reach here")

    def test_half_open_after_recovery_time(self):
        """Circuit should transition to half-open after recovery time."""
        breaker = CircuitBreaker(failure_threshold=1, recovery_time=0.1)

        # Trigger open state
        try:
            with breaker:
                raise ValueError("Failure")
        except ValueError:
            pass

        assert breaker.state == "OPEN"

        # Wait for recovery time
        time.sleep(0.15)

        # Should be half-open now
        with breaker:
            pass  # Success

        assert breaker.state == "CLOSED"

    def test_half_open_returns_to_open_on_failure(self):
        """Half-open circuit should return to open on failure."""
        breaker = CircuitBreaker(failure_threshold=1, recovery_time=0.05)

        # Trigger open state
        try:
            with breaker:
                raise ValueError("Failure")
        except ValueError:
            pass

        # Wait for recovery time
        time.sleep(0.06)

        # Fail during half-open
        try:
            with breaker:
                raise ValueError("Still failing")
        except ValueError:
            pass

        assert breaker.state == "OPEN"

    def test_resets_failure_count_on_success(self):
        """Success should reset the failure count."""
        breaker = CircuitBreaker(failure_threshold=3)

        # Two failures
        for _ in range(2):
            try:
                with breaker:
                    raise ValueError("Failure")
            except ValueError:
                pass

        assert breaker.failure_count == 2

        # Success resets count
        with breaker:
            pass

        assert breaker.failure_count == 0
        assert breaker.state == "CLOSED"
