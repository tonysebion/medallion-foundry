"""Tests for retry policies and circuit breakers.

These tests verify the resilience patterns used across the framework:
- RetryPolicy: Exponential backoff with jitter
- CircuitBreaker: Failure detection and recovery
- Unified error mapper: Backend-specific exception wrapping
"""

from __future__ import annotations

import pytest

from core.resilience import (
    CircuitBreaker,
    CircuitState,
    RetryPolicy,
    exception_to_domain_error,
    list_error_mappers,
    register_error_mapper,
)
from core.foundation.primitives.exceptions import (
    AuthenticationError,
    ExtractionError,
    RetryExhaustedError,
    StorageError,
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


class TestUnifiedErrorMapper:
    """Tests for the unified exception_to_domain_error function."""

    def test_list_error_mappers_includes_builtin(self) -> None:
        """list_error_mappers should include built-in backends."""
        mappers = list_error_mappers()
        assert "s3" in mappers
        assert "azure" in mappers
        assert "api" in mappers
        assert "local" in mappers

    def test_passthrough_storage_error(self) -> None:
        """StorageError should pass through unchanged."""
        original = StorageError("test error", backend_type="s3", operation="upload")
        result = exception_to_domain_error(original, "s3", "upload")
        assert result is original

    def test_passthrough_extraction_error(self) -> None:
        """ExtractionError should pass through unchanged."""
        original = ExtractionError("test error", extractor_type="api")
        result = exception_to_domain_error(original, "api", "fetch")
        assert result is original

    def test_passthrough_authentication_error(self) -> None:
        """AuthenticationError should pass through unchanged."""
        original = AuthenticationError("auth failed", auth_type="api")
        result = exception_to_domain_error(original, "api", "fetch")
        assert result is original

    def test_passthrough_retry_exhausted_error(self) -> None:
        """RetryExhaustedError should pass through unchanged."""
        original = RetryExhaustedError("retries exhausted", attempts=5, operation="upload")
        result = exception_to_domain_error(original, "s3", "upload")
        assert result is original

    def test_s3_mapper_wraps_generic_exception(self) -> None:
        """S3 mapper should wrap generic exceptions as StorageError."""
        exc = ValueError("some boto error")
        result = exception_to_domain_error(exc, "s3", "upload", "my-bucket")
        assert isinstance(result, StorageError)
        assert result.details.get("backend_type") == "s3"
        assert result.details.get("operation") == "upload"
        assert result.original_error is exc

    def test_azure_mapper_wraps_generic_exception(self) -> None:
        """Azure mapper should wrap generic exceptions as StorageError."""
        exc = RuntimeError("azure connection failed")
        result = exception_to_domain_error(exc, "azure", "download", "my-container")
        assert isinstance(result, StorageError)
        assert result.details.get("backend_type") == "azure"
        assert result.details.get("operation") == "download"
        assert result.original_error is exc

    def test_api_mapper_wraps_generic_exception(self) -> None:
        """API mapper should wrap generic exceptions as ExtractionError."""
        exc = ConnectionError("network error")
        result = exception_to_domain_error(exc, "api", "fetch")
        assert isinstance(result, ExtractionError)
        assert result.details.get("extractor_type") == "api"
        assert result.original_error is exc

    def test_local_mapper_wraps_generic_exception(self) -> None:
        """Local mapper should wrap generic exceptions as StorageError."""
        exc = FileNotFoundError("file not found")
        result = exception_to_domain_error(exc, "local", "read", "/path/to/file")
        assert isinstance(result, StorageError)
        assert result.details.get("backend_type") == "local"
        assert result.details.get("operation") == "read"
        assert result.original_error is exc

    def test_unknown_backend_uses_default_mapper(self) -> None:
        """Unknown backend type should use default mapper."""
        exc = ValueError("unknown error")
        result = exception_to_domain_error(exc, "unknown_backend", "operation")
        assert isinstance(result, ExtractionError)
        assert result.original_error is exc

    def test_case_insensitive_backend_type(self) -> None:
        """Backend type matching should be case-insensitive."""
        exc = ValueError("test")
        result_lower = exception_to_domain_error(exc, "s3", "upload")
        result_upper = exception_to_domain_error(exc, "S3", "upload")
        result_mixed = exception_to_domain_error(exc, "Azure", "upload")

        assert isinstance(result_lower, StorageError)
        assert isinstance(result_upper, StorageError)
        assert isinstance(result_mixed, StorageError)

    def test_register_custom_error_mapper(self) -> None:
        """Custom error mappers can be registered."""
        @register_error_mapper("custom_backend")
        def custom_mapper(exc, operation, context=None):
            return StorageError(
                f"Custom error: {exc}",
                backend_type="custom",
                operation=operation,
            )

        assert "custom_backend" in list_error_mappers()

        exc = RuntimeError("custom error")
        result = exception_to_domain_error(exc, "custom_backend", "custom_op")
        assert isinstance(result, StorageError)
        assert result.details.get("backend_type") == "custom"
        assert "Custom error" in str(result)
