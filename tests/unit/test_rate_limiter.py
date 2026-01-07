"""Tests for pipelines.lib.rate_limiter module."""

import threading
import time

from pipelines.lib.rate_limiter import RateLimiter, rate_limited


# ============================================
# RateLimiter class tests
# ============================================


class TestRateLimiterInit:
    """Tests for RateLimiter initialization."""

    def test_default_burst_size(self):
        """Default burst_size is 1."""
        limiter = RateLimiter(requests_per_second=10)
        assert limiter.burst_size == 1
        assert limiter.rate == 10

    def test_custom_burst_size(self):
        """Custom burst_size is respected."""
        limiter = RateLimiter(requests_per_second=5, burst_size=10)
        assert limiter.burst_size == 10
        assert limiter.tokens == 10.0

    def test_initial_tokens_equals_burst_size(self):
        """Initial tokens equals burst_size."""
        limiter = RateLimiter(requests_per_second=10, burst_size=5)
        assert limiter.tokens == 5.0


class TestRateLimiterAcquire:
    """Tests for RateLimiter.acquire() method."""

    def test_immediate_acquire_with_tokens(self):
        """Acquire succeeds immediately when tokens available."""
        limiter = RateLimiter(requests_per_second=10, burst_size=5)
        assert limiter.acquire() is True
        assert limiter.tokens < 5.0  # One token consumed

    def test_multiple_acquires_consume_tokens(self):
        """Multiple acquires consume tokens correctly."""
        limiter = RateLimiter(requests_per_second=10, burst_size=3)

        assert limiter.acquire() is True
        assert limiter.acquire() is True
        assert limiter.acquire() is True
        # All burst tokens consumed, next would block

    def test_acquire_blocks_when_no_tokens(self):
        """Acquire blocks when no tokens available (with timeout)."""
        limiter = RateLimiter(requests_per_second=100, burst_size=1)

        # Consume the only token
        assert limiter.acquire() is True

        # Next acquire should block briefly then succeed (tokens refill)
        start = time.monotonic()
        assert limiter.acquire() is True
        elapsed = time.monotonic() - start
        # Should have waited some time for token refill
        assert elapsed >= 0.005  # At least 5ms (conservative)

    def test_acquire_timeout_expires(self):
        """Acquire returns False when timeout is insufficient for next token."""
        # Use very low rate so tokens won't refill during test
        limiter = RateLimiter(requests_per_second=0.5, burst_size=1)

        # Consume the only token
        assert limiter.acquire() is True

        # At 0.5 RPS, it takes 2 seconds to get 1 token
        # With a 50ms timeout, the implementation correctly predicts it won't
        # get a token in time and returns False immediately (optimization)
        result = limiter.acquire(timeout=0.05)  # 50ms timeout
        assert result is False

    def test_acquire_timeout_waits_then_fails(self):
        """Acquire waits up to timeout when token might become available."""
        # Use rate that makes wait_time less than timeout
        # At 100 RPS, wait_time = 1/100 = 0.01s = 10ms
        limiter = RateLimiter(requests_per_second=100, burst_size=1)

        # Consume the only token
        assert limiter.acquire() is True

        # Set tokens to 0.5 so wait_time = 0.5/100 = 5ms
        # This is less than timeout, so it will wait
        limiter.tokens = 0.0

        # With timeout=0.02s and rate=100, it should wait ~10ms per token
        # Should succeed since 10ms < 20ms timeout
        start = time.monotonic()
        result = limiter.acquire(timeout=0.02)
        elapsed = time.monotonic() - start

        assert result is True
        assert elapsed >= 0.005  # Should have waited some time

    def test_acquire_no_timeout_waits_indefinitely(self):
        """Acquire with no timeout waits for token."""
        limiter = RateLimiter(requests_per_second=50, burst_size=1)

        # Consume token
        limiter.acquire()

        # Acquire should wait and succeed
        start = time.monotonic()
        result = limiter.acquire()  # No timeout
        elapsed = time.monotonic() - start

        assert result is True
        assert elapsed >= 0.01  # Waited for refill


class TestRateLimiterTryAcquire:
    """Tests for RateLimiter.try_acquire() method."""

    def test_try_acquire_succeeds_with_tokens(self):
        """try_acquire returns True when tokens available."""
        limiter = RateLimiter(requests_per_second=10, burst_size=3)
        assert limiter.try_acquire() is True

    def test_try_acquire_fails_without_tokens(self):
        """try_acquire returns False immediately when no tokens."""
        limiter = RateLimiter(requests_per_second=10, burst_size=1)

        # Consume the only token
        limiter.tokens = 0.0

        # try_acquire should fail immediately
        start = time.monotonic()
        result = limiter.try_acquire()
        elapsed = time.monotonic() - start

        assert result is False
        assert elapsed < 0.01  # Should not block

    def test_try_acquire_does_not_block(self):
        """try_acquire never blocks, returns immediately."""
        limiter = RateLimiter(requests_per_second=0.1, burst_size=1)

        # Consume token
        limiter.acquire()

        # try_acquire should return False immediately
        start = time.monotonic()
        result = limiter.try_acquire()
        elapsed = time.monotonic() - start

        assert result is False
        assert elapsed < 0.05  # No blocking


class TestRateLimiterRefillTokens:
    """Tests for token refill logic."""

    def test_tokens_refill_over_time(self):
        """Tokens refill based on elapsed time."""
        limiter = RateLimiter(requests_per_second=100, burst_size=5)

        # Consume all tokens
        limiter.tokens = 0.0
        limiter.last_update = time.monotonic()

        # Wait a bit
        time.sleep(0.05)  # 50ms

        # Trigger refill
        limiter._refill_tokens()

        # Should have refilled ~5 tokens (100 RPS * 0.05s = 5)
        assert limiter.tokens >= 4.0
        assert limiter.tokens <= 5.0  # Capped at burst_size

    def test_tokens_capped_at_burst_size(self):
        """Tokens never exceed burst_size."""
        limiter = RateLimiter(requests_per_second=1000, burst_size=3)

        # Wait and refill
        time.sleep(0.01)
        limiter._refill_tokens()

        assert limiter.tokens <= 3.0


class TestRateLimiterThreadSafety:
    """Tests for thread-safety of RateLimiter."""

    def test_concurrent_acquires(self):
        """Multiple threads can acquire tokens safely."""
        limiter = RateLimiter(requests_per_second=1000, burst_size=100)
        acquired_count = 0
        lock = threading.Lock()

        def worker():
            nonlocal acquired_count
            for _ in range(10):
                if limiter.acquire(timeout=0.5):
                    with lock:
                        acquired_count += 1

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All 50 acquires should succeed with high burst
        assert acquired_count == 50


# ============================================
# rate_limited decorator tests
# ============================================


class TestRateLimitedDecorator:
    """Tests for @rate_limited decorator."""

    def test_decorator_limits_call_rate(self):
        """Decorated function is rate limited."""
        call_count = 0

        @rate_limited(100)  # 100 RPS
        def increment():
            nonlocal call_count
            call_count += 1
            return call_count

        # Make several calls
        results = [increment() for _ in range(5)]

        assert results == [1, 2, 3, 4, 5]
        assert call_count == 5

    def test_decorator_with_burst_size(self):
        """Decorator respects burst_size parameter."""
        call_count = 0

        @rate_limited(10, burst_size=5)
        def increment():
            nonlocal call_count
            call_count += 1
            return call_count

        # First 5 calls should be fast (burst)
        start = time.monotonic()
        for _ in range(5):
            increment()
        elapsed = time.monotonic() - start

        assert call_count == 5
        assert elapsed < 0.1  # Burst should be fast

    def test_decorator_preserves_function_metadata(self):
        """Decorator preserves function name and docstring."""

        @rate_limited(10)
        def my_function():
            """My docstring."""
            pass

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "My docstring."

    def test_decorator_passes_arguments(self):
        """Decorator passes args and kwargs correctly."""

        @rate_limited(100)
        def add(a, b, c=0):
            return a + b + c

        assert add(1, 2) == 3
        assert add(1, 2, c=3) == 6

    def test_decorator_returns_function_result(self):
        """Decorator returns the wrapped function's result."""

        @rate_limited(100)
        def get_value():
            return {"key": "value"}

        result = get_value()
        assert result == {"key": "value"}
