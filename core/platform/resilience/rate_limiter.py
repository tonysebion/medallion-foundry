"""Token-bucket rate limiter for sync and async paths.

This module provides:
- RateLimiter: Token-bucket implementation for rate limiting
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Any, Dict, Optional

from core.platform.resilience.config import resolve_rate_limit_config

logger = logging.getLogger(__name__)


class RateLimiter:
    """Token-bucket rate limiter for sync and async paths.

    Configuration:
    - requests_per_second (float): tokens per second
    - burst (int): optional bucket capacity (defaults to ceil(rps))
    - component (str): optional label used when emitting rate-limit metrics
    """

    def __init__(
        self,
        requests_per_second: float,
        burst: Optional[int] = None,
        component: Optional[str] = None,
        emit_metrics: bool = True,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be > 0")
        self.rate = float(requests_per_second)
        self.capacity = burst if burst is not None else max(1, math.ceil(self.rate))
        self.tokens = float(self.capacity)
        self.last_refill = time.monotonic()
        self.component = component
        self._emit_metrics = bool(component) and emit_metrics

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        added = elapsed * self.rate
        if added > 0:
            self.tokens = min(self.capacity, self.tokens + added)
            self.last_refill = now
            self._emit_metric("refill")

    def acquire(self) -> None:
        """Acquire a token, blocking if necessary."""
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                self._emit_metric("acquire")
                return
            # sleep until a token is likely available
            deficit = 1.0 - self.tokens
            time.sleep(max(0.0, deficit / self.rate))

    async def async_acquire(self) -> None:
        """Acquire a token asynchronously, blocking if necessary."""
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                self._emit_metric("acquire")
                return
            deficit = 1.0 - self.tokens
            await asyncio.sleep(max(0.0, deficit / self.rate))

    def _emit_metric(self, phase: str) -> None:
        """Emit a rate limit metric."""
        if not self._emit_metrics or not self.component:
            return
        try:
            logger.info(
                "metric=rate_limit component=%s phase=%s tokens=%.2f capacity=%d rate=%.2f",
                self.component,
                phase,
                self.tokens,
                self.capacity,
                self.rate,
            )
        except Exception:
            logger.debug(
                "Failed to emit rate limit metric for %s phase=%s",
                self.component,
                phase,
            )

    @staticmethod
    def from_config(
        extractor_cfg: Optional[Dict[str, Any]],
        run_cfg: Optional[Dict[str, Any]],
        *,
        component: Optional[str] = None,
        env_var: Optional[str] = "BRONZE_API_RPS",
    ) -> Optional["RateLimiter"]:
        """Create RateLimiter from per-extractor or run configuration."""

        resolved = resolve_rate_limit_config(extractor_cfg, run_cfg, env_var=env_var)
        if resolved is None:
            return None

        rps, burst = resolved

        try:
            return RateLimiter(
                rps, burst=burst, component=component, emit_metrics=bool(component)
            )
        except Exception:
            return None


__all__ = [
    "RateLimiter",
]
