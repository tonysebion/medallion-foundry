"""Simple token-bucket rate limiter for sync and async paths.

Configuration:
- requests_per_second (float): tokens per second
- burst (int): optional bucket capacity (defaults to ceil(rps))

Both blocking acquire() and async async_acquire() are provided.
"""
from __future__ import annotations

import asyncio
import math
import time
from typing import Optional


class RateLimiter:
    def __init__(self, requests_per_second: float, burst: Optional[int] = None) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be > 0")
        self.rate = float(requests_per_second)
        self.capacity = burst if burst is not None else max(1, math.ceil(self.rate))
        self.tokens = float(self.capacity)
        self.last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        added = elapsed * self.rate
        if added > 0:
            self.tokens = min(self.capacity, self.tokens + added)
            self.last_refill = now

    def acquire(self) -> None:
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            # sleep until a token is likely available
            deficit = 1.0 - self.tokens
            time.sleep(max(0.0, deficit / self.rate))

    async def async_acquire(self) -> None:
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            deficit = 1.0 - self.tokens
            await asyncio.sleep(max(0.0, deficit / self.rate))

    @staticmethod
    def from_config(api_cfg: dict, run_cfg: dict) -> Optional["RateLimiter"]:
        # Priority: api.rate_limit.rps -> run.rate_limit_rps -> env BRONZE_API_RPS
        rps = None
        rl_cfg = api_cfg.get("rate_limit", {}) if isinstance(api_cfg, dict) else {}
        if isinstance(rl_cfg, dict) and "rps" in rl_cfg:
            rps = rl_cfg.get("rps")
        if rps is None:
            rps = run_cfg.get("rate_limit_rps") if isinstance(run_cfg, dict) else None
        if rps is None:
            import os
            env_val = os.environ.get("BRONZE_API_RPS")
            if env_val:
                try:
                    rps = float(env_val)
                except ValueError:
                    rps = None
        if not rps:
            return None
        try:
            return RateLimiter(float(rps))
        except Exception:
            return None
