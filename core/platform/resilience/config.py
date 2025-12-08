"""Helpers for parsing resilience-related configuration from intent configs."""

from __future__ import annotations

import os
from typing import Any, Mapping, MutableMapping, Optional, Tuple

from core.platform.resilience.constants import (
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_BASE_DELAY,
    DEFAULT_JITTER,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_DELAY,
)


RetryConfig = Mapping[str, Any]


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_retry_config(config: Optional[RetryConfig]) -> MutableMapping[str, Any]:
    """Normalize retry configuration and fill in defaults."""
    retry_kwargs: MutableMapping[str, Any] = {
        "max_attempts": DEFAULT_MAX_ATTEMPTS,
        "base_delay": float(DEFAULT_BASE_DELAY),
        "max_delay": float(DEFAULT_MAX_DELAY),
        "backoff_multiplier": float(DEFAULT_BACKOFF_MULTIPLIER),
        "jitter": float(DEFAULT_JITTER),
    }

    if not config:
        return retry_kwargs

    if (max_attempts := _coerce_int(config.get("max_attempts"))) is not None:
        retry_kwargs["max_attempts"] = max_attempts

    if (base_delay := _coerce_float(config.get("base_delay"))) is not None:
        retry_kwargs["base_delay"] = base_delay

    if (max_delay := _coerce_float(config.get("max_delay"))) is not None:
        retry_kwargs["max_delay"] = max_delay

    if (multiplier := _coerce_float(config.get("backoff_multiplier"))) is not None:
        retry_kwargs["backoff_multiplier"] = multiplier

    if (jitter := _coerce_float(config.get("jitter"))) is not None:
        retry_kwargs["jitter"] = jitter

    return retry_kwargs


def resolve_rate_limit_config(
    extractor_cfg: Optional[Mapping[str, Any]],
    run_cfg: Optional[Mapping[str, Any]],
    *,
    env_var: Optional[str] = "BRONZE_API_RPS",
) -> Optional[Tuple[float, Optional[int]]]:
    """Determine the requests-per-second and burst from config/env."""
    rps_value: Optional[float] = None
    burst_value: Optional[int] = None

    if isinstance(extractor_cfg, Mapping):
        rl_cfg = extractor_cfg.get("rate_limit")
        if isinstance(rl_cfg, Mapping):
            if (rps := rl_cfg.get("rps")) is not None:
                rps_value = _coerce_float(rps)
            if (burst := rl_cfg.get("burst")) is not None:
                burst_value = _coerce_int(burst)

    if rps_value is None and isinstance(run_cfg, Mapping):
        if (run_rps := run_cfg.get("rate_limit_rps")) is not None:
            rps_value = _coerce_float(run_rps)

    if rps_value is None and env_var:
        if (env_val := os.environ.get(env_var)) is not None:
            rps_value = _coerce_float(env_val)

    if not rps_value or rps_value <= 0:
        return None

    return rps_value, burst_value


__all__ = [
    "parse_retry_config",
    "resolve_rate_limit_config",
]
