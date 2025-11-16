from __future__ import annotations

from contextlib import contextmanager
import os
from typing import Any, Iterator

try:  # optional dependency
    from opentelemetry import trace as otel_trace  # type: ignore

    _OTEL_AVAILABLE = True
except Exception:  # pragma: no cover - optional
    otel_trace = None  # type: ignore
    _OTEL_AVAILABLE = False


def _enabled() -> bool:
    val = os.environ.get("BRONZE_TRACING", "0").lower()
    return val in ("1", "true", "yes", "on")


@contextmanager
def trace_span(name: str, **kwargs: Any) -> Iterator[None]:
    """Tracing span using OpenTelemetry if available and enabled.

    Falls back to a no-op span when OpenTelemetry is not installed or
    BRONZE_TRACING is not enabled. Attributes in kwargs are ignored to
    avoid importing SDK types; users can attach attributes via context
    managers externally if needed.
    """
    if (
        _OTEL_AVAILABLE and _enabled() and otel_trace is not None
    ):  # pragma: no cover - optional path
        tracer = otel_trace.get_tracer("bronze-foundry")
        with tracer.start_as_current_span(name):
            yield
        return
    # No-op by default
    yield
