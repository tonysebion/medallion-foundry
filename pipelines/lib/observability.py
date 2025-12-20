"""Observability utilities for pipelines.

Combines metrics collection with structured logging helpers so pipeline
runs can capture both operational metrics and JSON-friendly logs from the
same module.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "MetricPoint",
    "PhaseTimer",
    "PipelineMetrics",
    "MetricsCollector",
    "get_metrics_collector",
    "JSONFormatter",
    "PipelineLogger",
    "get_pipeline_logger",
    "setup_logging",
]


@dataclass
class MetricPoint:
    """A single metric data point."""

    name: str
    value: Any
    timestamp: datetime
    unit: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a serializable dictionary."""
        result = {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
        }
        if self.unit:
            result["unit"] = self.unit
        if self.tags:
            result["tags"] = self.tags
        return result


@dataclass
class PhaseTimer:
    """Timer tracking a named pipeline phase."""

    name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    def stop(self) -> float:
        """Stop the timer and return the duration (seconds)."""
        self.end_time = time.time()
        return self.duration

    @property
    def duration(self) -> float:
        """Duration in seconds."""
        end = self.end_time or time.time()
        return end - self.start_time

    @property
    def running(self) -> bool:
        """Whether the timer is still running."""
        return self.end_time is None


class PipelineMetrics:
    """Metrics for a single pipeline run.

    Collects phase timings, row counts, and other numeric metrics so pipeline
    executions can emit both runtime diagnostics and structured metrics.
    """

    def __init__(
        self,
        system: str,
        entity: str,
        run_date: str,
        layer: Optional[str] = None,
    ):
        self.system = system
        self.entity = entity
        self.run_date = run_date
        self.layer = layer

        self._start_time = time.time()
        self._end_time: Optional[float] = None
        self._phases: List[PhaseTimer] = []
        self._current_phase: Optional[PhaseTimer] = None
        self._metrics: List[MetricPoint] = []

    @contextmanager
    def time_phase(self, name: str) -> Generator[PhaseTimer, None, None]:
        """Context manager that tracks a phase duration."""
        timer = PhaseTimer(name=name)
        self._phases.append(timer)
        self._current_phase = timer
        try:
            yield timer
        finally:
            timer.stop()
            self._current_phase = None

    def record(
        self,
        name: str,
        value: Any,
        unit: Optional[str] = None,
        **tags: str,
    ) -> None:
        """Record a metric value with optional tags."""
        all_tags = {
            "system": self.system,
            "entity": self.entity,
            "run_date": self.run_date,
        }
        if self.layer:
            all_tags["layer"] = self.layer
        all_tags.update(tags)

        point = MetricPoint(
            name=name,
            value=value,
            timestamp=datetime.now(timezone.utc),
            unit=unit,
            tags=all_tags,
        )
        self._metrics.append(point)

    def finish(self) -> None:
        """Mark the run as complete."""
        self._end_time = time.time()

    @property
    def total_duration(self) -> float:
        """Total duration in seconds."""
        end = self._end_time or time.time()
        return end - self._start_time

    def summary(self) -> Dict[str, Any]:
        """Return a summary dictionary of the tracked metrics."""
        self.finish()

        return {
            "pipeline": {
                "system": self.system,
                "entity": self.entity,
                "run_date": self.run_date,
                "layer": self.layer,
            },
            "timing": {
                "total_seconds": round(self.total_duration, 3),
                "phases": {
                    p.name: round(p.duration, 3)
                    for p in self._phases
                },
            },
            "metrics": [m.to_dict() for m in self._metrics],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def to_log_dict(self) -> Dict[str, Any]:
        """Flatten metrics for structured logging."""
        result: Dict[str, Any] = {
            "pipeline_system": self.system,
            "pipeline_entity": self.entity,
            "pipeline_run_date": self.run_date,
            "total_duration_seconds": round(self.total_duration, 3),
        }
        if self.layer:
            result["pipeline_layer"] = self.layer

        for phase in self._phases:
            result[f"phase_{phase.name}_seconds"] = round(phase.duration, 3)

        for metric in self._metrics:
            key = f"metric_{metric.name}"
            if metric.unit:
                key = f"{key}_{metric.unit}"
            result[key] = metric.value

        return result


class MetricsCollector:
    """Aggregates PipelineMetrics across multiple runs."""

    def __init__(self):
        self._runs: List[PipelineMetrics] = []

    def create_metrics(
        self,
        system: str,
        entity: str,
        run_date: str,
        layer: Optional[str] = None,
    ) -> PipelineMetrics:
        """Create and track a new PipelineMetrics."""
        metrics = PipelineMetrics(
            system=system,
            entity=entity,
            run_date=run_date,
            layer=layer,
        )
        self._runs.append(metrics)
        return metrics

    def summary(self) -> Dict[str, Any]:
        """Summarize all collected runs."""
        total_duration = sum(m.total_duration for m in self._runs)
        successful = sum(1 for m in self._runs if m._end_time is not None)

        return {
            "total_runs": len(self._runs),
            "successful_runs": successful,
            "total_duration_seconds": round(total_duration, 3),
            "runs": [m.summary() for m in self._runs],
        }

    def clear(self) -> None:
        """Reset the stored runs."""
        self._runs.clear()


_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Return the singleton metrics collector."""
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
    return _collector


class JSONFormatter(logging.Formatter):
    """Formatter that renders log records as JSON."""

    def __init__(
        self,
        include_fields: Optional[List[str]] = None,
        exclude_fields: Optional[List[str]] = None,
    ):
        super().__init__()
        self.include_fields = include_fields or []
        self.exclude_fields = exclude_fields or []

    def format(self, record: logging.LogRecord) -> str:
        """Format a record as JSON."""
        payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.pathname:
            payload["source"] = {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName,
            }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        for field_name in self.include_fields:
            if hasattr(record, field_name):
                payload[field_name] = getattr(record, field_name)

        extra_attrs = {
            k: v
            for k, v in record.__dict__.items()
            if k not in logging.LogRecord("", 0, "", 0, "", (), None).__dict__
            and k not in ("message", "asctime")
            and k not in self.exclude_fields
        }
        if extra_attrs:
            payload["extra"] = extra_attrs

        return json.dumps(payload, default=str)


class PipelineLogger:
    """Logger with automatic pipeline context."""

    def __init__(self, name: str):
        self._logger = logging.getLogger(name)
        self._context: Dict[str, Any] = {}

    def set_context(self, **kwargs: Any) -> None:
        """Add contextual information to logs."""
        self._context.update(kwargs)

    def clear_context(self) -> None:
        """Remove pipeline context."""
        self._context.clear()

    def _log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        extra = kwargs.pop("extra", {})
        extra.update(self._context)
        self._logger.log(level, msg, *args, extra=extra, **kwargs)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        kwargs["exc_info"] = True
        self._log(logging.ERROR, msg, *args, **kwargs)

    def metric(
        self,
        name: str,
        value: Any,
        unit: Optional[str] = None,
        **tags: Any,
    ) -> None:
        """Log a metric with context."""
        extra = {
            "metric_name": name,
            "metric_value": value,
        }
        if unit:
            extra["metric_unit"] = unit
        extra.update(self._context)
        extra.update(tags)
        self._logger.info(f"METRIC {name}={value}", extra=extra)


def get_pipeline_logger(name: str) -> PipelineLogger:
    """Return a PipelineLogger for the given module name."""
    return PipelineLogger(name)


def setup_logging(
    verbose: bool = False,
    json_format: bool = False,
    log_file: Optional[str] = None,
) -> None:
    """Configure the root logger with optional JSON formatting."""
    level = logging.DEBUG if verbose else logging.INFO

    formatter = JSONFormatter() if json_format else logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
