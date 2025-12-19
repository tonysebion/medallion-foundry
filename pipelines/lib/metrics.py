"""Pipeline metrics collection.

Provides timing, counting, and metrics collection for pipeline execution.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

__all__ = [
    "PipelineMetrics",
    "PhaseTimer",
    "MetricPoint",
    "MetricsCollector",
    "get_metrics_collector",
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
        """Convert to dictionary for JSON serialization."""
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
    """Timer for tracking duration of pipeline phases."""

    name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    def stop(self) -> float:
        """Stop the timer and return duration in seconds."""
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

    Collects timing, row counts, and other metrics for a pipeline execution.

    Example:
        metrics = PipelineMetrics(system="claims", entity="header", run_date="2025-01-15")

        with metrics.time_phase("extract"):
            data = extract_data()
        metrics.record("rows_extracted", len(data))

        with metrics.time_phase("transform"):
            transformed = transform_data(data)
        metrics.record("rows_transformed", len(transformed))

        summary = metrics.summary()
    """

    def __init__(
        self,
        system: str,
        entity: str,
        run_date: str,
        layer: Optional[str] = None,
    ):
        """Initialize metrics for a pipeline run.

        Args:
            system: Source system name
            entity: Entity name
            run_date: Pipeline run date
            layer: Optional layer (bronze, silver)
        """
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
        """Context manager to time a pipeline phase.

        Args:
            name: Phase name (e.g., "connect", "extract", "write", "checksum")

        Yields:
            PhaseTimer instance
        """
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
        """Record a metric value.

        Args:
            name: Metric name (e.g., "rows_extracted", "file_size_bytes")
            value: Metric value
            unit: Optional unit
            **tags: Additional tags
        """
        # Add standard tags
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
        """Mark the pipeline run as finished."""
        self._end_time = time.time()

    @property
    def total_duration(self) -> float:
        """Total duration of the pipeline run in seconds."""
        end = self._end_time or time.time()
        return end - self._start_time

    def summary(self) -> Dict[str, Any]:
        """Generate a metrics summary.

        Returns:
            Dictionary with all collected metrics
        """
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
        """Generate a flat dictionary suitable for structured logging.

        Returns:
            Flat dictionary with all metrics
        """
        result: Dict[str, Any] = {
            "pipeline_system": self.system,
            "pipeline_entity": self.entity,
            "pipeline_run_date": self.run_date,
            "total_duration_seconds": round(self.total_duration, 3),
        }
        if self.layer:
            result["pipeline_layer"] = self.layer

        # Add phase timings
        for phase in self._phases:
            result[f"phase_{phase.name}_seconds"] = round(phase.duration, 3)

        # Add recorded metrics
        for metric in self._metrics:
            key = f"metric_{metric.name}"
            if metric.unit:
                key = f"{key}_{metric.unit}"
            result[key] = metric.value

        return result


class MetricsCollector:
    """Global metrics collector for aggregating pipeline metrics.

    Use for collecting metrics across multiple pipeline runs.

    Example:
        collector = get_metrics_collector()

        # Run pipelines
        for pipeline in pipelines:
            metrics = collector.create_metrics(pipeline.system, pipeline.entity, run_date)
            result = pipeline.run(run_date, metrics=metrics)

        # Get aggregated summary
        print(collector.summary())
    """

    def __init__(self):
        """Initialize the metrics collector."""
        self._runs: List[PipelineMetrics] = []

    def create_metrics(
        self,
        system: str,
        entity: str,
        run_date: str,
        layer: Optional[str] = None,
    ) -> PipelineMetrics:
        """Create a new PipelineMetrics instance and track it.

        Args:
            system: Source system name
            entity: Entity name
            run_date: Pipeline run date
            layer: Optional layer (bronze, silver)

        Returns:
            New PipelineMetrics instance
        """
        metrics = PipelineMetrics(
            system=system,
            entity=entity,
            run_date=run_date,
            layer=layer,
        )
        self._runs.append(metrics)
        return metrics

    def summary(self) -> Dict[str, Any]:
        """Generate summary of all collected metrics.

        Returns:
            Dictionary with aggregated metrics
        """
        total_duration = sum(m.total_duration for m in self._runs)
        successful = sum(1 for m in self._runs if m._end_time is not None)

        return {
            "total_runs": len(self._runs),
            "successful_runs": successful,
            "total_duration_seconds": round(total_duration, 3),
            "runs": [m.summary() for m in self._runs],
        }

    def clear(self) -> None:
        """Clear all collected metrics."""
        self._runs.clear()


# Global metrics collector instance
_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance.

    Returns:
        MetricsCollector singleton
    """
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
    return _collector
