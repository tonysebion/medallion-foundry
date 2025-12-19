"""Tests for pipelines metrics collection."""

import time
from datetime import datetime, timezone

import pytest

from pipelines.lib.metrics import (
    MetricPoint,
    MetricsCollector,
    PhaseTimer,
    PipelineMetrics,
    get_metrics_collector,
)


class TestMetricPoint:
    """Tests for MetricPoint dataclass."""

    def test_to_dict_basic(self):
        """Should convert to dictionary."""
        point = MetricPoint(
            name="rows_extracted",
            value=1000,
            timestamp=datetime(2025, 1, 15, 10, 30, 0),
        )

        result = point.to_dict()

        assert result["name"] == "rows_extracted"
        assert result["value"] == 1000
        assert "2025-01-15" in result["timestamp"]

    def test_to_dict_with_unit(self):
        """Should include unit when specified."""
        point = MetricPoint(
            name="duration",
            value=5.2,
            timestamp=datetime.now(timezone.utc),
            unit="seconds",
        )

        result = point.to_dict()

        assert result["unit"] == "seconds"

    def test_to_dict_with_tags(self):
        """Should include tags when specified."""
        point = MetricPoint(
            name="rows_extracted",
            value=1000,
            timestamp=datetime.now(timezone.utc),
            tags={"system": "claims", "entity": "header"},
        )

        result = point.to_dict()

        assert result["tags"]["system"] == "claims"
        assert result["tags"]["entity"] == "header"


class TestPhaseTimer:
    """Tests for PhaseTimer class."""

    def test_timing(self):
        """Should measure duration."""
        timer = PhaseTimer(name="extract")
        time.sleep(0.01)  # 10ms
        duration = timer.stop()

        assert duration >= 0.01
        assert timer.duration >= 0.01

    def test_running_property(self):
        """Should track running state."""
        timer = PhaseTimer(name="extract")

        assert timer.running is True
        timer.stop()
        assert timer.running is False


class TestPipelineMetrics:
    """Tests for PipelineMetrics class."""

    def test_initialization(self):
        """Should initialize with pipeline info."""
        metrics = PipelineMetrics(
            system="claims",
            entity="header",
            run_date="2025-01-15",
            layer="bronze",
        )

        assert metrics.system == "claims"
        assert metrics.entity == "header"
        assert metrics.run_date == "2025-01-15"
        assert metrics.layer == "bronze"

    def test_time_phase(self):
        """Should time pipeline phases."""
        metrics = PipelineMetrics(
            system="claims",
            entity="header",
            run_date="2025-01-15",
        )

        with metrics.time_phase("extract") as timer:
            time.sleep(0.01)

        assert len(metrics._phases) == 1
        assert metrics._phases[0].name == "extract"
        assert metrics._phases[0].duration >= 0.01

    def test_record_metric(self):
        """Should record metric values."""
        metrics = PipelineMetrics(
            system="claims",
            entity="header",
            run_date="2025-01-15",
        )

        metrics.record("rows_extracted", 1000, unit="rows")
        metrics.record("file_size", 1024, unit="bytes", format="parquet")

        assert len(metrics._metrics) == 2
        assert metrics._metrics[0].name == "rows_extracted"
        assert metrics._metrics[0].value == 1000
        assert metrics._metrics[0].tags["system"] == "claims"

    def test_summary(self):
        """Should generate summary."""
        metrics = PipelineMetrics(
            system="claims",
            entity="header",
            run_date="2025-01-15",
            layer="bronze",
        )

        with metrics.time_phase("extract"):
            time.sleep(0.01)
        metrics.record("rows", 1000)

        summary = metrics.summary()

        assert summary["pipeline"]["system"] == "claims"
        assert summary["pipeline"]["entity"] == "header"
        assert summary["timing"]["total_seconds"] >= 0.01
        assert "extract" in summary["timing"]["phases"]
        assert len(summary["metrics"]) == 1

    def test_to_log_dict(self):
        """Should generate flat dictionary for logging."""
        metrics = PipelineMetrics(
            system="claims",
            entity="header",
            run_date="2025-01-15",
            layer="bronze",
        )

        with metrics.time_phase("extract"):
            pass
        metrics.record("rows", 1000, unit="rows")

        log_dict = metrics.to_log_dict()

        assert log_dict["pipeline_system"] == "claims"
        assert log_dict["pipeline_entity"] == "header"
        assert log_dict["pipeline_layer"] == "bronze"
        assert "phase_extract_seconds" in log_dict
        assert "metric_rows_rows" in log_dict


class TestMetricsCollector:
    """Tests for MetricsCollector class."""

    def test_create_metrics(self):
        """Should create and track metrics."""
        collector = MetricsCollector()

        metrics = collector.create_metrics(
            system="claims",
            entity="header",
            run_date="2025-01-15",
        )

        assert isinstance(metrics, PipelineMetrics)
        assert len(collector._runs) == 1

    def test_summary(self):
        """Should generate summary of all runs."""
        collector = MetricsCollector()

        m1 = collector.create_metrics("claims", "header", "2025-01-15")
        m1.record("rows", 1000)
        m1.finish()

        m2 = collector.create_metrics("claims", "detail", "2025-01-15")
        m2.record("rows", 500)
        m2.finish()

        summary = collector.summary()

        assert summary["total_runs"] == 2
        assert summary["successful_runs"] == 2
        assert len(summary["runs"]) == 2

    def test_clear(self):
        """Should clear all metrics."""
        collector = MetricsCollector()

        collector.create_metrics("claims", "header", "2025-01-15")
        collector.create_metrics("claims", "detail", "2025-01-15")

        collector.clear()

        assert len(collector._runs) == 0


class TestGetMetricsCollector:
    """Tests for get_metrics_collector function."""

    def test_returns_collector(self):
        """Should return MetricsCollector instance."""
        collector = get_metrics_collector()
        assert isinstance(collector, MetricsCollector)

    def test_returns_singleton(self):
        """Should return same instance."""
        collector1 = get_metrics_collector()
        collector2 = get_metrics_collector()

        # Clear to avoid test pollution
        collector1.clear()

        assert collector1 is collector2
