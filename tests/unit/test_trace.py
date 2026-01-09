"""Unit tests for step tracing functionality."""

from __future__ import annotations

import time

import pytest

from pipelines.lib.trace import (
    PipelineStep,
    StepTracer,
    STEP_LABELS,
    init_tracer,
    get_tracer,
    step,
)


class TestPipelineStep:
    """Tests for the PipelineStep enum."""

    def test_all_steps_have_labels(self) -> None:
        """Every pipeline step should have a human-readable label."""
        for step_type in PipelineStep:
            assert step_type in STEP_LABELS, f"Missing label for {step_type}"

    def test_bronze_steps_exist(self) -> None:
        """Bronze steps should be defined."""
        bronze_steps = [s for s in PipelineStep if s.value.startswith("bronze_")]
        assert len(bronze_steps) >= 5, "Should have multiple bronze steps"
        assert PipelineStep.BRONZE_START in bronze_steps
        assert PipelineStep.BRONZE_READ_SOURCE in bronze_steps
        assert PipelineStep.BRONZE_WRITE_OUTPUT in bronze_steps

    def test_silver_steps_exist(self) -> None:
        """Silver steps should be defined."""
        silver_steps = [s for s in PipelineStep if s.value.startswith("silver_")]
        assert len(silver_steps) >= 5, "Should have multiple silver steps"
        assert PipelineStep.SILVER_START in silver_steps
        assert PipelineStep.SILVER_READ_BRONZE in silver_steps
        assert PipelineStep.SILVER_WRITE_OUTPUT in silver_steps


class TestStepTracer:
    """Tests for the StepTracer class."""

    def test_tracer_disabled_by_default(self) -> None:
        """Tracer should not record steps when disabled."""
        tracer = StepTracer(enabled=False)
        with tracer.step(PipelineStep.BRONZE_START):
            pass
        assert len(tracer._steps) == 0

    def test_tracer_records_steps_when_enabled(self) -> None:
        """Tracer should record steps when enabled."""
        tracer = StepTracer(enabled=True)
        with tracer.step(PipelineStep.BRONZE_START, "test.entity"):
            pass
        assert len(tracer._steps) == 1
        assert tracer._steps[0].step == PipelineStep.BRONZE_START
        assert tracer._steps[0].description == "test.entity"

    def test_step_timing_is_recorded(self) -> None:
        """Step duration should be recorded."""
        tracer = StepTracer(enabled=True)
        with tracer.step(PipelineStep.BRONZE_START):
            time.sleep(0.05)  # Sleep 50ms
        assert tracer._steps[0].duration_seconds >= 0.04  # Allow some tolerance

    def test_nested_steps_increase_indent(self) -> None:
        """Nested steps should have increasing indent levels."""
        tracer = StepTracer(enabled=True)
        with tracer.step(PipelineStep.BRONZE_START):
            with tracer.step(PipelineStep.BRONZE_READ_SOURCE):
                pass
        # Both steps should be recorded
        assert len(tracer._steps) == 2
        # Inner step should have higher indent level
        inner_step = tracer._steps[0]  # Inner completes first
        outer_step = tracer._steps[1]  # Outer completes last
        assert inner_step.indent_level == 1
        assert outer_step.indent_level == 0

    def test_detail_logs_within_step(self) -> None:
        """Details should be recorded within the current step."""
        tracer = StepTracer(enabled=True)
        with tracer.step(PipelineStep.BRONZE_START):
            tracer.detail("Read 100 records")
            tracer.detail("From source.csv")
        step_record = tracer._steps[0]
        assert len(step_record.details) == 2
        assert "100 records" in step_record.details[0]
        assert "source.csv" in step_record.details[1]

    def test_detail_with_kwargs(self) -> None:
        """Details should include keyword arguments."""
        tracer = StepTracer(enabled=True)
        with tracer.step(PipelineStep.BRONZE_START):
            tracer.detail("Processing", rows=100, file="test.csv")
        step_record = tracer._steps[0]
        assert "rows=100" in step_record.details[0]
        assert "file=test.csv" in step_record.details[0]

    def test_detail_noop_when_disabled(self) -> None:
        """Detail calls should be no-ops when tracer is disabled."""
        tracer = StepTracer(enabled=False)
        with tracer.step(PipelineStep.BRONZE_START):
            tracer.detail("This should not be recorded")
        # No steps recorded at all
        assert len(tracer._steps) == 0

    def test_summary_returns_all_step_timings(self) -> None:
        """Summary should include all recorded steps."""
        tracer = StepTracer(enabled=True)
        with tracer.step(PipelineStep.BRONZE_START):
            pass
        with tracer.step(PipelineStep.SILVER_START):
            pass
        summary = tracer.summary()
        assert "steps" in summary
        assert "total_duration_seconds" in summary
        assert len(summary["steps"]) == 2

    def test_summary_total_from_top_level_only(self) -> None:
        """Summary total should only include top-level steps."""
        tracer = StepTracer(enabled=True)
        with tracer.step(PipelineStep.BRONZE_START):
            time.sleep(0.02)
            with tracer.step(PipelineStep.BRONZE_READ_SOURCE):
                time.sleep(0.01)
        summary = tracer.summary()
        # Total should be ~0.03s (outer step), not 0.04s (inner + outer)
        # Inner time is already included in outer
        assert summary["total_duration_seconds"] >= 0.02
        assert summary["total_duration_seconds"] < 0.1

    def test_empty_summary(self) -> None:
        """Summary should handle no steps gracefully."""
        tracer = StepTracer(enabled=True)
        summary = tracer.summary()
        assert summary["steps"] == []
        assert summary["total_duration_seconds"] == 0.0

    def test_step_label_with_description(self) -> None:
        """Step record label should include description when provided."""
        tracer = StepTracer(enabled=True)
        with tracer.step(PipelineStep.BRONZE_START, "retail.orders"):
            pass
        step_record = tracer._steps[0]
        assert "Bronze" in step_record.label
        assert "retail.orders" in step_record.label

    def test_step_label_without_description(self) -> None:
        """Step record label should work without description."""
        tracer = StepTracer(enabled=True)
        with tracer.step(PipelineStep.BRONZE_READ_SOURCE):
            pass
        step_record = tracer._steps[0]
        assert "Reading source data" in step_record.label


class TestGlobalTracer:
    """Tests for global tracer functions."""

    def test_init_tracer_creates_instance(self) -> None:
        """init_tracer should create a new tracer instance."""
        tracer = init_tracer(enabled=True)
        assert tracer.enabled is True

    def test_get_tracer_returns_same_instance(self) -> None:
        """get_tracer should return the same instance."""
        init_tracer(enabled=True)
        tracer1 = get_tracer()
        tracer2 = get_tracer()
        assert tracer1 is tracer2

    def test_get_tracer_creates_disabled_if_not_initialized(self) -> None:
        """get_tracer should create disabled tracer if not initialized."""
        # Reset global state
        import pipelines.lib.trace as trace_module
        trace_module._tracer = None

        tracer = get_tracer()
        assert tracer.enabled is False

    def test_step_shortcut_function(self) -> None:
        """step() function should use global tracer."""
        init_tracer(enabled=True)
        with step(PipelineStep.BRONZE_START, "test"):
            pass
        tracer = get_tracer()
        assert len(tracer._steps) == 1


class TestStepTracerOutput:
    """Tests for step tracer output formatting."""

    def test_print_summary_no_error_when_empty(self, capsys) -> None:
        """print_summary should not error when no steps recorded."""
        tracer = StepTracer(enabled=True)
        tracer.print_summary()  # Should not raise

    def test_print_summary_disabled(self, capsys) -> None:
        """print_summary should do nothing when disabled."""
        tracer = StepTracer(enabled=False)
        with tracer.step(PipelineStep.BRONZE_START):
            pass
        tracer.print_summary()
        captured = capsys.readouterr()
        assert captured.err == ""


@pytest.mark.unit
class TestTracerIntegration:
    """Integration-style tests for tracer behavior."""

    def test_full_pipeline_trace_flow(self) -> None:
        """Test a realistic pipeline tracing scenario."""
        tracer = StepTracer(enabled=True)

        with tracer.step(PipelineStep.LOAD_CONFIG, "test_pipeline.yaml"):
            tracer.detail("Pipeline: test")

        with tracer.step(PipelineStep.BRONZE_START, "retail.orders"):
            with tracer.step(PipelineStep.BRONZE_CONNECT_SOURCE, "file_csv"):
                pass
            with tracer.step(PipelineStep.BRONZE_READ_SOURCE):
                tracer.detail("Read 1000 records")
            with tracer.step(PipelineStep.BRONZE_ADD_METADATA):
                pass
            with tracer.step(PipelineStep.BRONZE_WRITE_OUTPUT):
                tracer.detail("Wrote 1000 records")

        with tracer.step(PipelineStep.SILVER_START, "retail.orders"):
            with tracer.step(PipelineStep.SILVER_READ_BRONZE):
                tracer.detail("Read 1000 records")
            with tracer.step(PipelineStep.SILVER_DEDUPLICATE):
                tracer.detail("Curated to 950 records")
            with tracer.step(PipelineStep.SILVER_WRITE_OUTPUT):
                tracer.detail("Wrote 950 records")

        summary = tracer.summary()

        # Should have all steps recorded:
        # 1 load_config + 5 bronze (connect, read, add_meta, write, start) + 4 silver (read, dedupe, write, start) = 10
        assert len(summary["steps"]) == 10

        # Top-level steps should be 3 (load_config, bronze_start, silver_start)
        top_level = [s for s in summary["steps"] if s["indent_level"] == 0]
        assert len(top_level) == 3

    def test_exception_in_step_still_records(self) -> None:
        """Steps should be recorded even if an exception occurs."""
        tracer = StepTracer(enabled=True)

        with pytest.raises(ValueError):
            with tracer.step(PipelineStep.BRONZE_START):
                raise ValueError("Test error")

        # Step should still be recorded
        assert len(tracer._steps) == 1
        assert tracer._steps[0].duration_seconds >= 0
