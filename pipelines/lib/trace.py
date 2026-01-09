"""Step tracing for debug mode execution reporting.

Provides step-by-step execution visibility when --debug is enabled.
Each step reports its start, completion, and duration.

Example output (console):
    12:34:56 [STEP] Loading configuration...
    12:34:56 [DONE] Loading configuration (0.02s)
    12:34:56 [STEP] Bronze: retail.orders
    12:34:56   [STEP] Connecting to source (file_csv)...
    12:34:56   [DONE] Connecting to source (0.01s)
    12:34:56   [STEP] Reading source data...
    12:34:58   [INFO]   Read 15,432 records from ./data/orders.csv
    12:34:58   [DONE] Reading source data (1.52s)

Usage:
    from pipelines.lib.trace import init_tracer, step, PipelineStep

    # Initialize at CLI entry point
    init_tracer(enabled=args.debug)

    # Use in pipeline code
    with step(PipelineStep.BRONZE_START, "retail.orders"):
        with step(PipelineStep.BRONZE_READ_SOURCE):
            data = read_source()
            get_tracer().detail(f"Read {len(data)} records")
"""

from __future__ import annotations

import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generator, List, Optional


class PipelineStep(Enum):
    """Enumeration of trackable pipeline steps."""

    # Configuration
    LOAD_CONFIG = "load_config"
    VALIDATE_CONFIG = "validate_config"

    # Bronze steps
    BRONZE_START = "bronze_start"
    BRONZE_CONNECT_SOURCE = "bronze_connect_source"
    BRONZE_READ_SOURCE = "bronze_read_source"
    BRONZE_ADD_METADATA = "bronze_add_metadata"
    BRONZE_WRITE_OUTPUT = "bronze_write_output"
    BRONZE_WRITE_CHECKSUMS = "bronze_write_checksums"
    BRONZE_SAVE_WATERMARK = "bronze_save_watermark"
    BRONZE_COMPLETE = "bronze_complete"

    # Silver steps
    SILVER_START = "silver_start"
    SILVER_VALIDATE_SOURCE = "silver_validate_source"
    SILVER_READ_BRONZE = "silver_read_bronze"
    SILVER_SELECT_COLUMNS = "silver_select_columns"
    SILVER_APPLY_CDC = "silver_apply_cdc"
    SILVER_DEDUPLICATE = "silver_deduplicate"
    SILVER_BUILD_HISTORY = "silver_build_history"
    SILVER_ADD_METADATA = "silver_add_metadata"
    SILVER_WRITE_OUTPUT = "silver_write_output"
    SILVER_WRITE_ARTIFACTS = "silver_write_artifacts"
    SILVER_COMPLETE = "silver_complete"


# Human-readable labels for each step
STEP_LABELS: Dict[PipelineStep, str] = {
    PipelineStep.LOAD_CONFIG: "Loading configuration",
    PipelineStep.VALIDATE_CONFIG: "Validating configuration",
    PipelineStep.BRONZE_START: "Bronze",
    PipelineStep.BRONZE_CONNECT_SOURCE: "Connecting to source",
    PipelineStep.BRONZE_READ_SOURCE: "Reading source data",
    PipelineStep.BRONZE_ADD_METADATA: "Adding Bronze metadata columns",
    PipelineStep.BRONZE_WRITE_OUTPUT: "Writing to Bronze target",
    PipelineStep.BRONZE_WRITE_CHECKSUMS: "Writing checksums",
    PipelineStep.BRONZE_SAVE_WATERMARK: "Saving watermark",
    PipelineStep.BRONZE_COMPLETE: "Bronze complete",
    PipelineStep.SILVER_START: "Silver",
    PipelineStep.SILVER_VALIDATE_SOURCE: "Validating source checksums",
    PipelineStep.SILVER_READ_BRONZE: "Reading Bronze data",
    PipelineStep.SILVER_SELECT_COLUMNS: "Selecting columns",
    PipelineStep.SILVER_APPLY_CDC: "Applying CDC logic",
    PipelineStep.SILVER_DEDUPLICATE: "Applying deduplication",
    PipelineStep.SILVER_BUILD_HISTORY: "Building history",
    PipelineStep.SILVER_ADD_METADATA: "Adding Silver metadata columns",
    PipelineStep.SILVER_WRITE_OUTPUT: "Writing to Silver target",
    PipelineStep.SILVER_WRITE_ARTIFACTS: "Writing artifacts",
    PipelineStep.SILVER_COMPLETE: "Silver complete",
}


@dataclass
class StepRecord:
    """Record of a completed step with timing information."""

    step: PipelineStep
    description: Optional[str]
    start_time: float
    end_time: float
    indent_level: int
    details: List[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        """Duration in seconds."""
        return self.end_time - self.start_time

    @property
    def label(self) -> str:
        """Human-readable label for this step."""
        base_label = STEP_LABELS.get(self.step, self.step.value)
        if self.description:
            return f"{base_label}: {self.description}"
        return base_label


class StepTracer:
    """Tracer for step-by-step execution reporting.

    When enabled, prints each step as it starts and completes with timing.
    When disabled, all operations are no-ops with minimal overhead.
    """

    def __init__(self, enabled: bool = False, indent_size: int = 2) -> None:
        """Initialize the tracer.

        Args:
            enabled: Whether to actually trace (False = no-op mode)
            indent_size: Number of spaces per indent level
        """
        self._enabled = enabled
        self._indent_size = indent_size
        self._indent_level = 0
        self._steps: List[StepRecord] = []
        self._current_step_start: Optional[float] = None
        self._current_step: Optional[PipelineStep] = None
        self._current_description: Optional[str] = None
        self._current_details: List[str] = []

    @property
    def enabled(self) -> bool:
        """Whether tracing is enabled."""
        return self._enabled

    def _timestamp(self) -> str:
        """Get current timestamp for output."""
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]

    def _indent(self) -> str:
        """Get current indentation string."""
        return " " * (self._indent_level * self._indent_size)

    def _print(self, prefix: str, message: str) -> None:
        """Print a trace message with timestamp and indentation."""
        timestamp = self._timestamp()
        indent = self._indent()
        print(f"{timestamp} {indent}[{prefix}] {message}", file=sys.stderr)

    @contextmanager
    def step(
        self, step_type: PipelineStep, description: Optional[str] = None
    ) -> Generator[None, None, None]:
        """Context manager for tracking a step with automatic timing.

        Args:
            step_type: The type of step being executed
            description: Optional additional description (e.g., entity name)

        Yields:
            None - execute your code inside the with block
        """
        if not self._enabled:
            yield
            return

        # Build label
        label = STEP_LABELS.get(step_type, step_type.value)
        if description:
            label = f"{label}: {description}"

        # Print start
        self._print("STEP", f"{label}...")

        # Track state
        start_time = time.perf_counter()
        self._indent_level += 1
        prev_details = self._current_details
        self._current_details = []

        try:
            yield
        finally:
            # Calculate duration
            end_time = time.perf_counter()
            duration = end_time - start_time

            # Restore state
            self._indent_level -= 1
            details = self._current_details
            self._current_details = prev_details

            # Record step
            self._steps.append(
                StepRecord(
                    step=step_type,
                    description=description,
                    start_time=start_time,
                    end_time=end_time,
                    indent_level=self._indent_level,
                    details=details,
                )
            )

            # Print completion
            self._print("DONE", f"{label} ({duration:.3f}s)")

    def detail(self, message: str, **kwargs: Any) -> None:
        """Log a detail line within the current step.

        Args:
            message: Detail message to log
            **kwargs: Additional key-value pairs to include
        """
        if not self._enabled:
            return

        # Format message with kwargs if provided
        if kwargs:
            extras = ", ".join(f"{k}={v}" for k, v in kwargs.items())
            full_message = f"{message} ({extras})"
        else:
            full_message = message

        self._current_details.append(full_message)
        self._print("INFO", f"  {full_message}")

    def summary(self) -> Dict[str, Any]:
        """Return timing summary for all recorded steps.

        Returns:
            Dictionary with steps list and total duration
        """
        if not self._steps:
            return {"steps": [], "total_duration_seconds": 0.0}

        # Calculate total from top-level steps only
        top_level_steps = [s for s in self._steps if s.indent_level == 0]
        total_duration = sum(s.duration_seconds for s in top_level_steps)

        return {
            "steps": [
                {
                    "step": s.step.value,
                    "label": s.label,
                    "duration_seconds": s.duration_seconds,
                    "indent_level": s.indent_level,
                    "details": s.details,
                }
                for s in self._steps
            ],
            "total_duration_seconds": total_duration,
        }

    def print_summary(self) -> None:
        """Print a formatted execution summary to stderr."""
        if not self._enabled or not self._steps:
            return

        print("", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print("EXECUTION SUMMARY", file=sys.stderr)
        print("=" * 60, file=sys.stderr)

        # Group by top-level steps
        for step_record in self._steps:
            indent = "  " * step_record.indent_level
            label = step_record.label
            duration = step_record.duration_seconds

            # Format: "  Bronze: retail.orders        2.444s"
            line = f"{indent}{label}"
            # Right-align duration
            padding = max(1, 50 - len(line))
            print(f"{line}{' ' * padding}{duration:.3f}s", file=sys.stderr)

        # Total
        summary = self.summary()
        print("-" * 60, file=sys.stderr)
        total = summary["total_duration_seconds"]
        print(f"{'Total:':<50}{total:.3f}s", file=sys.stderr)
        print("=" * 60, file=sys.stderr)


# Global tracer instance
_tracer: Optional[StepTracer] = None


def init_tracer(enabled: bool = False) -> StepTracer:
    """Initialize the global tracer.

    Args:
        enabled: Whether to enable step tracing

    Returns:
        The initialized tracer instance
    """
    global _tracer
    _tracer = StepTracer(enabled=enabled)

    if enabled:
        print("", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print("DEBUG MODE ENABLED - Step-by-step execution tracing", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print("", file=sys.stderr)

    return _tracer


def get_tracer() -> StepTracer:
    """Get the global tracer instance.

    Returns:
        The global tracer (creates disabled one if not initialized)
    """
    global _tracer
    if _tracer is None:
        _tracer = StepTracer(enabled=False)
    return _tracer


@contextmanager
def step(
    step_type: PipelineStep, description: Optional[str] = None
) -> Generator[None, None, None]:
    """Shortcut context manager for step tracing.

    Args:
        step_type: The type of step being executed
        description: Optional additional description

    Yields:
        None - execute your code inside the with block
    """
    tracer = get_tracer()
    with tracer.step(step_type, description):
        yield
