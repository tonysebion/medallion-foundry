"""Pipeline runner decorator and utilities.

Provides the @pipeline decorator for adding logging, timing,
dry-run support, and other features to pipeline run functions.
"""

from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar

from pipelines.lib.observability import get_structlog_logger

# Use structlog for structured logging
logger = get_structlog_logger(__name__)

__all__ = ["pipeline"]

F = TypeVar("F", bound=Callable[..., Any])


def pipeline(
    name: str,
    *,
    log_level: int = logging.INFO,
) -> Callable[[F], F]:
    """Pipeline decorator with logging, timing, and optional features.

    Adds consistent logging, timing, and error handling to pipeline
    run functions.

    Args:
        name: Name of the pipeline for logging
        log_level: Logging level for pipeline messages

    Example:
        @pipeline("claims.header")
        def run(run_date: str) -> dict:
            bronze_result = bronze.run(run_date)
            silver_result = silver.run(run_date)
            return {"bronze": bronze_result, "silver": silver_result}
    """

    def decorator(fn: F) -> F:
        @wraps(fn)
        def wrapper(*args: Any, dry_run: bool = False, **kwargs: Any) -> Dict[str, Any]:
            pipeline_logger = get_structlog_logger(f"pipelines.{name}")

            if dry_run:
                pipeline_logger.info("pipeline_dry_run", pipeline=name)
                return {"dry_run": True, "pipeline": name}

            start = time.time()
            pipeline_logger.info("pipeline_started", pipeline=name)

            try:
                result = fn(*args, **kwargs)
                elapsed = time.time() - start

                pipeline_logger.info(
                    "pipeline_completed",
                    pipeline=name,
                    elapsed_seconds=round(elapsed, 2),
                )

                # Add timing to result if it's a dict
                if isinstance(result, dict):
                    result["_elapsed_seconds"] = elapsed
                    result["_pipeline"] = name

                return result

            except Exception as e:
                elapsed = time.time() - start
                pipeline_logger.error(
                    "pipeline_failed",
                    pipeline=name,
                    elapsed_seconds=round(elapsed, 2),
                    error=str(e),
                )
                raise

        return wrapper  # type: ignore

    return decorator


class PipelineResult:
    """Structured result from a pipeline run.

    Provides a consistent interface for accessing pipeline results
    with helper methods for common operations.
    """

    def __init__(
        self,
        success: bool,
        *,
        bronze: Optional[Dict[str, Any]] = None,
        silver: Optional[Dict[str, Any]] = None,
        elapsed_seconds: float = 0.0,
        pipeline_name: str = "",
        error: Optional[Exception] = None,
    ):
        self.success = success
        self.bronze = bronze or {}
        self.silver = silver or {}
        self.elapsed_seconds = elapsed_seconds
        self.pipeline_name = pipeline_name
        self.error = error

    @property
    def total_rows(self) -> int:
        """Get total rows processed across Bronze and Silver."""
        bronze_rows = self.bronze.get("row_count", 0)
        silver_rows = self.silver.get("row_count", 0)
        return bronze_rows + silver_rows

    @property
    def was_skipped(self) -> bool:
        """Check if the pipeline was skipped."""
        return self.bronze.get("skipped", False) or self.silver.get("skipped", False)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "bronze": self.bronze,
            "silver": self.silver,
            "elapsed_seconds": self.elapsed_seconds,
            "pipeline_name": self.pipeline_name,
            "error": str(self.error) if self.error else None,
        }

    def __repr__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"PipelineResult({status}, "
            f"bronze={self.bronze.get('row_count', 0)} rows, "
            f"silver={self.silver.get('row_count', 0)} rows, "
            f"elapsed={self.elapsed_seconds:.2f}s)"
        )


def run_pipeline(
    bronze_source: Any,
    silver_entity: Any,
    run_date: str,
    *,
    skip_bronze: bool = False,
    skip_silver: bool = False,
    dry_run: bool = False,
    **kwargs: Any,
) -> PipelineResult:
    """Run a Bronze → Silver pipeline with unified result handling.

    This is a convenience function for running standard pipelines
    that have both a Bronze source and Silver entity.

    Args:
        bronze_source: BronzeSource instance
        silver_entity: SilverEntity instance
        run_date: The date for this run
        skip_bronze: Skip Bronze extraction
        skip_silver: Skip Silver curation
        dry_run: Validate without executing
        **kwargs: Additional arguments passed to run methods

    Returns:
        PipelineResult with unified status and metrics

    Example:
        result = run_pipeline(bronze, silver, "2025-01-15")
        if result.success:
            print(f"Processed {result.total_rows} rows")
    """
    start = time.time()

    bronze_result: Dict[str, Any] = {}
    silver_result: Dict[str, Any] = {}
    error: Optional[Exception] = None

    try:
        # Run Bronze
        if not skip_bronze:
            bronze_result = bronze_source.run(run_date, dry_run=dry_run, **kwargs)

        # Run Silver
        if not skip_silver:
            silver_result = silver_entity.run(run_date, dry_run=dry_run, **kwargs)

        success = True

    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        error = e
        success = False

    elapsed = time.time() - start

    return PipelineResult(
        success=success,
        bronze=bronze_result,
        silver=silver_result,
        elapsed_seconds=elapsed,
        pipeline_name=f"{bronze_source.system}.{bronze_source.entity}",
        error=error,
    )


def run_bronze_only(
    bronze_source: Any,
    run_date: str,
    *,
    dry_run: bool = False,
    **kwargs: Any,
) -> PipelineResult:
    """Run only the Bronze extraction.

    Args:
        bronze_source: BronzeSource instance
        run_date: The date for this run
        dry_run: Validate without executing
        **kwargs: Additional arguments passed to run method

    Returns:
        PipelineResult with Bronze results only
    """
    start = time.time()

    try:
        bronze_result = bronze_source.run(run_date, dry_run=dry_run, **kwargs)
        success = True
        error = None
    except Exception as e:
        logger.exception("Bronze extraction failed: %s", e)
        bronze_result = {}
        success = False
        error = e

    elapsed = time.time() - start

    return PipelineResult(
        success=success,
        bronze=bronze_result,
        elapsed_seconds=elapsed,
        pipeline_name=f"{bronze_source.system}.{bronze_source.entity}:bronze",
        error=error,
    )


def run_silver_only(
    silver_entity: Any,
    run_date: str,
    *,
    dry_run: bool = False,
    **kwargs: Any,
) -> PipelineResult:
    """Run only the Silver curation.

    Args:
        silver_entity: SilverEntity instance
        run_date: The date for this run
        dry_run: Validate without executing
        **kwargs: Additional arguments passed to run method

    Returns:
        PipelineResult with Silver results only
    """
    start = time.time()

    try:
        silver_result = silver_entity.run(run_date, dry_run=dry_run, **kwargs)
        success = True
        error = None
    except Exception as e:
        logger.exception("Silver curation failed: %s", e)
        silver_result = {}
        success = False
        error = e

    elapsed = time.time() - start

    return PipelineResult(
        success=success,
        silver=silver_result,
        elapsed_seconds=elapsed,
        pipeline_name="silver",
        error=error,
    )
