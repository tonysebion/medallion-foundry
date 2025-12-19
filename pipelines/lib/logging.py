"""Logging utilities for pipelines.

Provides structured JSON logging option for production environments.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

__all__ = [
    "setup_logging",
    "JSONFormatter",
    "PipelineLogger",
    "get_pipeline_logger",
]


class JSONFormatter(logging.Formatter):
    """Formatter that outputs log records as JSON.

    Useful for log aggregation systems like ELK, Splunk, or CloudWatch.

    Example output:
        {"timestamp": "2025-01-15T10:30:00.123Z", "level": "INFO",
         "logger": "pipelines.bronze", "message": "Extracted 1000 rows"}
    """

    def __init__(
        self,
        include_fields: Optional[list[str]] = None,
        exclude_fields: Optional[list[str]] = None,
    ):
        """Initialize JSON formatter.

        Args:
            include_fields: Extra fields to include (from record.__dict__)
            exclude_fields: Fields to exclude from output
        """
        super().__init__()
        self.include_fields = include_fields or []
        self.exclude_fields = exclude_fields or []

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON."""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add location info
        if record.pathname:
            log_data["source"] = {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName,
            }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields from record
        for field in self.include_fields:
            if hasattr(record, field):
                log_data[field] = getattr(record, field)

        # Add any extra attributes set via extra= parameter
        extra_attrs = {
            k: v
            for k, v in record.__dict__.items()
            if k not in logging.LogRecord("", 0, "", 0, "", (), None).__dict__
            and k not in ("message", "asctime")
            and k not in self.exclude_fields
        }
        if extra_attrs:
            log_data["extra"] = extra_attrs

        return json.dumps(log_data, default=str)


class PipelineLogger:
    """Enhanced logger with pipeline-specific context.

    Provides structured logging with automatic context (system, entity, run_date).

    Example:
        logger = PipelineLogger("claims.header")
        logger.set_context(system="claims", entity="header", run_date="2025-01-15")
        logger.info("Starting extraction")  # Includes context automatically
    """

    def __init__(self, name: str):
        """Initialize pipeline logger.

        Args:
            name: Logger name (typically module path)
        """
        self._logger = logging.getLogger(name)
        self._context: Dict[str, Any] = {}

    def set_context(self, **kwargs: Any) -> None:
        """Set context fields that will be included in all log messages.

        Args:
            **kwargs: Context key-value pairs (system, entity, run_date, etc.)
        """
        self._context.update(kwargs)

    def clear_context(self) -> None:
        """Clear all context fields."""
        self._context.clear()

    def _log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        """Internal logging method that adds context."""
        extra = kwargs.pop("extra", {})
        extra.update(self._context)
        self._logger.log(level, msg, *args, extra=extra, **kwargs)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a debug message."""
        self._log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an info message."""
        self._log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a warning message."""
        self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an error message."""
        self._log(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an exception with traceback."""
        kwargs["exc_info"] = True
        self._log(logging.ERROR, msg, *args, **kwargs)

    def metric(
        self,
        name: str,
        value: Any,
        unit: Optional[str] = None,
        **tags: Any,
    ) -> None:
        """Log a metric value.

        Args:
            name: Metric name (e.g., "rows_extracted", "duration_seconds")
            value: Metric value
            unit: Optional unit (e.g., "rows", "seconds", "bytes")
            **tags: Additional tags for the metric
        """
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
    """Get a pipeline logger instance.

    Args:
        name: Logger name (typically module path)

    Returns:
        PipelineLogger instance
    """
    return PipelineLogger(name)


def setup_logging(
    verbose: bool = False,
    json_format: bool = False,
    log_file: Optional[str] = None,
) -> None:
    """Configure logging for pipeline execution.

    Args:
        verbose: Enable debug-level logging
        json_format: Use JSON output format (for log aggregation)
        log_file: Optional file path to write logs to
    """
    level = logging.DEBUG if verbose else logging.INFO

    # Choose formatter
    if json_format:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Suppress noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
