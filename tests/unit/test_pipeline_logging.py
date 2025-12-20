"""Tests for pipelines logging utilities."""

import json
import logging


from pipelines.lib.observability import (
    JSONFormatter,
    PipelineLogger,
    get_pipeline_logger,
    setup_logging,
)


class TestJSONFormatter:
    """Tests for JSONFormatter class."""

    def test_basic_format(self):
        """Should format log record as JSON."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/test/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert data["level"] == "INFO"
        assert data["logger"] == "test.logger"
        assert data["message"] == "Test message"
        assert "timestamp" in data
        assert data["timestamp"].endswith("Z")

    def test_format_with_args(self):
        """Should format message with arguments."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/test/file.py",
            lineno=42,
            msg="Processed %d rows",
            args=(100,),
            exc_info=None,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert data["message"] == "Processed 100 rows"

    def test_format_with_exception(self):
        """Should include exception info."""
        formatter = JSONFormatter()

        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.ERROR,
            pathname="/test/file.py",
            lineno=42,
            msg="Error occurred",
            args=(),
            exc_info=exc_info,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert "exception" in data
        assert "ValueError" in data["exception"]

    def test_format_with_extra(self):
        """Should include extra fields."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/test/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        # Add extra fields
        record.system = "claims"
        record.entity = "header"

        output = formatter.format(record)
        data = json.loads(output)

        assert "extra" in data
        assert data["extra"]["system"] == "claims"
        assert data["extra"]["entity"] == "header"

    def test_source_location(self):
        """Should include source location."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/test/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert "source" in data
        assert data["source"]["file"] == "/test/file.py"
        assert data["source"]["line"] == 42


class TestPipelineLogger:
    """Tests for PipelineLogger class."""

    def test_basic_logging(self):
        """Should log messages."""
        logger = PipelineLogger("test.pipeline")

        # Verify logger works without errors
        logger.info("Test message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.debug("Debug message")

    def test_set_context(self):
        """Should add context to log messages."""
        logger = PipelineLogger("test.pipeline")
        logger.set_context(system="claims", entity="header", run_date="2025-01-15")

        # Context is stored
        assert logger._context["system"] == "claims"
        assert logger._context["entity"] == "header"
        assert logger._context["run_date"] == "2025-01-15"

    def test_clear_context(self):
        """Should clear context."""
        logger = PipelineLogger("test.pipeline")
        logger.set_context(system="claims")
        logger.clear_context()

        assert len(logger._context) == 0

    def test_metric_logging(self):
        """Should log metrics."""
        logger = PipelineLogger("test.pipeline")
        logger.set_context(system="claims", entity="header")

        # Should not raise
        logger.metric("rows_extracted", 1000, unit="rows")
        logger.metric("duration", 5.2, unit="seconds", stage="extract")


class TestGetPipelineLogger:
    """Tests for get_pipeline_logger function."""

    def test_returns_pipeline_logger(self):
        """Should return PipelineLogger instance."""
        logger = get_pipeline_logger("test.pipeline")
        assert isinstance(logger, PipelineLogger)


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_default_setup(self):
        """Should configure logging with defaults."""
        setup_logging()

        logger = logging.getLogger()
        assert logger.level == logging.INFO

    def test_verbose_setup(self):
        """Should configure debug level when verbose."""
        setup_logging(verbose=True)

        logger = logging.getLogger()
        assert logger.level == logging.DEBUG

    def test_json_format_setup(self):
        """Should use JSON formatter when json_format=True."""
        setup_logging(json_format=True)

        logger = logging.getLogger()
        handlers = [h for h in logger.handlers if hasattr(h, "formatter")]

        # At least one handler should use JSONFormatter
        json_formatters = [
            h for h in handlers if isinstance(h.formatter, JSONFormatter)
        ]
        assert len(json_formatters) > 0

    def test_log_file_setup(self, tmp_path):
        """Should add file handler when log_file specified."""
        log_file = tmp_path / "test.log"
        setup_logging(log_file=str(log_file))

        logger = logging.getLogger()
        logger.info("Test message")

        # Clean up handlers
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

        # File should exist
        assert log_file.exists()
