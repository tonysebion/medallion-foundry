from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, Mapping, cast

import pytest

import bronze_extract
from core.logging_config import JSONFormatter, get_logger, setup_logging


def _reset_logging() -> None:
    """Reset logging state to the default configuration."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        handler.close()
        root.removeHandler(handler)
    setup_logging()


def test_setup_logging_writes_json_file(tmp_path: Path) -> None:
    """Ensure logging setup adds JSON console output and a rotating file handler."""
    log_path = tmp_path / "logs" / "app.log"
    root = logging.getLogger()
    try:
        setup_logging(
            level=logging.DEBUG,
            format_type="json",
            log_file=log_path,
            include_context=True,
        )
        handlers = list(root.handlers)
        console_handler = next(
            handler
            for handler in handlers
            if isinstance(handler, logging.StreamHandler)
        )
        file_handler = next(
            handler
            for handler in handlers
            if isinstance(handler, logging.handlers.RotatingFileHandler)
        )

        assert isinstance(console_handler.formatter, JSONFormatter)
        assert isinstance(file_handler.formatter, JSONFormatter)
        logging.getLogger("test.logger").info("hello test")

        file_handler.flush()
        contents = log_path.read_text(encoding="utf-8")
        records = [json.loads(line) for line in contents.splitlines() if line.strip()]
        assert any(record.get("message") == "hello test" for record in records)
    finally:
        _reset_logging()


def test_get_logger_with_extra_returns_adapter() -> None:
    adapter = get_logger("core.logging", extra={"system": "retail"})
    assert isinstance(adapter, logging.LoggerAdapter)
    extra = cast(Mapping[str, Any], adapter.extra)
    assert extra["system"] == "retail"


def test_list_storage_backends_contains_known_values() -> None:
    assert bronze_extract.list_storage_backends() == ["s3", "azure", "local"]


def test_cli_list_backends_flag_outputs_every_backend(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "argv", ["bronze_extract.py", "--list-backends"])
    result = bronze_extract.main()
    captured = capsys.readouterr()
    assert "Available storage backends" in captured.out
    assert "s3" in captured.out
    assert "azure" in captured.out
    assert "local" in captured.out
    assert result == 0
