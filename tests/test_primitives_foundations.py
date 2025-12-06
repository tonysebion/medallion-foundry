from __future__ import annotations

import io
import json
import logging
import os
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pytest

from core.primitives.foundations import base as foundations_base
from core.primitives.foundations import exceptions, logging as bf_logging, models, patterns
from core.primitives.foundations.patterns import LoadPattern


class TestEnum(foundations_base.RichEnumMixin, str, Enum):
    ALPHA = "alpha"
    BETA = "beta"

    _default = "ALPHA"


@dataclass
class ExampleConfig(foundations_base.SerializableMixin):
    name: str
    path: Path
    pattern: LoadPattern = LoadPattern.SNAPSHOT


def test_richenum_alias_and_default() -> None:
    assert TestEnum.normalize("ALPHA") == TestEnum.ALPHA
    assert TestEnum.normalize(None) == TestEnum.ALPHA
    with pytest.raises(ValueError):
        TestEnum.normalize("unknown")


def test_serializable_mixin_round_trip(tmp_path: Path) -> None:
    cfg = ExampleConfig(name="demo", path=tmp_path / "file.txt", pattern=LoadPattern.INCREMENTAL_MERGE)
    data = cfg.to_dict()
    assert data["path"].endswith("file.txt")
    assert data["pattern"] == "incremental_merge"

    restored = ExampleConfig.from_dict({"name": "demo", "path": str(tmp_path / "file.txt"), "pattern": "snapshot", "extra": "ignored"})
    assert restored.name == "demo"
    assert restored.pattern == LoadPattern.SNAPSHOT


def test_storage_error_str_includes_details() -> None:
    err = exceptions.StorageError(
        message="fail",
        backend_type="s3",
        operation="upload",
        file_path="/tmp/data",
    )
    text = str(err)
    assert "[STG001]" in text
    assert "backend_type=s3" in text


def test_emit_warnings() -> None:
    spec = exceptions.DeprecationSpec(code="D100", message="old", since="1.0", remove_in="2.0")
    with pytest.warns(exceptions.BronzeFoundryDeprecationWarning):
        exceptions.emit_deprecation(spec)

    with pytest.warns(exceptions.BronzeFoundryCompatibilityWarning):
        exceptions.emit_compat("compat", "C100")


def test_json_formatter_includes_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    formatter = bf_logging.JSONFormatter(include_context=True)
    record = logging.LogRecord("test", logging.INFO, __file__, 10, "message", args=(), exc_info=None)
    record.custom = "value"
    payload = json.loads(formatter.format(record))
    assert payload["level"] == "INFO"
    assert payload["custom"] == "value"


def test_human_readable_formatter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
    formatter = bf_logging.HumanReadableFormatter(use_colors=True, include_context=True)
    record = logging.LogRecord("test", logging.ERROR, __file__, 5, "boom", args=(), exc_info=None)
    output = formatter.format(record)
    assert "[ERROR]" in output
    assert "test_primitives_foundations" in output


def test_get_level_and_format_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRONZE_LOG_LEVEL", "debug")
    assert bf_logging.get_log_level_from_env() == logging.DEBUG
    monkeypatch.setenv("BRONZE_LOG_FORMAT", "json")
    assert bf_logging.get_log_format_from_env() == "json"


def test_setup_logging_creates_file(tmp_path: Path) -> None:
    log_file = tmp_path / "logs" / "out.log"
    bf_logging.setup_logging(level=logging.DEBUG, format_type="json", log_file=log_file, include_context=True)
    logger = logging.getLogger("primitives.test")
    logger.debug("hello")
    for handler in logging.getLogger().handlers:
        handler.flush()
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert "hello" in content
    logging.getLogger().handlers.clear()


def test_log_exception_and_performance(tmp_path: Path) -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger = logging.getLogger("primitives.performance")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    bf_logging.log_exception(logger, "fail", ValueError("boom"))
    bf_logging.log_performance(logger, "sync", 1.23, records=100)
    handler.flush()
    output = stream.getvalue()
    assert "fail" in output
    assert "Performance" in output


def test_silver_model_aliases_and_properties() -> None:
    assert models.SilverModel.normalize("scd1") == models.SilverModel.SCD_TYPE_1
    assert models.SilverModel.default_for_load_pattern(LoadPattern.CURRENT_HISTORY) == models.SilverModel.SCD_TYPE_2
    assert models.SilverModel.SCD_TYPE_2.requires_dedupe
    assert models.SilverModel.SCD_TYPE_2.emits_history


def test_load_pattern_properties() -> None:
    assert patterns.LoadPattern.normalize("full") == LoadPattern.SNAPSHOT
    assert LoadPattern.SNAPSHOT.chunk_prefix == "snapshot"
    assert LoadPattern.INCREMENTAL_APPEND.folder_name == "pattern=incremental_append"
    assert LoadPattern.INCREMENTAL_MERGE.is_incremental
    assert LoadPattern.INCREMENTAL_MERGE.requires_merge
