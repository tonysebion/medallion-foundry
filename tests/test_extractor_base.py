"""Tests for extractor registry and BaseExtractor watermark flow."""

from dataclasses import dataclass
from datetime import date

import pytest

from core.infrastructure.io.extractors.base import (
    EXTRACTOR_REGISTRY,
    register_extractor,
    list_extractor_types,
    get_extractor_class,
    BaseExtractor,
    ExtractionResult,
)
from core.foundation.state.watermark import Watermark


@register_extractor("test-source")
class TestExtractor(BaseExtractor):
    def fetch_records(self, cfg, run_date):
        return ([{"id": 1}, {"id": 2}], "cursor-1")

    def get_watermark_config(self, cfg):
        return {"column": "updated_at", "type": "timestamp", "enabled": True}


class DummyWatermarkStore:
    def __init__(self):
        self.saved = []
        self.watermark = Watermark(
            source_key="sys.tbl",
            watermark_column="updated_at",
            watermark_value="cursor-0",
        )

    def get(self, system, table, column, watermark_type):
        assert system == "sys"
        assert table == "tbl"
        assert column == "updated_at"
        return self.watermark

    def save(self, watermark):
        self.saved.append(watermark)


def test_extractor_registry_round_trip():
    assert "test-source" in list_extractor_types()
    assert get_extractor_class("test-source") is TestExtractor

    result = TestExtractor().fetch_with_watermark(
        {"source": {"system": "sys", "table": "tbl"}},
        run_date=date(2025, 1, 1),
        watermark_store=DummyWatermarkStore(),
        run_id="run-123",
    )
    assert isinstance(result, ExtractionResult)
    assert result.metadata["watermark_updated"] is True
    assert result.metadata["previous_watermark"] == "cursor-0"


def test_extraction_result_defaults_to_len_records():
    result = ExtractionResult(records=[{"x": 1}, {"y": 2}], record_count=0)
    assert result.record_count == 2


def test_registry_cleanup():
    EXTRACTOR_REGISTRY.pop("test-source", None)
