from __future__ import annotations

from datetime import date

from core.adapters.extractors.base import BaseExtractor
from core.primitives.state.watermark import (
    Watermark,
    WatermarkStore,
    WatermarkType,
)


class DummyExtractor(BaseExtractor):
    """Test stub extractor that reports a deterministic cursor."""

    def get_watermark_config(self, cfg):
        return {"enabled": True, "column": "cursor_col", "type": "timestamp"}

    def fetch_records(self, cfg, run_date):
        return ([{"cursor_col": "2025-01-03T00:00:00Z"}], "2025-01-03T00:00:00Z")


def test_fetch_with_watermark_records_previous_value(tmp_path):
    store = WatermarkStore(storage_backend="local", local_path=tmp_path)
    watermark = Watermark(
        source_key="tests.source",
        watermark_column="cursor_col",
        watermark_value="2025-01-02T00:00:00Z",
        watermark_type=WatermarkType.TIMESTAMP,
    )
    store.save(watermark)
    extractor = DummyExtractor()
    result = extractor.fetch_with_watermark(
        {"source": {"system": "tests", "table": "source"}},
        date.today(),
        watermark_store=store,
        run_id="run-123",
    )

    assert result.metadata["watermark_updated"] is True
    assert result.metadata["previous_watermark"] == "2025-01-02T00:00:00Z"
