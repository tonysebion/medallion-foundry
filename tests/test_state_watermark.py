"""Targeted tests for watermark helpers."""

from datetime import date
from unittest.mock import patch

import pytest

from core.foundation.state.watermark import Watermark, WatermarkType

MOCK_TIMESTAMP = "2025-01-01T00:00:00Z"


def test_watermark_defaults_created_at_using_helper() -> None:
    with patch("core.foundation.state.watermark._utc_isoformat", return_value=MOCK_TIMESTAMP):
        watermark = Watermark(
            source_key="sys.tbl",
            watermark_column="updated_at",
        )

    assert watermark.created_at == MOCK_TIMESTAMP


def test_watermark_update_sets_fields_and_timestamp() -> None:
    watermark = Watermark(
        source_key="sys.tbl",
        watermark_column="updated_at",
        watermark_value="2024-01-01T00:00:00Z",
    )

    with patch("core.foundation.state.watermark._utc_isoformat", return_value=MOCK_TIMESTAMP):
        watermark.update(
            new_value="2024-01-02T00:00:00Z",
            run_id="run-123",
            run_date=date(2024, 1, 2),
            record_count=42,
        )

    assert watermark.watermark_value == "2024-01-02T00:00:00Z"
    assert watermark.last_run_id == "run-123"
    assert watermark.last_run_date == "2024-01-02"
    assert watermark.record_count == 42
    assert watermark.updated_at == MOCK_TIMESTAMP


def test_watermark_compare_handles_timestamp_and_integer() -> None:
    timestamp_wm = Watermark(
        source_key="sys.tbl",
        watermark_column="updated_at",
        watermark_value="2024-01-01T00:00:00Z",
    )
    assert timestamp_wm.compare("2023-12-31T23:59:59Z") == -1
    assert timestamp_wm.compare("2024-01-01T00:00:00Z") == 0
    assert timestamp_wm.compare("2024-01-02T00:00:00Z") == 1

    integer_wm = Watermark(
        source_key="sys.tbl",
        watermark_column="seqnum",
        watermark_value="10",
        watermark_type=WatermarkType.INTEGER,
    )
    assert integer_wm.compare("5") == -1
    assert integer_wm.compare("10") == 0
    assert integer_wm.compare("15") == 1


def test_watermark_to_dict_and_from_dict_roundtrip() -> None:
    watermark = Watermark(
        source_key="sys.tbl",
        watermark_column="updated_at",
        watermark_value="2024-01-01T00:00:00Z",
        last_run_id="run-123",
        last_run_date="2024-01-01",
        record_count=5,
        extra={"tags": ["prod"]},
    )

    payload = watermark.to_dict()
    restored = Watermark.from_dict(payload)

    assert restored.source_key == watermark.source_key
    assert restored.extra == {"tags": ["prod"]}


def test_watermark_type_normalize_accepts_variants_and_rejects_invalid() -> None:
    assert WatermarkType.normalize("timestamp") == WatermarkType.TIMESTAMP
    assert WatermarkType.normalize(WatermarkType.DATE) == WatermarkType.DATE
    assert WatermarkType.normalize("DaTe") == WatermarkType.DATE
    with pytest.raises(ValueError):
        WatermarkType.normalize("unknown")
