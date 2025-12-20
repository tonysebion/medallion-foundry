import ibis
import pandas as pd
import pytest

from pipelines.lib.bronze import BronzeSource, SourceType


def _make_source(tmp_path, **overrides):
    config = {
        "system": "erp",
        "entity": "orders",
        "source_type": SourceType.FILE_CSV,
        "source_path": "data_{run_date}.csv",
        "target_path": str(tmp_path / "bronze"),
    }
    config.update(overrides)
    return BronzeSource(**config)


def test_merge_top_level_params_overrides_options(tmp_path):
    source = _make_source(
        tmp_path,
        options={
            "connection_name": "old_conn",
            "host": "old_host",
            "database": "old_db",
            "query": "SELECT 1",
        },
        connection="top_conn",
        host="top_host",
        database="top_db",
        query="SELECT 2",
    )

    assert source.options["connection_name"] == "top_conn"
    assert source.options["host"] == "top_host"
    assert source.options["database"] == "top_db"
    assert source.options["query"] == "SELECT 2"


def test_read_character_delimited_default_whitespace(tmp_path):
    source = _make_source(
        tmp_path,
        source_type=SourceType.FILE_SPACE_DELIMITED,
        source_path=str(tmp_path / "space.txt"),
        options={"csv_options": {}},
    )

    file_path = tmp_path / "space.txt"
    file_path.write_text("id amount\n1 10\n2 20\n")

    table = source._read_character_delimited(str(file_path))
    assert list(table.columns) == ["id", "amount"]

    result = table.execute()
    assert result["id"].tolist() == [1, 2]


def test_read_fixed_width_reads_columns(tmp_path):
    source = _make_source(
        tmp_path,
        source_type=SourceType.FILE_FIXED_WIDTH,
        source_path=str(tmp_path / "fixed.txt"),
        options={
            "columns": ["code", "value"],
            "widths": [2, 3],
        },
    )

    file_path = tmp_path / "fixed.txt"
    file_path.write_text("12abc\n34xyz\n")

    table = source._read_fixed_width(str(file_path))
    assert set(table.columns) == {"code", "value"}

    result = table.execute()
    assert result["code"].astype(str).tolist() == ["12", "34"]
    assert result["value"].tolist() == ["abc", "xyz"]


def test_read_fixed_width_requires_widths(tmp_path):
    source = _make_source(
        tmp_path,
        source_type=SourceType.FILE_FIXED_WIDTH,
        source_path=str(tmp_path / "fixed.txt"),
        options={
            "columns": ["code", "value"],
            "widths": [2, 3],
        },
    )

    source.options = {}
    source.options["csv_options"] = {}
    file_path = tmp_path / "fixed.txt"
    file_path.write_text("12abc\n")

    with pytest.raises(ValueError, match="Fixed-width files require"):
        source._read_fixed_width(str(file_path))


def test_get_max_watermark_returns_latest(tmp_path):
    source = _make_source(tmp_path)
    source.watermark_column = "wm"

    df = pd.DataFrame({"wm": ["2025-01-01", "2025-02-01"]})
    table = ibis.memtable(df)

    assert source._get_max_watermark(table) == "2025-02-01"


def test_get_max_watermark_missing_column(tmp_path):
    source = _make_source(tmp_path)
    source.watermark_column = "missing"

    df = pd.DataFrame({"wm": ["2025-01-01"]})
    table = ibis.memtable(df)

    assert source._get_max_watermark(table) is None
