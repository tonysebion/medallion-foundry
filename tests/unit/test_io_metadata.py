from pathlib import Path

import ibis
import pandas as pd
import pytest

from pipelines.lib.io import (
    OutputMetadata,
    _get_metadata_path,
    infer_column_types,
)


def test_output_metadata_round_trip(tmp_path):
    metadata = OutputMetadata(
        row_count=3,
        columns=[{"name": "id"}],
        written_at="2025-01-15T00:00:00Z",
        extra={"custom": "value"},
    )

    data = metadata.to_dict()
    assert data["row_count"] == 3
    assert data["custom"] == "value"

    round_trip = OutputMetadata.from_dict(data)
    assert round_trip.row_count == 3
    assert round_trip.extra["custom"] == "value"

    metadata_file = tmp_path / "meta.json"
    metadata_file.write_text(metadata.to_json(), encoding="utf-8")
    from_file = OutputMetadata.from_file(metadata_file)
    assert from_file.row_count == 3
    assert from_file.extra["custom"] == "value"


def test_get_metadata_path_variants(tmp_path):
    file_path = tmp_path / "data.parquet"
    file_path.write_text("data")
    assert _get_metadata_path(str(file_path)).endswith("_metadata.json")

    dir_path = tmp_path / "bronze"
    dir_path.mkdir()
    assert _get_metadata_path(str(dir_path)) == str(dir_path / "_metadata.json")

    glob_path = "bronze/*/data.parquet"
    assert _get_metadata_path(glob_path) == str(Path("bronze") / "_metadata.json")


def test_infer_column_types_from_records_and_table(tmp_path):
    records = [{"b": 2, "a": None}, {"a": 1, "c": "x"}]
    inferred = infer_column_types(records)
    names = {column["name"] for column in inferred}
    assert names == {"a", "b", "c"}

    df = pd.DataFrame({"id": [1], "value": [3.14]})
    table = ibis.memtable(df)
    ibis_columns = infer_column_types(table)
    assert any(
        column["name"] == "id" and column.get("sql_type") == "BIGINT"
        for column in ibis_columns
    )

    with pytest.raises(TypeError):
        infer_column_types("not a table or records")
