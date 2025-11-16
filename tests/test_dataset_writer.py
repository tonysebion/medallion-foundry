from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from core.silver.artifacts import DatasetWriter


def _make_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "region": ["east", "west", "east"],
            "value": [10, 20, 30],
        }
    )


def test_dataset_writer_partitions(tmp_path: Path) -> None:
    df = _make_df()
    writer = DatasetWriter(
        base_dir=tmp_path,
        primary_keys=["id"],
        partition_columns=["region"],
        error_cfg={"enabled": False},
        write_parquet=False,
        write_csv=True,
        parquet_compression="snappy",
    )
    files = writer.write_dataset("dataset", df)

    assert len(files) == 2
    assert (tmp_path / "region=east" / "dataset.csv").exists()
    assert (tmp_path / "region=west" / "dataset.csv").exists()


def test_dataset_writer_chunk_suffix(tmp_path: Path) -> None:
    df = _make_df()
    writer = DatasetWriter(
        base_dir=tmp_path,
        primary_keys=["id"],
        partition_columns=[],
        error_cfg={"enabled": False},
        write_parquet=False,
        write_csv=True,
        parquet_compression="snappy",
    )
    files = writer.write_dataset_chunk("dataset", df, "chunk1")

    assert all("chunk1" in path.name for path in files)
    assert any(path.suffix == ".csv" for path in files)


def test_dataset_writer_error_threshold(tmp_path: Path) -> None:
    df = pd.DataFrame({"id": [1, None], "value": [10, 20]})
    writer = DatasetWriter(
        base_dir=tmp_path,
        primary_keys=["id"],
        partition_columns=[],
        error_cfg={"enabled": True, "max_bad_records": 0},
        write_parquet=False,
        write_csv=True,
        parquet_compression="snappy",
    )

    with pytest.raises(ValueError):
        writer.write_dataset("dataset", df)
