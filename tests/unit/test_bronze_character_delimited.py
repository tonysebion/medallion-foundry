"""Unit tests covering character-delimited and fixed-width Bronze ingestion."""

from pathlib import Path

import pandas as pd

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType


def _write_space_file(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_bronze_reads_multi_space_default(tmp_path: Path):
    source_file = _write_space_file(
        tmp_path / "input" / "transactions.txt",
        "txn_id  account_id   amount\n"
        "1  A1  100.00\n"
        "2   A2   250.50\n",
    )

    bronze = BronzeSource(
        system="legacy",
        entity="transactions",
        source_type=SourceType.FILE_SPACE_DELIMITED,
        source_path=str(source_file),
        target_path=str(tmp_path / "bronze" / "system={system}" / "entity={entity}" / "dt={run_date}"),
        load_pattern=LoadPattern.FULL_SNAPSHOT,
    )

    result = bronze.run("2025-01-15")

    assert result["row_count"] == 2
    output = tmp_path / "bronze" / "system=legacy" / "entity=transactions" / "dt=2025-01-15" / "transactions.parquet"
    df = pd.read_parquet(output)
    assert len(df) == 2
    assert set(df["txn_id"]) == {1, 2}
    assert df["amount"].dtype == float


def test_bronze_respects_custom_delimiter(tmp_path: Path):
    source_file = _write_space_file(
        tmp_path / "input" / "transactions_pipe.txt",
        "txn_id|account_id|amount\n"
        "7|B1|12.34\n"
        "8|B2|56.78\n",
    )

    bronze = BronzeSource(
        system="legacy",
        entity="pipe_txn",
        source_type=SourceType.FILE_SPACE_DELIMITED,
        source_path=str(source_file),
        target_path=str(tmp_path / "bronze" / "system={system}" / "entity={entity}" / "dt={run_date}"),
        load_pattern=LoadPattern.FULL_SNAPSHOT,
        options={"csv_options": {"delimiter": "|"}},
    )

    result = bronze.run("2025-01-15")

    assert result["row_count"] == 2
    output = tmp_path / "bronze" / "system=legacy" / "entity=pipe_txn" / "dt=2025-01-15" / "pipe_txn.parquet"
    df = pd.read_parquet(output)
    assert len(df) == 2
    assert list(df["account_id"]) == ["B1", "B2"]


def test_bronze_reads_fixed_width_synonyms(tmp_path: Path):
    source_file = _write_space_file(
        tmp_path / "input" / "fixed_width.txt",
        "A12345\n"
        "B67890\n",
    )

    bronze = BronzeSource(
        system="legacy",
        entity="fixed",
        source_type=SourceType.FILE_SPACE_DELIMITED,
        source_path=str(source_file),
        target_path=str(tmp_path / "bronze" / "system={system}" / "entity={entity}" / "dt={run_date}"),
        load_pattern=LoadPattern.FULL_SNAPSHOT,
        options={
            "csv_options": {
                "column_names": ["code", "value"],
                "field_widths": [1, 5],
            }
        },
    )

    result = bronze.run("2025-01-15")

    assert result["row_count"] == 2
    output = tmp_path / "bronze" / "system=legacy" / "entity=fixed" / "dt=2025-01-15" / "fixed.parquet"
    df = pd.read_parquet(output)
    assert list(df["code"].astype(str)) == ["A", "B"]
    assert list(df["value"].astype(str)) == ["12345", "67890"]


def test_bronze_reads_fixed_width_colspec_alias(tmp_path: Path):
    source_file = _write_space_file(
        tmp_path / "input" / "fixed_width2.txt",
        "CabcdeX12\n"
        "DvwxyzY34\n",
    )

    bronze = BronzeSource(
        system="legacy",
        entity="fixed2",
        source_type=SourceType.FILE_SPACE_DELIMITED,
        source_path=str(source_file),
        target_path=str(tmp_path / "bronze" / "system={system}" / "entity={entity}" / "dt={run_date}"),
        load_pattern=LoadPattern.FULL_SNAPSHOT,
        options={
            "csv_options": {
                "columns": ["category", "payload", "code"],
                "column_specs": [(0, 1), (1, 6), (6, 9)],
            }
        },
    )

    result = bronze.run("2025-01-15")

    assert result["row_count"] == 2
    output = tmp_path / "bronze" / "system=legacy" / "entity=fixed2" / "dt=2025-01-15" / "fixed2.parquet"
    df = pd.read_parquet(output)
    assert list(df["category"].astype(str)) == ["C", "D"]
    assert list(df["payload"].astype(str)) == ["abcde", "vwxyz"]
    assert list(df["code"].astype(str)) == ["X12", "Y34"]
