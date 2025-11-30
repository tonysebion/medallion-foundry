from __future__ import annotations

import csv
import json
from pathlib import Path
from datetime import date

import pytest

from core.extractors.file_extractor import FileExtractor


def _build_config(path: Path, **kwargs) -> dict:
    cfg = {
        "source": {
            "file": {
                "path": str(path),
            },
            "run": {},
        }
    }
    cfg["source"]["file"].update(kwargs)
    return cfg


def test_csv_limit_and_columns(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.csv"
    rows = [
        {"id": 1, "name": "alpha", "extra": "x"},
        {"id": 2, "name": "beta", "extra": "y"},
    ]
    with file_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    cfg = _build_config(file_path, format="csv", limit_rows=1, columns=["id", "name"])
    extractor = FileExtractor()
    records, cursor = extractor.fetch_records(cfg, date.today())  # run_date unused

    assert len(records) == 1
    assert records[0] == {"id": "1", "name": "alpha"}
    assert cursor is None


def test_tsv_without_header_requires_fieldnames(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.tsv"
    file_path.write_text("1\talpha\n2\tbeta\n", encoding="utf-8")

    cfg = _build_config(
        file_path, format="tsv", has_header=False, fieldnames=["id", "name"]
    )
    extractor = FileExtractor()
    records, _ = extractor.fetch_records(cfg, date.today())

    assert records[0]["name"] == "alpha"


def test_json_path_loading(tmp_path: Path) -> None:
    payload = {"envelope": {"items": [{"id": 10}, {"id": 20}]}}
    file_path = tmp_path / "payload.json"
    file_path.write_text(json.dumps(payload), encoding="utf-8")

    cfg = _build_config(file_path, format="json", json_path="envelope.items")
    extractor = FileExtractor()
    records, _ = extractor.fetch_records(cfg, date.today())

    assert [r["id"] for r in records] == [10, 20]


def test_json_lines_loading(tmp_path: Path) -> None:
    file_path = tmp_path / "lines.jsonl"
    file_path.write_text('{"id":1}\n{"id":2}\n', encoding="utf-8")

    cfg = _build_config(file_path, format="jsonl")
    extractor = FileExtractor()
    records, _ = extractor.fetch_records(cfg, date.today())

    assert len(records) == 2


def test_invalid_format_raises(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.txt"
    file_path.write_text("", encoding="utf-8")

    cfg = _build_config(file_path, format="xml")
    extractor = FileExtractor()

    with pytest.raises(ValueError):
        extractor.fetch_records(cfg, date.today())


def test_csv_without_fieldnames_raises(tmp_path: Path) -> None:
    file_path = tmp_path / "data.tsv"
    file_path.write_text("alpha\tbeta\n", encoding="utf-8")
    cfg = _build_config(file_path, format="tsv", has_header=False)
    extractor = FileExtractor()

    with pytest.raises(ValueError):
        extractor.fetch_records(cfg, date.today())
