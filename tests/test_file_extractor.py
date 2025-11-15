"""Tests for the local file extractor."""

from datetime import date

from extractors.file_extractor import FileExtractor


def _base_config(file_config):
    return {
        "source": {
            "type": "file",
            "system": "offline",
            "table": "sample",
            "file": file_config,
            "run": {}
        }
    }


class TestFileExtractor:
    def test_reads_csv_file(self, tmp_path):
        data_file = tmp_path / "data.csv"
        data_file.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

        cfg = _base_config({"path": str(data_file), "format": "csv"})
        extractor = FileExtractor()

        records, cursor = extractor.fetch_records(cfg, date.today())

        assert cursor is None
        assert len(records) == 2
        assert records[0]["name"] == "Alice"

    def test_limit_rows_and_column_filter(self, tmp_path):
        data_file = tmp_path / "data.csv"
        data_file.write_text("id,name,city\n1,Alice,NYC\n2,Bob,SEA\n3,Eve,SF\n", encoding="utf-8")

        cfg = _base_config(
            {
                "path": str(data_file),
                "format": "csv",
                "limit_rows": 2,
                "columns": ["name"],
            }
        )
        extractor = FileExtractor()

        records, _ = extractor.fetch_records(cfg, date.today())

        assert len(records) == 2
        assert list(records[0].keys()) == ["name"]

    def test_reads_json_lines(self, tmp_path):
        data_file = tmp_path / "data.jsonl"
        data_file.write_text('{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n', encoding="utf-8")

        cfg = _base_config({"path": str(data_file), "format": "jsonl"})
        extractor = FileExtractor()

        records, _ = extractor.fetch_records(cfg, date.today())

        assert len(records) == 2
        assert records[1]["name"] == "Bob"
