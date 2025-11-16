"""Extractor that loads records from a local file.

This is primarily intended for offline development and testing scenarios
where hitting a live API or database isn't possible.
"""

import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import date

import pandas as pd

from core.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class FileExtractor(BaseExtractor):
    """Load records from a local file in CSV, TSV, JSON, JSONL, or Parquet format."""

    SUPPORTED_FORMATS = {"csv", "tsv", "json", "jsonl", "parquet"}

    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,  # noqa: ARG002 - included for interface compatibility
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        source_cfg = cfg["source"]
        file_cfg = source_cfg["file"]

        file_path = Path(file_cfg["path"]).expanduser()
        if not file_path.is_absolute():
            file_path = file_path.resolve()

        if not file_path.exists():
            raise FileNotFoundError(f"Local data file not found: {file_path}")

        file_format = file_cfg.get("format")
        if not file_format:
            file_format = file_path.suffix.lstrip(".")
        file_format = (file_format or "csv").lower()

        if file_format not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported file format '{file_format}'. "
                f"Supported formats: {sorted(self.SUPPORTED_FORMATS)}"
            )

        logger.info(f"Loading records from {file_path} as {file_format.upper()}")

        if file_format in {"csv", "tsv"}:
            delimiter = file_cfg.get("delimiter") or (
                "\t" if file_format == "tsv" else ","
            )
            records = self._read_csv(file_path, delimiter, file_cfg)
        elif file_format == "json":
            records = self._read_json(file_path, file_cfg)
        elif file_format == "jsonl":
            records = self._read_json_lines(file_path, file_cfg)
        elif file_format == "parquet":
            records = self._read_parquet(file_path, file_cfg)
        else:
            raise ValueError(f"Unsupported file format '{file_format}'")

        limit_rows = file_cfg.get("limit_rows")
        if limit_rows:
            records = records[:limit_rows]

        columns = file_cfg.get("columns")
        if columns:
            filtered = []
            for record in records:
                filtered.append({col: record.get(col) for col in columns})
            records = filtered

        logger.info(f"Loaded {len(records)} records from local file {file_path}")
        return records, None

    def _read_csv(
        self, file_path: Path, delimiter: str, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        encoding = file_cfg.get("encoding", "utf-8")
        has_header = file_cfg.get("has_header", True)
        fieldnames = file_cfg.get("fieldnames")

        if not has_header and not fieldnames:
            raise ValueError(
                "CSV/TSV files without headers require 'fieldnames' in source.file config"
            )

        with file_path.open("r", encoding=encoding, newline="") as handle:
            if has_header:
                reader = csv.DictReader(handle, delimiter=delimiter)
            else:
                reader = csv.DictReader(
                    handle, fieldnames=fieldnames, delimiter=delimiter
                )
            records = [dict(row) for row in reader]

        return records

    def _read_json(
        self, file_path: Path, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        encoding = file_cfg.get("encoding", "utf-8")
        data_path = file_cfg.get("json_path")

        with file_path.open("r", encoding=encoding) as handle:
            data = json.load(handle)

        if data_path:
            for part in data_path.split("."):
                if isinstance(data, dict):
                    data = data.get(part, [])
                else:
                    raise ValueError(
                        f"Cannot follow json_path '{data_path}' in file {file_path}"
                    )

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            return [data]

        raise ValueError(
            f"JSON file {file_path} must contain an object or list of objects"
        )

    def _read_json_lines(
        self, file_path: Path, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        encoding = file_cfg.get("encoding", "utf-8")
        records: List[Dict[str, Any]] = []

        with file_path.open("r", encoding=encoding) as handle:
            for line in handle:
                if not line.strip():
                    continue
                records.append(json.loads(line))

        return records

    def _read_parquet(
        self, file_path: Path, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        columns = file_cfg.get("columns")
        df = pd.read_parquet(file_path, columns=columns)
        return df.to_dict(orient="records")
