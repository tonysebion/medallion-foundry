"""Extractor that loads records from local or remote files.

Supports reading from local filesystem, S3, and other storage backends
using the fsspec streaming interface.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast, TYPE_CHECKING
from datetime import date

import pandas as pd
import fsspec

from core.extractors.base import BaseExtractor
from core.storage.uri import StorageURI
from core.storage.filesystem import create_filesystem

if TYPE_CHECKING:
    from core.config.environment import EnvironmentConfig

logger = logging.getLogger(__name__)


class FileExtractor(BaseExtractor):
    """Load records from local or remote files using streaming.

    Supports CSV, TSV, JSON, JSONL, and Parquet formats from:
    - Local filesystem
    - S3 (via s3:// URIs)
    - Azure Blob Storage (future)
    """

    SUPPORTED_FORMATS = {"csv", "tsv", "json", "jsonl", "parquet"}

    def __init__(self, env_config: Optional["EnvironmentConfig"] = None):
        """Initialize FileExtractor with optional environment config.

        Args:
            env_config: Environment configuration for S3/cloud storage credentials
        """
        self.env_config = env_config

    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,  # noqa: ARG002 - included for interface compatibility
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        source_cfg = cfg["source"]
        file_cfg = source_cfg["file"]

        # Parse storage URI (supports local paths and s3:// URIs)
        path_str = file_cfg["path"]

        # CRITICAL SAFETY CHECK: Prevent reading metadata files as data
        filename = Path(path_str).name
        if filename.startswith("_"):
            raise ValueError(
                f"Cannot read metadata file as data: {path_str}. "
                f"Files starting with '_' are reserved for metadata."
            )

        uri = StorageURI.parse(path_str)

        # Determine file format
        file_format = file_cfg.get("format")
        if not file_format:
            file_format = Path(uri.key).suffix.lstrip(".")
        file_format = (file_format or "csv").lower()

        if file_format not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported file format '{file_format}'. "
                f"Supported formats: {sorted(self.SUPPORTED_FORMATS)}"
            )

        # Create filesystem (local or S3)
        fs = create_filesystem(uri, self.env_config)
        fsspec_path = uri.to_fsspec_path(self.env_config)

        logger.info(f"Loading records from {fsspec_path} as {file_format.upper()}")

        # Read based on format using streaming
        if file_format in {"csv", "tsv"}:
            delimiter = file_cfg.get("delimiter") or (
                "\t" if file_format == "tsv" else ","
            )
            records = self._read_csv_streaming(fs, fsspec_path, delimiter, file_cfg)
        elif file_format == "json":
            records = self._read_json_streaming(fs, fsspec_path, file_cfg)
        elif file_format == "jsonl":
            records = self._read_jsonl_streaming(fs, fsspec_path, file_cfg)
        elif file_format == "parquet":
            records = self._read_parquet_streaming(fs, fsspec_path, file_cfg)
        else:
            raise ValueError(f"Unsupported file format '{file_format}'")

        # Apply filtering
        limit_rows = file_cfg.get("limit_rows")
        if limit_rows:
            records = records[:limit_rows]

        columns = file_cfg.get("columns")
        if columns:
            filtered = []
            for record in records:
                filtered.append({col: record.get(col) for col in columns})
            records = filtered

        logger.info(f"Loaded {len(records)} records from {fsspec_path}")
        return records, None

    def _read_csv_streaming(
        self,
        fs: fsspec.AbstractFileSystem,
        path: str,
        delimiter: str,
        file_cfg: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Read CSV using pandas with fsspec streaming."""
        encoding = file_cfg.get("encoding", "utf-8")
        has_header = file_cfg.get("has_header", True)
        fieldnames = file_cfg.get("fieldnames")

        if not has_header and not fieldnames:
            raise ValueError(
                "CSV/TSV files without headers require 'fieldnames' in source.file config"
            )

        # Use pandas with fsspec for streaming
        with fs.open(path, "r", encoding=encoding) as f:
            if has_header:
                df = pd.read_csv(f, delimiter=delimiter)
            else:
                df = pd.read_csv(f, delimiter=delimiter, names=fieldnames, header=None)

        return cast(List[Dict[str, Any]], df.to_dict(orient="records"))

    def _read_parquet_streaming(
        self, fs: fsspec.AbstractFileSystem, path: str, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Read Parquet using pandas with fsspec streaming."""
        columns = file_cfg.get("columns")

        # Pandas can read Parquet directly from fsspec filesystem
        with fs.open(path, "rb") as f:
            df = pd.read_parquet(f, columns=columns)

        return cast(List[Dict[str, Any]], df.to_dict(orient="records"))

    def _read_json_streaming(
        self, fs: fsspec.AbstractFileSystem, path: str, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Read JSON file via fsspec."""
        encoding = file_cfg.get("encoding", "utf-8")
        data_path = file_cfg.get("json_path")

        with fs.open(path, "r", encoding=encoding) as f:
            data = json.load(f)

        if data_path:
            for part in data_path.split("."):
                if isinstance(data, dict):
                    data = data.get(part, [])
                else:
                    raise ValueError(
                        f"Cannot follow json_path '{data_path}' in file {path}"
                    )

        if isinstance(data, list):
            return cast(List[Dict[str, Any]], data)

        if isinstance(data, dict):
            return [data]

        raise ValueError(f"JSON file {path} must contain an object or list of objects")

    def _read_jsonl_streaming(
        self, fs: fsspec.AbstractFileSystem, path: str, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Read JSON Lines file via fsspec."""
        encoding = file_cfg.get("encoding", "utf-8")
        records: List[Dict[str, Any]] = []

        with fs.open(path, "r", encoding=encoding) as f:
            for line in f:
                line_str = (
                    line.strip()
                    if isinstance(line, str)
                    else line.decode(encoding).strip()
                )
                if not line_str:
                    continue
                records.append(json.loads(line_str))

        return records

    # Legacy methods for backward compatibility (deprecated)
    def _read_csv(
        self, file_path: Path, delimiter: str, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Legacy CSV reader - use _read_csv_streaming instead."""
        fs = fsspec.filesystem("file")
        return self._read_csv_streaming(fs, str(file_path), delimiter, file_cfg)

    def _read_json(
        self, file_path: Path, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Legacy JSON reader - use _read_json_streaming instead."""
        fs = fsspec.filesystem("file")
        return self._read_json_streaming(fs, str(file_path), file_cfg)

    def _read_json_lines(
        self, file_path: Path, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Legacy JSONL reader - use _read_jsonl_streaming instead."""
        fs = fsspec.filesystem("file")
        return self._read_jsonl_streaming(fs, str(file_path), file_cfg)

    def _read_parquet(
        self, file_path: Path, file_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Legacy Parquet reader - use _read_parquet_streaming instead."""
        fs = fsspec.filesystem("file")
        return self._read_parquet_streaming(fs, str(file_path), file_cfg)
