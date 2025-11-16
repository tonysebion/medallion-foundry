"""Public SilverArtifactWriter interface and default implementation."""
from __future__ import annotations
from typing import Protocol, Dict, Any, List
from typing import Mapping
from pathlib import Path
import pandas as pd

from core.silver.artifacts import write_silver_outputs as _artifact_write_silver_outputs

class SilverArtifactWriter(Protocol):
    def write(
        self,
        df: pd.DataFrame,
        *,
        primary_keys: List[str],
        order_column: str | None,
        write_parquet: bool,
        write_csv: bool,
        parquet_compression: str,
        artifact_names: Dict[str, str],
        partition_columns: List[str],
        error_cfg: Dict[str, Any],
        silver_model: Any,  # SilverModel
        output_dir,
        ) -> Mapping[str, List[Path]]:  # artifact label -> list of path objects
        ...

class DefaultSilverArtifactWriter:
    def write(
        self,
        df: pd.DataFrame,
        *,
        primary_keys: List[str],
        order_column: str | None,
        write_parquet: bool,
        write_csv: bool,
        parquet_compression: str,
        artifact_names: Dict[str, str],
        partition_columns: List[str],
        error_cfg: Dict[str, Any],
        silver_model: Any,
        output_dir,
    ) -> Mapping[str, List[Path]]:
        outputs = _artifact_write_silver_outputs(
            df,
            primary_keys,
            order_column,
            write_parquet,
            write_csv,
            parquet_compression,
            artifact_names,
            partition_columns,
            error_cfg,
            silver_model,
            output_dir,
        )
        return outputs

def get_silver_writer(kind: str | None = None) -> SilverArtifactWriter:
    """Factory for selecting a silver artifact writer.

    Currently only returns the default implementation; `kind` reserved
    for future extensions (e.g., transactional writers).
    """
    return DefaultSilverArtifactWriter()
