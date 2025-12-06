"""Public SilverArtifactWriter interface and default implementation."""

from __future__ import annotations
from typing import Protocol, Dict, Any, List
from typing import Mapping
from pathlib import Path
import pandas as pd

from core.pipeline.silver.artifacts import write_silver_outputs as _artifact_write_silver_outputs


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
        chunk_tag: str | None = None,
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
        chunk_tag: str | None = None,
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
            chunk_tag,
        )
        return outputs


class TransactionalSilverArtifactWriter:
    """Write artifacts atomically via temp staging directory then rename.

    Ensures readers never see partial files (best-effort on local fs).
    """

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
        chunk_tag: str | None = None,
    ) -> Mapping[str, List[Path]]:
        staging = Path(output_dir) / "_staging"
        staging.mkdir(parents=True, exist_ok=True)
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
            staging,
            chunk_tag,
        )
        final_outputs: Dict[str, List[Path]] = {}
        for label, paths in outputs.items():
            moved: List[Path] = []
            for p in paths:
                target = Path(output_dir) / p.name
                p.replace(target)
                moved.append(target)
            final_outputs[label] = moved
        try:
            # Best-effort cleanup
            for leftover in staging.iterdir():
                pass
            staging.rmdir()
        except Exception:
            pass
        return final_outputs


def get_silver_writer(kind: str | None = None) -> SilverArtifactWriter:
    if kind == "transactional":
        return TransactionalSilverArtifactWriter()
    return DefaultSilverArtifactWriter()
