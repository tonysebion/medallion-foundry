"""Public SilverArtifactWriter interface and default implementation."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Protocol

import pandas as pd

from core.domain.services.pipelines.silver.io import (
    write_silver_outputs as _artifact_write_silver_outputs,
    WriteConfig,
)

logger = logging.getLogger(__name__)


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
        write_cfg = WriteConfig(
            write_parquet=write_parquet,
            write_csv=write_csv,
            parquet_compression=parquet_compression,
        )
        outputs = _artifact_write_silver_outputs(
            df,
            primary_keys,
            order_column,
            write_cfg,
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
        write_cfg = WriteConfig(
            write_parquet=write_parquet,
            write_csv=write_csv,
            parquet_compression=parquet_compression,
        )
        staging = Path(output_dir) / "_staging"
        staging.mkdir(parents=True, exist_ok=True)
        outputs = _artifact_write_silver_outputs(
            df,
            primary_keys,
            order_column,
            write_cfg,
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

        # Best-effort staging directory cleanup with logging
        try:
            if staging.exists() and staging.is_dir():
                remaining = list(staging.iterdir())
                if remaining:
                    logger.debug(
                        "Staging directory %s has %d leftover items",
                        staging,
                        len(remaining),
                    )
                else:
                    staging.rmdir()
                    logger.debug("Removed empty staging directory %s", staging)
        except Exception as e:
            logger.warning("Failed to clean staging directory %s: %s", staging, e)

        return final_outputs


def get_silver_writer(kind: str | None = None) -> SilverArtifactWriter:
    if kind == "transactional":
        return TransactionalSilverArtifactWriter()
    return DefaultSilverArtifactWriter()
