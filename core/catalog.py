"""
Placeholder catalog integration hooks.

Future implementations can call external services (e.g., OpenMetadata) from here.
"""

from __future__ import annotations

import logging
from typing import Iterable, Mapping, Any

logger = logging.getLogger(__name__)


def notify_catalog(event_name: str, payload: Mapping[str, Any]) -> None:
    """
    Lightweight hook for emitting catalog events.

    Currently this only logs at INFO level so operators can verify the payload.
    Downstream integrations can replace this function with actual API calls.
    """
    logger.info("Catalog event '%s': %s", event_name, payload)


def report_schema_snapshot(
    dataset_id: str, schema: Iterable[Mapping[str, Any]]
) -> None:
    """
    Future hook to push schema snapshots to a catalog (e.g., OpenMetadata).

    For now this simply logs the schema in case operators want to build off it later.
    """
    logger.info("Catalog schema snapshot for %s: %s", dataset_id, list(schema))


def report_run_metadata(dataset_id: str, metadata: Mapping[str, Any]) -> None:
    """Hook to surface run-level metadata such as batch timestamps or record counts."""
    logger.info("Catalog run metadata for %s: %s", dataset_id, metadata)


def report_lineage(
    source_dataset: str, target_dataset: str, metadata: Mapping[str, Any]
) -> None:
    """Hook to emit lineage between datasets (Bronze -> Silver)."""
    logger.info(
        "Catalog lineage: %s -> %s (%s)", source_dataset, target_dataset, metadata
    )


def report_quality_snapshot(dataset_id: str, metrics: Mapping[str, Any]) -> None:
    """Hook to report data quality metrics (placeholder)."""
    logger.info("Catalog quality snapshot for %s: %s", dataset_id, metrics)
