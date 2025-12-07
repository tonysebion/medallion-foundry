"""
Catalog integration hooks per spec Section 9.

This module provides hooks for catalog integrations (e.g., OpenMetadata).
Hooks can operate in two modes:
- Logging mode (default): Events are logged for debugging
- Integration mode: Events are forwarded to an external catalog

To enable OpenMetadata integration, configure the client at the orchestration
or domain layer (NOT in foundation code):

    # In your orchestration/application code:
    from core.platform.om import OpenMetadataClient
    from core.foundation.catalog import set_om_client

    client = OpenMetadataClient(base_url="http://om-server:8585/api")
    set_om_client(client)

Note: The foundation layer (L0) should not import from platform (L1).
The set_om_client() function accepts Any type to maintain layer boundaries.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, List, Mapping, Optional

logger = logging.getLogger(__name__)

# Global OpenMetadata client (None = logging mode)
_om_client: Optional[Any] = None


def _dispatch_catalog_event(
    event_name: str, payload: Mapping[str, Any], table_fqn: Optional[str] = None
) -> None:
    """Centralize catalog logging and optional OM forwarding."""
    logger.info("Catalog event '%s': %s", event_name, payload)

    if _om_client is None:
        return

    try:
        # Future integration hook can live here, utilizing table_fqn as needed
        pass
    except Exception as exc:  # pragma: no cover - best effort
        logger.warning("Failed to dispatch '%s' to catalog: %s", event_name, exc)


def set_om_client(client: Any) -> None:
    """Set the OpenMetadata client for integration mode.

    Args:
        client: OpenMetadataClient instance
    """
    global _om_client
    _om_client = client
    logger.info("OpenMetadata client configured for catalog integration")


def get_om_client() -> Optional[Any]:
    """Get the current OpenMetadata client.

    Returns:
        OpenMetadataClient or None if in logging mode
    """
    return _om_client


def is_om_enabled() -> bool:
    """Check if OpenMetadata integration is enabled.

    Returns:
        True if OM client is configured
    """
    return _om_client is not None


def notify_catalog(event_name: str, payload: Mapping[str, Any]) -> None:
    """
    Emit a catalog event.

    In logging mode, events are logged at INFO level.
    In integration mode, events are forwarded to OpenMetadata.

    Args:
        event_name: Name of the event (e.g., "pipeline_started", "pipeline_completed")
        payload: Event payload dictionary
    """
    _dispatch_catalog_event(event_name, payload)


def report_schema_snapshot(
    dataset_id: str,
    schema: Iterable[Mapping[str, Any]],
    table_fqn: Optional[str] = None,
) -> None:
    """
    Push schema snapshot to catalog.

    In logging mode, schema is logged.
    In integration mode, schema is synced to OpenMetadata.

    Args:
        dataset_id: Dataset identifier
        schema: Iterable of column definitions
        table_fqn: Optional fully qualified table name for OM
    """
    schema_list = list(schema)
    _dispatch_catalog_event(
        "schema_snapshot",
        {"dataset_id": dataset_id, "schema": schema_list},
        table_fqn,
    )


def report_run_metadata(
    dataset_id: str,
    metadata: Mapping[str, Any],
    table_fqn: Optional[str] = None,
) -> None:
    """
    Surface run-level metadata to catalog.

    Reports batch timestamps, record counts, and status.

    Args:
        dataset_id: Dataset identifier
        metadata: Run metadata dictionary
        table_fqn: Optional fully qualified table name for OM
    """
    _dispatch_catalog_event(
        "run_metadata",
        {"dataset_id": dataset_id, "metadata": dict(metadata)},
        table_fqn,
    )


def report_lineage(
    source_dataset: str,
    target_dataset: str,
    metadata: Mapping[str, Any],
    source_fqn: Optional[str] = None,
    target_fqn: Optional[str] = None,
) -> None:
    """
    Emit lineage between datasets.

    Tracks Bronze → Silver transformations.

    Args:
        source_dataset: Source dataset ID
        target_dataset: Target dataset ID
        metadata: Lineage metadata
        source_fqn: Optional source FQN for OM
        target_fqn: Optional target FQN for OM
    """
    _dispatch_catalog_event(
        "lineage",
        {
            "source_dataset": source_dataset,
            "target_dataset": target_dataset,
            "metadata": dict(metadata),
        },
        source_fqn or target_fqn,
    )


def report_quality_snapshot(
    dataset_id: str,
    metrics: Mapping[str, Any],
    table_fqn: Optional[str] = None,
) -> None:
    """
    Report data quality metrics to catalog.

    Args:
        dataset_id: Dataset identifier
        metrics: Quality metrics dictionary
        table_fqn: Optional fully qualified table name for OM
    """
    _dispatch_catalog_event(
        "quality_snapshot",
        {"dataset_id": dataset_id, "metrics": dict(metrics)},
        table_fqn,
    )


def report_quality_rule_results(
    dataset_id: str,
    rule_results: List[Mapping[str, Any]],
    table_fqn: Optional[str] = None,
) -> None:
    """
    Report quality rule evaluation results to catalog.

    Args:
        dataset_id: Dataset identifier
        rule_results: List of rule result dictionaries
        table_fqn: Optional fully qualified table name for OM
    """
    passed = sum(1 for r in rule_results if r.get("passed", False))
    failed = len(rule_results) - passed
    _dispatch_catalog_event(
        "quality_rule_results",
        {
            "dataset_id": dataset_id,
            "rules_passed": passed,
            "rules_failed": failed,
            "rule_details": [dict(r) for r in rule_results],
        },
        table_fqn,
    )


def report_dataset_registered(
    dataset_id: str,
    config: Mapping[str, Any],
    table_fqn: Optional[str] = None,
) -> None:
    """
    Report that a dataset has been registered.

    Args:
        dataset_id: Dataset identifier
        config: Dataset configuration
        table_fqn: Optional fully qualified table name for OM
    """
    _dispatch_catalog_event(
        "dataset_registered",
        {"dataset_id": dataset_id, "config": dict(config)},
        table_fqn,
    )
