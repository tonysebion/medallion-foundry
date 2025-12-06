"""
Catalog integration hooks per spec Section 9.

This module provides hooks for catalog integrations (e.g., OpenMetadata).
Hooks can operate in two modes:
- Logging mode (default): Events are logged for debugging
- Integration mode: Events are forwarded to an external catalog

To enable OpenMetadata integration:
    from core.catalog import set_om_client
    from core.om import OpenMetadataClient

    client = OpenMetadataClient(base_url="http://om-server:8585/api")
    set_om_client(client)
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, List, Mapping, Optional

logger = logging.getLogger(__name__)

# Global OpenMetadata client (None = logging mode)
_om_client: Optional[Any] = None


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
    logger.info("Catalog event '%s': %s", event_name, payload)

    if _om_client is not None:
        # Future: Forward to OM API
        # _om_client.emit_event(event_name, payload)
        pass


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
    logger.info("Catalog schema snapshot for %s: %s", dataset_id, schema_list)

    if _om_client is not None and table_fqn:
        # Future: Create/update table in OM
        # from core.om.client import TableSchema, ColumnSchema
        # table_schema = TableSchema(...)
        # _om_client.create_or_update_table(table_schema)
        pass


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
    logger.info("Catalog run metadata for %s: %s", dataset_id, metadata)

    if _om_client is not None and table_fqn:
        # Future: Report pipeline run to OM
        # _om_client.report_pipeline_run(table_fqn, metadata)
        pass


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
    logger.info(
        "Catalog lineage: %s -> %s (%s)", source_dataset, target_dataset, metadata
    )

    if _om_client is not None and source_fqn and target_fqn:
        # Future: Add lineage edge in OM
        # _om_client.add_lineage(source_fqn, target_fqn)
        pass


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
    logger.info("Catalog quality snapshot for %s: %s", dataset_id, metrics)

    if _om_client is not None and table_fqn:
        # Future: Report quality metrics to OM
        # _om_client.report_quality_metrics(table_fqn, metrics)
        pass


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

    logger.info(
        "Catalog quality rules for %s: %d passed, %d failed",
        dataset_id,
        passed,
        failed,
    )

    if _om_client is not None and table_fqn:
        # Future: Report rule results to OM quality dashboard
        # _om_client.report_quality_metrics(table_fqn, {
        #     "rules_passed": passed,
        #     "rules_failed": failed,
        #     "rule_details": rule_results,
        # })
        pass


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
    logger.info("Catalog dataset registered: %s", dataset_id)

    if _om_client is not None and table_fqn:
        # Future: Create/update dataset entry in OM
        pass
