"""External catalog integration for bronze-foundry (L0 Foundation).

This package contains integrations with external systems:
- client: OpenMetadata API client (types and base client)
- hooks: Catalog notification hooks (schema, lineage, quality)
- webhooks: Webhook notifications for lifecycle events
- tracing: OpenTelemetry tracing wrapper

Note: yaml_generator has been moved to core.domain.catalog because it
depends on L1 modules (core.platform.om).
"""

from .hooks import (
    notify_catalog,
    report_schema_snapshot,
    report_quality_snapshot,
    report_run_metadata,
    report_lineage,
    report_quality_rule_results,
    report_dataset_registered,
)
from .webhooks import fire_webhooks
from .tracing import trace_span

# Client is optional - import explicitly if needed
# from .client import OpenMetadataClient

__all__ = [
    # Catalog hooks
    "notify_catalog",
    "report_schema_snapshot",
    "report_quality_snapshot",
    "report_run_metadata",
    "report_lineage",
    "report_quality_rule_results",
    "report_dataset_registered",
    # Webhooks
    "fire_webhooks",
    # Tracing
    "trace_span",
]
