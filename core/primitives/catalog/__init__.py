"""External catalog integration for bronze-foundry.

This package contains integrations with external systems:
- client: OpenMetadata API client
- hooks: Catalog notification hooks (schema, lineage, quality)
- webhooks: Webhook notifications for lifecycle events
- tracing: OpenTelemetry tracing wrapper
- yaml_generator: YAML manifest generation for catalog ingestion
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

# Client and YAML generator are optional - import explicitly if needed
# from .client import OpenMetadataClient
# from .yaml_generator import generate_yaml_manifest

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
