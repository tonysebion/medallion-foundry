"""OpenMetadata integration module per spec Section 9.

This module provides:
- OpenMetadata API client (stub for future implementation)
- YAML skeleton generation from OM schema
- Catalog hooks ready for OM write-back

Note: Full OM integration is planned for future phase per spec.
Current implementation provides:
- Schema pull from OM → YAML skeleton generation
- Placeholder hooks for OM write-back
"""

from core.om.client import OpenMetadataClient
from core.om.yaml_generator import generate_yaml_skeleton

__all__ = [
    "OpenMetadataClient",
    "generate_yaml_skeleton",
]
