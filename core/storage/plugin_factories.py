"""Built-in storage backend factories."""

from __future__ import annotations

import logging
from typing import Dict, Any

from .registry import register_backend

logger = logging.getLogger(__name__)


@register_backend("s3")
def _s3_factory(config: Dict[str, Any]):
    from core.storage.plugins.s3 import S3Storage

    return S3Storage(config)


@register_backend("gcs")
def _gcs_factory(config: Dict[str, Any]):
    from core.gcs_storage import GCSStorage

    return GCSStorage(config)


@register_backend("local")
def _local_factory(config: Dict[str, Any]):
    from core.storage.plugins.local_storage import LocalStorage

    return LocalStorage(config)


try:
    import core.storage.plugins.azure_storage  # noqa: F401 register decorator executed on import
except ImportError as exc:
    logger.debug("Azure backend not available: %s", exc)
