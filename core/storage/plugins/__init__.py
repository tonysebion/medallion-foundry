from __future__ import annotations

from typing import TYPE_CHECKING

# Always available backends
from .local_storage import LocalStorage
from .s3 import S3Storage

__all__ = ["LocalStorage", "S3Storage"]

# Optional Azure backend (requires azure-storage-blob + azure-identity)
try:
    from .azure_storage import AzureStorage

    __all__.append("AzureStorage")
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    if TYPE_CHECKING:
        from .azure_storage import AzureStorage  # noqa: F401
