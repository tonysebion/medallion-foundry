from __future__ import annotations

from .azure_storage import AzureStorage
from .local_storage import LocalStorage
from .s3 import S3Storage

__all__ = ["AzureStorage", "LocalStorage", "S3Storage"]
