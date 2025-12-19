"""Storage backend abstraction for pipelines.

Provides a unified interface for reading and writing data to different
storage backends: local filesystem, AWS S3, and Azure Data Lake Storage (ADLS).

Usage:
    from pipelines.lib.storage import get_storage

    # Local filesystem
    storage = get_storage("./data/bronze/")

    # AWS S3
    storage = get_storage("s3://my-bucket/bronze/")

    # Azure ADLS
    storage = get_storage("abfss://container@account.dfs.core.windows.net/bronze/")
"""

from pipelines.lib.storage.base import StorageBackend, StorageResult
from pipelines.lib.storage.local import LocalStorage
from pipelines.lib.storage.s3 import S3Storage
from pipelines.lib.storage.adls import ADLSStorage

__all__ = [
    "StorageBackend",
    "StorageResult",
    "LocalStorage",
    "S3Storage",
    "ADLSStorage",
    "get_storage",
    "parse_uri",
]


def parse_uri(path: str) -> tuple[str, str]:
    """Parse a storage URI into scheme and path.

    Args:
        path: Storage path (local path or cloud URI)

    Returns:
        Tuple of (scheme, path) where scheme is 'local', 's3', or 'abfs'

    Examples:
        >>> parse_uri("./data/bronze/")
        ('local', './data/bronze/')
        >>> parse_uri("s3://my-bucket/bronze/")
        ('s3', 'my-bucket/bronze/')
        >>> parse_uri("abfss://container@account.dfs.core.windows.net/bronze/")
        ('abfs', 'container@account.dfs.core.windows.net/bronze/')
    """
    if path.startswith("s3://"):
        return ("s3", path[5:])
    elif path.startswith("abfss://"):
        return ("abfs", path[8:])
    elif path.startswith("wasbs://"):
        return ("abfs", path[8:])
    elif path.startswith("az://"):
        return ("abfs", path[5:])
    else:
        return ("local", path)


def get_storage(path: str, **options) -> StorageBackend:
    """Get the appropriate storage backend for a path.

    Automatically detects the storage type from the path prefix and
    returns the corresponding backend instance.

    Args:
        path: Storage path (local path or cloud URI)
        **options: Backend-specific options (credentials, etc.)

    Returns:
        StorageBackend instance for the detected storage type

    Examples:
        >>> storage = get_storage("./data/")
        >>> storage = get_storage("s3://my-bucket/data/")
        >>> storage = get_storage("abfss://container@account.dfs.core.windows.net/")

    Environment Variables:
        S3:
            AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

        ADLS:
            AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
            or AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
    """
    scheme, _ = parse_uri(path)

    if scheme == "s3":
        return S3Storage(path, **options)
    elif scheme == "abfs":
        return ADLSStorage(path, **options)
    else:
        return LocalStorage(path, **options)
