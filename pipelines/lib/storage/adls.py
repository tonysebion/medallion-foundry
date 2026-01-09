"""Azure Data Lake Storage (ADLS) Gen2 storage backend."""

from __future__ import annotations

import fnmatch
import logging
import os
from typing import Any, List, Optional

from pipelines.lib.storage.base import FileInfo, StorageBackend, StorageResult
from pipelines.lib.storage_config import get_config_value

logger = logging.getLogger(__name__)

__all__ = ["ADLSStorage"]


class ADLSStorage(StorageBackend):
    """Azure Data Lake Storage Gen2 storage backend.

    Provides storage operations on Azure Data Lake Storage using fsspec/adlfs.

    Supports both abfss:// and wasbs:// URI schemes.

    Example:
        >>> storage = ADLSStorage("abfss://container@account.dfs.core.windows.net/bronze/")
        >>> storage.exists("2025-01-15/data.parquet")
        True
        >>> files = storage.list_files(pattern="*.parquet")
        >>> for f in files:
        ...     print(f.path, f.size)

    URI Formats:
        abfss://container@account.dfs.core.windows.net/path
        wasbs://container@account.blob.core.windows.net/path

    Environment Variables:
        AZURE_STORAGE_ACCOUNT: Storage account name
        AZURE_STORAGE_KEY: Storage account key
        AZURE_STORAGE_CONNECTION_STRING: Full connection string

        For service principal auth:
        AZURE_CLIENT_ID: Service principal client ID
        AZURE_CLIENT_SECRET: Service principal secret
        AZURE_TENANT_ID: Azure AD tenant ID

    Options:
        account_name: Storage account name
        account_key: Storage account key
        connection_string: Full connection string
        client_id: Service principal client ID
        client_secret: Service principal secret
        tenant_id: Azure AD tenant ID
        anon: If True, use anonymous access (for public containers)
    """

    def __init__(self, base_path: str, **options: Any) -> None:
        super().__init__(base_path, **options)
        self._fs: Optional[Any] = None
        self._container: str = ""
        self._account: Optional[str] = None
        self._prefix: str = ""
        self._parse_path()

    def _parse_path(self) -> None:
        """Parse ADLS URI into account, container, and prefix."""
        path = self.base_path

        # Handle different URI schemes
        if path.startswith("abfss://"):
            path = path[8:]
        elif path.startswith("wasbs://"):
            path = path[8:]
        elif path.startswith("az://"):
            path = path[5:]

        # Format: container@account.dfs.core.windows.net/path
        # or: container@account.blob.core.windows.net/path
        if "@" in path:
            container_account, rest = path.split("/", 1) if "/" in path else (path, "")
            self._container, account_host = container_account.split("@", 1)
            self._account = account_host.split(".")[0]
            self._prefix = rest
        else:
            # Fallback: container/path format (account from env)
            parts = path.split("/", 1)
            self._container = parts[0]
            self._prefix = parts[1] if len(parts) > 1 else ""
            self._account = self.options.get("account_name") or os.environ.get("AZURE_STORAGE_ACCOUNT")

    @property
    def scheme(self) -> str:
        return "abfs"

    @property
    def fs(self):
        """Lazy-load the ADLS filesystem."""
        if self._fs is None:
            try:
                import adlfs
            except ImportError:
                raise ImportError(
                    "adlfs is required for ADLS storage. "
                    "Install with: pip install adlfs"
                )

            # Build options from environment and passed options
            # Using shared helper that handles ${VAR} expansion from YAML
            fs_options = {}

            # Account name
            account_name = get_config_value(self.options, "account_name", "AZURE_STORAGE_ACCOUNT") or self._account
            if account_name:
                fs_options["account_name"] = account_name

            # Account key
            account_key = get_config_value(self.options, "account_key", "AZURE_STORAGE_KEY")
            if account_key:
                fs_options["account_key"] = account_key

            # Connection string
            connection_string = get_config_value(self.options, "connection_string", "AZURE_STORAGE_CONNECTION_STRING")
            if connection_string:
                fs_options["connection_string"] = connection_string

            # Service principal auth
            client_id = get_config_value(self.options, "client_id", "AZURE_CLIENT_ID")
            client_secret = get_config_value(self.options, "client_secret", "AZURE_CLIENT_SECRET")
            tenant_id = get_config_value(self.options, "tenant_id", "AZURE_TENANT_ID")

            if client_id and client_secret and tenant_id:
                fs_options["client_id"] = client_id
                fs_options["client_secret"] = client_secret
                fs_options["tenant_id"] = tenant_id

            # Anonymous access
            if self.options.get("anon"):
                fs_options["anon"] = True

            self._fs = adlfs.AzureBlobFileSystem(**fs_options)

        return self._fs

    def _get_adls_path(self, path: str) -> str:
        """Get the full ADLS path (container/prefix/path)."""
        if path.startswith(("abfss://", "wasbs://", "az://")):
            # Parse and extract path portion
            parsed = path
            for prefix in ("abfss://", "wasbs://", "az://"):
                if parsed.startswith(prefix):
                    parsed = parsed[len(prefix):]
                    break
            if "@" in parsed:
                _, rest = parsed.split("/", 1) if "/" in parsed else (parsed, "")
                return f"{self._container}/{rest}"
            return parsed

        if not path:
            return f"{self._container}/{self._prefix}".rstrip("/")

        prefix = self._prefix.rstrip("/") if self._prefix else ""
        if prefix:
            return f"{self._container}/{prefix}/{path.lstrip('/')}"
        else:
            return f"{self._container}/{path.lstrip('/')}"

    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        adls_path = self._get_adls_path(path)

        # Handle glob patterns
        if "*" in adls_path or "?" in adls_path:
            try:
                matches = self.fs.glob(adls_path)
                return len(matches) > 0
            except Exception:
                return False

        try:
            result: bool = self.fs.exists(adls_path)
            return result
        except Exception as e:
            logger.warning("Error checking existence of %s: %s", adls_path, e)
            return False

    def list_files(
        self,
        path: str = "",
        pattern: Optional[str] = None,
        recursive: bool = False,
    ) -> List[FileInfo]:
        """List files at a path."""
        adls_path = self._get_adls_path(path)

        try:
            if recursive:
                items = self.fs.find(adls_path, detail=True)
            else:
                items = self.fs.ls(adls_path, detail=True)

            files: List[FileInfo] = []

            # Handle both dict and list responses
            if isinstance(items, dict):
                for key, info in items.items():
                    if info.get("type") == "file":
                        name = key.split("/")[-1]
                        if pattern and not fnmatch.fnmatch(name, pattern):
                            continue

                        files.append(
                            FileInfo(
                                path=key,
                                size=info.get("size", 0),
                                modified=info.get("last_modified"),
                            )
                        )
            else:
                for info in items:
                    if isinstance(info, dict) and info.get("type") == "file":
                        name = info.get("name", "").split("/")[-1]
                        if pattern and not fnmatch.fnmatch(name, pattern):
                            continue
                        files.append(
                            FileInfo(
                                path=info.get("name", ""),
                                size=info.get("size", 0),
                                modified=info.get("last_modified"),
                            )
                        )

            return sorted(files, key=lambda f: f.path)

        except Exception as e:
            logger.warning("Error listing %s: %s", adls_path, e)
            return []

    def read_bytes(self, path: str) -> bytes:
        """Read file contents as bytes."""
        adls_path = self._get_adls_path(path)
        with self.fs.open(adls_path, "rb") as f:
            content: bytes = f.read()
            return content

    def write_bytes(self, path: str, data: bytes) -> StorageResult:
        """Write bytes to a file."""
        adls_path = self._get_adls_path(path)

        try:
            with self.fs.open(adls_path, "wb") as f:
                f.write(data)

            return StorageResult(
                success=True,
                path=f"abfss://{self._container}@{self._account}.dfs.core.windows.net/{adls_path.split('/', 1)[1] if '/' in adls_path else ''}",
                files_written=[adls_path],
                bytes_written=len(data),
            )
        except Exception as e:
            logger.error("Failed to write %s: %s", adls_path, e)
            return StorageResult(
                success=False,
                path=adls_path,
                error=str(e),
            )

    def delete(self, path: str) -> bool:
        """Delete a file or directory."""
        adls_path = self._get_adls_path(path)

        try:
            if self.fs.isdir(adls_path):
                self.fs.rm(adls_path, recursive=True)
            else:
                self.fs.rm(adls_path)
            return True
        except Exception as e:
            logger.error("Failed to delete %s: %s", adls_path, e)
            return False

    def makedirs(self, path: str) -> None:
        """Create directories (ADLS supports hierarchical namespaces)."""
        adls_path = self._get_adls_path(path)
        try:
            self.fs.mkdirs(adls_path, exist_ok=True)
        except Exception as e:
            # Some ADLS configurations don't support explicit directory creation
            logger.debug("makedirs on %s: %s (may be expected)", adls_path, e)

    def copy(self, src: str, dst: str) -> StorageResult:
        """Copy a file (uses server-side copy for efficiency)."""
        src_path = self._get_adls_path(src)
        dst_path = self._get_adls_path(dst)

        try:
            self.fs.copy(src_path, dst_path)
            info = self.fs.info(dst_path)
            size = info.get("size", 0)

            return StorageResult(
                success=True,
                path=dst_path,
                files_written=[dst_path],
                bytes_written=size,
            )
        except Exception as e:
            logger.error("Failed to copy %s to %s: %s", src_path, dst_path, e)
            return StorageResult(
                success=False,
                path=dst_path,
                error=str(e),
            )
