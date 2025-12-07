"""Azure Blob Storage / ADLS Gen2 backend for medallion-foundry."""

from __future__ import annotations

import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from .base import BaseCloudStorage, HealthCheckResult

logger = logging.getLogger(__name__)


class AzureStorage(BaseCloudStorage):
    """Azure Blob Storage / ADLS Gen2 backend.

    Inherits resilience patterns (circuit breakers, retry) from BaseCloudStorage.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize Azure storage backend from configuration.

        Args:
            config: Platform configuration dictionary containing:
                - bronze.azure_container: Azure container name
                - bronze.azure_prefix: Optional prefix for all paths
                - azure_connection: Connection configuration
        """
        bronze_cfg = config.get("bronze", {})
        azure_cfg = config.get("azure_connection", {})

        self.container_name = bronze_cfg.get("azure_container")
        if not self.container_name:
            raise ValueError("bronze.azure_container is required for Azure storage")

        self.prefix = bronze_cfg.get("azure_prefix", "").strip("/")
        self._client = self._build_client(azure_cfg)
        self.container: ContainerClient = self._client.get_container_client(
            self.container_name
        )

        try:
            self.container.get_container_properties()
        except ResourceNotFoundError:
            logger.info("Creating Azure container %s", self.container_name)
            self.container.create_container()
        except AzureError as exc:
            logger.warning(
                "Unable to verify Azure container %s: %s", self.container_name, exc
            )

        # Initialize circuit breakers from base class
        super().__init__()

    def _build_client(self, azure_cfg: Dict[str, Any]) -> BlobServiceClient:
        """Build the BlobServiceClient using the first available credential."""
        conn_env = azure_cfg.get("connection_string_env")
        if conn_env:
            conn = os.environ.get(conn_env)
            if conn:
                logger.debug("Azure storage using connection string from %s", conn_env)
                return BlobServiceClient.from_connection_string(conn)

        account_env = azure_cfg.get("account_name_env")
        key_env = azure_cfg.get("account_key_env")
        account_url: str | None = None
        if account_env and key_env:
            account = os.environ.get(account_env)
            key = os.environ.get(key_env)
            if account and key:
                account_url = f"https://{account}.blob.core.windows.net"
                logger.debug("Azure storage using account key for %s", account_url)
                return BlobServiceClient(account_url=account_url, credential=key)

        logger.debug("Azure storage using DefaultAzureCredential")
        account_url_any = azure_cfg.get("account_url")
        if account_url_any:
            account_url = str(account_url_any)
        if account_url:
            return BlobServiceClient(
                account_url=account_url, credential=DefaultAzureCredential()
            )
        raise ValueError(
            "azure.account_url must be provided in config when connection_string or account_key are not used"
        )

    def get_backend_type(self) -> str:
        """Get backend type identifier."""
        return "azure"

    def _build_remote_path(self, remote_path: str) -> str:
        """Build full blob path from remote path and prefix."""
        clean = remote_path.lstrip("/")
        return f"{self.prefix}/{clean}" if self.prefix else clean

    def _should_retry(self, exc: BaseException) -> bool:
        """Determine if an Azure exception should trigger a retry."""
        if isinstance(exc, ResourceNotFoundError):
            return False
        return isinstance(exc, AzureError)

    def _do_upload(self, local_path: str, remote_key: str) -> bool:
        """Perform the actual Azure upload."""
        with open(local_path, "rb") as handle:
            self.container.upload_blob(remote_key, handle, overwrite=True)
        logger.info(
            "Uploaded %s to azure://%s/%s",
            Path(local_path).name,
            self.container_name,
            remote_key,
        )
        return True

    def _do_download(self, remote_key: str, local_path: str) -> bool:
        """Perform the actual Azure download."""
        blob = self.container.get_blob_client(remote_key)
        with open(local_path, "wb") as handle:
            stream = blob.download_blob()
            stream.readinto(handle)
        logger.info(
            "Downloaded azure://%s/%s to %s",
            self.container_name,
            remote_key,
            local_path,
        )
        return True

    def _do_list(self, prefix: str) -> List[str]:
        """Perform the actual Azure list operation."""
        files = [blob.name for blob in self.container.list_blobs(name_starts_with=prefix)]
        logger.debug("Listed %d files with prefix '%s'", len(files), prefix)
        return files

    def _do_delete(self, remote_key: str) -> bool:
        """Perform the actual Azure delete."""
        self.container.delete_blob(remote_key)
        logger.info("Deleted azure://%s/%s", self.container_name, remote_key)
        return True

    def health_check(self) -> HealthCheckResult:
        """Verify Azure Blob Storage connectivity and permissions.

        Checks:
        - Container exists and is accessible
        - Write permission (upload test blob)
        - Read permission (download test blob)
        - List permission (list blobs)
        - Delete permission (delete test blob)

        Returns:
            HealthCheckResult with permission checks, capabilities, and any errors
        """
        errors: List[str] = []
        permissions: Dict[str, bool] = {
            "read": False,
            "write": False,
            "list": False,
            "delete": False,
        }
        capabilities: Dict[str, bool] = {
            "versioning": False,
            "multipart_upload": True,  # Azure always supports block blobs
        }

        start_time = time.monotonic()
        test_blob_name = self._build_remote_path(f"_health_check_{uuid.uuid4().hex}.tmp")
        test_content = b"health_check_test"

        try:
            # Check container exists
            try:
                self.container.get_container_properties()
            except ResourceNotFoundError:
                errors.append(f"Container '{self.container_name}' not found")
                return HealthCheckResult(
                    is_healthy=False,
                    errors=errors,
                    capabilities=capabilities,
                    checked_permissions=permissions,
                    latency_ms=(time.monotonic() - start_time) * 1000,
                )
            except AzureError as e:
                errors.append(f"Container access failed: {type(e).__name__}")
                return HealthCheckResult(
                    is_healthy=False,
                    errors=errors,
                    capabilities=capabilities,
                    checked_permissions=permissions,
                    latency_ms=(time.monotonic() - start_time) * 1000,
                )

            # Test write permission
            try:
                self.container.upload_blob(test_blob_name, test_content, overwrite=True)
                permissions["write"] = True
            except AzureError as e:
                errors.append(f"Write permission failed: {type(e).__name__}")

            # Test read permission
            if permissions["write"]:
                try:
                    blob = self.container.get_blob_client(test_blob_name)
                    content = blob.download_blob().readall()
                    permissions["read"] = content == test_content
                    if not permissions["read"]:
                        errors.append("Read content mismatch")
                except AzureError as e:
                    errors.append(f"Read permission failed: {type(e).__name__}")

            # Test list permission
            try:
                list(self.container.list_blobs(name_starts_with=self._build_remote_path("_health_check_")))
                permissions["list"] = True
            except AzureError as e:
                errors.append(f"List permission failed: {type(e).__name__}")

            # Test delete permission
            if permissions["write"]:
                try:
                    self.container.delete_blob(test_blob_name)
                    permissions["delete"] = True
                except AzureError as e:
                    errors.append(f"Delete permission failed: {type(e).__name__}")

        except AzureError as e:
            errors.append(f"Azure connection error: {type(e).__name__}")
        except Exception as e:
            errors.append(f"Unexpected error during health check: {e}")

        latency_ms = (time.monotonic() - start_time) * 1000
        is_healthy = all(permissions.values()) and len(errors) == 0

        return HealthCheckResult(
            is_healthy=is_healthy,
            capabilities=capabilities,
            errors=errors,
            latency_ms=latency_ms,
            checked_permissions=permissions,
        )

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to Azure Blob Storage.

        Args:
            local_path: Path to local file
            remote_path: Destination path (relative to container/prefix)

        Returns:
            True if upload succeeded

        Raises:
            AzureError: If upload fails after retries
        """
        remote_key = self._build_remote_path(remote_path)
        try:
            return self._execute_with_resilience(
                lambda: self._do_upload(local_path, remote_key),
                "azure_upload",
                breaker_key="upload",
                retry_if=self._should_retry,
            )
        except AzureError as exc:
            logger.error("Azure upload failed [%s]: %s", remote_key, exc)
            raise

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from Azure Blob Storage.

        Args:
            remote_path: Path in Azure (relative to container/prefix)
            local_path: Destination path on local filesystem

        Returns:
            True if download succeeded

        Raises:
            AzureError: If download fails after retries
        """
        remote_key = self._build_remote_path(remote_path)
        try:
            return self._execute_with_resilience(
                lambda: self._do_download(remote_key, local_path),
                "azure_download",
                breaker_key="download",
                retry_if=self._should_retry,
            )
        except AzureError as exc:
            logger.error("Azure download failed [%s]: %s", remote_key, exc)
            raise

    def list_files(self, prefix: str) -> List[str]:
        """List files in Azure Blob Storage with given prefix.

        Args:
            prefix: Path prefix to filter files

        Returns:
            List of blob names matching the prefix

        Raises:
            AzureError: If listing fails
        """
        full_prefix = self._build_remote_path(prefix)
        try:
            return self._execute_with_resilience(
                lambda: self._do_list(full_prefix),
                "azure_list",
                breaker_key="list",
                retry_if=self._should_retry,
            )
        except AzureError as exc:
            logger.error("Azure list failed [%s]: %s", full_prefix, exc)
            raise

    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from Azure Blob Storage.

        Args:
            remote_path: Path to file in Azure

        Returns:
            True if deletion succeeded

        Raises:
            AzureError: If deletion fails after retries
        """
        remote_key = self._build_remote_path(remote_path)
        try:
            return self._execute_with_resilience(
                lambda: self._do_delete(remote_key),
                "azure_delete",
                breaker_key="delete",
                retry_if=self._should_retry,
            )
        except AzureError as exc:
            logger.error("Azure delete failed [%s]: %s", remote_key, exc)
            raise


class AzureStorageBackend(AzureStorage):
    """Backward-compatible wrapper expected by integration tests."""

    def upload(self, local_path: Path | str, remote_path: str) -> bool:
        return self.upload_file(str(local_path), remote_path)

    def download(self, remote_path: str, local_path: Path | str) -> bool:
        return self.download_file(remote_path, str(local_path))

    def delete(self, remote_path: str) -> bool:
        return self.delete_file(remote_path)
