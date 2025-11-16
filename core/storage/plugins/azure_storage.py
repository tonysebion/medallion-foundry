"""Azure Blob Storage / ADLS Gen2 backend for medallion-foundry."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from core.storage.backend import StorageBackend
from core.storage.registry import register_backend
from core.retry import RetryPolicy, execute_with_retry, CircuitBreaker

logger = logging.getLogger(__name__)


@register_backend("azure")
class AzureStorage(StorageBackend):
    """Azure Blob Storage / ADLS Gen2 backend."""

    def __init__(self, config: Dict[str, Any]) -> None:
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

        # circuit breakers per operation
        def _emit(state: str) -> None:
            logger.info("metric=breaker_state component=azure_storage state=%s", state)

        self._breaker_upload = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit,
        )
        self._breaker_download = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit,
        )
        self._breaker_list = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit,
        )
        self._breaker_delete = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit,
        )

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
        if account_env and key_env:
            account = os.environ.get(account_env)
            key = os.environ.get(key_env)
            if account and key:
                account_url = f"https://{account}.blob.core.windows.net"
                logger.debug("Azure storage using account key for %s", account_url)
                return BlobServiceClient(account_url=account_url, credential=key)

        logger.debug("Azure storage using DefaultAzureCredential")
        return BlobServiceClient(
            account_url=azure_cfg.get("account_url"),
            credential=DefaultAzureCredential(),
        )

    def _remote_path(self, remote_path: str) -> str:
        clean = remote_path.lstrip("/")
        return f"{self.prefix}/{clean}" if self.prefix else clean

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        blob_path = self._remote_path(remote_path)

        def _retry_if(exc: BaseException) -> bool:
            if isinstance(exc, ResourceNotFoundError):
                return False
            return isinstance(exc, AzureError)

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),
            retry_if=_retry_if,
        )

        try:

            def _once() -> bool:
                with open(local_path, "rb") as handle:
                    self.container.upload_blob(blob_path, handle, overwrite=True)
                return True

            return execute_with_retry(
                _once,
                policy=policy,
                breaker=self._breaker_upload,
                operation_name="azure_upload",
            )
        except AzureError as exc:
            logger.error("Azure upload failed [%s]: %s", blob_path, exc)
            raise

    def download_file(self, remote_path: str, local_path: str) -> bool:
        blob_path = self._remote_path(remote_path)

        def _retry_if(exc: BaseException) -> bool:
            if isinstance(exc, ResourceNotFoundError):
                return False
            return isinstance(exc, AzureError)

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),
            retry_if=_retry_if,
        )

        try:

            def _once() -> bool:
                blob = self.container.get_blob_client(blob_path)
                with open(local_path, "wb") as handle:
                    stream = blob.download_blob()
                    stream.readinto(handle)
                return True

            return execute_with_retry(
                _once,
                policy=policy,
                breaker=self._breaker_download,
                operation_name="azure_download",
            )
        except AzureError as exc:
            logger.error("Azure download failed [%s]: %s", blob_path, exc)
            raise

    def list_files(self, prefix: str) -> List[str]:
        blob_prefix = self._remote_path(prefix)

        def _retry_if(exc: BaseException) -> bool:
            return isinstance(exc, AzureError) and not isinstance(
                exc, ResourceNotFoundError
            )

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),
            retry_if=_retry_if,
        )

        try:

            def _once() -> List[str]:
                return [
                    blob.name
                    for blob in self.container.list_blobs(name_starts_with=blob_prefix)
                ]

            return execute_with_retry(
                _once,
                policy=policy,
                breaker=self._breaker_list,
                operation_name="azure_list",
            )
        except AzureError as exc:
            logger.error("Azure list failed [%s]: %s", blob_prefix, exc)
            raise

    def delete_file(self, remote_path: str) -> bool:
        blob_path = self._remote_path(remote_path)

        def _retry_if(exc: BaseException) -> bool:
            return isinstance(exc, AzureError) and not isinstance(
                exc, ResourceNotFoundError
            )

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),
            retry_if=_retry_if,
        )

        try:

            def _once() -> bool:
                self.container.delete_blob(blob_path)
                return True

            return execute_with_retry(
                _once,
                policy=policy,
                breaker=self._breaker_delete,
                operation_name="azure_delete",
            )
        except AzureError as exc:
            logger.error("Azure delete failed [%s]: %s", blob_path, exc)
            raise

    def get_backend_type(self) -> str:
        return "azure"
