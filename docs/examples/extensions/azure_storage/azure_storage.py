"""Azure Blob Storage / ADLS Gen2 backend example.

This is an EXAMPLE implementation showing how to extend medallion-foundry
with Azure storage support. To use this:

1. Install dependencies:
   pip install azure-storage-blob>=12.19.0 azure-identity>=1.15.0

2. Copy this file to core/azure_storage.py

3. Update core/storage.py factory to include Azure backend

4. Configure your YAML with storage_backend: "azure"

See AZURE_STORAGE_EXTENSION.md for complete setup instructions.
"""

import os
import logging
from typing import Dict, Any, List, TYPE_CHECKING

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# Import the base class (assumes you've copied to core/ directory). Use
# TYPE_CHECKING so mypy/type-checkers see the real type, but at runtime we
# provide a fallback implementation in case the file is run in isolation.
if TYPE_CHECKING:
    from core.infrastructure.storage import StorageBackend
else:
    # Fallback for when viewing as example
    from abc import ABC, abstractmethod

    class StorageBackend(ABC):
        @abstractmethod
        def upload_file(self, local_path: str, remote_path: str) -> bool:
            pass

        @abstractmethod
        def download_file(self, remote_path: str, local_path: str) -> bool:
            pass

        @abstractmethod
        def list_files(self, prefix: str) -> List[str]:
            pass

        @abstractmethod
        def delete_file(self, remote_path: str) -> bool:
            pass

        @abstractmethod
        def get_backend_type(self) -> str:
            pass

    # At runtime, StorageBackend is the fallback implementation.


logger = logging.getLogger(__name__)


class AzureStorage(StorageBackend):
    """Azure Blob Storage / ADLS Gen2 backend.

    Supports both Azure Blob Storage and Azure Data Lake Storage Gen2 (ADLS).
    ADLS Gen2 is blob storage with hierarchical namespace enabled.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize Azure storage client from configuration.

        Args:
            config: Platform configuration dictionary containing:
                - bronze.azure_container: Container name (like S3 bucket)
                - bronze.azure_prefix: Optional prefix for all paths
                - azure_connection.connection_string_env: Environment variable for connection string
                - OR azure_connection.account_name_env + account_key_env: For account key auth
                - OR azure_connection.tenant_id_env + client_id_env + client_secret_env: For service principal
        """
        bronze_cfg = config.get("bronze", {})
        azure_cfg = config.get("azure_connection", {})

        # Storage settings
        self.container_name = bronze_cfg.get("azure_container")
        if not self.container_name:
            raise ValueError("azure_container is required in bronze configuration")

        self.prefix = bronze_cfg.get("azure_prefix", "").rstrip("/")

        # Initialize blob service client
        self.blob_service_client = self._build_client(azure_cfg)
        self.container_client = self.blob_service_client.get_container_client(
            self.container_name
        )

        # Ensure container exists
        try:
            if not self.container_client.exists():
                self.container_client.create_container()
                logger.info(f"Created Azure container: {self.container_name}")
            else:
                logger.debug(f"Using existing Azure container: {self.container_name}")
        except AzureError as e:
            logger.warning(f"Could not verify/create container: {e}")

    def _build_client(self, azure_cfg: Dict[str, Any]) -> BlobServiceClient:
        """Build Azure Blob Service Client with various auth methods."""

        # Method 1: Connection string (easiest for dev/test)
        conn_str_env = azure_cfg.get("connection_string_env")
        if conn_str_env:
            conn_str = os.environ.get(conn_str_env)
            if conn_str:
                logger.debug("Using Azure connection string authentication")
                return BlobServiceClient.from_connection_string(conn_str)

        # Method 2: Account key
        account_name_env = azure_cfg.get("account_name_env")
        account_key_env = azure_cfg.get("account_key_env")

        if account_name_env and account_key_env:
            account_name = os.environ.get(account_name_env)
            account_key = os.environ.get(account_key_env)

            if account_name and account_key:
                account_url = f"https://{account_name}.blob.core.windows.net"
                logger.debug(
                    f"Using Azure account key authentication for {account_url}"
                )
                return BlobServiceClient(
                    account_url=account_url, credential=account_key
                )

        # Method 3: Service Principal (recommended for production)
        tenant_id_env = azure_cfg.get("tenant_id_env")
        client_id_env = azure_cfg.get("client_id_env")
        client_secret_env = azure_cfg.get("client_secret_env")

        if tenant_id_env and client_id_env and client_secret_env:
            tenant_id = os.environ.get(tenant_id_env)
            client_id = os.environ.get(client_id_env)
            client_secret = os.environ.get(client_secret_env)

            if tenant_id and client_id and client_secret:
                from azure.identity import ClientSecretCredential

                account_name = os.environ.get(
                    account_name_env or "AZURE_STORAGE_ACCOUNT"
                )
                if not account_name:
                    raise ValueError(
                        "account_name_env required for service principal auth"
                    )

                account_url = f"https://{account_name}.blob.core.windows.net"
                client_credential = ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret,
                )
                logger.debug(
                    f"Using Azure service principal authentication for {account_url}"
                )
                return BlobServiceClient(
                    account_url=account_url, credential=client_credential
                )

        # Method 4: Managed Identity (for Azure VMs/Functions)
        if azure_cfg.get("use_managed_identity", False):
            from azure.identity import ManagedIdentityCredential

            account_name = os.environ.get(account_name_env or "AZURE_STORAGE_ACCOUNT")
            if not account_name:
                raise ValueError("account_name_env required for managed identity")

            account_url = f"https://{account_name}.blob.core.windows.net"
            managed_credential = ManagedIdentityCredential()
            logger.debug(
                f"Using Azure managed identity authentication for {account_url}"
            )
            return BlobServiceClient(
                account_url=account_url, credential=managed_credential
            )

        raise ValueError(
            "No valid Azure authentication found. Provide one of: "
            "connection_string_env, account_name_env+account_key_env, "
            "tenant_id_env+client_id_env+client_secret_env, or use_managed_identity=true"
        )

    def _build_blob_path(self, remote_path: str) -> str:
        """Build full blob path from remote path and prefix."""
        if self.prefix:
            return f"{self.prefix}/{remote_path}"
        return remote_path

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(AzureError),
        reraise=True,
    )
    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to Azure Blob Storage.

        Args:
            local_path: Path to local file
            remote_path: Destination path in Azure (relative to container/prefix)

        Returns:
            True if upload succeeded

        Raises:
            AzureError: If upload fails after retries
        """
        blob_path = self._build_blob_path(remote_path)

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_path
            )

            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            logger.info(
                f"Uploaded {os.path.basename(local_path)} to azure://{self.container_name}/{blob_path}"
            )
            return True
        except AzureError as e:
            logger.error(f"Failed to upload {local_path} to Azure: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading to Azure: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(AzureError),
        reraise=True,
    )
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
        blob_path = self._build_blob_path(remote_path)

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_path
            )

            with open(local_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

            logger.info(
                f"Downloaded azure://{self.container_name}/{blob_path} to {local_path}"
            )
            return True
        except AzureError as e:
            logger.error(f"Failed to download {remote_path} from Azure: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error downloading from Azure: {e}")
            raise

    def list_files(self, prefix: str) -> List[str]:
        """List files in Azure container with given prefix.

        Args:
            prefix: Path prefix to filter files

        Returns:
            List of blob names matching the prefix

        Raises:
            AzureError: If listing fails
        """
        full_prefix = self._build_blob_path(prefix)

        try:
            blob_list = self.container_client.list_blobs(name_starts_with=full_prefix)
            files = [blob.name for blob in blob_list]
            logger.debug(f"Listed {len(files)} files with prefix '{full_prefix}'")
            return files
        except AzureError as e:
            logger.error(f"Failed to list files in Azure: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing Azure files: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(AzureError),
        reraise=True,
    )
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from Azure Blob Storage.

        Args:
            remote_path: Path to file in Azure

        Returns:
            True if deletion succeeded

        Raises:
            AzureError: If deletion fails after retries
        """
        blob_path = self._build_blob_path(remote_path)

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_path
            )
            blob_client.delete_blob()
            logger.info(f"Deleted azure://{self.container_name}/{blob_path}")
            return True
        except AzureError as e:
            logger.error(f"Failed to delete {remote_path} from Azure: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting from Azure: {e}")
            raise

    def get_backend_type(self) -> str:
        """Get backend type identifier."""
        return "azure"
