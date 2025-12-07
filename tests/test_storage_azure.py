"""Unit tests for Azure storage backend with mocks."""

from unittest.mock import Mock, MagicMock, patch
from pathlib import Path

import pytest

# Import Azure exceptions for mocking - wrapped to handle optional dependency
try:
    from azure.core.exceptions import AzureError, ResourceNotFoundError
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    AzureError = Exception
    ResourceNotFoundError = Exception


@pytest.fixture
def azure_config():
    """Standard Azure configuration for tests."""
    return {
        "bronze": {
            "azure_container": "test-container",
            "azure_prefix": "bronze-data",
        },
        "azure_connection": {
            "connection_string_env": "AZURE_STORAGE_CONNECTION_STRING",
        },
    }


@pytest.fixture
def mock_container():
    """Create a mock container client with common methods."""
    container = Mock()
    container.get_container_properties = Mock()
    container.create_container = Mock()
    container.upload_blob = Mock()
    container.list_blobs = Mock(return_value=[])
    container.delete_blob = Mock()

    blob_client = Mock()
    blob_client.download_blob = Mock()
    container.get_blob_client = Mock(return_value=blob_client)

    return container


@pytest.fixture
def mock_blob_service(mock_container):
    """Create a mock BlobServiceClient."""
    service = Mock()
    service.get_container_client = Mock(return_value=mock_container)
    return service


@pytest.mark.skipif(not AZURE_AVAILABLE, reason="Azure SDK not installed")
class TestAzureStorageInit:
    """Tests for AzureStorage initialization."""

    def test_requires_container(self, monkeypatch):
        """Test that azure_container is required."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        from core.infrastructure.io.storage import AzureStorage

        config = {"bronze": {}, "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"}}
        with pytest.raises(ValueError, match="azure_container is required"):
            AzureStorage(config)

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_uses_prefix(self, mock_blob_class, mock_container, monkeypatch):
        """Test that prefix is applied correctly."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        from core.infrastructure.io.storage import AzureStorage

        config = {
            "bronze": {"azure_container": "test-container", "azure_prefix": "my-prefix/"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorage(config)

        assert storage.prefix == "my-prefix"  # Trailing slash stripped
        assert storage._build_remote_path("file.txt") == "my-prefix/file.txt"

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_no_prefix(self, mock_blob_class, mock_container, monkeypatch):
        """Test without prefix."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        from core.infrastructure.io.storage import AzureStorage

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorage(config)

        assert storage.prefix == ""
        assert storage._build_remote_path("file.txt") == "file.txt"

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_backend_type(self, mock_blob_class, mock_container, monkeypatch):
        """Test backend type identifier."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        from core.infrastructure.io.storage import AzureStorage

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorage(config)

        assert storage.get_backend_type() == "azure"


@pytest.mark.skipif(not AZURE_AVAILABLE, reason="Azure SDK not installed")
class TestAzureStorageUpload:
    """Tests for Azure upload operations."""

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_upload_file_success(self, mock_blob_class, mock_container, monkeypatch, tmp_path):
        """Test successful file upload."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        from core.infrastructure.io.storage import AzureStorage

        config = {
            "bronze": {"azure_container": "test-container", "azure_prefix": "data"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorage(config)

        source = tmp_path / "test.txt"
        source.write_text("Hello, Azure!")

        result = storage.upload_file(str(source), "test.txt")

        assert result is True
        mock_container.upload_blob.assert_called_once()
        call_args = mock_container.upload_blob.call_args
        assert call_args[0][0] == "data/test.txt"  # Full path with prefix


@pytest.mark.skipif(not AZURE_AVAILABLE, reason="Azure SDK not installed")
class TestAzureStorageDownload:
    """Tests for Azure download operations."""

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_download_file_success(self, mock_blob_class, mock_container, monkeypatch, tmp_path):
        """Test successful file download."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        # Setup mock download stream
        mock_stream = Mock()
        mock_stream.readinto = Mock(side_effect=lambda f: f.write(b"Downloaded content!"))
        blob_client = mock_container.get_blob_client.return_value
        blob_client.download_blob.return_value = mock_stream

        from core.infrastructure.io.storage import AzureStorage

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorage(config)

        target = tmp_path / "downloaded.txt"
        result = storage.download_file("test.txt", str(target))

        assert result is True
        mock_container.get_blob_client.assert_called_once_with("test.txt")


@pytest.mark.skipif(not AZURE_AVAILABLE, reason="Azure SDK not installed")
class TestAzureStorageList:
    """Tests for Azure list operations."""

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_list_files_with_prefix(self, mock_blob_class, mock_container, monkeypatch):
        """Test listing files with prefix filter."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        # Mock blob listing
        mock_blob1 = Mock()
        mock_blob1.name = "prefix/file1.txt"
        mock_blob2 = Mock()
        mock_blob2.name = "prefix/file2.txt"
        mock_container.list_blobs.return_value = [mock_blob1, mock_blob2]

        from core.infrastructure.io.storage import AzureStorage

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorage(config)

        files = storage.list_files("prefix/")

        assert len(files) == 2
        assert "prefix/file1.txt" in files
        assert "prefix/file2.txt" in files
        mock_container.list_blobs.assert_called_once_with(name_starts_with="prefix/")


@pytest.mark.skipif(not AZURE_AVAILABLE, reason="Azure SDK not installed")
class TestAzureStorageDelete:
    """Tests for Azure delete operations."""

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_delete_file_success(self, mock_blob_class, mock_container, monkeypatch):
        """Test successful file deletion."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        from core.infrastructure.io.storage import AzureStorage

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorage(config)

        result = storage.delete_file("to_delete.txt")

        assert result is True
        mock_container.delete_blob.assert_called_once_with("to_delete.txt")


@pytest.mark.skipif(not AZURE_AVAILABLE, reason="Azure SDK not installed")
class TestAzureStorageRetry:
    """Tests for Azure retry behavior."""

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_should_retry_on_azure_error(self, mock_blob_class, mock_container, monkeypatch):
        """Test that generic AzureError is retryable."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        from core.infrastructure.io.storage import AzureStorage
        from azure.core.exceptions import AzureError

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorage(config)

        assert storage._should_retry(AzureError("generic azure error")) is True

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_should_not_retry_on_not_found(self, mock_blob_class, mock_container, monkeypatch):
        """Test that ResourceNotFoundError is not retryable."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        from core.infrastructure.io.storage import AzureStorage
        from azure.core.exceptions import ResourceNotFoundError

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorage(config)

        assert storage._should_retry(ResourceNotFoundError("Not found")) is False


@pytest.mark.skipif(not AZURE_AVAILABLE, reason="Azure SDK not installed")
class TestAzureStorageBackendWrapper:
    """Tests for the backward-compatible wrapper."""

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_wrapper_upload(self, mock_blob_class, mock_container, monkeypatch, tmp_path):
        """Test wrapper upload method accepts Path."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        from core.infrastructure.io.storage import AzureStorageBackend

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorageBackend(config)

        source = tmp_path / "test.txt"
        source.write_text("wrapper test")

        result = storage.upload(source, "wrapper/test.txt")
        assert result is True

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_wrapper_download(self, mock_blob_class, mock_container, monkeypatch, tmp_path):
        """Test wrapper download method accepts Path."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        # Setup mock download
        mock_stream = Mock()
        mock_stream.readinto = Mock(side_effect=lambda f: f.write(b"content"))
        blob_client = mock_container.get_blob_client.return_value
        blob_client.download_blob.return_value = mock_stream

        from core.infrastructure.io.storage import AzureStorageBackend

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorageBackend(config)

        target = tmp_path / "downloaded.txt"
        result = storage.download("test.txt", target)

        assert result is True

    @patch("core.infrastructure.io.storage.azure.BlobServiceClient")
    def test_wrapper_delete(self, mock_blob_class, mock_container, monkeypatch):
        """Test wrapper delete method."""
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")

        mock_blob_class.from_connection_string.return_value.get_container_client.return_value = mock_container

        from core.infrastructure.io.storage import AzureStorageBackend

        config = {
            "bronze": {"azure_container": "test-container"},
            "azure_connection": {"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"},
        }
        storage = AzureStorageBackend(config)

        result = storage.delete("wrapper/delete.txt")
        assert result is True
