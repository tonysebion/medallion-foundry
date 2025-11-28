"""Integration tests for Azure storage backend using Azurite emulator.

Run with: RUN_INTEGRATION=1 pytest tests/test_azure_integration.py -v

Setup Azurite:
  docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0

Environment:
  AZURE_STORAGE_ACCOUNT=devstoreaccount1
  AZURE_STORAGE_KEY=Eby8vdM02xNOcqFlEeXSqX3+Z5iOcqvGr5aFZONi0gMtOzLb5Mu1GqxEEphH1k1FzY3J3hf9RzqWz3J3hf9RzqWz3J3hf==
  AZURE_STORAGE_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1
"""

import os
import pytest

# Skip entire module if Azure dependencies not available
pytest.importorskip(
    "azure.storage.blob", reason="Azure storage dependencies not installed"
)
pytest.importorskip(
    "azure.identity", reason="Azure identity dependencies not installed"
)

from core.storage.plugins.azure_storage import AzureStorageBackend  # noqa: E402


@pytest.fixture
def azurite_config():
    """Config for Azurite local emulator."""
    return {
        "bronze": {
            "storage_backend": "azure",
            "azure": {
                "container_name": "bronze-test",
                "account_name_env": "AZURE_STORAGE_ACCOUNT",
                "account_key_env": "AZURE_STORAGE_KEY",
                "endpoint_env": "AZURE_STORAGE_ENDPOINT",
            },
        }
    }


@pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION"),
    reason="Set RUN_INTEGRATION=1 to run integration tests against Azurite",
)
def test_azure_upload_download_roundtrip(azurite_config, tmp_path):
    """Test upload and download against Azurite."""
    backend = AzureStorageBackend(azurite_config)

    # Create test file
    test_file = tmp_path / "test_data.txt"
    test_file.write_text("Hello Azurite")

    # Upload
    remote_path = "integration_test/data.txt"
    backend.upload(test_file, remote_path)

    # Download
    download_file = tmp_path / "downloaded.txt"
    backend.download(remote_path, download_file)

    # Verify
    assert download_file.read_text() == "Hello Azurite"

    # Cleanup
    backend.delete(remote_path)


@pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION"),
    reason="Set RUN_INTEGRATION=1 to run integration tests",
)
def test_azure_list_files(azurite_config, tmp_path):
    """Test listing files in Azure container."""
    backend = AzureStorageBackend(azurite_config)

    # Upload multiple files
    for i in range(3):
        test_file = tmp_path / f"test_{i}.txt"
        test_file.write_text(f"Content {i}")
        backend.upload(test_file, f"list_test/file_{i}.txt")

    # List files
    files = backend.list_files("list_test/")
    assert len(files) >= 3

    # Cleanup
    for i in range(3):
        backend.delete(f"list_test/file_{i}.txt")
