"""Integration tests for S3 storage backend using LocalStack.

Run with: RUN_INTEGRATION=1 pytest tests/test_s3_integration.py -v

Setup LocalStack:
  docker run -p 4566:4566 localstack/localstack

Environment:
  AWS_ACCESS_KEY_ID=test
  AWS_SECRET_ACCESS_KEY=test
  AWS_ENDPOINT_URL=http://localhost:4566
  AWS_DEFAULT_REGION=us-east-1
"""

import os
import pytest
from core.storage.plugins.s3 import S3StorageBackend


@pytest.fixture
def localstack_config():
    """Config for LocalStack S3 emulator."""
    return {
        "bronze": {
            "storage_backend": "s3",
            "s3": {
                "bucket_name": "bronze-test-bucket",
                "access_key_env": "AWS_ACCESS_KEY_ID",
                "secret_key_env": "AWS_SECRET_ACCESS_KEY",
                "region": "us-east-1",
                "endpoint_url_env": "AWS_ENDPOINT_URL",
            },
        }
    }


@pytest.fixture
def s3_backend(localstack_config):
    """S3 backend connected to LocalStack."""
    backend = S3StorageBackend(localstack_config)

    # Create bucket if not exists
    try:
        backend.client.head_bucket(Bucket=backend.bucket_name)
    except Exception:
        backend.client.create_bucket(Bucket=backend.bucket_name)

    yield backend


@pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION"),
    reason="Set RUN_INTEGRATION=1 to run integration tests against LocalStack",
)
def test_s3_upload_download_roundtrip(s3_backend, tmp_path):
    """Test upload and download against LocalStack S3."""
    # Create test file
    test_file = tmp_path / "test_data.txt"
    test_file.write_text("Hello LocalStack")

    # Upload
    remote_path = "integration_test/data.txt"
    s3_backend.upload(test_file, remote_path)

    # Download
    download_file = tmp_path / "downloaded.txt"
    s3_backend.download(remote_path, download_file)

    # Verify
    assert download_file.read_text() == "Hello LocalStack"

    # Cleanup
    s3_backend.delete(remote_path)


@pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION"),
    reason="Set RUN_INTEGRATION=1 to run integration tests",
)
def test_s3_list_files(s3_backend, tmp_path):
    """Test listing files in S3 bucket."""
    # Upload multiple files
    for i in range(3):
        test_file = tmp_path / f"test_{i}.txt"
        test_file.write_text(f"Content {i}")
        s3_backend.upload(test_file, f"list_test/file_{i}.txt")

    # List files
    files = s3_backend.list_files("list_test/")
    assert len(files) >= 3

    # Cleanup
    for i in range(3):
        s3_backend.delete(f"list_test/file_{i}.txt")


@pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION"),
    reason="Set RUN_INTEGRATION=1 to run integration tests",
)
def test_s3_retry_on_throttle(s3_backend, tmp_path, monkeypatch):
    """Test that S3 backend retries on throttle errors."""
    # This is a smoke test; real throttling requires rate limiting the emulator
    test_file = tmp_path / "retry_test.txt"
    test_file.write_text("Retry test")

    # Should complete without error even with retries enabled
    s3_backend.upload(test_file, "retry_test/data.txt")
    s3_backend.delete("retry_test/data.txt")
