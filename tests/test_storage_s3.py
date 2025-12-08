"""Unit tests for S3 storage backend with moto mocking."""


from typing import Any, Dict

import boto3
import pytest
from moto import mock_aws

from core.infrastructure.io.storage import S3Storage

ConfigDict = Dict[str, Dict[str, Any]]


@pytest.fixture
def s3_config():
    """Standard S3 configuration for tests."""
    return {
        "bronze": {
            "s3_bucket": "test-bucket",
            "s3_prefix": "bronze-data",
        },
        "s3_connection": {
            "endpoint_url_env": None,
            "access_key_env": "AWS_ACCESS_KEY_ID",
            "secret_key_env": "AWS_SECRET_ACCESS_KEY",
        },
    }


@pytest.fixture
def aws_credentials(monkeypatch):
    """Mock AWS credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_client(aws_credentials):
    """Create a mocked S3 client."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client


@pytest.fixture
def s3_storage(aws_credentials, s3_config):
    """Create an S3Storage instance with mocked AWS."""
    with mock_aws():
        # Create the bucket first
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")

        storage = S3Storage(s3_config)
        yield storage


class TestS3StorageInit:
    """Tests for S3Storage initialization."""

    def test_requires_bucket(self, aws_credentials):
        """Test that s3_bucket is required."""
        with mock_aws():
            config: ConfigDict = {"bronze": {}, "s3_connection": {}}
            with pytest.raises(ValueError, match="s3_bucket is required"):
                S3Storage(config)

    def test_uses_prefix(self, aws_credentials):
        """Test that prefix is applied correctly."""
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")

            config: ConfigDict = {
                "bronze": {"s3_bucket": "test-bucket", "s3_prefix": "my-prefix"},
                "s3_connection": {},
            }
            storage = S3Storage(config)
            assert storage._build_remote_path("file.txt") == "my-prefix/file.txt"

    def test_no_prefix(self, aws_credentials):
        """Test without prefix."""
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")

            config: ConfigDict = {
                "bronze": {"s3_bucket": "test-bucket"},
                "s3_connection": {},
            }
            storage = S3Storage(config)
            assert storage._build_remote_path("file.txt") == "file.txt"

    def test_backend_type(self, s3_storage):
        """Test backend type identifier."""
        assert s3_storage.get_backend_type() == "s3"


class TestS3StorageUpload:
    """Tests for S3 upload operations."""

    def test_upload_file_success(self, s3_storage, tmp_path):
        """Test successful file upload."""
        source = tmp_path / "test.txt"
        source.write_text("Hello, S3!")

        result = s3_storage.upload_file(str(source), "data/test.txt")

        assert result is True
        # Verify file exists in S3
        response = s3_storage.client.get_object(
            Bucket="test-bucket",
            Key="bronze-data/data/test.txt"
        )
        assert response["Body"].read().decode() == "Hello, S3!"

    def test_upload_overwrites_existing(self, s3_storage, tmp_path):
        """Test that upload overwrites existing files."""
        source = tmp_path / "test.txt"

        # Upload first version
        source.write_text("Version 1")
        s3_storage.upload_file(str(source), "overwrite.txt")

        # Upload second version
        source.write_text("Version 2")
        s3_storage.upload_file(str(source), "overwrite.txt")

        # Verify second version
        response = s3_storage.client.get_object(
            Bucket="test-bucket",
            Key="bronze-data/overwrite.txt"
        )
        assert response["Body"].read().decode() == "Version 2"


class TestS3StorageDownload:
    """Tests for S3 download operations."""

    def test_download_file_success(self, s3_storage, tmp_path):
        """Test successful file download."""
        # Upload a file first
        source = tmp_path / "upload.txt"
        source.write_text("Download me!")
        s3_storage.upload_file(str(source), "download/test.txt")

        # Download it
        target = tmp_path / "downloaded.txt"
        result = s3_storage.download_file("download/test.txt", str(target))

        assert result is True
        assert target.read_text() == "Download me!"

    def test_download_nonexistent_raises(self, s3_storage, tmp_path):
        """Test downloading nonexistent file raises error."""
        target = tmp_path / "nofile.txt"

        with pytest.raises(Exception):  # ClientError from boto3
            s3_storage.download_file("nonexistent/file.txt", str(target))


class TestS3StorageList:
    """Tests for S3 list operations."""

    def test_list_files_with_prefix(self, s3_storage, tmp_path):
        """Test listing files with prefix filter."""
        source = tmp_path / "test.txt"
        source.write_text("content")

        # Upload files to different paths
        s3_storage.upload_file(str(source), "dir1/file1.txt")
        s3_storage.upload_file(str(source), "dir1/file2.txt")
        s3_storage.upload_file(str(source), "dir2/file3.txt")

        # List dir1 only
        files = s3_storage.list_files("dir1/")

        assert len(files) == 2
        assert any("file1.txt" in f for f in files)
        assert any("file2.txt" in f for f in files)
        assert not any("file3.txt" in f for f in files)

    def test_list_empty_prefix(self, s3_storage, tmp_path):
        """Test listing with empty result."""
        files = s3_storage.list_files("nonexistent/")
        assert files == []


class TestS3StorageDelete:
    """Tests for S3 delete operations."""

    def test_delete_file_success(self, s3_storage, tmp_path):
        """Test successful file deletion."""
        source = tmp_path / "delete.txt"
        source.write_text("Delete me")
        s3_storage.upload_file(str(source), "to_delete.txt")

        # Verify exists
        files = s3_storage.list_files("to_delete")
        assert len(files) == 1

        # Delete
        result = s3_storage.delete_file("to_delete.txt")
        assert result is True

        # Verify deleted
        files = s3_storage.list_files("to_delete")
        assert len(files) == 0

    def test_delete_nonexistent_succeeds(self, s3_storage):
        """Test deleting nonexistent file succeeds (S3 behavior)."""
        # S3 delete_object doesn't error for non-existent keys
        result = s3_storage.delete_file("does/not/exist.txt")
        assert result is True


class TestS3StorageRetry:
    """Tests for S3 retry behavior."""

    def test_should_retry_on_boto_error(self, s3_storage):
        """Test that BotoCoreError is retryable."""
        from botocore.exceptions import BotoCoreError

        # BotoCoreError is abstract, create a concrete subclass
        class TestBotoError(BotoCoreError):
            fmt = "Test error"

        assert s3_storage._should_retry(TestBotoError()) is True

    def test_should_retry_on_throttle(self, s3_storage):
        """Test that throttling errors are retryable."""
        from botocore.exceptions import ClientError

        error_response = {
            "Error": {"Code": "SlowDown"},
            "ResponseMetadata": {"HTTPStatusCode": 503},
        }
        exc = ClientError(error_response, "ListObjects")

        assert s3_storage._should_retry(exc) is True

    def test_should_not_retry_on_not_found(self, s3_storage):
        """Test that 404 errors are not retryable."""
        from botocore.exceptions import ClientError

        error_response = {
            "Error": {"Code": "NoSuchKey"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }
        exc = ClientError(error_response, "GetObject")

        assert s3_storage._should_retry(exc) is False

