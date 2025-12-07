"""Tests for StateStorageBackend base class."""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.foundation.state.storage import StateStorageBackend


class ConcreteStorageBackend(StateStorageBackend):
    """Concrete implementation for testing the base class."""

    def __init__(self, storage_backend: str = "local", s3_bucket: str | None = None):
        self.storage_backend = storage_backend
        self.s3_bucket = s3_bucket


class TestParseS3Path:
    """Tests for _parse_s3_path static method."""

    def test_parse_s3_path_with_prefix(self) -> None:
        bucket, key = StateStorageBackend._parse_s3_path("s3://my-bucket/path/to/file.json")
        assert bucket == "my-bucket"
        assert key == "path/to/file.json"

    def test_parse_s3_path_without_prefix(self) -> None:
        bucket, key = StateStorageBackend._parse_s3_path("my-bucket/path/to/file.json")
        assert bucket == "my-bucket"
        assert key == "path/to/file.json"

    def test_parse_s3_path_bucket_only(self) -> None:
        bucket, key = StateStorageBackend._parse_s3_path("s3://my-bucket")
        assert bucket == "my-bucket"
        assert key == ""

    def test_parse_s3_path_with_nested_path(self) -> None:
        bucket, key = StateStorageBackend._parse_s3_path("s3://bucket/a/b/c/d.json")
        assert bucket == "bucket"
        assert key == "a/b/c/d.json"


class TestLocalJsonOperations:
    """Tests for local filesystem JSON operations."""

    def test_load_json_local_success(self, tmp_path: Path) -> None:
        backend = ConcreteStorageBackend()
        test_data = {"key": "value", "nested": {"a": 1}}

        file_path = tmp_path / "test.json"
        file_path.write_text(json.dumps(test_data))

        result = backend._load_json_local(file_path)
        assert result == test_data

    def test_load_json_local_not_found(self, tmp_path: Path) -> None:
        backend = ConcreteStorageBackend()
        file_path = tmp_path / "nonexistent.json"

        with pytest.raises(FileNotFoundError):
            backend._load_json_local(file_path)

    def test_save_json_local_creates_parent_dirs(self, tmp_path: Path) -> None:
        backend = ConcreteStorageBackend()
        test_data = {"key": "value"}

        file_path = tmp_path / "nested" / "dir" / "test.json"
        backend._save_json_local(file_path, test_data)

        assert file_path.exists()
        loaded = json.loads(file_path.read_text())
        assert loaded == test_data

    def test_delete_local_existing_file(self, tmp_path: Path) -> None:
        backend = ConcreteStorageBackend()

        file_path = tmp_path / "to_delete.json"
        file_path.write_text("{}")

        result = backend._delete_local(file_path)
        assert result is True
        assert not file_path.exists()

    def test_delete_local_nonexistent_file(self, tmp_path: Path) -> None:
        backend = ConcreteStorageBackend()
        file_path = tmp_path / "nonexistent.json"

        result = backend._delete_local(file_path)
        assert result is False

    def test_list_json_local(self, tmp_path: Path) -> None:
        backend = ConcreteStorageBackend()

        # Create some test files
        (tmp_path / "file1_watermark.json").write_text("{}")
        (tmp_path / "file2_watermark.json").write_text("{}")
        (tmp_path / "other.txt").write_text("")

        result = backend._list_json_local(tmp_path, "*_watermark.json")
        assert len(result) == 2
        assert all(p.suffix == ".json" for p in result)

    def test_list_json_local_empty_dir(self, tmp_path: Path) -> None:
        backend = ConcreteStorageBackend()
        result = backend._list_json_local(tmp_path, "*.json")
        assert result == []

    def test_list_json_local_nonexistent_dir(self, tmp_path: Path) -> None:
        backend = ConcreteStorageBackend()
        nonexistent = tmp_path / "does_not_exist"
        result = backend._list_json_local(nonexistent, "*.json")
        assert result == []


class TestS3JsonOperations:
    """Tests for S3 JSON operations with mocked boto3."""

    def test_load_json_s3_success(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="s3", s3_bucket="test-bucket")
        test_data = {"key": "value"}

        mock_s3 = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(test_data).encode("utf-8")
        mock_s3.get_object.return_value = {"Body": mock_body}

        with patch.object(backend, "_get_boto3_client", return_value=mock_s3):
            result = backend._load_json_s3("test-bucket", "path/to/file.json")

        assert result == test_data
        mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="path/to/file.json")

    def test_load_json_s3_not_found(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="s3", s3_bucket="test-bucket")

        mock_s3 = MagicMock()
        mock_s3.exceptions.NoSuchKey = Exception
        mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey("Not found")

        with patch.object(backend, "_get_boto3_client", return_value=mock_s3):
            with pytest.raises(FileNotFoundError):
                backend._load_json_s3("test-bucket", "nonexistent.json")

    def test_save_json_s3_success(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="s3", s3_bucket="test-bucket")
        test_data = {"key": "value"}

        mock_s3 = MagicMock()

        with patch.object(backend, "_get_boto3_client", return_value=mock_s3):
            backend._save_json_s3("test-bucket", "path/to/file.json", test_data)

        mock_s3.put_object.assert_called_once()
        call_args = mock_s3.put_object.call_args
        assert call_args.kwargs["Bucket"] == "test-bucket"
        assert call_args.kwargs["Key"] == "path/to/file.json"
        # Verify the body is valid JSON
        body = call_args.kwargs["Body"].decode("utf-8")
        assert json.loads(body) == test_data

    def test_delete_s3_success(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="s3", s3_bucket="test-bucket")

        mock_s3 = MagicMock()

        with patch.object(backend, "_get_boto3_client", return_value=mock_s3):
            result = backend._delete_s3("test-bucket", "path/to/file.json")

        assert result is True
        mock_s3.delete_object.assert_called_once_with(Bucket="test-bucket", Key="path/to/file.json")

    def test_delete_s3_failure(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="s3", s3_bucket="test-bucket")

        mock_s3 = MagicMock()
        mock_s3.delete_object.side_effect = Exception("Delete failed")

        with patch.object(backend, "_get_boto3_client", return_value=mock_s3):
            result = backend._delete_s3("test-bucket", "path/to/file.json")

        assert result is False

    def test_list_json_s3(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="s3", s3_bucket="test-bucket")

        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "prefix/file1.json"},
                    {"Key": "prefix/file2.json"},
                    {"Key": "prefix/other.txt"},
                ]
            }
        ]
        mock_s3.get_paginator.return_value = mock_paginator

        with patch.object(backend, "_get_boto3_client", return_value=mock_s3):
            result = backend._list_json_s3("test-bucket", "prefix/", ".json")

        assert len(result) == 2
        assert "prefix/file1.json" in result
        assert "prefix/file2.json" in result


class TestDispatchStorageOperation:
    """Tests for _dispatch_storage_operation method."""

    def test_dispatch_to_local(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="local")

        local_result = "local_value"
        s3_result = "s3_value"

        result = backend._dispatch_storage_operation(
            local_op=lambda: local_result,
            s3_op=lambda: s3_result,
        )

        assert result == local_result

    def test_dispatch_to_s3(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="s3", s3_bucket="bucket")

        local_result = "local_value"
        s3_result = "s3_value"

        result = backend._dispatch_storage_operation(
            local_op=lambda: local_result,
            s3_op=lambda: s3_result,
        )

        assert result == s3_result


class TestGetBoto3Client:
    """Tests for _get_boto3_client method."""

    def test_get_boto3_client_import_error(self) -> None:
        backend = ConcreteStorageBackend()

        with patch.dict("sys.modules", {"boto3": None}):
            # Force reimport to trigger ImportError
            with patch("builtins.__import__", side_effect=ImportError("No boto3")):
                with pytest.raises(ImportError, match="boto3 required"):
                    backend._get_boto3_client()


class TestLoadJsonS3ByKey:
    """Tests for _load_json_s3_by_key helper method."""

    def test_returns_data_on_success(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="s3", s3_bucket="bucket")
        test_data = {"key": "value"}

        with patch.object(backend, "_load_json_s3", return_value=test_data):
            result = backend._load_json_s3_by_key("bucket", "key.json")

        assert result == test_data

    def test_returns_none_on_error(self) -> None:
        backend = ConcreteStorageBackend(storage_backend="s3", s3_bucket="bucket")

        with patch.object(backend, "_load_json_s3", side_effect=Exception("Error")):
            result = backend._load_json_s3_by_key("bucket", "key.json")

        assert result is None
