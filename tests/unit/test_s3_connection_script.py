"""Tests for the S3 connection test script."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestS3ConnectionScript:
    """Tests for scripts/test_s3_connection.py."""

    def test_config_validation_fails_with_placeholder(self, capsys):
        """Test that script fails when config has placeholder values."""
        import scripts.test_s3_connection as script

        # Reset to placeholder
        script.S3_ACCESS_KEY = "your-access-key-here"
        result = script.main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Please edit the configuration values" in captured.out

    def test_all_operations_succeed(self, capsys):
        """Test successful S3 operations."""
        import scripts.test_s3_connection as script

        # Configure script
        script.S3_ACCESS_KEY = "test-key"
        script.S3_SECRET_KEY = "test-secret"
        script.S3_BUCKET = "test-bucket"
        script.S3_ENDPOINT_URL = "http://localhost:9000"

        # Mock S3Storage at the import location inside main()
        mock_storage = MagicMock()
        mock_storage.write_bytes.return_value = MagicMock(
            success=True, bytes_written=45, error=None
        )
        mock_storage.exists.return_value = True
        mock_storage.list_files.return_value = [
            MagicMock(path="test/hello_world_20250107.json")
        ]
        mock_storage.delete.return_value = True

        # Mock datetime for predictable content
        with patch("scripts.test_s3_connection.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "20250107_120000"
            mock_dt.now.return_value.isoformat.return_value = "2025-01-07T12:00:00"

            # Generate expected content to match what read_bytes should return
            import json

            test_data = {
                "message": "Hello, S3!",
                "timestamp": "2025-01-07T12:00:00",
                "source": "test_s3_connection.py",
            }
            expected_bytes = json.dumps(test_data, indent=2).encode("utf-8")
            mock_storage.read_bytes.return_value = expected_bytes

            with patch(
                "pipelines.lib.storage.s3.S3Storage", return_value=mock_storage
            ):
                result = script.main()

        assert result == 0
        captured = capsys.readouterr()
        assert "All 4 tests passed" in captured.out

    def test_write_failure(self, capsys):
        """Test handling of write failure."""
        import scripts.test_s3_connection as script

        script.S3_ACCESS_KEY = "test-key"
        script.S3_SECRET_KEY = "test-secret"
        script.S3_BUCKET = "test-bucket"

        mock_storage = MagicMock()
        mock_storage.write_bytes.return_value = MagicMock(
            success=False, error="Access Denied", bytes_written=0
        )
        mock_storage.exists.return_value = False
        mock_storage.read_bytes.side_effect = Exception("File not found")
        mock_storage.list_files.return_value = []
        mock_storage.delete.return_value = False

        with patch("scripts.test_s3_connection.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "20250107_120000"
            mock_dt.now.return_value.isoformat.return_value = "2025-01-07T12:00:00"

            with patch(
                "pipelines.lib.storage.s3.S3Storage", return_value=mock_storage
            ):
                result = script.main()

        assert result == 1
        captured = capsys.readouterr()
        assert "FAILED" in captured.out

    def test_s3_storage_initialized_with_correct_options(self, capsys):
        """Test that S3Storage is initialized with Nutanix-compatible options."""
        import scripts.test_s3_connection as script

        script.S3_ACCESS_KEY = "test-key"
        script.S3_SECRET_KEY = "test-secret"
        script.S3_BUCKET = "test-bucket"
        script.S3_ENDPOINT_URL = "https://objects.nutanix.local:443"
        script.S3_REGION = "us-east-1"

        mock_storage = MagicMock()
        mock_storage.write_bytes.return_value = MagicMock(success=True, bytes_written=45)
        mock_storage.exists.return_value = True
        mock_storage.read_bytes.return_value = b"{}"
        mock_storage.list_files.return_value = [MagicMock(path="test/file.json")]
        mock_storage.delete.return_value = True

        with patch("scripts.test_s3_connection.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "20250107_120000"
            mock_dt.now.return_value.isoformat.return_value = "2025-01-07T12:00:00"

            with patch(
                "pipelines.lib.storage.s3.S3Storage", return_value=mock_storage
            ) as mock_cls:
                script.main()

        # Verify S3Storage was called with correct parameters
        mock_cls.assert_called_once()
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["endpoint_url"] == "https://objects.nutanix.local:443"
        assert call_kwargs["key"] == "test-key"
        assert call_kwargs["secret"] == "test-secret"
        assert call_kwargs["region"] == "us-east-1"
        assert call_kwargs["signature_version"] == "s3v4"
        assert call_kwargs["addressing_style"] == "path"

    def test_exception_handling(self, capsys):
        """Test that exceptions are handled gracefully."""
        import scripts.test_s3_connection as script

        script.S3_ACCESS_KEY = "test-key"
        script.S3_SECRET_KEY = "test-secret"
        script.S3_BUCKET = "test-bucket"

        mock_storage = MagicMock()
        mock_storage.write_bytes.side_effect = Exception("Connection refused")
        mock_storage.exists.side_effect = Exception("Connection refused")
        mock_storage.read_bytes.side_effect = Exception("Connection refused")
        mock_storage.list_files.side_effect = Exception("Connection refused")
        mock_storage.delete.side_effect = Exception("Connection refused")

        with patch("scripts.test_s3_connection.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "20250107_120000"
            mock_dt.now.return_value.isoformat.return_value = "2025-01-07T12:00:00"

            with patch(
                "pipelines.lib.storage.s3.S3Storage", return_value=mock_storage
            ):
                result = script.main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Connection refused" in captured.out
