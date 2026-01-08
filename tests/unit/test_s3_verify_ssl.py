"""Tests for S3 SSL verification bypass support.

Tests that s3_verify_ssl option is properly handled:
- Default behavior: verify=False (for self-signed certificates)
- Explicit s3_verify_ssl: true enables verification
- AWS_S3_VERIFY_SSL environment variable support
- Options take precedence over environment variables
"""

from __future__ import annotations

import os
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from pipelines.lib.bronze import _extract_storage_options


class TestExtractStorageOptions:
    """Tests for _extract_storage_options function."""

    def test_s3_verify_ssl_false_from_options(self) -> None:
        """Test that s3_verify_ssl: false is extracted from options."""
        options: Dict[str, Any] = {"s3_verify_ssl": False}
        result = _extract_storage_options(options)
        assert result["verify_ssl"] is False

    def test_s3_verify_ssl_true_from_options(self) -> None:
        """Test that s3_verify_ssl: true is extracted from options."""
        options: Dict[str, Any] = {"s3_verify_ssl": True}
        result = _extract_storage_options(options)
        assert result["verify_ssl"] is True

    def test_s3_verify_ssl_defaults_to_false(self) -> None:
        """Test that verify_ssl defaults to False when not specified."""
        options: Dict[str, Any] = {"endpoint_url": "http://localhost:9000"}
        with patch.dict(os.environ, {}, clear=True):
            # Clear any existing AWS_S3_VERIFY_SSL
            os.environ.pop("AWS_S3_VERIFY_SSL", None)
            result = _extract_storage_options(options)
        assert result["verify_ssl"] is False

    def test_s3_verify_ssl_true_from_environment(self) -> None:
        """Test that verify_ssl=True when AWS_S3_VERIFY_SSL=true.

        Note: With empty options, function still needs at least one option
        to trigger the environment check. We use a dummy option.
        """
        options: Dict[str, Any] = {"endpoint_url": "http://localhost:9000"}
        with patch.dict(os.environ, {"AWS_S3_VERIFY_SSL": "true"}, clear=False):
            result = _extract_storage_options(options)
        assert result["verify_ssl"] is True

    def test_s3_verify_ssl_true_from_environment_uppercase(self) -> None:
        """Test that verify_ssl=True when AWS_S3_VERIFY_SSL=TRUE."""
        options: Dict[str, Any] = {"endpoint_url": "http://localhost:9000"}
        with patch.dict(os.environ, {"AWS_S3_VERIFY_SSL": "TRUE"}, clear=False):
            result = _extract_storage_options(options)
        # Environment variable is case-insensitive (lowercased)
        assert result["verify_ssl"] is True

    def test_s3_verify_ssl_true_from_environment_yes(self) -> None:
        """Test that verify_ssl=True when AWS_S3_VERIFY_SSL=yes."""
        options: Dict[str, Any] = {"endpoint_url": "http://localhost:9000"}
        with patch.dict(os.environ, {"AWS_S3_VERIFY_SSL": "yes"}, clear=False):
            result = _extract_storage_options(options)
        assert result["verify_ssl"] is True

    def test_s3_verify_ssl_true_from_environment_one(self) -> None:
        """Test that verify_ssl=True when AWS_S3_VERIFY_SSL=1."""
        options: Dict[str, Any] = {"endpoint_url": "http://localhost:9000"}
        with patch.dict(os.environ, {"AWS_S3_VERIFY_SSL": "1"}, clear=False):
            result = _extract_storage_options(options)
        assert result["verify_ssl"] is True

    def test_s3_verify_ssl_false_from_environment(self) -> None:
        """Test that verify_ssl=False when AWS_S3_VERIFY_SSL=false."""
        options: Dict[str, Any] = {"endpoint_url": "http://localhost:9000"}
        with patch.dict(os.environ, {"AWS_S3_VERIFY_SSL": "false"}, clear=False):
            result = _extract_storage_options(options)
        assert result["verify_ssl"] is False

    def test_options_take_precedence_over_env(self) -> None:
        """Test that options take precedence over environment variables."""
        options: Dict[str, Any] = {"s3_verify_ssl": True}
        with patch.dict(os.environ, {"AWS_S3_VERIFY_SSL": "false"}):
            result = _extract_storage_options(options)
        assert result["verify_ssl"] is True

    def test_options_false_takes_precedence_over_env_true(self) -> None:
        """Test that options=False takes precedence over env=true."""
        options: Dict[str, Any] = {"s3_verify_ssl": False}
        with patch.dict(os.environ, {"AWS_S3_VERIFY_SSL": "true"}):
            result = _extract_storage_options(options)
        assert result["verify_ssl"] is False

    def test_empty_options_defaults_to_false(self) -> None:
        """Test that empty options with endpoint defaults verify_ssl to False."""
        options: Dict[str, Any] = {"endpoint_url": "http://localhost:9000"}
        with patch.dict(os.environ, {}, clear=False):
            # Ensure AWS_S3_VERIFY_SSL is not set
            os.environ.pop("AWS_S3_VERIFY_SSL", None)
            result = _extract_storage_options(options)
        assert result["verify_ssl"] is False

    def test_none_options_returns_empty_dict(self) -> None:
        """Test that None options returns empty dict."""
        result = _extract_storage_options(None)
        assert result == {}


class TestS3StorageClient:
    """Tests for S3Storage client SSL verification."""

    def test_verify_false_passed_to_boto3_by_default(self) -> None:
        """Test that verify=False is passed to boto3.client by default."""
        with patch("pipelines.lib.storage.s3.boto3.client") as mock_boto3_client:
            mock_boto3_client.return_value = MagicMock()

            # Import after patching
            from pipelines.lib.storage.s3 import S3Storage

            storage = S3Storage("s3://test-bucket/")
            # Access client property to trigger lazy load
            _ = storage.client

            mock_boto3_client.assert_called_once()
            call_kwargs = mock_boto3_client.call_args[1]
            assert call_kwargs.get("verify") is False

    def test_verify_false_passed_when_option_set(self) -> None:
        """Test that verify=False is passed when verify_ssl=False in options."""
        with patch("pipelines.lib.storage.s3.boto3.client") as mock_boto3_client:
            mock_boto3_client.return_value = MagicMock()

            from pipelines.lib.storage.s3 import S3Storage

            storage = S3Storage("s3://test-bucket/", verify_ssl=False)
            _ = storage.client

            mock_boto3_client.assert_called_once()
            call_kwargs = mock_boto3_client.call_args[1]
            assert call_kwargs.get("verify") is False

    def test_verify_not_passed_when_true(self) -> None:
        """Test that verify is not passed (default boto3 behavior) when verify_ssl=True."""
        with patch("pipelines.lib.storage.s3.boto3.client") as mock_boto3_client:
            mock_boto3_client.return_value = MagicMock()

            from pipelines.lib.storage.s3 import S3Storage

            storage = S3Storage("s3://test-bucket/", verify_ssl=True)
            _ = storage.client

            mock_boto3_client.assert_called_once()
            call_kwargs = mock_boto3_client.call_args[1]
            # verify should not be in kwargs when True (let boto3 use its default)
            assert "verify" not in call_kwargs

    def test_verify_from_environment_variable_true(self) -> None:
        """Test that verify_ssl is read from environment when not in options."""
        with patch("pipelines.lib.storage.s3.boto3.client") as mock_boto3_client:
            mock_boto3_client.return_value = MagicMock()

            with patch.dict(os.environ, {"AWS_S3_VERIFY_SSL": "true"}):
                from pipelines.lib.storage.s3 import S3Storage

                storage = S3Storage("s3://test-bucket/")
                storage._client = None  # Reset to force new client creation
                _ = storage.client

            # When env var is "true", verify should not be in kwargs
            call_kwargs = mock_boto3_client.call_args[1]
            assert "verify" not in call_kwargs

    def test_verify_from_environment_variable_false(self) -> None:
        """Test that verify=False when AWS_S3_VERIFY_SSL=false in environment."""
        with patch("pipelines.lib.storage.s3.boto3.client") as mock_boto3_client:
            mock_boto3_client.return_value = MagicMock()

            with patch.dict(os.environ, {"AWS_S3_VERIFY_SSL": "false"}):
                from pipelines.lib.storage.s3 import S3Storage

                storage = S3Storage("s3://test-bucket/")
                storage._client = None  # Reset to force new client creation
                _ = storage.client

            call_kwargs = mock_boto3_client.call_args[1]
            assert call_kwargs.get("verify") is False
