"""Tests for enhanced error taxonomy with error codes."""

from core.exceptions import (
    BronzeFoundryError,
    ConfigValidationError,
    ExtractionError,
    StorageError,
    AuthenticationError,
    PaginationError,
    StateManagementError,
    DataQualityError,
    RetryExhaustedError,
)


def test_base_error_code():
    """Test that base exception has error code."""
    err = BronzeFoundryError("Test error")
    assert err.error_code == "ERR000"
    assert "[ERR000]" in str(err)


def test_config_validation_error_code():
    """Test ConfigValidationError has correct error code."""
    err = ConfigValidationError("Invalid config", config_path="/path/to/config.yaml")
    assert err.error_code == "CFG001"
    assert "[CFG001]" in str(err)
    assert "config_path=/path/to/config.yaml" in str(err)


def test_extraction_error_code():
    """Test ExtractionError has correct error code."""
    err = ExtractionError(
        "Extraction failed", extractor_type="api", system="test_system"
    )
    assert err.error_code == "EXT001"
    assert "[EXT001]" in str(err)


def test_storage_error_code():
    """Test StorageError has correct error code."""
    err = StorageError("Upload failed", backend_type="s3", operation="upload")
    assert err.error_code == "STG001"
    assert "[STG001]" in str(err)


def test_authentication_error_code():
    """Test AuthenticationError has correct error code."""
    err = AuthenticationError("Auth failed", auth_type="bearer")
    assert err.error_code == "AUTH001"
    assert "[AUTH001]" in str(err)


def test_pagination_error_code():
    """Test PaginationError has correct error code."""
    err = PaginationError("Pagination failed", pagination_type="cursor")
    assert err.error_code == "PAGE001"
    assert "[PAGE001]" in str(err)


def test_state_management_error_code():
    """Test StateManagementError has correct error code."""
    err = StateManagementError("State error", state_file="/path/to/state.json")
    assert err.error_code == "STATE001"
    assert "[STATE001]" in str(err)


def test_data_quality_error_code():
    """Test DataQualityError has correct error code."""
    err = DataQualityError("Quality check failed", check_type="schema")
    assert err.error_code == "QUAL001"
    assert "[QUAL001]" in str(err)


def test_retry_exhausted_error_code():
    """Test RetryExhaustedError has correct error code."""
    err = RetryExhaustedError("All retries failed", attempts=5, operation="api_call")
    assert err.error_code == "RETRY001"
    assert "[RETRY001]" in str(err)


def test_error_with_details():
    """Test error string includes details."""
    err = StorageError(
        "Upload failed",
        backend_type="s3",
        operation="upload",
        file_path="/local/file.csv",
        remote_path="bronze/data.csv",
    )

    error_str = str(err)
    assert "[STG001]" in error_str
    assert "Upload failed" in error_str
    assert "backend_type=s3" in error_str
    assert "operation=upload" in error_str


def test_error_with_original_exception():
    """Test error wraps original exception."""
    original = ValueError("Original error")
    err = ExtractionError("Wrapped error", original_error=original)

    assert err.original_error is original
    assert "ValueError" in str(err)


def test_custom_error_code_override():
    """Test that error code can be overridden."""
    err = BronzeFoundryError("Test", error_code="CUSTOM001")
    assert err.error_code == "CUSTOM001"
    assert "[CUSTOM001]" in str(err)


def test_exception_inheritance():
    """Test exception hierarchy."""
    assert issubclass(ConfigValidationError, BronzeFoundryError)
    assert issubclass(ExtractionError, BronzeFoundryError)
    assert issubclass(StorageError, BronzeFoundryError)
    assert issubclass(AuthenticationError, ExtractionError)
    assert issubclass(PaginationError, ExtractionError)


def test_error_details_dict():
    """Test that details are accessible as dict."""
    err = StorageError(
        "Test error",
        backend_type="azure",
        operation="download",
        remote_path="container/blob",
    )

    assert err.details["backend_type"] == "azure"
    assert err.details["operation"] == "download"
    assert err.details["remote_path"] == "container/blob"
