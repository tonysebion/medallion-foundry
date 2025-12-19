"""Tests for pipelines/lib/errors.py - structured exception hierarchy."""

import pytest

from pipelines.lib.errors import (
    PipelineError,
    BronzeExtractionError,
    SilverCurationError,
    ConnectionError,
    ConfigurationError,
    ChecksumError,
    SourceNotFoundError,
    ValidationError,
)


class TestPipelineError:
    """Tests for base PipelineError class."""

    def test_basic_message(self):
        """Test error with just a message."""
        error = PipelineError("Something went wrong")
        assert "Something went wrong" in str(error)

    def test_with_system_and_entity(self):
        """Test error with system and entity context."""
        error = PipelineError(
            "Extraction failed",
            system="claims",
            entity="header",
        )
        assert "[claims.header]" in str(error)
        assert "Extraction failed" in str(error)

    def test_with_details(self):
        """Test error with details dict."""
        error = PipelineError(
            "Connection error",
            details={"host": "myserver.com", "port": 1433},
        )
        assert "host: myserver.com" in str(error)
        assert "port: 1433" in str(error)

    def test_with_suggestion(self):
        """Test error with fix suggestion."""
        error = PipelineError(
            "Missing credentials",
            suggestion="Set the DB_PASSWORD environment variable",
        )
        assert "Set the DB_PASSWORD environment variable" in str(error)

    def test_to_dict(self):
        """Test conversion to dictionary."""
        error = PipelineError(
            "Test error",
            system="test_system",
            entity="test_entity",
            details={"key": "value"},
            suggestion="Fix it",
        )
        d = error.to_dict()
        assert d["error_type"] == "PipelineError"
        assert d["system"] == "test_system"
        assert d["entity"] == "test_entity"
        assert d["details"]["key"] == "value"
        assert d["suggestion"] == "Fix it"


class TestBronzeExtractionError:
    """Tests for BronzeExtractionError."""

    def test_with_source(self):
        """Test error with BronzeSource context."""
        from pipelines.lib.bronze import BronzeSource, SourceType

        source = BronzeSource(
            system="claims",
            entity="header",
            source_type=SourceType.FILE_CSV,
            source_path="/data/claims.csv",
            target_path="/bronze/claims/header/",
        )

        error = BronzeExtractionError(
            "Failed to read file",
            source=source,
        )

        assert "claims.header" in str(error)
        assert "file_csv" in str(error)

    def test_with_cause(self):
        """Test error with underlying cause."""
        from pipelines.lib.bronze import BronzeSource, SourceType

        source = BronzeSource(
            system="test",
            entity="table",
            source_type=SourceType.FILE_CSV,
            source_path="/data/test.csv",
            target_path="/bronze/test/",
        )

        original = ValueError("Invalid data format")
        error = BronzeExtractionError(
            "Extraction failed",
            source=source,
            cause=original,
        )

        assert "Invalid data format" in str(error)
        assert "ValueError" in str(error)


class TestSilverCurationError:
    """Tests for SilverCurationError."""

    def test_with_entity(self):
        """Test error with SilverEntity context."""
        from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

        entity = SilverEntity(
            source_path="/bronze/claims/header/*.parquet",
            target_path="/silver/claims/header/",
            natural_keys=["ClaimID"],
            change_timestamp="LastUpdated",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
        )

        error = SilverCurationError(
            "Deduplication failed",
            entity=entity,
        )

        assert "Deduplication failed" in str(error)
        assert "current_only" in str(error)


class TestConnectionError:
    """Tests for ConnectionError."""

    def test_with_connection_details(self):
        """Test error with connection info."""
        error = ConnectionError(
            "Cannot connect to database",
            connection_name="claims_db",
            host="myserver.database.com",
        )

        assert "Cannot connect to database" in str(error)
        assert "claims_db" in str(error)
        assert "myserver.database.com" in str(error)

    def test_default_suggestion(self):
        """Test default suggestion is included."""
        error = ConnectionError("Connection failed")
        assert "reachable" in str(error) or "credentials" in str(error)


class TestConfigurationError:
    """Tests for ConfigurationError."""

    def test_with_field_and_value(self):
        """Test error with field info."""
        error = ConfigurationError(
            "Invalid value for field",
            field="watermark_column",
            value="nonexistent_col",
        )

        assert "watermark_column" in str(error)
        assert "nonexistent_col" in str(error)


class TestChecksumError:
    """Tests for ChecksumError."""

    def test_with_checksum_details(self):
        """Test error with checksum mismatch info."""
        error = ChecksumError(
            "Checksum mismatch detected",
            file_path="/data/claims.parquet",
            expected="abc123",
            actual="def456",
        )

        assert "/data/claims.parquet" in str(error)
        assert "abc123" in str(error)
        assert "def456" in str(error)

    def test_default_suggestion(self):
        """Test default suggestion is included."""
        error = ChecksumError("Data integrity error")
        assert "corrupted" in str(error).lower() or "re-run" in str(error).lower()


class TestSourceNotFoundError:
    """Tests for SourceNotFoundError."""

    def test_with_path(self):
        """Test error with missing path."""
        error = SourceNotFoundError(
            "Source file not found",
            path="/bronze/claims/header/2025-01-15/*.parquet",
        )

        assert "/bronze/claims/header/2025-01-15/*.parquet" in str(error)

    def test_default_suggestion(self):
        """Test default suggestion mentions Bronze."""
        error = SourceNotFoundError("Data not found")
        assert "Bronze" in str(error) or "source" in str(error).lower()


class TestValidationError:
    """Tests for ValidationError."""

    def test_with_issues_list(self):
        """Test error with list of issues."""
        error = ValidationError(
            "Configuration validation failed",
            issues=[
                "system is required",
                "target_path is required",
                "watermark_column required for incremental loads",
            ],
        )

        assert "system is required" in str(error)
        assert "target_path is required" in str(error)
        assert "watermark_column" in str(error)

    def test_issue_count_in_details(self):
        """Test issue count is captured."""
        error = ValidationError(
            "Validation failed",
            issues=["issue1", "issue2", "issue3"],
        )

        assert error.to_dict()["details"]["issue_count"] == 3
