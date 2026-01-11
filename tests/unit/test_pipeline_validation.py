"""Tests for pipeline-specific validation helpers."""

import pytest

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
from pipelines.lib.config_loader import (
    ValidationSeverity,
    format_validation_report,
    validate_and_raise,
    validate_bronze_source,
    validate_silver_entity,
)


def _make_valid_bronze_source() -> BronzeSource:
    return BronzeSource(
        system="sys",
        entity="orders",
        source_type=SourceType.FILE_CSV,
        source_path="/tmp/orders/{run_date}.csv",
        target_path="/tmp/bronze/system={system}/entity={entity}/dt={run_date}/",
    )


def _make_valid_silver_entity() -> SilverEntity:
    return SilverEntity(
        source_path="/tmp/bronze/system=sys/entity=orders/dt={run_date}/*.parquet",
        target_path="/tmp/silver/orders/",
        natural_keys=["order_id"],
        change_timestamp="updated_at",
        entity_kind=EntityKind.STATE,
        history_mode=HistoryMode.CURRENT_ONLY,
    )


class TestBronzeSourceValidation:
    def test_missing_required_fields_reported(self):
        source = _make_valid_bronze_source()
        source.system = ""
        source.entity = ""
        source.source_path = ""
        source.target_path = ""

        issues = validate_bronze_source(source)
        error_fields = {
            issue.field
            for issue in issues
            if issue.severity == ValidationSeverity.ERROR
        }
        assert {"system", "entity", "target_path", "source_path"} <= error_fields

    def test_incremental_requires_watermark(self):
        source = _make_valid_bronze_source()
        source.load_pattern = LoadPattern.INCREMENTAL_APPEND
        source.watermark_column = None

        issues = validate_bronze_source(source)
        assert any(issue.field == "watermark_column" for issue in issues)


class TestSilverEntityValidation:
    def test_missing_attributes_reported(self):
        entity = _make_valid_silver_entity()
        entity.source_path = ""
        entity.target_path = ""
        entity.natural_keys = []
        entity.change_timestamp = ""

        issues = validate_silver_entity(entity)
        error_fields = [
            issue.field
            for issue in issues
            if issue.severity == ValidationSeverity.ERROR
        ]
        assert "source_path" in error_fields
        assert "target_path" in error_fields
        assert "natural_keys" in error_fields
        assert "change_timestamp" in error_fields

    def test_warning_for_event_full_history(self):
        entity = _make_valid_silver_entity()
        entity.entity_kind = EntityKind.EVENT
        entity.history_mode = HistoryMode.FULL_HISTORY

        issues = validate_silver_entity(entity)
        warnings = [
            issue for issue in issues if issue.severity == ValidationSeverity.WARNING
        ]
        assert any("history_mode" == issue.field for issue in warnings)


class TestValidationHelpers:
    def test_format_report_empty(self):
        assert format_validation_report([]) == "Configuration is valid."

    def test_format_report_includes_messages(self):
        entity = _make_valid_silver_entity()
        entity.source_path = ""
        entity.target_path = ""
        issues = validate_silver_entity(entity)
        report = format_validation_report(issues)
        assert "Source path is required" in report

    def test_validate_and_raise_errors_propagate(self):
        source = _make_valid_bronze_source()
        source.source_type = SourceType.DATABASE_MSSQL
        source.options = {}
        source.target_path = ""
        with pytest.raises(ValueError, match="Configuration validation failed"):
            validate_and_raise(source=source)


class TestBronzeSourceValidateMethod:
    """Tests for BronzeSource.validate() method."""

    def test_validate_returns_empty_for_valid_config(self, tmp_path):
        """Valid configuration returns no issues."""
        # Create a source file
        source_file = tmp_path / "data.csv"
        source_file.write_text("id,name\n1,test\n")

        source = BronzeSource(
            system="test",
            entity="table",
            source_type=SourceType.FILE_CSV,
            source_path=str(source_file),
            target_path=str(tmp_path / "bronze"),
        )

        issues = source.validate(check_connectivity=False)
        assert issues == []

    def test_validate_with_connectivity_check_for_missing_file(self, tmp_path):
        """Connectivity check reports missing file."""
        source = BronzeSource(
            system="test",
            entity="table",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "nonexistent.csv"),
            target_path=str(tmp_path / "bronze"),
        )

        issues = source.validate(check_connectivity=True)
        assert any("not found" in issue.lower() for issue in issues)

    def test_validate_skips_connectivity_for_template_path(self, tmp_path):
        """Connectivity check skipped for paths with {run_date} template."""
        source = BronzeSource(
            system="test",
            entity="table",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "{run_date}" / "data.csv"),
            target_path=str(tmp_path / "bronze"),
        )

        # Should not report missing file since path contains template
        issues = source.validate(check_connectivity=True)
        assert not any("not found" in issue.lower() for issue in issues)

    def test_validate_with_run_date_resolves_path(self, tmp_path):
        """Connectivity check with run_date resolves template."""
        # Create dated directory
        dated_dir = tmp_path / "2025-01-15"
        dated_dir.mkdir()
        (dated_dir / "data.csv").write_text("id\n1\n")

        source = BronzeSource(
            system="test",
            entity="table",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "{run_date}" / "data.csv"),
            target_path=str(tmp_path / "bronze"),
        )

        issues = source.validate("2025-01-15", check_connectivity=True)
        assert issues == []


class TestSilverEntityValidateMethod:
    """Tests for SilverEntity.validate() method."""

    def test_validate_returns_empty_for_valid_config(self, tmp_path):
        """Valid configuration returns no issues."""
        # Create source data
        source_dir = tmp_path / "bronze"
        source_dir.mkdir()

        entity = SilverEntity(
            source_path=str(source_dir / "*.parquet"),
            target_path=str(tmp_path / "silver"),
            natural_keys=["id"],
            change_timestamp="updated_at",
        )

        issues = entity.validate(check_source=False)
        assert issues == []

    def test_validate_with_source_check_for_missing_data(self, tmp_path):
        """Source check reports missing Bronze data."""
        entity = SilverEntity(
            source_path=str(tmp_path / "bronze" / "{run_date}" / "*.parquet"),
            target_path=str(tmp_path / "silver"),
            natural_keys=["id"],
            change_timestamp="updated_at",
        )

        issues = entity.validate("2025-01-15", check_source=True)
        assert any("not found" in issue.lower() for issue in issues)

    def test_validate_with_existing_source_data(self, tmp_path):
        """Source check passes when data exists."""
        import pandas as pd

        # Create source data
        source_dir = tmp_path / "bronze" / "2025-01-15"
        source_dir.mkdir(parents=True)

        # Write a parquet file using pandas
        df = pd.DataFrame([{"id": 1, "name": "test", "updated_at": "2025-01-15"}])
        df.to_parquet(str(source_dir / "data.parquet"))

        entity = SilverEntity(
            source_path=str(tmp_path / "bronze" / "{run_date}" / "*.parquet"),
            target_path=str(tmp_path / "silver"),
            natural_keys=["id"],
            change_timestamp="updated_at",
        )

        issues = entity.validate("2025-01-15", check_source=True)
        assert issues == []
