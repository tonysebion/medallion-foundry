"""Tests for pipeline-specific validation helpers."""

import pytest

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
from pipelines.lib.validate import (
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
        error_fields = {issue.field for issue in issues if issue.severity == ValidationSeverity.ERROR}
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
        error_fields = [issue.field for issue in issues if issue.severity == ValidationSeverity.ERROR]
        assert "source_path" in error_fields
        assert "target_path" in error_fields
        assert "natural_keys" in error_fields
        assert "change_timestamp" in error_fields

    def test_warning_for_event_full_history(self):
        entity = _make_valid_silver_entity()
        entity.entity_kind = EntityKind.EVENT
        entity.history_mode = HistoryMode.FULL_HISTORY

        issues = validate_silver_entity(entity)
        warnings = [issue for issue in issues if issue.severity == ValidationSeverity.WARNING]
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
