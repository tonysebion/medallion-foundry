"""Tests for EVENT assertion helpers."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from tests.data_validation.assertions import EventAssertions


class TestEventNoSCD2Columns:
    """Test assert_no_scd2_columns for events."""

    def test_no_scd2_columns_pass(self):
        """Event data without SCD2 columns should pass."""
        df = pd.DataFrame([
            {"event_id": "e1", "event_type": "click", "ts": datetime(2025, 1, 1)},
        ])
        result = EventAssertions.assert_no_scd2_columns(df)
        assert result.passed is True

    def test_has_scd2_columns_fail(self):
        """Event data with SCD2 columns should fail."""
        df = pd.DataFrame([
            {"event_id": "e1", "effective_from": datetime(2025, 1, 1)},
        ])
        result = EventAssertions.assert_no_scd2_columns(df)
        assert result.passed is False


class TestEventRecordsImmutable:
    """Test assert_records_immutable."""

    def test_immutable_pass(self):
        """Unchanged records should pass."""
        before = pd.DataFrame([
            {"event_id": "e1", "data": "original"},
            {"event_id": "e2", "data": "original"},
        ])
        after = pd.DataFrame([
            {"event_id": "e1", "data": "original"},
            {"event_id": "e2", "data": "original"},
            {"event_id": "e3", "data": "new"},  # New record OK
        ])
        result = EventAssertions.assert_records_immutable(before, after, "event_id")
        assert result.passed is True

    def test_modified_record_fail(self):
        """Modified existing record should fail."""
        before = pd.DataFrame([
            {"event_id": "e1", "data": "original"},
        ])
        after = pd.DataFrame([
            {"event_id": "e1", "data": "modified"},  # Changed!
        ])
        result = EventAssertions.assert_records_immutable(before, after, "event_id")
        assert result.passed is False

    def test_empty_before(self):
        """Empty before DataFrame should pass."""
        before = pd.DataFrame({"event_id": [], "data": []})
        after = pd.DataFrame([
            {"event_id": "e1", "data": "new"},
        ])
        result = EventAssertions.assert_records_immutable(before, after, "event_id")
        assert result.passed is True


class TestEventAppendOnly:
    """Test assert_append_only."""

    def test_append_only_pass(self):
        """Only adding records should pass."""
        before = pd.DataFrame([
            {"event_id": "e1"},
            {"event_id": "e2"},
        ])
        after = pd.DataFrame([
            {"event_id": "e1"},
            {"event_id": "e2"},
            {"event_id": "e3"},  # New
        ])
        result = EventAssertions.assert_append_only(before, after, "event_id")
        assert result.passed is True

    def test_record_removed_fail(self):
        """Removed records should fail."""
        before = pd.DataFrame([
            {"event_id": "e1"},
            {"event_id": "e2"},
        ])
        after = pd.DataFrame([
            {"event_id": "e1"},
            # e2 removed!
        ])
        result = EventAssertions.assert_append_only(before, after, "event_id")
        assert result.passed is False
        assert "e2" in result.details["removed_keys"]


class TestEventNoExactDuplicates:
    """Test assert_no_exact_duplicates."""

    def test_no_duplicates_pass(self):
        """Unique rows should pass."""
        df = pd.DataFrame([
            {"event_id": "e1", "data": "a"},
            {"event_id": "e2", "data": "b"},
            {"event_id": "e3", "data": "c"},
        ])
        result = EventAssertions.assert_no_exact_duplicates(df)
        assert result.passed is True

    def test_exact_duplicates_fail(self):
        """Exact duplicate rows should fail."""
        df = pd.DataFrame([
            {"event_id": "e1", "data": "a"},
            {"event_id": "e1", "data": "a"},  # Exact duplicate
            {"event_id": "e2", "data": "b"},
        ])
        result = EventAssertions.assert_no_exact_duplicates(df)
        assert result.passed is False

    def test_near_duplicates_pass(self):
        """Near duplicates (same key, different data) should pass."""
        df = pd.DataFrame([
            {"event_id": "e1", "data": "a"},
            {"event_id": "e1", "data": "b"},  # Same key, different data - not exact
        ])
        result = EventAssertions.assert_no_exact_duplicates(df)
        assert result.passed is True

    def test_empty_dataframe(self):
        """Empty DataFrame should pass."""
        df = pd.DataFrame({"event_id": [], "data": []})
        result = EventAssertions.assert_no_exact_duplicates(df)
        assert result.passed is True
