"""Tests for multi-record-type fixed-width file parsing.

Tests the parent-child (ABABBB) pattern support:
- Parent records define master data (e.g., Customer)
- Child records belong to preceding parent (e.g., Addresses)
- Output modes: flatten, parent_only, child_only
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pytest

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType


class TestParentChildFixedWidth:
    """Tests for parent-child fixed-width file parsing."""

    @pytest.fixture
    def sample_parent_child_file(self, tmp_path: Path) -> Path:
        """Create a sample parent-child fixed-width file.

        Line format after type indicator:
        - A lines: customer_id(7) + name(20) = 27 chars
        - B lines: street(15) + city(11) + state(2) + zip(5) = 33 chars
        """
        # Each line is carefully aligned to match the widths in parent_child_options
        # A lines: customer_id=7, name=20
        # B lines: street=15, city=11, state=2, zip=5
        #
        # Using explicit padding to ensure exact widths
        lines = [
            "A" + "CUST001".ljust(7) + "John Smith".ljust(20),
            "B" + "123 Main Street".ljust(15) + "Anytown".ljust(11) + "NY" + "12345",
            "B" + "456 Work Avenue".ljust(15) + "Business".ljust(11) + "NY" + "10001",
            "A" + "CUST002".ljust(7) + "Jane Doe".ljust(20),
            "B" + "789 Oak Lane".ljust(15) + "Suburb".ljust(11) + "CA" + "90210",
            "A" + "CUST003".ljust(7) + "Bob Wilson".ljust(20),
            "B" + "111 Pine Street".ljust(15) + "Downtown".ljust(11) + "TX" + "75001",
            "B" + "222 Elm Road".ljust(15) + "Uptown".ljust(11) + "TX" + "75002",
            "B" + "333 Maple Drive".ljust(15) + "Midtown".ljust(11) + "TX" + "75003",
        ]
        content = "\n".join(lines) + "\n"
        file_path = tmp_path / "customers_2025-01-15.txt"
        file_path.write_text(content)
        return file_path

    @pytest.fixture
    def parent_child_options(self) -> Dict[str, Any]:
        """Standard options for parent-child parsing."""
        return {
            "record_type_position": [0, 1],
            "record_types": [
                {
                    "type": "A",
                    "role": "parent",
                    "columns": ["customer_id", "name"],
                    "widths": [7, 20],
                },
                {
                    "type": "B",
                    "role": "child",
                    "columns": ["street", "city", "state", "zip"],
                    "widths": [15, 11, 2, 5],
                },
            ],
            "output_mode": "flatten",
        }

    def test_flatten_mode_repeats_parent_on_each_child(
        self, sample_parent_child_file: Path, parent_child_options: Dict[str, Any]
    ) -> None:
        """Test that flatten mode produces one row per child with parent columns."""
        bronze = BronzeSource(
            system="test",
            entity="customers",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(sample_parent_child_file),
            target_path=str(sample_parent_child_file.parent / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=parent_child_options,
        )

        # Execute the read
        table = bronze._read_fixed_width(str(sample_parent_child_file))
        df = table.to_pandas()

        # Should have 6 rows (one per address)
        assert len(df) == 6, f"Expected 6 rows, got {len(df)}"

        # Check columns
        expected_columns = ["customer_id", "name", "street", "city", "state", "zip"]
        assert list(df.columns) == expected_columns

        # CUST001 should appear twice (2 addresses)
        cust001_rows = df[df["customer_id"] == "CUST001"]
        assert len(cust001_rows) == 2
        assert cust001_rows["name"].iloc[0] == "John Smith"
        assert cust001_rows["name"].iloc[1] == "John Smith"

        # CUST002 should appear once
        cust002_rows = df[df["customer_id"] == "CUST002"]
        assert len(cust002_rows) == 1
        assert cust002_rows["name"].iloc[0] == "Jane Doe"

        # CUST003 should appear three times
        cust003_rows = df[df["customer_id"] == "CUST003"]
        assert len(cust003_rows) == 3
        assert cust003_rows["name"].iloc[0] == "Bob Wilson"

    def test_variable_children_per_parent(
        self, sample_parent_child_file: Path, parent_child_options: Dict[str, Any]
    ) -> None:
        """Test that variable number of children per parent works correctly."""
        bronze = BronzeSource(
            system="test",
            entity="customers",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(sample_parent_child_file),
            target_path=str(sample_parent_child_file.parent / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=parent_child_options,
        )

        table = bronze._read_fixed_width(str(sample_parent_child_file))
        df = table.to_pandas()

        # Verify the structure: ABB, AB, ABBB
        customer_counts = df.groupby("customer_id").size()
        assert customer_counts["CUST001"] == 2  # AB pattern
        assert customer_counts["CUST002"] == 1  # AB pattern (1 child)
        assert customer_counts["CUST003"] == 3  # ABBB pattern

    def test_parent_only_mode(
        self, sample_parent_child_file: Path, parent_child_options: Dict[str, Any]
    ) -> None:
        """Test that parent_only mode extracts only parent records."""
        parent_child_options["output_mode"] = "parent_only"

        bronze = BronzeSource(
            system="test",
            entity="customers",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(sample_parent_child_file),
            target_path=str(sample_parent_child_file.parent / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=parent_child_options,
        )

        table = bronze._read_fixed_width(str(sample_parent_child_file))
        df = table.to_pandas()

        # Should have 3 rows (one per customer)
        assert len(df) == 3

        # Should only have parent columns
        assert list(df.columns) == ["customer_id", "name"]

        # Verify customers
        assert set(df["customer_id"]) == {"CUST001", "CUST002", "CUST003"}

    def test_child_only_mode(
        self, sample_parent_child_file: Path, parent_child_options: Dict[str, Any]
    ) -> None:
        """Test that child_only mode extracts only child records."""
        parent_child_options["output_mode"] = "child_only"

        bronze = BronzeSource(
            system="test",
            entity="customers",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(sample_parent_child_file),
            target_path=str(sample_parent_child_file.parent / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=parent_child_options,
        )

        table = bronze._read_fixed_width(str(sample_parent_child_file))
        df = table.to_pandas()

        # Should have 6 rows (one per address)
        assert len(df) == 6

        # Should only have child columns (no parent columns)
        assert list(df.columns) == ["street", "city", "state", "zip"]

        # Verify first address
        assert df["street"].iloc[0] == "123 Main Street"
        assert df["city"].iloc[0] == "Anytown"
        assert df["state"].iloc[0] == "NY"
        assert df["zip"].iloc[0] == "12345"

    def test_error_on_orphan_child(self, tmp_path: Path) -> None:
        """Test that child record without parent raises error."""
        # File starts with a child record (no parent)
        content = """\
B123 Main Street   Anytown    NY12345
ACUST001John Smith
B456 Work Avenue   Business   NY10001
"""
        file_path = tmp_path / "orphan_child.txt"
        file_path.write_text(content)

        options = {
            "record_type_position": [0, 1],
            "record_types": [
                {
                    "type": "A",
                    "role": "parent",
                    "columns": ["customer_id", "name"],
                    "widths": [7, 20],
                },
                {
                    "type": "B",
                    "role": "child",
                    "columns": ["street", "city", "state", "zip"],
                    "widths": [15, 11, 2, 5],
                },
            ],
            "output_mode": "flatten",
        }

        bronze = BronzeSource(
            system="test",
            entity="orphan",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(file_path),
            target_path=str(tmp_path / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=options,
        )

        with pytest.raises(ValueError, match="Child record at line 1 has no parent"):
            bronze._read_fixed_width(str(file_path))

    def test_skip_role_ignores_lines(self, tmp_path: Path) -> None:
        """Test that lines with role=skip are ignored."""
        content = """\
HHEADER_LINE_IGNORED
ACUST001John Smith
B123 Main Street   Anytown    NY12345
TTRAILER_LINE_IGNORED
"""
        file_path = tmp_path / "with_skip.txt"
        file_path.write_text(content)

        options = {
            "record_type_position": [0, 1],
            "record_types": [
                {
                    "type": "H",
                    "role": "skip",  # Header - ignore
                },
                {
                    "type": "A",
                    "role": "parent",
                    "columns": ["customer_id", "name"],
                    "widths": [7, 20],
                },
                {
                    "type": "B",
                    "role": "child",
                    "columns": ["street", "city", "state", "zip"],
                    "widths": [15, 11, 2, 5],
                },
                {
                    "type": "T",
                    "role": "skip",  # Trailer - ignore
                },
            ],
            "output_mode": "flatten",
        }

        bronze = BronzeSource(
            system="test",
            entity="with_skip",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(file_path),
            target_path=str(tmp_path / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=options,
        )

        table = bronze._read_fixed_width(str(file_path))
        df = table.to_pandas()

        # Should have 1 row (only the A+B pair)
        assert len(df) == 1
        assert df["customer_id"].iloc[0] == "CUST001"

    def test_unknown_record_type_ignored(self, tmp_path: Path) -> None:
        """Test that unknown record types are silently ignored."""
        content = """\
ACUST001John Smith
B123 Main Street   Anytown    NY12345
XUNKNOWN_LINE_TYPE
B456 Work Avenue   Business   NY10001
"""
        file_path = tmp_path / "unknown_type.txt"
        file_path.write_text(content)

        options = {
            "record_type_position": [0, 1],
            "record_types": [
                {
                    "type": "A",
                    "role": "parent",
                    "columns": ["customer_id", "name"],
                    "widths": [7, 20],
                },
                {
                    "type": "B",
                    "role": "child",
                    "columns": ["street", "city", "state", "zip"],
                    "widths": [15, 11, 2, 5],
                },
            ],
            "output_mode": "flatten",
        }

        bronze = BronzeSource(
            system="test",
            entity="unknown_type",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(file_path),
            target_path=str(tmp_path / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=options,
        )

        table = bronze._read_fixed_width(str(file_path))
        df = table.to_pandas()

        # Should have 2 rows (both B lines, X line ignored)
        assert len(df) == 2
        assert df["street"].iloc[0] == "123 Main Street"
        assert df["street"].iloc[1] == "456 Work Avenue"

    def test_two_char_type_indicator(self, tmp_path: Path) -> None:
        """Test parsing with two-character type indicator."""
        content = """\
01CUST001John Smith
02123 Main Street   Anytown    NY12345
02456 Work Avenue   Business   NY10001
01CUST002Jane Doe
02789 Oak Lane      Suburb     CA90210
"""
        file_path = tmp_path / "two_char_type.txt"
        file_path.write_text(content)

        options = {
            "record_type_position": [0, 2],  # Two characters for type
            "record_types": [
                {
                    "type": "01",
                    "role": "parent",
                    "columns": ["customer_id", "name"],
                    "widths": [7, 20],
                },
                {
                    "type": "02",
                    "role": "child",
                    "columns": ["street", "city", "state", "zip"],
                    "widths": [15, 11, 2, 5],
                },
            ],
            "output_mode": "flatten",
        }

        bronze = BronzeSource(
            system="test",
            entity="two_char",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(file_path),
            target_path=str(tmp_path / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=options,
        )

        table = bronze._read_fixed_width(str(file_path))
        df = table.to_pandas()

        # Should have 3 rows
        assert len(df) == 3
        assert df["customer_id"].iloc[0] == "CUST001"
        assert df["customer_id"].iloc[1] == "CUST001"
        assert df["customer_id"].iloc[2] == "CUST002"

    def test_missing_parent_config_raises_error(self, tmp_path: Path) -> None:
        """Test that missing parent role raises ValueError."""
        file_path = tmp_path / "test.txt"
        file_path.write_text("ATEST\nBDATA")

        options = {
            "record_type_position": [0, 1],
            "record_types": [
                # No parent role defined!
                {
                    "type": "B",
                    "role": "child",
                    "columns": ["data"],
                    "widths": [10],
                },
            ],
            "output_mode": "flatten",
        }

        bronze = BronzeSource(
            system="test",
            entity="no_parent",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(file_path),
            target_path=str(tmp_path / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=options,
        )

        with pytest.raises(
            ValueError, match="Parent-child pattern requires one 'parent'"
        ):
            bronze._read_fixed_width(str(file_path))

    def test_missing_child_config_raises_error(self, tmp_path: Path) -> None:
        """Test that missing child role raises ValueError."""
        file_path = tmp_path / "test.txt"
        file_path.write_text("ATEST\nBDATA")

        options = {
            "record_type_position": [0, 1],
            "record_types": [
                {
                    "type": "A",
                    "role": "parent",
                    "columns": ["data"],
                    "widths": [10],
                },
                # No child role defined!
            ],
            "output_mode": "flatten",
        }

        bronze = BronzeSource(
            system="test",
            entity="no_child",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(file_path),
            target_path=str(tmp_path / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=options,
        )

        with pytest.raises(
            ValueError, match="Parent-child pattern requires one 'parent'"
        ):
            bronze._read_fixed_width(str(file_path))

    def test_empty_lines_ignored(self, tmp_path: Path) -> None:
        """Test that empty lines are skipped."""
        content = """\
ACUST001John Smith

B123 Main Street   Anytown    NY12345

ACUST002Jane Doe
B789 Oak Lane      Suburb     CA90210
"""
        file_path = tmp_path / "with_empty.txt"
        file_path.write_text(content)

        options = {
            "record_type_position": [0, 1],
            "record_types": [
                {
                    "type": "A",
                    "role": "parent",
                    "columns": ["customer_id", "name"],
                    "widths": [7, 20],
                },
                {
                    "type": "B",
                    "role": "child",
                    "columns": ["street", "city", "state", "zip"],
                    "widths": [15, 11, 2, 5],
                },
            ],
            "output_mode": "flatten",
        }

        bronze = BronzeSource(
            system="test",
            entity="with_empty",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(file_path),
            target_path=str(tmp_path / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=options,
        )

        table = bronze._read_fixed_width(str(file_path))
        df = table.to_pandas()

        # Should have 2 rows
        assert len(df) == 2
        assert df["customer_id"].iloc[0] == "CUST001"
        assert df["customer_id"].iloc[1] == "CUST002"


class TestSingleRecordTypeStillWorks:
    """Verify that single-record-type fixed-width files still work."""

    def test_single_record_type_unchanged(self, tmp_path: Path) -> None:
        """Test that standard fixed-width parsing is not affected."""
        content = """\
TXN0000001ACCT00000001        125.502025-01-15
TXN0000002ACCT00000002       1250.002025-01-15
"""
        file_path = tmp_path / "transactions.txt"
        file_path.write_text(content)

        # Standard options WITHOUT record_type_position
        options = {
            "columns": ["txn_id", "account_id", "amount", "date"],
            "widths": [10, 20, 12, 10],
        }

        bronze = BronzeSource(
            system="test",
            entity="transactions",
            source_type=SourceType.FILE_FIXED_WIDTH,
            source_path=str(file_path),
            target_path=str(tmp_path / "output"),
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options=options,
        )

        table = bronze._read_fixed_width(str(file_path))
        df = table.to_pandas()

        # Should have 2 rows with standard parsing
        assert len(df) == 2
        assert list(df.columns) == ["txn_id", "account_id", "amount", "date"]
        assert df["txn_id"].iloc[0] == "TXN0000001"
