"""Tests for CDC (Change Data Capture) processing.

Tests the apply_cdc function and delete mode handling:
- ignore: Filter out delete records
- tombstone: Keep deleted records with _deleted=true
- hard_delete: Filter out deletes (removal happens in Silver layer)
"""

import pytest
import ibis

from pipelines.lib.curate import apply_cdc
from pipelines.lib.silver import DeleteMode
from pipelines.lib.config_loader import (
    load_silver_from_yaml,
    load_pipeline,
    YAMLConfigError,
)


class TestDeleteModeEnum:
    """Tests for DeleteMode enum."""

    def test_delete_mode_values(self):
        """Test DeleteMode enum has expected values."""
        assert DeleteMode.IGNORE.value == "ignore"
        assert DeleteMode.TOMBSTONE.value == "tombstone"
        assert DeleteMode.HARD_DELETE.value == "hard_delete"

    def test_delete_mode_from_string(self):
        """Test DeleteMode can be accessed by value."""
        assert DeleteMode("ignore") == DeleteMode.IGNORE
        assert DeleteMode("tombstone") == DeleteMode.TOMBSTONE
        assert DeleteMode("hard_delete") == DeleteMode.HARD_DELETE


class TestApplyCDC:
    """Tests for apply_cdc function."""

    @pytest.fixture
    def cdc_data(self):
        """Create sample CDC data with I/U/D operations."""
        con = ibis.duckdb.connect()
        data = {
            "customer_id": [1, 1, 2, 2, 3],
            "name": ["Alice", "Alice Updated", "Bob", "Bob Updated", "Charlie"],
            "op": ["I", "U", "I", "D", "I"],
            "updated_at": [
                "2025-01-01",
                "2025-01-02",
                "2025-01-01",
                "2025-01-03",
                "2025-01-01",
            ],
        }
        return con.create_table("cdc_data", data)

    @pytest.fixture
    def cdc_options(self):
        """Standard CDC options."""
        return {"operation_column": "op"}

    def test_apply_cdc_ignore_mode(self, cdc_data, cdc_options):
        """Test ignore mode filters out delete records."""
        result = apply_cdc(
            cdc_data,
            ["customer_id"],
            "updated_at",
            "ignore",
            cdc_options,
        )
        df = result.execute()

        # Should have 2 records: Alice Updated (latest for customer 1), Charlie
        # Bob should be filtered out because latest op is D
        assert len(df) == 2
        assert set(df["customer_id"].tolist()) == {1, 3}
        # op column should be dropped
        assert "op" not in df.columns
        assert "_deleted" not in df.columns

    def test_apply_cdc_tombstone_mode(self, cdc_data, cdc_options):
        """Test tombstone mode keeps deleted records with _deleted flag."""
        result = apply_cdc(
            cdc_data,
            ["customer_id"],
            "updated_at",
            "tombstone",
            cdc_options,
        )
        df = result.execute()

        # Should have 3 records: all customers
        assert len(df) == 3
        assert set(df["customer_id"].tolist()) == {1, 2, 3}
        # op column should be dropped
        assert "op" not in df.columns
        # _deleted column should exist
        assert "_deleted" in df.columns
        # Customer 2 should be marked as deleted
        deleted_row = df[df["customer_id"] == 2]
        assert bool(deleted_row["_deleted"].iloc[0]) is True
        # Others should not be deleted
        assert bool(df[df["customer_id"] == 1]["_deleted"].iloc[0]) is False
        assert bool(df[df["customer_id"] == 3]["_deleted"].iloc[0]) is False

    def test_apply_cdc_hard_delete_mode(self, cdc_data, cdc_options):
        """Test hard_delete mode filters out deletes (same as ignore)."""
        result = apply_cdc(
            cdc_data,
            ["customer_id"],
            "updated_at",
            "hard_delete",
            cdc_options,
        )
        df = result.execute()

        # Same behavior as ignore for the apply_cdc function
        # (actual removal from existing data happens in Silver layer)
        assert len(df) == 2
        assert set(df["customer_id"].tolist()) == {1, 3}
        assert "op" not in df.columns
        assert "_deleted" not in df.columns

    def test_apply_cdc_custom_codes(self):
        """Test CDC with custom operation codes."""
        con = ibis.duckdb.connect()
        data = {
            "id": [1, 2, 3],
            "value": ["A", "B", "C"],
            "operation": ["INSERT", "DELETE", "UPDATE"],
            "ts": ["2025-01-01", "2025-01-02", "2025-01-01"],
        }
        t = con.create_table("custom_cdc", data)

        cdc_options = {
            "operation_column": "operation",
            "insert_code": "INSERT",
            "update_code": "UPDATE",
            "delete_code": "DELETE",
        }

        result = apply_cdc(t, ["id"], "ts", "ignore", cdc_options)
        df = result.execute()

        # Should filter out id=2 (DELETE)
        assert len(df) == 2
        assert set(df["id"].tolist()) == {1, 3}

    def test_apply_cdc_missing_operation_column_raises(self, cdc_data):
        """Test that missing operation_column raises error."""
        with pytest.raises(ValueError, match="cdc_options with operation_column"):
            apply_cdc(cdc_data, ["customer_id"], "updated_at", "ignore", None)

        with pytest.raises(ValueError, match="cdc_options with operation_column"):
            apply_cdc(cdc_data, ["customer_id"], "updated_at", "ignore", {})

    def test_apply_cdc_invalid_column_raises(self, cdc_data):
        """Test that invalid operation column raises error."""
        with pytest.raises(
            ValueError, match="Operation column 'nonexistent' not found"
        ):
            apply_cdc(
                cdc_data,
                ["customer_id"],
                "updated_at",
                "ignore",
                {"operation_column": "nonexistent"},
            )

    def test_apply_cdc_invalid_delete_mode_raises(self, cdc_data, cdc_options):
        """Test that invalid delete_mode raises error."""
        with pytest.raises(ValueError, match="Unknown delete_mode"):
            apply_cdc(
                cdc_data,
                ["customer_id"],
                "updated_at",
                "invalid_mode",
                cdc_options,
            )


class TestConfigLoaderCDC:
    """Tests for CDC configuration in config_loader."""

    def test_load_silver_with_delete_mode_ignore(self, tmp_path):
        """Test loading Silver config with delete_mode=ignore."""
        config = {
            "domain": "test",
            "subject": "test",
            "unique_columns": ["customer_id"],
            "last_updated_column": "updated_at",
            "delete_mode": "ignore",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.delete_mode == DeleteMode.IGNORE

    def test_load_silver_with_delete_mode_tombstone(self, tmp_path):
        """Test loading Silver config with delete_mode=tombstone requires CDC model."""
        config = {
            "model": "cdc_current",  # CDC model is required for tombstone/hard_delete
            "domain": "test",
            "subject": "test",
            "unique_columns": ["customer_id"],
            "last_updated_column": "updated_at",
            "delete_mode": "tombstone",  # Overrides the model default (ignore)
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.delete_mode == DeleteMode.TOMBSTONE

    def test_load_silver_with_delete_mode_hard_delete(self, tmp_path):
        """Test loading Silver config with delete_mode=hard_delete requires CDC model."""
        config = {
            "model": "cdc_current",  # CDC model is required for tombstone/hard_delete
            "domain": "test",
            "subject": "test",
            "unique_columns": ["customer_id"],
            "last_updated_column": "updated_at",
            "delete_mode": "hard_delete",  # Overrides the model default (ignore)
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.delete_mode == DeleteMode.HARD_DELETE

    def test_load_silver_default_delete_mode(self, tmp_path):
        """Test default delete_mode is ignore."""
        config = {
            "domain": "test",
            "subject": "test",
            "unique_columns": ["customer_id"],
            "last_updated_column": "updated_at",
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.delete_mode == DeleteMode.IGNORE

    def test_load_silver_invalid_delete_mode_raises(self, tmp_path):
        """Test invalid delete_mode raises error."""
        config = {
            "domain": "test",
            "subject": "test",
            "unique_columns": ["customer_id"],
            "last_updated_column": "updated_at",
            "delete_mode": "invalid",
        }
        with pytest.raises(YAMLConfigError, match="Invalid delete_mode"):
            load_silver_from_yaml(config, tmp_path)

    def test_load_silver_with_cdc_options(self, tmp_path):
        """Test loading Silver config with cdc_options."""
        config = {
            "domain": "test",
            "subject": "test",
            "unique_columns": ["customer_id"],
            "last_updated_column": "updated_at",
            "cdc_options": {
                "operation_column": "op",
            },
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.cdc_options is not None
        assert silver.cdc_options["operation_column"] == "op"

    def test_load_silver_cdc_options_with_custom_codes(self, tmp_path):
        """Test loading Silver config with custom CDC codes."""
        config = {
            "domain": "test",
            "subject": "test",
            "unique_columns": ["customer_id"],
            "last_updated_column": "updated_at",
            "cdc_options": {
                "operation_column": "operation",
                "insert_code": "C",
                "update_code": "U",
                "delete_code": "D",
            },
        }
        silver = load_silver_from_yaml(config, tmp_path)
        assert silver.cdc_options["operation_column"] == "operation"
        assert silver.cdc_options["insert_code"] == "C"
        assert silver.cdc_options["update_code"] == "U"
        assert silver.cdc_options["delete_code"] == "D"

    def test_load_silver_cdc_options_missing_operation_column_raises(self, tmp_path):
        """Test cdc_options without operation_column raises error."""
        config = {
            "domain": "test",
            "subject": "test",
            "unique_columns": ["customer_id"],
            "last_updated_column": "updated_at",
            "cdc_options": {
                "insert_code": "I",
            },
        }
        with pytest.raises(YAMLConfigError, match="operation_column is required"):
            load_silver_from_yaml(config, tmp_path)


class TestPipelineWithCDC:
    """Tests for full pipeline with CDC configuration."""

    def test_pipeline_with_cdc_options(self, tmp_path):
        """Test loading full pipeline with CDC options."""
        yaml_content = """
name: test_cdc_pipeline
bronze:
  system: test
  entity: customers
  source_type: file_csv
  source_path: ./data/customers.csv
  load_pattern: cdc
  input_mode: append_log

silver:
  domain: test
  subject: customers
  unique_columns: [customer_id]
  last_updated_column: updated_at
  delete_mode: tombstone
  cdc_options:
    operation_column: op
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.silver.delete_mode == DeleteMode.TOMBSTONE
        assert pipeline.silver.cdc_options is not None
        assert pipeline.silver.cdc_options["operation_column"] == "op"

    def test_pipeline_cdc_with_scd2(self, tmp_path):
        """Test CDC configuration with SCD2 history mode."""
        yaml_content = """
name: test_cdc_scd2
bronze:
  system: test
  entity: products
  source_type: file_csv
  source_path: ./data/products.csv
  load_pattern: cdc
  input_mode: append_log

silver:
  domain: test
  subject: products
  unique_columns: [product_id]
  last_updated_column: updated_at
  model: scd_type_2
  delete_mode: tombstone
  cdc_options:
    operation_column: _op
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        # Model should set history_mode to full_history
        from pipelines.lib.silver import HistoryMode

        assert pipeline.silver.history_mode == HistoryMode.FULL_HISTORY
        # delete_mode and cdc_options should be set
        assert pipeline.silver.delete_mode == DeleteMode.TOMBSTONE
        assert pipeline.silver.cdc_options["operation_column"] == "_op"
