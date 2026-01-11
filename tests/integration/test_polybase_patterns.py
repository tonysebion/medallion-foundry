"""Integration tests for PolyBase DDL generation across all pattern combinations.

These tests verify that the PolyBase DDL generated for each Bronze→Silver pattern
combination is correct and includes the appropriate columns, views, and functions.
"""

import json
import os
import pytest
import boto3
from botocore.config import Config as BotoConfig

from pipelines.lib.polybase import (
    PolyBaseConfig,
    generate_from_metadata_dict,
)

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "mdf")


def is_minio_available() -> bool:
    """Check if MinIO is reachable."""
    try:
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1",
            config=BotoConfig(signature_version="s3v4"),
        )
        client.list_buckets()
        return True
    except Exception:
        return False


# Skip all tests in this module if MinIO is not available
pytestmark = pytest.mark.skipif(not is_minio_available(), reason="MinIO not available")


@pytest.fixture
def polybase_config():
    """Standard PolyBase configuration for tests."""
    return PolyBaseConfig(
        data_source_name="silver_source",
        data_source_location="wasbs://silver@account.blob.core.windows.net/",
    )


@pytest.fixture
def minio_client():
    """Create MinIO/S3 client."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=BotoConfig(signature_version="s3v4"),
    )


def get_metadata_from_minio(client, bucket: str, prefix: str) -> dict:
    """Read _metadata.json from MinIO."""
    key = f"{prefix}/_metadata.json"
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except client.exceptions.NoSuchKey:
        pytest.skip(f"No metadata at {key} - run demo script first")
    except Exception as e:
        pytest.skip(f"Could not read metadata: {e}")


def get_polybase_ddl_from_minio(client, bucket: str, prefix: str) -> str:
    """Read _polybase.sql from MinIO."""
    key = f"{prefix}/_polybase.sql"
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")
    except client.exceptions.NoSuchKey:
        pytest.skip(f"No polybase DDL at {key} - run demo script first")
    except Exception as e:
        pytest.skip(f"Could not read DDL: {e}")


@pytest.mark.integration
class TestPolyBaseDDLForPatterns:
    """Verify PolyBase DDL is correct for each pattern combination."""

    def test_snapshot_scd1_generates_current_view_only(
        self, minio_client, polybase_config
    ):
        """Snapshot SCD1 should have current view without SCD2 functions."""
        metadata = get_metadata_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=snapshot_scd1/dt=2025-01-12",
        )

        ddl = generate_from_metadata_dict(
            metadata, polybase_config, entity_name="snapshot_scd1"
        )

        # Should have current view
        assert "vw_snapshot_scd1_state_current" in ddl
        # Should NOT have SCD2 functions (no effective_from in SCD1)
        assert "fn_snapshot_scd1_state_as_of" not in ddl
        assert "fn_snapshot_scd1_state_history" not in ddl

    def test_snapshot_scd2_has_point_in_time_functions(
        self, minio_client, polybase_config
    ):
        """Snapshot SCD2 should have point-in-time and history functions."""
        metadata = get_metadata_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=snapshot_scd2/dt=2025-01-12",
        )

        ddl = generate_from_metadata_dict(
            metadata, polybase_config, entity_name="snapshot_scd2"
        )

        # Should have current view with is_current filter
        assert "vw_snapshot_scd2_state_current" in ddl
        assert "is_current = 1" in ddl
        # Should have SCD2 functions
        assert "fn_snapshot_scd2_state_as_of" in ddl
        assert "effective_from <= @as_of_date" in ddl
        assert "fn_snapshot_scd2_state_history" in ddl

    def test_cdc_scd1_tombstone_filters_deleted_in_view(
        self, minio_client, polybase_config
    ):
        """CDC SCD1 with tombstone should filter _deleted in current view."""
        metadata = get_metadata_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=cdc_scd1_tombstone/dt=2025-01-12",
        )

        ddl = generate_from_metadata_dict(
            metadata, polybase_config, entity_name="cdc_scd1_tombstone"
        )

        # Should filter deleted records
        assert "_deleted = 0" in ddl or "_deleted IS NULL" in ddl

    def test_cdc_scd2_tombstone_excludes_deleted_from_point_in_time(
        self, minio_client, polybase_config
    ):
        """CDC SCD2 with tombstone should exclude deleted from point-in-time function."""
        metadata = get_metadata_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=cdc_scd2_tombstone/dt=2025-01-12",
        )

        ddl = generate_from_metadata_dict(
            metadata, polybase_config, entity_name="cdc_scd2_tombstone"
        )

        # Point-in-time should exclude deleted
        assert "fn_cdc_scd2_tombstone_state_as_of" in ddl
        # The deleted filter should appear after the temporal filter
        assert "_deleted = 0 OR _deleted IS NULL" in ddl

    def test_event_pattern_has_date_functions(self, minio_client, polybase_config):
        """Event pattern should have date range and daily summary views."""
        metadata = get_metadata_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=cdc_event/dt=2025-01-12",
        )

        ddl = generate_from_metadata_dict(
            metadata, polybase_config, entity_name="cdc_event"
        )

        # Should have event-specific functions
        assert "fn_cdc_event_events_for_dates" in ddl or "fn_cdc_event_for_dates" in ddl
        assert "vw_cdc_event" in ddl


@pytest.mark.integration
class TestPolyBaseDDLColumns:
    """Verify PolyBase DDL includes correct columns for each pattern."""

    def test_scd2_metadata_has_temporal_columns(self, minio_client):
        """SCD2 metadata should include effective_from, effective_to, is_current."""
        metadata = get_metadata_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=snapshot_scd2/dt=2025-01-12",
        )

        column_names = [col["name"] for col in metadata.get("columns", [])]
        assert "effective_from" in column_names
        assert "effective_to" in column_names
        assert "is_current" in column_names

    def test_tombstone_metadata_has_deleted_column(self, minio_client):
        """Tombstone mode metadata should include _deleted column."""
        metadata = get_metadata_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=cdc_scd1_tombstone/dt=2025-01-12",
        )

        column_names = [col["name"] for col in metadata.get("columns", [])]
        assert "_deleted" in column_names

    def test_hard_delete_no_deleted_column(self, minio_client):
        """Hard delete mode should NOT have _deleted column."""
        metadata = get_metadata_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=cdc_scd1_hard/dt=2025-01-12",
        )

        column_names = [col["name"] for col in metadata.get("columns", [])]
        # Hard delete physically removes records, no _deleted column needed
        assert "_deleted" not in column_names


@pytest.mark.integration
class TestPolyBaseCompositeKeys:
    """Test PolyBase DDL with composite natural keys."""

    def test_composite_key_generates_multi_param_history(self, polybase_config):
        """Composite key should generate multi-parameter history function."""
        # Create metadata with composite key
        metadata = {
            "columns": [
                {"name": "order_id", "sql_type": "BIGINT", "nullable": False},
                {"name": "line_id", "sql_type": "INT", "nullable": False},
                {"name": "product", "sql_type": "NVARCHAR(255)", "nullable": True},
                {"name": "effective_from", "sql_type": "DATETIME2", "nullable": True},
                {"name": "effective_to", "sql_type": "DATETIME2", "nullable": True},
                {"name": "is_current", "sql_type": "BIT", "nullable": True},
            ],
            "entity_kind": "state",
            "history_mode": "full_history",
            "natural_keys": ["order_id", "line_id"],
            "change_timestamp": "effective_from",
        }

        ddl = generate_from_metadata_dict(
            metadata, polybase_config, entity_name="order_lines"
        )

        # Should have composite key parameters
        assert "@key_0" in ddl
        assert "@key_1" in ddl
        assert "[order_id] = @key_0" in ddl
        assert "[line_id] = @key_1" in ddl

    def test_single_key_uses_simple_parameter(self, polybase_config):
        """Single natural key should use simple @key_value parameter."""
        metadata = {
            "columns": [
                {"name": "customer_id", "sql_type": "BIGINT", "nullable": False},
                {"name": "name", "sql_type": "NVARCHAR(255)", "nullable": True},
                {"name": "effective_from", "sql_type": "DATETIME2", "nullable": True},
                {"name": "effective_to", "sql_type": "DATETIME2", "nullable": True},
                {"name": "is_current", "sql_type": "BIT", "nullable": True},
            ],
            "entity_kind": "state",
            "history_mode": "full_history",
            "natural_keys": ["customer_id"],
            "change_timestamp": "effective_from",
        }

        ddl = generate_from_metadata_dict(
            metadata, polybase_config, entity_name="customers"
        )

        # Should have simple parameter
        assert "@key_value NVARCHAR(255)" in ddl
        assert "[customer_id] = @key_value" in ddl


@pytest.mark.integration
class TestGeneratedPolybaseDDLFromDemo:
    """Test the actual _polybase.sql files generated by the demo script."""

    def test_snapshot_scd1_ddl_file(self, minio_client):
        """Verify snapshot_scd1 generated DDL is valid."""
        ddl = get_polybase_ddl_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=snapshot_scd1/dt=2025-01-12",
        )

        assert "CREATE EXTERNAL TABLE" in ddl
        assert "CREATE OR ALTER VIEW" in ddl

    def test_snapshot_scd2_ddl_file(self, minio_client):
        """Verify snapshot_scd2 generated DDL has SCD2 functions."""
        ddl = get_polybase_ddl_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=snapshot_scd2/dt=2025-01-12",
        )

        assert "CREATE EXTERNAL TABLE" in ddl
        assert "fn_" in ddl and "_as_of" in ddl  # Point-in-time function
        assert "effective_from" in ddl

    def test_cdc_tombstone_ddl_file_has_deleted_filter(self, minio_client):
        """Verify CDC tombstone generated DDL filters _deleted."""
        ddl = get_polybase_ddl_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=cdc_scd1_tombstone/dt=2025-01-12",
        )

        assert "CREATE EXTERNAL TABLE" in ddl
        assert "_deleted" in ddl

    def test_event_ddl_file_has_date_functions(self, minio_client):
        """Verify event entity generated DDL has date functions."""
        ddl = get_polybase_ddl_from_minio(
            minio_client,
            MINIO_BUCKET,
            "demo/silver/domain=demo/subject=cdc_event/dt=2025-01-12",
        )

        assert "CREATE EXTERNAL TABLE" in ddl
        assert "for_date" in ddl.lower()  # Date query function
