"""Integration tests for YAML pipeline templates with MinIO.

Tests that work1.yaml and work2.yaml templates actually work end-to-end
with real S3 operations against MinIO.

These tests verify the "take and go" principle:
- Copy a YAML file, set env vars, run it, and it works on the first try.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Generator

import pytest

from tests.integration.conftest import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    is_minio_available,
    list_objects_in_prefix,
    requires_minio,
)


# =============================================================================
# Test Configuration
# =============================================================================

TEMPLATES_DIR = Path(__file__).parent.parent.parent / "pipelines" / "templates"
WORK1_YAML = TEMPLATES_DIR / "work1.yaml"
WORK2_YAML = TEMPLATES_DIR / "work2.yaml"
SAMPLE_DATA_DIR = TEMPLATES_DIR / "data"


@pytest.fixture
def template_test_prefix() -> str:
    """Generate a unique prefix for template test isolation."""
    import uuid
    return f"template-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def cleanup_template_prefix(
    minio_client, minio_bucket, template_test_prefix
) -> Generator[str, None, None]:
    """Yield test prefix and cleanup all objects under it after test."""
    yield template_test_prefix

    # Cleanup: delete all objects with this prefix
    try:
        paginator = minio_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=minio_bucket, Prefix=template_test_prefix):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    minio_client.delete_objects(
                        Bucket=minio_bucket,
                        Delete={"Objects": objects},
                    )
    except Exception:
        pass  # Best effort cleanup


@pytest.fixture
def env_with_minio(template_test_prefix: str) -> Dict[str, str]:
    """Environment variables configured for MinIO access."""
    return {
        **os.environ.copy(),
        "S3_ENDPOINT_URL": MINIO_ENDPOINT,
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_S3_VERIFY_SSL": "false",
    }


# =============================================================================
# Helper Functions
# =============================================================================


def run_pipeline(yaml_path: Path, run_date: str, env: Dict[str, str]) -> subprocess.CompletedProcess:
    """Run a pipeline YAML file and return the result."""
    cmd = [
        sys.executable,
        "-m",
        "pipelines",
        str(yaml_path),
        "--date",
        run_date,
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        cwd=Path(__file__).parent.parent.parent,  # Project root
    )
    return result


def create_test_yaml(
    template_path: Path,
    output_path: Path,
    replacements: Dict[str, str],
) -> Path:
    """Create a test YAML file from template with string replacements."""
    content = template_path.read_text()
    for old, new in replacements.items():
        content = content.replace(old, new)
    output_path.write_text(content)
    return output_path


# =============================================================================
# Test: work1.yaml (CSV to S3)
# =============================================================================


@requires_minio
class TestWork1CsvToS3:
    """Test work1.yaml template: CSV file to S3 storage."""

    def test_work1_template_exists(self) -> None:
        """Verify work1.yaml template exists."""
        assert WORK1_YAML.exists(), f"work1.yaml not found at {WORK1_YAML}"

    def test_work1_sample_data_exists(self) -> None:
        """Verify sample CSV data exists for work1.yaml."""
        csv_path = SAMPLE_DATA_DIR / "education_2025-01-15.csv"
        assert csv_path.exists(), f"Sample data not found at {csv_path}"

    def test_work1_yaml_has_schema_reference(self) -> None:
        """Verify work1.yaml has JSON schema reference for IDE autocomplete."""
        content = WORK1_YAML.read_text()
        assert "yaml-language-server: $schema" in content, (
            "work1.yaml should have schema reference for IDE autocomplete"
        )

    def test_work1_yaml_is_self_documenting(self) -> None:
        """Verify work1.yaml has extensive comments for non-Python users."""
        content = WORK1_YAML.read_text()
        # Check for key documentation sections
        assert "BEFORE YOU RUN" in content, "Missing 'BEFORE YOU RUN' section"
        assert "TO RUN:" in content, "Missing 'TO RUN:' section"
        assert "OUTPUT FILES CREATED" in content, "Missing 'OUTPUT FILES CREATED' section"
        # Check for field explanations
        assert "# SYSTEM:" in content, "Missing SYSTEM field explanation"
        assert "# ENTITY:" in content, "Missing ENTITY field explanation"
        assert "# SOURCE_TYPE:" in content, "Missing SOURCE_TYPE field explanation"

    def test_work1_bronze_to_s3(
        self,
        minio_client,
        minio_bucket: str,
        cleanup_template_prefix: str,
        env_with_minio: Dict[str, str],
        tmp_path: Path,
    ) -> None:
        """Test work1.yaml Bronze layer writes to S3."""
        # Create test YAML with unique prefix
        test_yaml = tmp_path / "work1_test.yaml"
        replacements = {
            "s3://bronze-bucket/": f"s3://{minio_bucket}/{cleanup_template_prefix}/bronze/",
            "s3://silver-bucket/": f"s3://{minio_bucket}/{cleanup_template_prefix}/silver/",
        }
        create_test_yaml(WORK1_YAML, test_yaml, replacements)

        # Run Bronze only
        result = run_pipeline(test_yaml, "2025-01-15", env_with_minio)

        # Check for success (or expected output)
        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        # Verify Bronze output exists in S3
        bronze_prefix = f"{cleanup_template_prefix}/bronze/"
        objects = list_objects_in_prefix(minio_client, minio_bucket, bronze_prefix)

        # Should have at least data.parquet
        parquet_files = [o for o in objects if o.endswith(".parquet")]
        assert len(parquet_files) > 0, f"No parquet files found in {bronze_prefix}"


# =============================================================================
# Test: work2.yaml (Fixed-Width to S3)
# =============================================================================


@requires_minio
class TestWork2FixedWidthToS3:
    """Test work2.yaml template: Fixed-width file to S3 storage."""

    def test_work2_template_exists(self) -> None:
        """Verify work2.yaml template exists."""
        assert WORK2_YAML.exists(), f"work2.yaml not found at {WORK2_YAML}"

    def test_work2_sample_data_exists(self) -> None:
        """Verify sample fixed-width data exists for work2.yaml."""
        txt_path = SAMPLE_DATA_DIR / "daily_transactions_2025-01-15.txt"
        assert txt_path.exists(), f"Sample data not found at {txt_path}"

    def test_work2_sample_data_format(self) -> None:
        """Verify sample fixed-width data has correct format (92 chars per line)."""
        txt_path = SAMPLE_DATA_DIR / "daily_transactions_2025-01-15.txt"
        with open(txt_path, "r") as f:
            for i, line in enumerate(f, 1):
                line = line.rstrip("\n\r")
                assert len(line) == 92, (
                    f"Line {i} has {len(line)} chars, expected 92: [{line}]"
                )

    def test_work2_yaml_has_schema_reference(self) -> None:
        """Verify work2.yaml has JSON schema reference for IDE autocomplete."""
        content = WORK2_YAML.read_text()
        assert "yaml-language-server: $schema" in content, (
            "work2.yaml should have schema reference for IDE autocomplete"
        )

    def test_work2_yaml_is_self_documenting(self) -> None:
        """Verify work2.yaml has extensive comments for non-Python users."""
        content = WORK2_YAML.read_text()
        # Check for key documentation sections
        assert "WHAT IS A MULTI-RECORD FIXED-WIDTH FILE?" in content, "Missing fixed-width explanation"
        assert "columns:" in content, "Missing columns field"
        assert "widths:" in content, "Missing widths field"

    def test_work2_bronze_to_s3(
        self,
        minio_client,
        minio_bucket: str,
        cleanup_template_prefix: str,
        env_with_minio: Dict[str, str],
        tmp_path: Path,
    ) -> None:
        """Test work2.yaml Bronze layer writes to S3."""
        # Create test YAML with unique prefix
        test_yaml = tmp_path / "work2_test.yaml"
        replacements = {
            "s3://bronze-bucket/": f"s3://{minio_bucket}/{cleanup_template_prefix}/bronze/",
            "s3://silver-bucket/": f"s3://{minio_bucket}/{cleanup_template_prefix}/silver/",
        }
        create_test_yaml(WORK2_YAML, test_yaml, replacements)

        # Run Bronze only
        result = run_pipeline(test_yaml, "2025-01-15", env_with_minio)

        # Check for success (or expected output)
        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        # Verify Bronze output exists in S3
        bronze_prefix = f"{cleanup_template_prefix}/bronze/"
        objects = list_objects_in_prefix(minio_client, minio_bucket, bronze_prefix)

        # Should have at least data.parquet
        parquet_files = [o for o in objects if o.endswith(".parquet")]
        assert len(parquet_files) > 0, f"No parquet files found in {bronze_prefix}"


# =============================================================================
# Test: work3.yaml (Parent-Child Fixed-Width to S3)
# =============================================================================


WORK3_YAML = TEMPLATES_DIR / "work3.yaml"


@requires_minio
class TestWork3ParentChildToS3:
    """Test work3.yaml template: Parent-child fixed-width file to S3 storage."""

    def test_work3_template_exists(self) -> None:
        """Verify work3.yaml template exists."""
        assert WORK3_YAML.exists(), f"work3.yaml not found at {WORK3_YAML}"

    def test_work3_sample_data_exists(self) -> None:
        """Verify sample parent-child data exists for work3.yaml."""
        txt_path = SAMPLE_DATA_DIR / "customer_addresses_2025-01-15.txt"
        assert txt_path.exists(), f"Sample data not found at {txt_path}"

    def test_work3_sample_data_has_parent_child_structure(self) -> None:
        """Verify sample data has A (parent) and B (child) lines."""
        txt_path = SAMPLE_DATA_DIR / "customer_addresses_2025-01-15.txt"
        content = txt_path.read_text()
        lines = content.strip().split("\n")

        # Should have A lines (parents) and B lines (children)
        a_lines = [l for l in lines if l.startswith("A")]
        b_lines = [l for l in lines if l.startswith("B")]

        assert len(a_lines) == 3, f"Expected 3 parent (A) lines, got {len(a_lines)}"
        assert len(b_lines) == 6, f"Expected 6 child (B) lines, got {len(b_lines)}"

    def test_work3_yaml_has_schema_reference(self) -> None:
        """Verify work3.yaml has JSON schema reference for IDE autocomplete."""
        content = WORK3_YAML.read_text()
        assert "yaml-language-server: $schema" in content, (
            "work3.yaml should have schema reference for IDE autocomplete"
        )

    def test_work3_yaml_is_self_documenting(self) -> None:
        """Verify work3.yaml has extensive comments for non-Python users."""
        content = WORK3_YAML.read_text()
        # Check for key documentation sections
        assert "ABABBB" in content, "Missing ABABBB pattern explanation"
        assert "record_type_position" in content, "Missing record_type_position field"
        assert "role: parent" in content, "Missing parent role"
        assert "role: child" in content, "Missing child role"
        assert "output_mode:" in content, "Missing output_mode field"

    def test_work3_bronze_to_s3(
        self,
        minio_client,
        minio_bucket: str,
        cleanup_template_prefix: str,
        env_with_minio: Dict[str, str],
        tmp_path: Path,
    ) -> None:
        """Test work3.yaml Bronze layer writes flattened parent-child data to S3."""
        # Create test YAML with unique prefix
        test_yaml = tmp_path / "work3_test.yaml"
        replacements = {
            "s3://bronze-bucket/": f"s3://{minio_bucket}/{cleanup_template_prefix}/bronze/",
            "s3://silver-bucket/": f"s3://{minio_bucket}/{cleanup_template_prefix}/silver/",
        }
        create_test_yaml(WORK3_YAML, test_yaml, replacements)

        # Run Bronze only
        result = run_pipeline(test_yaml, "2025-01-15", env_with_minio)

        # Check for success (or expected output)
        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        # Verify Bronze output exists in S3
        bronze_prefix = f"{cleanup_template_prefix}/bronze/"
        objects = list_objects_in_prefix(minio_client, minio_bucket, bronze_prefix)

        # Should have at least data.parquet
        parquet_files = [o for o in objects if o.endswith(".parquet")]
        assert len(parquet_files) > 0, f"No parquet files found in {bronze_prefix}"


# =============================================================================
# Test: YAML Validation Error Messages
# =============================================================================


class TestYamlValidationErrors:
    """Test that YAML validation shows all missing fields at once."""

    def test_bronze_missing_multiple_fields_shows_all(self, tmp_path: Path) -> None:
        """Test that missing Bronze fields are all reported at once."""
        # Create YAML with multiple missing required fields
        yaml_content = """
bronze:
  source_path: "./data/test.csv"
"""
        yaml_path = tmp_path / "incomplete.yaml"
        yaml_path.write_text(yaml_content)

        from pipelines.lib.config_loader import YAMLConfigError, load_pipeline

        with pytest.raises(YAMLConfigError) as exc_info:
            load_pipeline(yaml_path)

        error_msg = str(exc_info.value)
        # Should mention ALL missing fields, not just the first one
        assert "system" in error_msg.lower(), "Error should mention missing 'system'"
        assert "entity" in error_msg.lower(), "Error should mention missing 'entity'"
        assert "source_type" in error_msg.lower(), "Error should mention missing 'source_type'"

    def test_silver_missing_multiple_fields_shows_all(self, tmp_path: Path) -> None:
        """Test that missing Silver fields are all reported at once."""
        # Create YAML with missing Silver required fields
        yaml_content = """
silver:
  model: full_merge_dedupe
"""
        yaml_path = tmp_path / "incomplete_silver.yaml"
        yaml_path.write_text(yaml_content)

        from pipelines.lib.config_loader import YAMLConfigError, load_pipeline

        with pytest.raises(YAMLConfigError) as exc_info:
            load_pipeline(yaml_path)

        error_msg = str(exc_info.value)
        # Should mention ALL missing fields
        assert "natural_keys" in error_msg.lower(), "Error should mention missing 'natural_keys'"
        assert "change_timestamp" in error_msg.lower(), "Error should mention missing 'change_timestamp'"


# =============================================================================
# Test: SSL Verification Default Behavior
# =============================================================================


class TestSSLVerificationDefault:
    """Test that SSL verification defaults to False (for self-signed certs)."""

    def test_s3_verify_ssl_default_in_schema(self) -> None:
        """Verify JSON schema has s3_verify_ssl default=false."""
        import json

        schema_path = TEMPLATES_DIR.parent / "schema" / "pipeline.schema.json"
        with open(schema_path) as f:
            schema = json.load(f)

        # Check Bronze section
        bronze_props = schema["definitions"]["bronze"]["properties"]
        assert "s3_verify_ssl" in bronze_props, "s3_verify_ssl not in Bronze schema"
        assert bronze_props["s3_verify_ssl"]["default"] is False, (
            "s3_verify_ssl should default to false"
        )

        # Check Silver section
        silver_props = schema["definitions"]["silver"]["properties"]
        assert "s3_verify_ssl" in silver_props, "s3_verify_ssl not in Silver schema"
        assert silver_props["s3_verify_ssl"]["default"] is False, (
            "s3_verify_ssl should default to false"
        )
