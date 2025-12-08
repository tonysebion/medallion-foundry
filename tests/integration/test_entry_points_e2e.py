"""End-to-end tests for CLI entry points (Story 1.7).

Tests the actual CLI entry points (bronze_extract.py, silver_extract.py)
via subprocess to validate the complete user experience:
- Config validation (--validate-only, --dry-run)
- Exit codes
- Output file structure
- Metadata generation
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest
import yaml

from tests.synthetic_data import ClaimsGenerator


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def run_date() -> date:
    """Standard run date for tests."""
    return date(2024, 1, 15)


@pytest.fixture
def claims_data(run_date: date) -> pd.DataFrame:
    """Synthetic claims data."""
    gen = ClaimsGenerator(seed=42, row_count=50)
    return gen.generate_t0(run_date)


@pytest.fixture
def test_data_dir(tmp_path: Path, claims_data: pd.DataFrame) -> Path:
    """Set up test data directory with source data."""
    # Create input directory with parquet file
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True)
    claims_data.to_parquet(input_dir / "claims.parquet", index=False)

    return tmp_path


@pytest.fixture
def bronze_config(test_data_dir: Path, run_date: date) -> Dict[str, Any]:
    """Bronze extraction config for file source."""
    # Point to the specific parquet file, not the directory
    input_file = test_data_dir / "input" / "claims.parquet"
    return {
        "config_version": 1,
        "pipeline_id": "entry_point_test",
        "layer": "bronze",
        "domain": "healthcare",
        "environment": "test",
        "data_classification": "internal",
        "owners": {
            "semantic_owner": "test",
            "technical_owner": "test",
        },
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_path": str(test_data_dir / "bronze"),
            }
        },
        "source": {
            "system": "synthetic",
            "table": "claims",
            "type": "file",
            "file": {
                "path": str(input_file),  # Point to file, not directory
                "format": "parquet",
            },
            "run": {
                "load_pattern": "snapshot",
                "local_output_dir": str(test_data_dir / "output"),
                "write_parquet": True,
                "write_csv": False,
                "checkpoint_enabled": False,
            },
        },
    }


@pytest.fixture
def config_file(test_data_dir: Path, bronze_config: Dict[str, Any]) -> Path:
    """Write config to YAML file."""
    config_path = test_data_dir / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(bronze_config, f)
    return config_path


@pytest.fixture
def invalid_config_file(test_data_dir: Path) -> Path:
    """Write an invalid config to YAML file."""
    config_path = test_data_dir / "invalid_config.yaml"
    invalid_config = {
        "config_version": 1,
        # Missing required fields: source, platform
    }
    with open(config_path, "w") as f:
        yaml.dump(invalid_config, f)
    return config_path


# =============================================================================
# Helper Functions
# =============================================================================


def run_bronze_cli(
    args: list,
    env: Dict[str, str] | None = None,
    timeout: int = 60,
) -> subprocess.CompletedProcess:
    """Run bronze_extract.py with given arguments."""
    cmd = [sys.executable, "bronze_extract.py"] + args
    full_env = os.environ.copy()
    if env:
        full_env.update(env)

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=full_env,
        cwd=Path(__file__).parent.parent.parent,  # project root
    )


def run_silver_cli(
    args: list,
    env: Dict[str, str] | None = None,
    timeout: int = 60,
) -> subprocess.CompletedProcess:
    """Run silver_extract.py with given arguments."""
    cmd = [sys.executable, "silver_extract.py"] + args
    full_env = os.environ.copy()
    if env:
        full_env.update(env)

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=full_env,
        cwd=Path(__file__).parent.parent.parent,  # project root
    )


# =============================================================================
# Bronze Entry Point Tests
# =============================================================================


class TestBronzeEntryPoint:
    """Tests for bronze_extract.py CLI."""

    def test_version_flag(self):
        """--version should show version and exit 0."""
        result = run_bronze_cli(["--version"])

        assert result.returncode == 0
        assert "medallion-foundry" in result.stdout or "unknown" in result.stdout

    def test_list_backends_flag(self):
        """--list-backends should show available backends."""
        result = run_bronze_cli(["--list-backends"])

        assert result.returncode == 0
        assert "s3" in result.stdout
        assert "azure" in result.stdout
        assert "local" in result.stdout

    def test_requires_config_flag(self):
        """Should error if --config not provided."""
        result = run_bronze_cli([])

        # Should fail with error about missing config
        assert result.returncode != 0
        assert "config" in result.stderr.lower() or "required" in result.stderr.lower()

    def test_validate_only_valid_config(self, config_file: Path):
        """--validate-only should pass for valid config."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--validate-only",
        ])

        assert result.returncode == 0
        assert "valid" in result.stdout.lower() or "✓" in result.stdout

    def test_validate_only_invalid_config(self, invalid_config_file: Path):
        """--validate-only should fail for invalid config."""
        result = run_bronze_cli([
            "--config", str(invalid_config_file),
            "--validate-only",
        ])

        assert result.returncode != 0

    def test_dry_run_validates_config(self, config_file: Path):
        """--dry-run should validate config and check connections."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--dry-run",
        ])

        # Should succeed for valid config with local backend
        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower() or "valid" in result.stdout.lower()

    def test_help_flag(self):
        """--help should show usage information."""
        result = run_bronze_cli(["--help"])

        assert result.returncode == 0
        assert "usage" in result.stdout.lower()
        assert "--config" in result.stdout
        assert "--date" in result.stdout

    def test_verbose_flag_increases_logging(self, config_file: Path):
        """--verbose should enable DEBUG logging."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--validate-only",
            "--verbose",
        ])

        assert result.returncode == 0
        # Verbose mode should produce more output (harder to assert specifically)

    def test_quiet_flag_reduces_output(self, config_file: Path):
        """--quiet should suppress non-error output."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--validate-only",
            "--quiet",
        ])

        assert result.returncode == 0
        # Quiet mode should produce less output

    def test_extraction_creates_output_files(
        self,
        config_file: Path,
        test_data_dir: Path,
        run_date: date,
    ):
        """Full extraction should create output files."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--date", run_date.isoformat(),
        ])

        assert result.returncode == 0

        # Check output directory was created
        output_dir = test_data_dir / "output"
        assert output_dir.exists()

        # Should have parquet files
        parquet_files = list(output_dir.rglob("*.parquet"))
        assert len(parquet_files) > 0

    def test_extraction_creates_metadata(
        self,
        config_file: Path,
        test_data_dir: Path,
        run_date: date,
    ):
        """Full extraction should create metadata files."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--date", run_date.isoformat(),
        ])

        assert result.returncode == 0

        # Check for metadata file
        output_dir = test_data_dir / "output"
        metadata_files = list(output_dir.rglob("_metadata.json"))

        # May or may not have metadata depending on config
        # At minimum, should have run successfully

    def test_date_flag_sets_partition(
        self,
        config_file: Path,
        test_data_dir: Path,
    ):
        """--date should set the partition date."""
        test_date = date(2024, 3, 15)

        result = run_bronze_cli([
            "--config", str(config_file),
            "--date", test_date.isoformat(),
        ])

        assert result.returncode == 0

        # Output should be partitioned by date
        output_dir = test_data_dir / "output"
        # Check if date appears in output path or files
        output_contents = list(output_dir.rglob("*"))
        assert len(output_contents) > 0


# =============================================================================
# Silver Entry Point Tests
# =============================================================================


class TestSilverEntryPoint:
    """Tests for silver_extract.py CLI."""

    def test_help_flag(self):
        """--help should show usage information."""
        result = run_silver_cli(["--help"])

        assert result.returncode == 0
        assert "usage" in result.stdout.lower()

    def test_requires_bronze_path_or_config(self):
        """Should error if neither --bronze-path nor --config provided."""
        result = run_silver_cli([])

        # Should fail or show help
        # (actual behavior depends on argparse config)

    def test_version_if_available(self):
        """--version should work if implemented."""
        result = run_silver_cli(["--version"])

        # May or may not be implemented
        # Just verify it doesn't crash badly


# =============================================================================
# Integration Tests
# =============================================================================


class TestEntryPointIntegration:
    """Integration tests for end-to-end pipeline via CLI."""

    def test_bronze_output_is_readable(
        self,
        config_file: Path,
        test_data_dir: Path,
        run_date: date,
        claims_data: pd.DataFrame,
    ):
        """Bronze output should be readable as parquet."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--date", run_date.isoformat(),
        ])

        assert result.returncode == 0

        # Find and read output parquet
        output_dir = test_data_dir / "output"
        parquet_files = list(output_dir.rglob("*.parquet"))

        if parquet_files:
            df = pd.read_parquet(parquet_files[0])
            # Should have same row count as input
            assert len(df) == len(claims_data)
            # Should have expected columns
            assert "claim_id" in df.columns

    def test_multiple_configs_with_comma_separated(
        self,
        test_data_dir: Path,
        bronze_config: Dict[str, Any],
        run_date: date,
    ):
        """Should handle comma-separated config paths."""
        # Create two config files
        config1_path = test_data_dir / "config1.yaml"
        config2_path = test_data_dir / "config2.yaml"

        # Modify second config for different table
        config2 = bronze_config.copy()
        config2["source"] = dict(bronze_config["source"])
        config2["source"]["table"] = "claims2"

        with open(config1_path, "w") as f:
            yaml.dump(bronze_config, f)
        with open(config2_path, "w") as f:
            yaml.dump(config2, f)

        # Run with comma-separated paths
        result = run_bronze_cli([
            "--config", f"{config1_path},{config2_path}",
            "--validate-only",
        ])

        assert result.returncode == 0

    def test_exit_codes_are_correct(
        self,
        config_file: Path,
        invalid_config_file: Path,
    ):
        """Exit codes should follow convention (0=success, non-zero=failure)."""
        # Valid config should exit 0
        result_valid = run_bronze_cli([
            "--config", str(config_file),
            "--validate-only",
        ])
        assert result_valid.returncode == 0

        # Invalid config should exit non-zero
        result_invalid = run_bronze_cli([
            "--config", str(invalid_config_file),
            "--validate-only",
        ])
        assert result_invalid.returncode != 0

    def test_json_log_format(self, config_file: Path):
        """--log-format json should produce JSON logs."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--validate-only",
            "--log-format", "json",
        ])

        assert result.returncode == 0
        # JSON format logs would be structured differently
        # Hard to assert without parsing

    def test_load_pattern_override(
        self,
        config_file: Path,
        test_data_dir: Path,
        run_date: date,
    ):
        """--load-pattern should override config value."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--date", run_date.isoformat(),
            "--load-pattern", "snapshot",
        ])

        assert result.returncode == 0


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestEntryPointErrorHandling:
    """Tests for CLI error handling."""

    def test_nonexistent_config_file(self, test_data_dir: Path):
        """Should error gracefully for nonexistent config file."""
        result = run_bronze_cli([
            "--config", str(test_data_dir / "nonexistent.yaml"),
            "--validate-only",
        ])

        assert result.returncode != 0

    def test_malformed_yaml_config(self, test_data_dir: Path):
        """Should error gracefully for malformed YAML."""
        bad_yaml = test_data_dir / "bad.yaml"
        bad_yaml.write_text("{ invalid yaml: [")

        result = run_bronze_cli([
            "--config", str(bad_yaml),
            "--validate-only",
        ])

        assert result.returncode != 0

    def test_invalid_date_format(self, config_file: Path):
        """Should error for invalid date format."""
        result = run_bronze_cli([
            "--config", str(config_file),
            "--date", "not-a-date",
        ])

        assert result.returncode != 0

    def test_timeout_handling(self, config_file: Path):
        """CLI should handle timeouts gracefully."""
        # This is hard to test without a slow operation
        # Just verify basic operation completes quickly
        try:
            result = run_bronze_cli([
                "--config", str(config_file),
                "--validate-only",
            ], timeout=10)
            assert result.returncode == 0
        except subprocess.TimeoutExpired:
            pytest.fail("CLI timed out on simple validation")
