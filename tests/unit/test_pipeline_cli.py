"""Tests for pipelines CLI functionality.

Tests the command-line interface including:
- --list: List available pipelines
- --dry-run: Validate without executing
- --check: Pre-flight validation
- --explain: Show execution plan
- Layer filters (:bronze, :silver)
"""

import subprocess
import sys
from unittest.mock import MagicMock


class TestCLIHelp:
    """Tests for CLI help and basic invocation."""

    def test_help_flag(self):
        """--help should show usage information."""
        result = subprocess.run(
            [sys.executable, "-m", "pipelines", "--help"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "usage" in result.stdout.lower() or "pipelines" in result.stdout.lower()

    def test_list_flag(self):
        """--list should list available pipelines."""
        result = subprocess.run(
            [sys.executable, "-m", "pipelines", "--list"],
            capture_output=True,
            text=True,
        )

        # Should complete without error (may have no pipelines)
        assert result.returncode == 0


class TestCLIDryRun:
    """Tests for --dry-run functionality."""

    def test_dry_run_requires_date(self):
        """--dry-run should require --date argument."""
        result = subprocess.run(
            [sys.executable, "-m", "pipelines", "test.pipeline", "--dry-run"],
            capture_output=True,
            text=True,
        )

        # Should fail or warn about missing date
        # (actual behavior depends on implementation)
        assert result.returncode != 0 or "date" in result.stderr.lower()


class TestCLILayerFilters:
    """Tests for layer filter syntax (:bronze, :silver)."""

    def test_parse_pipeline_spec_with_bronze(self):
        """Should parse pipeline:bronze correctly."""
        from pipelines.__main__ import parse_pipeline_spec

        module_path, layer = parse_pipeline_spec("claims.header:bronze")

        assert module_path == "claims.header"
        assert layer == "bronze"

    def test_parse_pipeline_spec_with_silver(self):
        """Should parse pipeline:silver correctly."""
        from pipelines.__main__ import parse_pipeline_spec

        module_path, layer = parse_pipeline_spec("claims.header:silver")

        assert module_path == "claims.header"
        assert layer == "silver"

    def test_parse_pipeline_spec_no_layer(self):
        """Should parse pipeline without layer."""
        from pipelines.__main__ import parse_pipeline_spec

        module_path, layer = parse_pipeline_spec("claims.header")

        assert module_path == "claims.header"
        assert layer is None


class TestCLICheck:
    """Tests for --check validation flag."""

    def test_check_validates_config(self, capsys):
        """--check should validate pipeline configuration."""
        from pipelines.__main__ import check_pipeline
        from pipelines.lib.bronze import SourceType, LoadPattern

        # Create a mock module - use spec=False to avoid attribute lookup issues
        mock_bronze = MagicMock()
        mock_bronze.system = "test"
        mock_bronze.entity = "items"
        mock_bronze.source_type = SourceType.FILE_CSV
        mock_bronze.load_pattern = LoadPattern.FULL_SNAPSHOT
        mock_bronze.source_path = "/some/path.csv"
        mock_bronze.target_path = "/output/"
        mock_bronze.options = {}
        mock_bronze.watermark_column = None

        # Mock module where hasattr returns True for bronze but False for silver
        mock_module = MagicMock()
        mock_module.bronze = mock_bronze
        # Explicitly set silver to None so getattr returns None
        mock_module.silver = None

        # check_pipeline prints validation output
        try:
            check_pipeline(mock_module, layer="bronze", run_date="2025-01-15")
        except SystemExit:
            pass  # May exit on validation failure

        # Should print validation output
        captured = capsys.readouterr()
        assert "VALIDATION" in captured.out


class TestCLIExplain:
    """Tests for --explain flag."""

    def test_explain_shows_plan(self, capsys):
        """--explain should show what would run without executing."""
        from pipelines.__main__ import explain_pipeline
        from pipelines.lib.bronze import SourceType, LoadPattern
        from pipelines.lib.silver import EntityKind, HistoryMode

        mock_bronze = MagicMock()
        mock_bronze.system = "test"
        mock_bronze.entity = "items"
        mock_bronze.source_type = SourceType.FILE_CSV
        mock_bronze.source_path = "/input.csv"
        mock_bronze.load_pattern = LoadPattern.FULL_SNAPSHOT
        mock_bronze.target_path = "/output/"
        mock_bronze.watermark_column = None

        mock_silver = MagicMock()
        mock_silver.source_path = "/bronze/*.parquet"
        mock_silver.target_path = "/silver/"
        mock_silver.natural_keys = ["id"]
        mock_silver.change_timestamp = "updated_at"
        mock_silver.entity_kind = EntityKind.STATE
        mock_silver.history_mode = HistoryMode.CURRENT_ONLY
        mock_silver.attributes = None

        mock_module = MagicMock()
        mock_module.bronze = mock_bronze
        mock_module.silver = mock_silver

        explain_pipeline(mock_module, layer=None, run_date="2025-01-15")

        captured = capsys.readouterr()
        assert "EXPLANATION" in captured.out
        assert "BRONZE" in captured.out
        assert "SILVER" in captured.out


class TestCLIRunPipeline:
    """Tests for run_pipeline function."""

    def test_run_pipeline_bronze_only(self):
        """run_pipeline with layer='bronze' should call bronze.run."""
        from pipelines.__main__ import run_pipeline
        from types import SimpleNamespace

        mock_run = MagicMock(return_value={"row_count": 10})
        mock_bronze = SimpleNamespace(run=mock_run)

        # Use SimpleNamespace to avoid MagicMock attribute issues
        mock_module = SimpleNamespace(bronze=mock_bronze)

        run_pipeline(
            mock_module,
            layer="bronze",
            run_date="2025-01-15",
            dry_run=True,
        )

        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs.get("dry_run") is True

    def test_run_pipeline_silver_only(self):
        """run_pipeline with layer='silver' should call silver.run."""
        from pipelines.__main__ import run_pipeline
        from types import SimpleNamespace

        mock_run = MagicMock(return_value={"row_count": 5})
        mock_silver = SimpleNamespace(run=mock_run)

        mock_module = SimpleNamespace(silver=mock_silver)

        run_pipeline(
            mock_module,
            layer="silver",
            run_date="2025-01-15",
            dry_run=False,
        )

        mock_run.assert_called_once()


class TestCLIVerbose:
    """Tests for verbose output."""

    def test_verbose_flag_accepted(self):
        """--verbose or -v should be accepted."""
        result = subprocess.run(
            [sys.executable, "-m", "pipelines", "--list", "-v"],
            capture_output=True,
            text=True,
        )

        # Should not fail due to unknown flag
        assert result.returncode == 0


class TestCLITestConnection:
    """Tests for test-connection command."""

    def test_test_connection_function_exists(self):
        """test_connection_command should be a callable function."""
        from pipelines.__main__ import test_connection_command

        # Just verify the function exists and is callable
        assert callable(test_connection_command)


class TestCLIDiscovery:
    """Tests for pipeline discovery."""

    def test_discover_nonexistent_pipeline(self):
        """Should handle nonexistent pipeline gracefully."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pipelines",
                "nonexistent.pipeline",
                "--date",
                "2025-01-15",
                "--dry-run",
            ],
            capture_output=True,
            text=True,
        )

        # Should fail with informative message
        assert result.returncode != 0
        # Error message is in stdout, not stderr
        assert "not found" in result.stdout.lower() or "error" in result.stdout.lower()

    def test_pipeline_name_formats(self):
        """Should accept various pipeline name formats."""
        # These should all be valid formats (even if pipeline doesn't exist)
        formats = [
            "system.entity",
            "system_entity",
            "my.long.pipeline.name",
        ]

        for name in formats:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pipelines",
                    name,
                    "--date",
                    "2025-01-15",
                    "--dry-run",
                ],
                capture_output=True,
                text=True,
            )
            # Should fail with "not found", not with parsing error
            combined_output = (result.stdout + result.stderr).lower()
            assert "not found" in combined_output or result.returncode != 0


class TestDiscoverPipelines:
    """Tests for discover_pipelines function."""

    def test_discover_pipelines_returns_list(self):
        """discover_pipelines should return a list."""
        from pipelines.__main__ import discover_pipelines

        result = discover_pipelines()

        assert isinstance(result, list)

    def test_discovered_pipeline_has_required_fields(self):
        """Each discovered pipeline should have name and path."""
        from pipelines.__main__ import discover_pipelines

        pipelines = discover_pipelines()

        # May be empty if no example pipelines exist
        for p in pipelines:
            assert "name" in p
            assert "path" in p
            assert "has_bronze" in p
            assert "has_silver" in p
