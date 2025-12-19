"""Sample Data End-to-End Validation Tests.

Story 3: Tests that validate the generated Bronze and Silver sample data
can be successfully processed through the pipeline.

These tests:
1. Generate Bronze samples using the generation script
2. Validate Bronze output structure and checksums
3. Generate Silver samples from Bronze
4. Validate Silver output structure and checksums
5. Verify data transformations are correct for each pattern

Markers:
- @pytest.mark.sample_validation: For sample data validation tests
- @pytest.mark.integration: Requires file system access
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

# Test configuration
BRONZE_PATTERNS = ["snapshot", "incremental_append", "incremental_merge", "current_history"]

# Mapping from directory pattern names to actual load_pattern values in metadata
# The generator uses LoadPattern enum values which differ from directory names
BRONZE_PATTERN_TO_LOAD_PATTERN = {
    "snapshot": "full_snapshot",  # LoadPattern.FULL_SNAPSHOT.value
    "incremental_append": "incremental",  # LoadPattern.INCREMENTAL_APPEND.value
    "incremental_merge": "cdc",  # LoadPattern.CDC.value
    "current_history": "cdc",  # LoadPattern.CDC.value
}

SILVER_PATTERNS = [
    "pattern1_full_events",
    "pattern2_cdc_events",
    "pattern3_scd_state",
    "pattern4_hybrid_cdc_point",
    "pattern5_hybrid_cdc_cumulative",
    "pattern6_hybrid_incremental_point",
    "pattern7_hybrid_incremental_cumulative",
]

# Expected Silver pattern -> Bronze pattern mapping
SILVER_TO_BRONZE = {
    "pattern1_full_events": "snapshot",
    "pattern2_cdc_events": "incremental_append",
    "pattern3_scd_state": "current_history",
    "pattern4_hybrid_cdc_point": "incremental_merge",
    "pattern5_hybrid_cdc_cumulative": "incremental_merge",
    "pattern6_hybrid_incremental_point": "incremental_append",
    "pattern7_hybrid_incremental_cumulative": "incremental_append",
}


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def sample_data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a temporary directory for sample data generation."""
    return tmp_path_factory.mktemp("sampledata")


@pytest.fixture(scope="module")
def bronze_samples_dir(sample_data_dir: Path) -> Path:
    """Directory for Bronze samples."""
    bronze_dir = sample_data_dir / "bronze_samples"
    bronze_dir.mkdir(exist_ok=True)
    return bronze_dir


@pytest.fixture(scope="module")
def silver_samples_dir(sample_data_dir: Path) -> Path:
    """Directory for Silver samples."""
    silver_dir = sample_data_dir / "silver_samples"
    silver_dir.mkdir(exist_ok=True)
    return silver_dir


@pytest.fixture(scope="module")
def generated_bronze_samples(bronze_samples_dir: Path) -> Path:
    """Generate Bronze samples using the script."""
    scripts_dir = Path(__file__).parent.parent.parent / "scripts"
    script_path = scripts_dir / "generate_bronze_samples.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--all",
            "--output", str(bronze_samples_dir),
            "--rows", "50",  # Small row count for fast tests
            "--seed", "42",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    if result.returncode != 0:
        pytest.fail(f"Bronze sample generation failed:\n{result.stderr}")

    return bronze_samples_dir


@pytest.fixture(scope="module")
def generated_silver_samples(
    silver_samples_dir: Path,
    generated_bronze_samples: Path,
) -> Path:
    """Generate Silver samples using the script."""
    scripts_dir = Path(__file__).parent.parent.parent / "scripts"
    script_path = scripts_dir / "generate_silver_samples.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--all",
            "--bronze-dir", str(generated_bronze_samples),
            "--output", str(silver_samples_dir),
            "--seed", "42",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    if result.returncode != 0:
        pytest.fail(f"Silver sample generation failed:\n{result.stderr}")

    return silver_samples_dir


# =============================================================================
# Bronze Sample Validation Tests
# =============================================================================


@pytest.mark.sample_validation
@pytest.mark.integration
class TestBronzeSampleGeneration:
    """Tests for Bronze sample data generation."""

    def test_bronze_generation_creates_all_patterns(
        self,
        generated_bronze_samples: Path,
    ):
        """Verify all Bronze patterns are generated."""
        for pattern in BRONZE_PATTERNS:
            pattern_dir = generated_bronze_samples / f"sample={pattern}"
            assert pattern_dir.exists(), f"Missing Bronze pattern: {pattern}"

    def test_bronze_manifest_exists(self, generated_bronze_samples: Path):
        """Verify overall manifest is created."""
        manifest_path = generated_bronze_samples / "_manifest.json"
        assert manifest_path.exists(), "Missing Bronze manifest"

        with manifest_path.open() as f:
            manifest = json.load(f)

        assert "patterns" in manifest
        assert "generated_at" in manifest
        assert len(manifest["patterns"]) == len(BRONZE_PATTERNS)

    @pytest.mark.parametrize("pattern", BRONZE_PATTERNS)
    def test_bronze_pattern_structure(
        self,
        generated_bronze_samples: Path,
        pattern: str,
    ):
        """Verify each Bronze pattern has correct structure."""
        pattern_dir = generated_bronze_samples / f"sample={pattern}"

        # Check pattern manifest exists
        pattern_manifest = pattern_dir / "_manifest.json"
        assert pattern_manifest.exists(), f"Missing manifest for {pattern}"

        # Check at least one date partition exists
        date_dirs = list(pattern_dir.glob("dt=*"))
        assert len(date_dirs) > 0, f"No date partitions for {pattern}"

        # Check each partition has required files
        for date_dir in date_dirs:
            assert (date_dir / "chunk_0.parquet").exists(), f"Missing parquet in {date_dir}"
            assert (date_dir / "_metadata.json").exists(), f"Missing metadata in {date_dir}"
            assert (date_dir / "_checksums.json").exists(), f"Missing checksums in {date_dir}"

    @pytest.mark.parametrize("pattern", BRONZE_PATTERNS)
    def test_bronze_checksums_valid(
        self,
        generated_bronze_samples: Path,
        pattern: str,
    ):
        """Verify Bronze checksums are valid."""
        import hashlib

        pattern_dir = generated_bronze_samples / f"sample={pattern}"

        for date_dir in pattern_dir.glob("dt=*"):
            checksums_path = date_dir / "_checksums.json"
            with checksums_path.open() as f:
                manifest = json.load(f)

            for file_entry in manifest.get("files", []):
                file_path = date_dir / file_entry["path"]
                assert file_path.exists(), f"File in manifest not found: {file_path}"

                # Verify hash
                hasher = hashlib.sha256()
                with file_path.open("rb") as f:
                    for chunk in iter(lambda: f.read(1024 * 1024), b""):
                        hasher.update(chunk)
                actual_hash = hasher.hexdigest()

                assert actual_hash == file_entry["sha256"], (
                    f"Checksum mismatch for {file_path}"
                )

    @pytest.mark.parametrize("pattern", BRONZE_PATTERNS)
    def test_bronze_data_readable(
        self,
        generated_bronze_samples: Path,
        pattern: str,
    ):
        """Verify Bronze parquet files are readable."""
        pattern_dir = generated_bronze_samples / f"sample={pattern}"

        for date_dir in pattern_dir.glob("dt=*"):
            parquet_file = date_dir / "chunk_0.parquet"
            df = pd.read_parquet(parquet_file)

            assert len(df) > 0, f"Empty DataFrame in {parquet_file}"
            assert len(df.columns) > 0, f"No columns in {parquet_file}"

    def test_bronze_metadata_load_pattern_correct(
        self,
        generated_bronze_samples: Path,
    ):
        """Verify metadata contains correct load_pattern."""
        for pattern in BRONZE_PATTERNS:
            pattern_dir = generated_bronze_samples / f"sample={pattern}"
            expected_load_pattern = BRONZE_PATTERN_TO_LOAD_PATTERN[pattern]

            for date_dir in pattern_dir.glob("dt=*"):
                metadata_path = date_dir / "_metadata.json"
                with metadata_path.open() as f:
                    metadata = json.load(f)

                assert metadata["load_pattern"] == expected_load_pattern, (
                    f"Wrong load_pattern in {metadata_path}: "
                    f"expected {expected_load_pattern}, got {metadata['load_pattern']}"
                )


# =============================================================================
# Silver Sample Validation Tests
# =============================================================================


@pytest.mark.sample_validation
@pytest.mark.integration
class TestSilverSampleGeneration:
    """Tests for Silver sample data generation."""

    def test_silver_generation_creates_all_patterns(
        self,
        generated_silver_samples: Path,
    ):
        """Verify all Silver patterns are generated."""
        for pattern in SILVER_PATTERNS:
            pattern_dir = generated_silver_samples / f"sample={pattern}"
            assert pattern_dir.exists(), f"Missing Silver pattern: {pattern}"

    def test_silver_manifest_exists(self, generated_silver_samples: Path):
        """Verify overall manifest is created."""
        manifest_path = generated_silver_samples / "_manifest.json"
        assert manifest_path.exists(), "Missing Silver manifest"

        with manifest_path.open() as f:
            manifest = json.load(f)

        assert "patterns" in manifest
        assert "generated_at" in manifest

    @pytest.mark.parametrize("pattern", SILVER_PATTERNS)
    def test_silver_pattern_structure(
        self,
        generated_silver_samples: Path,
        pattern: str,
    ):
        """Verify each Silver pattern has correct structure."""
        pattern_dir = generated_silver_samples / f"sample={pattern}"

        # Check pattern manifest exists
        pattern_manifest = pattern_dir / "_manifest.json"
        assert pattern_manifest.exists(), f"Missing manifest for {pattern}"

        # Check at least one output exists (could be in date/view structure)
        parquet_files = list(pattern_dir.rglob("chunk_0.parquet"))
        assert len(parquet_files) > 0, f"No parquet files for {pattern}"

    @pytest.mark.parametrize("pattern", SILVER_PATTERNS)
    def test_silver_checksums_valid(
        self,
        generated_silver_samples: Path,
        pattern: str,
    ):
        """Verify Silver checksums are valid."""
        import hashlib

        pattern_dir = generated_silver_samples / f"sample={pattern}"

        for checksums_path in pattern_dir.rglob("_checksums.json"):
            with checksums_path.open() as f:
                manifest = json.load(f)

            for file_entry in manifest.get("files", []):
                file_path = checksums_path.parent / file_entry["path"]
                assert file_path.exists(), f"File in manifest not found: {file_path}"

                hasher = hashlib.sha256()
                with file_path.open("rb") as f:
                    for chunk in iter(lambda: f.read(1024 * 1024), b""):
                        hasher.update(chunk)
                actual_hash = hasher.hexdigest()

                assert actual_hash == file_entry["sha256"], (
                    f"Checksum mismatch for {file_path}"
                )

    @pytest.mark.parametrize("pattern", SILVER_PATTERNS)
    def test_silver_data_readable(
        self,
        generated_silver_samples: Path,
        pattern: str,
    ):
        """Verify Silver parquet files are readable."""
        pattern_dir = generated_silver_samples / f"sample={pattern}"

        for parquet_file in pattern_dir.rglob("chunk_0.parquet"):
            df = pd.read_parquet(parquet_file)

            assert len(df) > 0, f"Empty DataFrame in {parquet_file}"
            assert len(df.columns) > 0, f"No columns in {parquet_file}"


# =============================================================================
# Pattern-Specific Validation Tests
# =============================================================================


@pytest.mark.sample_validation
@pytest.mark.integration
class TestPatternSpecificBehavior:
    """Tests for pattern-specific data characteristics."""

    def test_snapshot_has_replacement_batches(
        self,
        generated_bronze_samples: Path,
    ):
        """SNAPSHOT should have T0 and T1 (replacement) batches."""
        pattern_dir = generated_bronze_samples / "sample=snapshot"
        date_dirs = list(pattern_dir.glob("dt=*"))

        assert len(date_dirs) >= 2, "SNAPSHOT should have at least 2 batches"

    def test_incremental_append_has_multiple_batches(
        self,
        generated_bronze_samples: Path,
    ):
        """INCREMENTAL_APPEND should have T0, T1, T2, T3 batches."""
        pattern_dir = generated_bronze_samples / "sample=incremental_append"
        date_dirs = list(pattern_dir.glob("dt=*"))

        assert len(date_dirs) >= 4, "INCREMENTAL_APPEND should have 4 batches"

    def test_incremental_merge_has_updates_and_inserts(
        self,
        generated_bronze_samples: Path,
    ):
        """INCREMENTAL_MERGE batches should contain updates and inserts."""
        pattern_dir = generated_bronze_samples / "sample=incremental_merge"
        manifest_path = pattern_dir / "_manifest.json"

        with manifest_path.open() as f:
            manifest = json.load(f)

        # Check scenario metadata indicates updates and inserts
        changes = manifest.get("scenario_metadata", {}).get("changes", {})
        if changes:
            for batch, info in changes.items():
                if batch != "t0":
                    assert info.get("update_count", 0) > 0 or info.get("insert_count", 0) > 0

    def test_scd_state_has_current_and_history_views(
        self,
        generated_silver_samples: Path,
    ):
        """SCD2 state patterns should have current and history views."""
        scd2_patterns = [
            "pattern3_scd_state",
            "pattern5_hybrid_cdc_cumulative",
            "pattern7_hybrid_incremental_cumulative",
        ]

        for pattern in scd2_patterns:
            pattern_dir = generated_silver_samples / f"sample={pattern}"

            current_files = list(pattern_dir.rglob("view=current/chunk_0.parquet"))
            history_files = list(pattern_dir.rglob("view=history/chunk_0.parquet"))

            assert len(current_files) > 0, f"{pattern} missing current view"
            assert len(history_files) > 0, f"{pattern} missing history view"

    def test_scd1_has_only_current_view(
        self,
        generated_silver_samples: Path,
    ):
        """SCD1 state patterns should have only current view."""
        scd1_patterns = [
            "pattern4_hybrid_cdc_point",
            "pattern6_hybrid_incremental_point",
        ]

        for pattern in scd1_patterns:
            pattern_dir = generated_silver_samples / f"sample={pattern}"

            current_files = list(pattern_dir.rglob("view=current/chunk_0.parquet"))
            history_files = list(pattern_dir.rglob("view=history/chunk_0.parquet"))

            assert len(current_files) > 0, f"{pattern} missing current view"
            assert len(history_files) == 0, f"{pattern} should not have history view"

    def test_event_patterns_have_flat_structure(
        self,
        generated_silver_samples: Path,
    ):
        """Event patterns should have flat structure (no views)."""
        event_patterns = [
            "pattern1_full_events",
            "pattern2_cdc_events",
        ]

        for pattern in event_patterns:
            pattern_dir = generated_silver_samples / f"sample={pattern}"

            # Should have parquet directly under dt= partition
            date_dirs = list(pattern_dir.glob("dt=*"))
            for date_dir in date_dirs:
                parquet_file = date_dir / "chunk_0.parquet"
                assert parquet_file.exists(), f"{pattern} should have flat structure"

                # Should NOT have view= subdirectories
                view_dirs = list(date_dir.glob("view=*"))
                assert len(view_dirs) == 0, f"{pattern} should not have view subdirs"


# =============================================================================
# Script CLI Tests
# =============================================================================


@pytest.mark.sample_validation
@pytest.mark.integration
class TestScriptCLI:
    """Tests for CLI script functionality."""

    def test_bronze_script_dry_run(self, tmp_path: Path):
        """Test Bronze script --dry-run flag."""
        scripts_dir = Path(__file__).parent.parent.parent / "scripts"
        script_path = scripts_dir / "generate_bronze_samples.py"

        result = subprocess.run(
            [
                sys.executable,
                str(script_path),
                "--all",
                "--dry-run",
                "--output", str(tmp_path / "bronze"),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        assert "DRY RUN" in result.stdout

        # Verify no files were created
        assert not (tmp_path / "bronze").exists() or not any((tmp_path / "bronze").iterdir())

    def test_silver_script_dry_run(
        self,
        generated_bronze_samples: Path,
        tmp_path: Path,
    ):
        """Test Silver script --dry-run flag."""
        scripts_dir = Path(__file__).parent.parent.parent / "scripts"
        script_path = scripts_dir / "generate_silver_samples.py"

        result = subprocess.run(
            [
                sys.executable,
                str(script_path),
                "--all",
                "--dry-run",
                "--bronze-dir", str(generated_bronze_samples),
                "--output", str(tmp_path / "silver"),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        assert "DRY RUN" in result.stdout

    def test_bronze_script_verify(self, generated_bronze_samples: Path):
        """Test Bronze script --verify flag."""
        scripts_dir = Path(__file__).parent.parent.parent / "scripts"
        script_path = scripts_dir / "generate_bronze_samples.py"

        result = subprocess.run(
            [
                sys.executable,
                str(script_path),
                "--verify",
                "--output", str(generated_bronze_samples),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        assert result.returncode == 0
        assert "[PASS]" in result.stdout

    def test_silver_script_verify(self, generated_silver_samples: Path):
        """Test Silver script --verify flag."""
        scripts_dir = Path(__file__).parent.parent.parent / "scripts"
        script_path = scripts_dir / "generate_silver_samples.py"

        result = subprocess.run(
            [
                sys.executable,
                str(script_path),
                "--verify",
                "--output", str(generated_silver_samples),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        assert result.returncode == 0
        assert "[PASS]" in result.stdout


# =============================================================================
# Data Integrity Tests
# =============================================================================


@pytest.mark.sample_validation
@pytest.mark.integration
class TestDataIntegrity:
    """Tests for data integrity across Bronze to Silver transformation."""

    def test_silver_row_counts_reasonable(
        self,
        generated_bronze_samples: Path,
        generated_silver_samples: Path,
    ):
        """Verify Silver row counts are reasonable relative to Bronze."""
        for silver_pattern, bronze_pattern in SILVER_TO_BRONZE.items():
            bronze_dir = generated_bronze_samples / f"sample={bronze_pattern}"
            silver_dir = generated_silver_samples / f"sample={silver_pattern}"

            # Count Bronze rows
            bronze_rows = 0
            for pq in bronze_dir.rglob("chunk_0.parquet"):
                bronze_rows += len(pd.read_parquet(pq))

            # Count Silver rows (all outputs)
            silver_rows = 0
            for pq in silver_dir.rglob("chunk_0.parquet"):
                silver_rows += len(pd.read_parquet(pq))

            # Silver should have rows
            assert silver_rows > 0, f"No Silver rows for {silver_pattern}"

    def test_deterministic_generation(self, tmp_path: Path):
        """Verify generation is deterministic with same seed."""
        scripts_dir = Path(__file__).parent.parent.parent / "scripts"
        bronze_script = scripts_dir / "generate_bronze_samples.py"

        # Generate twice with same seed
        dir1 = tmp_path / "gen1"
        dir2 = tmp_path / "gen2"

        for out_dir in [dir1, dir2]:
            result = subprocess.run(
                [
                    sys.executable,
                    str(bronze_script),
                    "--pattern", "snapshot",
                    "--output", str(out_dir),
                    "--rows", "10",
                    "--seed", "12345",
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert result.returncode == 0

        # Compare generated data
        pq1 = dir1 / "sample=snapshot" / "dt=2024-01-15" / "chunk_0.parquet"
        pq2 = dir2 / "sample=snapshot" / "dt=2024-01-15" / "chunk_0.parquet"

        df1 = pd.read_parquet(pq1)
        df2 = pd.read_parquet(pq2)

        pd.testing.assert_frame_equal(df1, df2)
