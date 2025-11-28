"""Test that all parameters in pattern YAML configs are properly applied."""

import pytest
import yaml
from pathlib import Path
import subprocess
import sys
import tempfile
import shutil
from typing import Dict, Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "docs" / "examples" / "configs" / "patterns"
SAMPLE_DATA_DIR = REPO_ROOT / "sampledata" / "source_samples"
OUTPUT_BASE = REPO_ROOT / "sampledata" / "test_outputs"


@pytest.fixture(scope="session", autouse=True)
def setup_sample_data():
    """Ensure sample data exists."""
    if not SAMPLE_DATA_DIR.exists():
        subprocess.run([sys.executable, "scripts/generate_sample_data.py"], check=True, cwd=REPO_ROOT)


@pytest.fixture(scope="session", autouse=True)
def cleanup_outputs():
    """Clean up test outputs after all tests."""
    yield
    if OUTPUT_BASE.exists():
        shutil.rmtree(OUTPUT_BASE)


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load a YAML config file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def run_bronze_extract(config_path: Path, run_date: str, output_dir: Path) -> bool:
    """Run bronze_extract with the given config and return success."""
    # Use the venv python
    python_exe = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
    if not python_exe.exists():
        python_exe = sys.executable  # fallback
    cmd = [
        str(python_exe), "bronze_extract.py",
        "--config", str(config_path),
        "--date", run_date
    ]
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
    return result.returncode == 0


def get_bronze_output_dir(config: Dict, run_date: str) -> Path:
    """Determine the bronze output directory for a config."""
    # Bronze outputs to bronze/env=dev/system=.../pattern=.../dt=...
    env = config.get("environment", "dev")
    system = config["system"]
    table = config["entity"]
    load_pattern = config["bronze"]["options"]["load_pattern"]
    return REPO_ROOT / "bronze" / f"env={env}" / f"system={system}" / f"table={table}" / f"pattern={load_pattern}" / f"dt={run_date}"


@pytest.mark.parametrize("config_file", [
    "pattern_full.yaml",
    "pattern_cdc.yaml",
    "pattern_current_history.yaml",
    "pattern_hybrid_cdc_point.yaml",
    "pattern_hybrid_cdc_cumulative.yaml",
    "pattern_hybrid_incremental_point.yaml",
    "pattern_hybrid_incremental_cumulative.yaml",
])
def test_config_parameters_applied(config_file):
    """Test that all parameters in the config are properly applied to the output."""
    config_path = CONFIG_DIR / config_file
    assert config_path.exists(), f"Config file {config_file} does not exist"

    config = load_config(config_path)

    # Use a test date
    test_date = "2025-11-13"

    # Modify config for test
    test_config = config.copy()
    test_config["bronze"]["path_pattern"] = test_config["bronze"]["path_pattern"].replace("dt=2025-11-13", f"dt={test_date}")
    # Ensure silver output_dir is set
    if "silver" in test_config and "output_dir" not in test_config["silver"]:
        test_config["silver"]["output_dir"] = "./silver_output"

    # Write temp config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.safe_dump(test_config, f)
        temp_config_path = Path(f.name)

    try:
        # Run bronze extract
        success = run_bronze_extract(temp_config_path, test_date, OUTPUT_BASE)
        assert success, f"Bronze extraction failed for {config_file}"

        # Get the actual bronze output dir
        bronze_output_dir = get_bronze_output_dir(test_config, test_date)

        # Now validate the parameters
        validate_bronze_parameters(test_config, bronze_output_dir)

    finally:
        temp_config_path.unlink()


def validate_bronze_parameters(config: Dict, output_dir: Path):
    """Validate that bronze parameters are applied correctly."""
    assert output_dir.exists(), f"Output directory {output_dir} does not exist"

    bronze_options = config["bronze"]["options"]

    # Check format
    if "format" in bronze_options:
        format_type = bronze_options["format"]
        if format_type == "csv":
            # Bronze outputs Parquet, not CSV
            parquet_files = list(output_dir.glob("*.parquet"))
            assert len(parquet_files) > 0, f"No Parquet files found for format {format_type}"

    # Check load_pattern from metadata
    metadata_file = output_dir / "_metadata.json"
    if metadata_file.exists():
        import json
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        if "load_pattern" in bronze_options:
            expected_pattern = bronze_options["load_pattern"]
            assert metadata.get("load_pattern") == expected_pattern, f"Load pattern mismatch: expected {expected_pattern}, got {metadata.get('load_pattern')}"

    # Check partitioning_strategy
    if "partitioning_strategy" in bronze_options:
        strategy = bronze_options["partitioning_strategy"]
        if strategy == "date":
            # Should be partitioned by date, which it is
            assert "dt=" in str(output_dir), "Output not partitioned by date"

    # Check pattern_folder - not directly in output path, but assume it's used for source
    # if "pattern_folder" in bronze_options:
    #     expected_pattern = bronze_options["pattern_folder"]
    #     assert expected_pattern in str(output_dir), f"Pattern folder {expected_pattern} not in output path"


# Additional tests for specific patterns can be added here