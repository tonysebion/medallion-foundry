import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_black_formatting_check():
    """Ensure the repository is formatted with black using line-length 120.

    This mirrors the developer tooling and the `run_tests.py --black-check` behavior
    so that style violations are surfaced as a pytest failure in CI and locally.
    """
    targets = [
        "core",
        "extractors",
        "tests",
        "bronze_extract.py",
        "silver_extract.py",
    ]
    # Only include targets that exist in the repo; avoids scanning sampledata/site dir
    targets = [str(ROOT / t) for t in targets if (ROOT / t).exists()]

    cmd = [sys.executable, "-m", "black", "--check", "--line-length=120", *targets]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        raise AssertionError(
            "black check timed out; try running black directly to diagnose"
        )

    if result.returncode != 0:
        # Include stdout/stderr in assertion to help fix quickly
        raise AssertionError(
            "black check failed; run 'black --line-length=120 .' to fix formatting\n"
            + result.stdout
            + result.stderr
        )
