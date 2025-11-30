import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_flake8_lint_check():
    """Run flake8 to ensure linting rules are satisfied across the repo.

    Restrict targets to avoid scanning large auto-generated sample data or site artifacts.
    """
    targets = [
        "core",
        "extractors",
        "tests",
        "bronze_extract.py",
        "silver_extract.py",
    ]
    targets = [str(ROOT / t) for t in targets if (ROOT / t).exists()]

    cmd = [
        sys.executable,
        "-m",
        "flake8",
        "--max-line-length=120",
        *targets,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        raise AssertionError("flake8 linting timed out; run flake8 directly to inspect")

    if result.returncode != 0:
        raise AssertionError(
            "flake8 linting failed; fix issues reported by flake8\n"
            + result.stdout
            + result.stderr
        )
