#!/usr/bin/env python
"""
Test runner script for medallion-foundry.

This script provides a simple way to run tests locally or in CI/CD environments
without requiring GitHub Actions. Supports multiple testing modes and quality checks.

Usage:
    python run_tests.py                    # Run all tests
    python run_tests.py --unit             # Run only unit tests
    python run_tests.py --coverage         # Run with coverage report
    python run_tests.py --all-checks       # Run all quality checks
    python run_tests.py --help             # Show all options
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent


def _filter_existing_targets(targets: list[str]) -> list[str]:
    found: list[str] = []
    for target in targets:
        candidate = ROOT_DIR / target if not Path(target).is_absolute() else Path(target)
        if candidate.exists():
            found.append(target)
        else:
            print(f"[WARN] Skipping missing target {target}")
    return found


def _ensure_venv_python():
    """Re-run the script under `.venv` python so pytest inherits the project virtualenv."""
    if os.name == "nt":
        candidate = ROOT_DIR / ".venv" / "Scripts" / "python.exe"
    else:
        candidate = ROOT_DIR / ".venv" / "bin" / "python"

    if candidate.exists():
        candidate = candidate.resolve()
        current = Path(sys.executable).resolve()
        if current != candidate:
            print(f"Re-launching tests under virtual environment: {candidate}")
            os.execv(str(candidate), [str(candidate)] + sys.argv)


def _ensure_pythonioencoding():
    """Default PYTHONIOENCODING to UTF-8 if it is not already set."""
    if os.environ.get("PYTHONIOENCODING") != "utf-8":
        os.environ["PYTHONIOENCODING"] = "utf-8"


def _prepare_environment():
    """Make sure the process uses the expected encoding and virtualenv before doing anything else."""
    _ensure_pythonioencoding()
    _ensure_venv_python()


def run_command(cmd: list, description: str) -> bool:
    """Run a command and return True if successful."""
    print(f"\n{'='*80}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*80}\n")

    result = subprocess.run(cmd)
    success = result.returncode == 0

    if success:
        print(f"\n✅ {description} - PASSED")
    else:
        print(f"\n❌ {description} - FAILED")

    return success


def main():
    _prepare_environment()
    parser = argparse.ArgumentParser(
        description="Run medallion-foundry tests and quality checks"
    )
    parser.add_argument("--unit", action="store_true", help="Run only unit tests")
    parser.add_argument(
        "--integration", action="store_true", help="Run only integration tests"
    )
    parser.add_argument(
        "--coverage", action="store_true", help="Run tests with coverage report"
    )
    parser.add_argument(
        "--html-coverage", action="store_true", help="Generate HTML coverage report"
    )
    parser.add_argument("--mypy", action="store_true", help="Run mypy type checking")
    parser.add_argument("--ruff", action="store_true", help="Run ruff linting")
    parser.add_argument(
        "--with-lint",
        "--lint",
        action="store_true",
        help="Run tests together with ruff linting for a single command",
    )
    parser.add_argument(
        "--black-check", action="store_true", help="Check code formatting with black"
    )
    parser.add_argument(
        "--all-checks",
        action="store_true",
        help="Run all quality checks (tests, mypy, ruff, black)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    results = []

    # Determine what to run
    run_tests = True
    run_mypy = args.mypy or args.all_checks
    run_ruff = args.ruff or args.with_lint or args.all_checks
    run_black = args.black_check or args.all_checks

    # Run tests
    if run_tests:
        pytest_cmd = [sys.executable, "-m", "pytest"]

        # Add verbosity
        if args.verbose:
            pytest_cmd.append("-vv")

        # Add test selection
        if args.unit:
            pytest_cmd.extend(["-m", "unit"])
        elif args.integration:
            pytest_cmd.extend(["-m", "integration"])

        # Add coverage
        if args.coverage or args.html_coverage or args.all_checks:
            pytest_cmd.extend(
                [
                    "--cov=core",
                    "--cov=extractors",
                    "--cov-report=term-missing",
                ]
            )
            if args.html_coverage or args.all_checks:
                pytest_cmd.append("--cov-report=html")

        results.append(run_command(pytest_cmd, "Unit Tests"))

    # Run mypy type checking
    if run_mypy:
        mypy_targets = _filter_existing_targets(
            ["core", "extractors", "bronze_extract.py"]
        )
        if not mypy_targets:
            print("No mypy targets found; skipping type checking")
            results.append(True)
        else:
            mypy_cmd = ["mypy", *mypy_targets, "--config-file=mypy.ini"]
            results.append(run_command(mypy_cmd, "Type Checking (mypy)"))

    # Run ruff linting
    if run_ruff:
        ruff_cmd = [
            "ruff",
            "check",
            ".",
        ]
        results.append(run_command(ruff_cmd, "Linting (ruff)"))

    # Run black format checking
    if run_black:
        black_targets = [
            "core",
            "tests",
            "bronze_extract.py",
            "silver_extract.py",
        ]
        extractors_path = ROOT_DIR / "extractors"
        if extractors_path.exists():
            black_targets.insert(1, "extractors")
        black_cmd = [
            "black",
            "--check",
            "--line-length=120",
            *black_targets,
        ]
        results.append(run_command(black_cmd, "Code Formatting (black)"))

    # Print summary
    print(f"\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")

    passed = sum(results)
    total = len(results)

    print(f"\nPassed: {passed}/{total}")

    if all(results):
        print("\n✅ ALL CHECKS PASSED!")
        return 0
    else:
        print("\n❌ SOME CHECKS FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
