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

import sys
import subprocess
import argparse
from pathlib import Path


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
    parser = argparse.ArgumentParser(description="Run medallion-foundry tests and quality checks")
    parser.add_argument("--unit", action="store_true", help="Run only unit tests")
    parser.add_argument("--integration", action="store_true", help="Run only integration tests")
    parser.add_argument("--coverage", action="store_true", help="Run tests with coverage report")
    parser.add_argument("--html-coverage", action="store_true", help="Generate HTML coverage report")
    parser.add_argument("--mypy", action="store_true", help="Run mypy type checking")
    parser.add_argument("--flake8", action="store_true", help="Run flake8 linting")
    parser.add_argument("--black-check", action="store_true", help="Check code formatting with black")
    parser.add_argument("--all-checks", action="store_true", help="Run all quality checks (tests, mypy, flake8, black)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    results = []
    project_root = Path(__file__).parent
    
    # Determine what to run
    run_tests = True
    run_mypy = args.mypy or args.all_checks
    run_flake8 = args.flake8 or args.all_checks
    run_black = args.black_check or args.all_checks
    
    # Run tests
    if run_tests:
        pytest_cmd = ["pytest"]
        
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
            pytest_cmd.extend([
                "--cov=core",
                "--cov=extractors",
                "--cov-report=term-missing",
            ])
            if args.html_coverage or args.all_checks:
                pytest_cmd.append("--cov-report=html")
        
        results.append(run_command(pytest_cmd, "Unit Tests"))
    
    # Run mypy type checking
    if run_mypy:
        mypy_cmd = [
            "mypy",
            "core",
            "extractors",
            "bronze_extract.py",
            "--config-file=mypy.ini"
        ]
        results.append(run_command(mypy_cmd, "Type Checking (mypy)"))
    
    # Run flake8 linting
    if run_flake8:
        flake8_cmd = [
            "flake8",
            "core",
            "extractors",
            "tests",
            "bronze_extract.py",
            "--max-line-length=120",
            "--exclude=.venv,__pycache__,.git",
            "--ignore=E203,W503"  # Black compatibility
        ]
        results.append(run_command(flake8_cmd, "Linting (flake8)"))
    
    # Run black format checking
    if run_black:
        black_cmd = [
            "black",
            "--check",
            "--line-length=120",
            "core",
            "extractors",
            "tests",
            "bronze_extract.py"
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
