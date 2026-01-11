#!/usr/bin/env python
"""Validate all pipeline YAML configuration files.

This script validates pipeline YAML files against the JSON schema
and optionally runs dry-run validation to ensure configs load correctly.

Examples:
    # Schema validation only (fast)
    python scripts/validate_yaml_configs.py

    # Full validation including dry-run
    python scripts/validate_yaml_configs.py --dry-run

    # Verbose output
    python scripts/validate_yaml_configs.py -v

    # Validate specific directories
    python scripts/validate_yaml_configs.py --dirs pipelines/examples

Exit codes:
    0 - All validations passed
    1 - One or more validations failed
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def validate_schema(
    yaml_file: Path, schema: dict, verbose: bool = False
) -> Tuple[bool, str]:
    """Validate a YAML file against the JSON schema.

    Args:
        yaml_file: Path to the YAML file
        schema: Loaded JSON schema dict
        verbose: Print additional info

    Returns:
        Tuple of (success, error_message)
    """
    try:
        import yaml
        import jsonschema
    except ImportError as e:
        return (
            False,
            f"Missing dependency: {e}. Install with: pip install pyyaml jsonschema",
        )

    try:
        config = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
        jsonschema.validate(config, schema)
        return True, ""
    except yaml.YAMLError as e:
        return False, f"YAML parse error: {e}"
    except jsonschema.ValidationError as e:
        return False, f"Schema validation: {e.message}"
    except Exception as e:
        return False, f"Error: {e}"


def validate_dry_run(
    yaml_file: Path, python_exe: Path, verbose: bool = False
) -> Tuple[bool, str]:
    """Run dry-run validation on a YAML file.

    Args:
        yaml_file: Path to the YAML file
        python_exe: Path to Python executable
        verbose: Print additional info

    Returns:
        Tuple of (success, error_message)
    """
    try:
        # Use relative path from project root (CLI has issues with absolute paths on Windows)
        try:
            rel_path = yaml_file.relative_to(project_root)
        except ValueError:
            rel_path = yaml_file

        result = subprocess.run(
            [
                str(python_exe),
                "-m",
                "pipelines",
                str(rel_path),
                "--date",
                "2025-01-15",
                "--dry-run",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(project_root),
        )
        if result.returncode == 0:
            return True, ""
        else:
            # Extract last meaningful error line
            lines = (result.stderr or result.stdout).strip().split("\n")
            error_lines = [
                line for line in lines if line.strip() and not line.startswith("2")
            ][-3:]
            return False, "\n".join(error_lines)
    except subprocess.TimeoutExpired:
        return False, "Timeout (60s)"
    except Exception as e:
        return False, f"Error: {e}"


def find_yaml_files(directories: List[Path]) -> List[Path]:
    """Find all YAML files in the given directories.

    Args:
        directories: List of directory paths

    Returns:
        List of YAML file paths
    """
    yaml_files = []
    for directory in directories:
        if directory.exists():
            yaml_files.extend(directory.glob("*.yaml"))
    return sorted(yaml_files)


def main():
    parser = argparse.ArgumentParser(
        description="Validate pipeline YAML configuration files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dirs",
        nargs="+",
        type=Path,
        default=[
            project_root / "pipelines" / "examples",
        ],
        help="Directories to search for YAML files (default: pipelines/examples)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Also run dry-run validation (slower but more thorough)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=project_root / "pipelines" / "schema" / "pipeline.schema.json",
        help="Path to JSON schema file",
    )

    args = parser.parse_args()

    # Check dependencies
    try:
        import yaml  # noqa: F401
        import jsonschema  # noqa: F401
    except ImportError:
        print(
            "ERROR: Missing dependencies. Install with: pip install pyyaml jsonschema"
        )
        sys.exit(1)

    # Load schema
    if not args.schema.exists():
        print(f"ERROR: Schema not found: {args.schema}")
        sys.exit(1)

    schema = json.loads(args.schema.read_text(encoding="utf-8"))

    # Find YAML files
    yaml_files = find_yaml_files(args.dirs)
    if not yaml_files:
        print("No YAML files found in specified directories")
        sys.exit(1)

    print(f"Validating {len(yaml_files)} YAML files...")
    print()

    # Find Python executable
    python_exe = Path(sys.executable)

    # Validate each file
    schema_passed = 0
    schema_failed = 0
    dry_run_passed = 0
    dry_run_failed = 0
    schema_errors: List[Tuple[Path, str]] = []
    dry_run_errors: List[Tuple[Path, str]] = []

    for yaml_file in yaml_files:
        # Schema validation
        success, error = validate_schema(yaml_file, schema, args.verbose)
        if success:
            schema_passed += 1
            status = "PASS"
        else:
            schema_failed += 1
            schema_errors.append((yaml_file, error))
            status = "FAIL"

        if args.verbose or not success:
            print(f"[Schema] {status}: {yaml_file.name}")
            if not success:
                for line in error.split("\n"):
                    print(f"  {line}")

        # Dry-run validation (if requested and schema passed)
        if args.dry_run and success:
            success, error = validate_dry_run(yaml_file, python_exe, args.verbose)
            if success:
                dry_run_passed += 1
                status = "PASS"
            else:
                dry_run_failed += 1
                dry_run_errors.append((yaml_file, error))
                status = "FAIL"

            if args.verbose or not success:
                print(f"[DryRun] {status}: {yaml_file.name}")
                if not success:
                    for line in error.split("\n"):
                        print(f"  {line}")

        # Print pass status in non-verbose mode
        if not args.verbose and success and not args.dry_run:
            print(f"PASS: {yaml_file.name}")
        elif not args.verbose and success and args.dry_run:
            print(f"PASS: {yaml_file.name} (schema + dry-run)")

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Schema validation: {schema_passed} passed, {schema_failed} failed")
    if args.dry_run:
        print(f"Dry-run validation: {dry_run_passed} passed, {dry_run_failed} failed")

    # Exit with error if any failures
    if schema_failed > 0 or dry_run_failed > 0:
        print()
        print("FAILED - See errors above")
        sys.exit(1)
    else:
        print()
        print("All validations passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
