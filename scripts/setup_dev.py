#!/usr/bin/env python3
"""Development environment setup script for bronze-foundry.

This script sets up a complete development environment on any platform.
Run it after cloning the repository:

    python scripts/setup_dev.py

Or with options:
    python scripts/setup_dev.py --skip-samples  # Skip sample data generation
    python scripts/setup_dev.py --extras azure  # Include azure dependencies
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], description: str, check: bool = True) -> bool:
    """Run a command and print status."""
    print(f"\n{'='*60}")
    print(f"  {description}")
    print(f"{'='*60}")
    print(f"  Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, check=check, capture_output=False)
        if result.returncode == 0:
            print(f"  [OK] {description}")
            return True
        else:
            print(f"  [WARN] {description} (exit code {result.returncode})")
            return False
    except subprocess.CalledProcessError as e:
        print(f"  [FAIL] {description}")
        print(f"  Error: {e}")
        return False
    except FileNotFoundError:
        print(f"  [FAIL] Command not found: {cmd[0]}")
        return False


def get_python_cmd() -> str:
    """Get the appropriate python command for this platform."""
    return sys.executable


def get_venv_activate_cmd(venv_path: Path) -> str:
    """Get the activate command for this platform."""
    if platform.system() == "Windows":
        return str(venv_path / "Scripts" / "activate")
    else:
        return f"source {venv_path / 'bin' / 'activate'}"


def main():
    parser = argparse.ArgumentParser(description="Set up bronze-foundry development environment")
    parser.add_argument("--skip-samples", action="store_true", help="Skip sample data generation")
    parser.add_argument("--skip-venv", action="store_true", help="Skip virtual environment creation")
    parser.add_argument("--extras", nargs="*", choices=["azure", "db", "tui"], default=[],
                       help="Additional extras to install")
    args = parser.parse_args()

    # Find project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    os.chdir(project_root)

    print("\n" + "="*60)
    print("  bronze-foundry Development Setup")
    print("="*60)
    print(f"  Project root: {project_root}")
    print(f"  Python: {sys.version}")
    print(f"  Platform: {platform.system()} {platform.machine()}")

    venv_path = project_root / ".venv"
    python_cmd = get_python_cmd()

    # Step 1: Create virtual environment
    if not args.skip_venv:
        if venv_path.exists():
            print(f"\n  Virtual environment already exists at {venv_path}")
            response = input("  Recreate it? [y/N]: ").strip().lower()
            if response == "y":
                shutil.rmtree(venv_path)
                run_command([python_cmd, "-m", "venv", str(venv_path)],
                           "Creating virtual environment")
        else:
            run_command([python_cmd, "-m", "venv", str(venv_path)],
                       "Creating virtual environment")

    # Determine pip command in venv
    if platform.system() == "Windows":
        pip_cmd = str(venv_path / "Scripts" / "pip")
        venv_python = str(venv_path / "Scripts" / "python")
    else:
        pip_cmd = str(venv_path / "bin" / "pip")
        venv_python = str(venv_path / "bin" / "python")

    # Step 2: Upgrade pip
    run_command([venv_python, "-m", "pip", "install", "--upgrade", "pip"],
               "Upgrading pip")

    # Step 3: Install core dependencies
    run_command([pip_cmd, "install", "-r", "requirements.txt"],
               "Installing core dependencies")

    # Step 4: Install dev dependencies
    run_command([pip_cmd, "install", "-r", "requirements-dev.txt"],
               "Installing development dependencies")

    # Step 5: Install package in editable mode
    extras_str = ",".join(["dev"] + args.extras) if args.extras else "dev"
    run_command([pip_cmd, "install", "-e", f".[{extras_str}]"],
               f"Installing bronze-foundry in editable mode with extras: {extras_str}")

    # Step 6: Generate sample data
    if not args.skip_samples:
        run_command([venv_python, "scripts/generate_bronze_samples.py", "--all"],
                   "Generating Bronze sample data", check=False)
        run_command([venv_python, "scripts/generate_silver_samples.py", "--all"],
                   "Generating Silver sample data", check=False)

    # Step 7: Run a quick test to verify setup
    print("\n" + "="*60)
    print("  Verifying setup...")
    print("="*60)

    verify_result = subprocess.run(
        [venv_python, "-c", "from pipelines.lib.bronze import BronzeSource; print('  [OK] Imports work')"],
        capture_output=True,
        text=True
    )
    if verify_result.returncode != 0:
        print("  [WARN] Import verification failed")
        print(f"  {verify_result.stderr}")
    else:
        print(verify_result.stdout.strip())

    # Print success message
    print("\n" + "="*60)
    print("  Setup Complete!")
    print("="*60)
    print(f"""
  To activate the virtual environment:

    {get_venv_activate_cmd(venv_path)}

  Then you can run:

    python -m pipelines --list              # List available pipelines
    python -m pytest tests/unit/ -v         # Run unit tests
    python -m ruff check .                  # Lint code
    python -m ruff format .                 # Format code

  See README.md for more information.
""")


if __name__ == "__main__":
    main()
