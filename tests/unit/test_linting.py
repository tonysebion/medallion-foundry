"""Ensure the codebase remains Ruff-clean."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_ruff_check() -> None:
    """Raise if Ruff finds issues in the primary modules."""
    command = [
        sys.executable,
        "-m",
        "ruff",
        "check",
        "core",
        "pipelines",
        "tests/unit",
        "tests/pattern_verification",
    ]

    process = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    if process.returncode != 0:
        pytest.fail(
            "Ruff reported issues.\n\n"
            f"Command: {' '.join(command)}\n\n"
            f"{process.stdout.strip()}"
        )
