"""Ensure the curated Silver sample tree is regenerated as part of the suite."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(".").resolve()


def test_generate_silver_samples() -> None:
    """Rebuild the bronze→silver samples so every suite run exercises production paths."""
    script = REPO_ROOT / "scripts" / "generate_silver_samples.py"
    subprocess.run(
        [sys.executable, str(script), "--formats", "both", "--limit", "5"],
        check=True,
        cwd=REPO_ROOT,
    )
    silver_root = REPO_ROOT / "sampledata" / "silver_samples"
    assert silver_root.exists()
    assert any(
        silver_root.iterdir()
    ), "Silver sample tree should contain generated partitions"
