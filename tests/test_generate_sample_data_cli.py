import json
import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "generate_sample_data.py"
BASE_DIR = REPO_ROOT / "sampledata" / "source_samples"


def test_cli_help_runs():
    # Ensure --help prints and exit 0
    out = subprocess.run(["python", str(SCRIPT), "--help"], capture_output=True)
    assert out.returncode == 0
    assert b"Generate Bronze source sample datasets" in out.stdout


def test_generate_small_dataset(tmp_path):
    # Run with small sizes to verify generation succeeds and metadata recorded
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    # Generate a very small dataset; it shouldn't take long
    run = subprocess.run(
        [
            "python",
            str(SCRIPT),
            "--days",
            "1",
            "--full-row-count",
            "50",
            "--cdc-row-count",
            "20",
            "--output-base-dir",
            str(tmp_path),
            "--skip-mirror",
        ],
        env=env,
        capture_output=True,
    )
    assert run.returncode == 0
    # Validate a couple of metadata files exist
    # pattern1, first day
    sample_md = (
        Path(tmp_path)
        / "sample=pattern1_full_events"
        / "system=retail_demo"
        / "table=orders"
        / "dt=2025-11-13"
        / "_metadata.json"
    )
    assert sample_md.exists()
    data = json.loads(sample_md.read_text(encoding="utf-8"))
    assert data["record_count"] >= 50
