"""Helper to run Bronze + Silver for an intent config end-to-end."""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Bronze extract followed by Silver promotion for an intent config."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the intent YAML (e.g., docs/examples/configs/file_complex.yaml)",
    )
    parser.add_argument(
        "--date",
        help="Logical run date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Only run Silver (assumes Bronze outputs already exist).",
    )
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Only run Bronze (useful when debugging upstream).",
    )

    args = parser.parse_args()
    run_date = (
        dt.date.fromisoformat(args.date) if args.date else dt.date.today()
    ).isoformat()
    bronze_cmd = [
        sys.executable,
        "bronze_extract.py",
        "--config",
        args.config,
        "--date",
        run_date,
    ]
    silver_cmd = [
        sys.executable,
        "silver_extract.py",
        "--config",
        args.config,
        "--date",
        run_date,
    ]

    if not args.skip_bronze:
        print(f"Running Bronze: {' '.join(bronze_cmd)}")
        subprocess.run(bronze_cmd, check=True)

    if not args.skip_silver:
        print(f"Running Silver: {' '.join(silver_cmd)}")
        subprocess.run(silver_cmd, check=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
