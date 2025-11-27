#!/usr/bin/env python
"""
Run Bronze extraction for all pattern configurations.

This script runs Bronze extraction (source to Bronze) for all pattern configurations
in the sample data, using the appropriate dates for each pattern type.

Usage:
    python scripts/run_all_bronze_patterns.py

Or skip sample generation if already done:
    python scripts/run_all_bronze_patterns.py --skip-sample-generation

Output:
    All Bronze outputs are saved to: sampledata/bronze_outputs/
"""

import argparse
import subprocess
import sys
import yaml
import tempfile
import shutil
from pathlib import Path

# All pattern configurations with their appropriate run dates
PATTERN_CONFIGS = [
    ("docs/examples/configs/patterns/pattern_full.yaml", ["2025-11-13"]),
    ("docs/examples/configs/patterns/pattern_cdc.yaml", ["2025-11-13"]),
    ("docs/examples/configs/patterns/pattern_current_history.yaml", ["2025-11-13"]),
    ("docs/examples/configs/patterns/pattern_hybrid_cdc_point.yaml", ["2025-11-24"]),
    ("docs/examples/configs/patterns/pattern_hybrid_cdc_cumulative.yaml", ["2025-11-24"]),
    ("docs/examples/configs/patterns/pattern_hybrid_incremental_point.yaml", ["2025-11-24"]),
    ("docs/examples/configs/patterns/pattern_hybrid_incremental_cumulative.yaml", ["2025-11-24"]),
    ("docs/examples/configs/examples/file_example.yaml", ["2025-11-13"]),
    ("docs/examples/configs/patterns/file_cdc_example.yaml", ["2025-11-13"]),
    ("docs/examples/configs/patterns/file_current_history_example.yaml", ["2025-11-13"]),
]

def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"\n{'='*60}")
    print(f"🔄 {description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("✅ SUCCESS")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ FAILED (exit code: {e.returncode})")
        if e.stdout:
            print("STDOUT:", e.stdout.strip())
        if e.stderr:
            print("STDERR:", e.stderr.strip())
        return False


def get_available_dates():
    """Get all available dates from the sample data."""
    pattern1_path = Path("sampledata/source_samples/pattern1_full_events/system=retail_demo/table=orders/pattern=pattern1_full_events")
    if not pattern1_path.exists():
        return ["2025-11-13"]  # fallback

    dates = []
    for item in pattern1_path.iterdir():
        if item.is_dir() and item.name.startswith("dt="):
            date_str = item.name.replace("dt=", "")
            dates.append(date_str)
    return sorted(dates)


def fix_file_example_config(original_path, run_date, temp_dir, output_base_dir):
    """Fix configs to use the correct date and output directory."""
    config = yaml.safe_load(Path(original_path).read_text())

    # For pattern configs that use date parameters, update the path_pattern
    if "bronze" in config and "path_pattern" in config["bronze"]:
        # Replace any dt=YYYY-MM-DD with the current run_date
        import re
        config["bronze"]["path_pattern"] = re.sub(
            r'dt=\d{4}-\d{2}-\d{2}',
            f'dt={run_date}',
            config["bronze"]["path_pattern"]
        )

    # Set output directory to sampledata/bronze_outputs
    if "bronze" in config:
        if "options" not in config["bronze"]:
            config["bronze"]["options"] = {}
        config["bronze"]["options"]["local_output_dir"] = str(output_base_dir)

    # Also set silver output if it exists
    if "silver" in config:
        config["silver"]["output_dir"] = str(output_base_dir / "silver")

    config_name = Path(original_path).name
    # Write to temp file
    temp_config = temp_dir / f"temp_{config_name}_{run_date.replace('-', '')}"
    temp_config.write_text(yaml.safe_dump(config))
    return str(temp_config)

def main():
    parser = argparse.ArgumentParser(description="Run Bronze extraction for all pattern configs")
    parser.add_argument(
        "--skip-sample-generation",
        action="store_true",
        help="Skip sample data generation (assumes it already exists)"
    )

    args = parser.parse_args()

    print("🚀 Bronze Pattern Extraction Runner")
    print("="*60)
    print("Running all pattern configurations with their appropriate dates...")

    # Check if we're in the right directory
    if not Path("scripts/generate_sample_data.py").exists():
        print("❌ Error: Please run this script from the medallion-foundry root directory")
        sys.exit(1)

    # Generate sample data if needed
    if not args.skip_sample_generation:
        if not run_command(
            [sys.executable, "scripts/generate_sample_data.py"],
            "Generating sample data"
        ):
            print("❌ Failed to generate sample data")
            sys.exit(1)

    print(f"Configs to run: {len(PATTERN_CONFIGS)}")

    # Set up output directory
    output_base = Path("sampledata/bronze_outputs")
    output_base.mkdir(parents=True, exist_ok=True)

    # Run Bronze extraction for each pattern config and its dates
    total_runs = sum(len(dates) for _, dates in PATTERN_CONFIGS)
    run_count = 0
    results = []

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        for config_path, run_dates in PATTERN_CONFIGS:
            config_name = Path(config_path).name

            for run_date in run_dates:
                run_count += 1
                description = f"[{run_count}/{total_runs}] Bronze extraction: {config_name} ({run_date})"

                # Fix configs to use correct dates and output directory
                actual_config_path = fix_file_example_config(config_path, run_date, temp_path, output_base)

                success = run_command(
                    [sys.executable, "bronze_extract.py",
                     "--config", actual_config_path,
                     "--date", run_date],
                    description
                )
                results.append((config_name, run_date, success))

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")

    successful = sum(1 for _, _, success in results if success)
    total = len(results)

    print(f"\nCompleted: {successful}/{total} runs")

    if successful == total:
        print("\n✅ ALL BRONZE EXTRACTIONS SUCCEEDED!")
        print(f"\n📁 Outputs saved to: {output_base}/")
        print("   Bronze data: sampledata/bronze_outputs/")
    else:
        print("\n❌ SOME BRONZE EXTRACTIONS FAILED")
        print("\nFailed runs:")
        for config_name, run_date, success in results:
            if not success:
                print(f"   ❌ {config_name} ({run_date})")

    return 0 if successful == total else 1

if __name__ == "__main__":
    sys.exit(main())