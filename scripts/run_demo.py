#!/usr/bin/env python
"""
Demo Runner for medallion-foundry

This script provides a one-click demo experience that:
1. Generates sample data
2. Runs Bronze extraction
3. Runs Silver promotion
4. Shows the results

Usage:
    python scripts/run_demo.py

Or for interactive mode:
    python scripts/run_demo.py --interactive
"""

import argparse
import subprocess
import sys
from pathlib import Path

def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"\n{'='*60}")
    print(f"🔄 {description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("✅ SUCCESS")
        if result.stdout.strip():
            print("Output:", result.stdout.strip())
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ FAILED (exit code: {e.returncode})")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(description="Run medallion-foundry demo")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    parser.add_argument("--skip-samples", action="store_true", help="Skip sample data generation")
    args = parser.parse_args()

    print("🚀 medallion-foundry Demo Runner")
    print("="*60)

    # Check if we're in the right directory
    if not Path("scripts/generate_sample_data.py").exists():
        print("❌ Error: Please run this script from the medallion-foundry root directory")
        sys.exit(1)

    # Step 1: Generate sample data
    if not args.skip_samples:
        if not run_command(
            [sys.executable, "scripts/generate_sample_data.py"],
            "Generating sample data"
        ):
            print("❌ Failed to generate sample data")
            sys.exit(1)

    # Step 2: Run Bronze extraction
    if not run_command(
        [sys.executable, "bronze_extract.py",
         "--config", "docs/examples/configs/examples/file_example.yaml",
         "--date", "2025-11-13"],
        "Running Bronze extraction"
    ):
        print("❌ Bronze extraction failed")
        sys.exit(1)

    # Step 3: Run Silver promotion
    if not run_command(
        [sys.executable, "silver_extract.py",
         "--config", "docs/examples/configs/examples/file_example.yaml",
         "--date", "2025-11-13"],
        "Running Silver promotion"
    ):
        print("❌ Silver promotion failed")
        sys.exit(1)

    # Step 4: Show results
    print(f"\n{'='*60}")
    print("🎉 Demo completed successfully!")
    print(f"{'='*60}")

    print("\n📁 Generated files:")
    print("  Bronze data: output/system=retail_demo/table=orders/dt=2025-11-13/")
    print("  Silver data: silver_output/domain=retail_demo/entity=orders/v1/load_date=2025-11-13/")

    print("\n🔍 Key files to examine:")
    print("  Metadata: output/system=retail_demo/table=orders/dt=2025-11-13/_metadata.json")
    print("  Sample data: output/system=retail_demo/table=orders/dt=2025-11-13/part-0001.parquet")

    print("\n📝 Next steps:")
    print("1. Examine the generated files above")
    print("2. Copy docs/examples/configs/examples/file_example.yaml to config/my_config.yaml")
    print("3. Edit the config for your data source")
    print("4. Run: python bronze_extract.py --config config/my_config.yaml --date YYYY-MM-DD")

    if args.interactive:
        input("\nPress Enter to continue to customization guide...")

        print(f"\n{'='*60}")
        print("🎨 Customization Guide")
        print(f"{'='*60}")

        print("\n1. 📋 Choose your starting config:")
        print("   API sources: docs/examples/configs/examples/api_example.yaml")
        print("   Database: docs/examples/configs/examples/db_example.yaml")
        print("   Files: docs/examples/configs/examples/file_example.yaml")
        print("   Custom: docs/examples/configs/examples/custom_example.yaml")

        print("\n2. 📁 Copy to your config directory:")
        print("   mkdir config")
        print("   copy docs/examples/configs/examples/api_example.yaml config/my_api.yaml")

        print("\n3. ✏️  Edit the config:")
        print("   - Change system/entity names")
        print("   - Update connection details")
        print("   - Set environment variables for secrets")
        print("   - Adjust date ranges and patterns")

        print("\n4. 🧪 Test with dry-run:")
        print("   python bronze_extract.py --config config/my_api.yaml --dry-run")

        print("\n5. 🚀 Run extraction:")
        print("   python bronze_extract.py --config config/my_api.yaml --date 2025-11-13")

        print("\n📚 For detailed guidance, see:")
        print("   docs/usage/onboarding/intent-owner-guide.md")
        print("   docs/usage/patterns/QUICK_REFERENCE.md")

if __name__ == "__main__":
    main()