#!/usr/bin/env python
"""CLI script to generate pattern test data.

This script generates multi-batch test data for pattern verification testing.
Data can be used for manual testing, debugging, or creating assertion files.

Examples:
    # Generate SNAPSHOT scenario (1000 rows)
    python scripts/generate_pattern_test_data.py --pattern snapshot --rows 1000

    # Generate INCREMENTAL_MERGE scenario with 4 batches
    python scripts/generate_pattern_test_data.py --pattern incremental_merge --rows 1000 --batches 4

    # Generate all patterns to a specific directory
    python scripts/generate_pattern_test_data.py --all --output ./test_data/

    # Generate with custom seed and change rates
    python scripts/generate_pattern_test_data.py --pattern incremental_merge \\
        --rows 500 --seed 123 --update-rate 0.3 --insert-rate 0.15

    # Generate SCD2 data with specific entity count
    python scripts/generate_pattern_test_data.py --pattern current_history \\
        --entities 100 --changes-per-entity 5
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tests.integration.pattern_data import (
    PatternTestDataGenerator,
    PatternScenario,
    generate_all_pattern_scenarios,
    PatternAssertions,
    create_snapshot_assertions,
    create_incremental_append_assertions,
    create_incremental_merge_assertions,
    create_scd2_assertions,
)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate pattern test data for Bronze verification",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--pattern",
        choices=["snapshot", "incremental_append", "incremental_merge", "current_history"],
        help="Load pattern to generate data for",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate data for all patterns",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("./pattern_test_data"),
        help="Output directory (default: ./pattern_test_data)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of rows for T0 batch (default: 1000)",
    )
    parser.add_argument(
        "--batches",
        type=int,
        default=4,
        help="Number of batches for incremental patterns (default: 4)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--update-rate",
        type=float,
        default=0.2,
        help="Update rate for incremental_merge (default: 0.2)",
    )
    parser.add_argument(
        "--insert-rate",
        type=float,
        default=0.1,
        help="Insert rate for incremental patterns (default: 0.1)",
    )
    parser.add_argument(
        "--entities",
        type=int,
        default=500,
        help="Number of entities for current_history (default: 500)",
    )
    parser.add_argument(
        "--changes-per-entity",
        type=int,
        default=3,
        help="Changes per entity for current_history (default: 3)",
    )
    parser.add_argument(
        "--format",
        choices=["parquet", "csv", "json"],
        default="parquet",
        help="Output format (default: parquet)",
    )
    parser.add_argument(
        "--generate-assertions",
        action="store_true",
        help="Generate assertion YAML files alongside data",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be generated without writing files",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.pattern and not args.all:
        parser.error("Either --pattern or --all must be specified")

    return args


def write_scenario(
    scenario: PatternScenario,
    output_dir: Path,
    format: str = "parquet",
    generate_assertions: bool = False,
    verbose: bool = False,
) -> None:
    """Write scenario data to output directory."""
    pattern_dir = output_dir / scenario.pattern
    pattern_dir.mkdir(parents=True, exist_ok=True)

    # Write each batch
    for batch_name, batch_df in scenario.batches.items():
        if format == "parquet":
            file_path = pattern_dir / f"{batch_name}.parquet"
            batch_df.to_parquet(file_path, index=False)
        elif format == "csv":
            file_path = pattern_dir / f"{batch_name}.csv"
            batch_df.to_csv(file_path, index=False)
        elif format == "json":
            file_path = pattern_dir / f"{batch_name}.json"
            batch_df.to_json(file_path, orient="records", indent=2, date_format="iso")

        if verbose:
            print(f"  Wrote {file_path} ({len(batch_df)} rows)")

    # Write metadata
    metadata_path = pattern_dir / "_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(scenario.metadata, f, indent=2, default=str)
    if verbose:
        print(f"  Wrote {metadata_path}")

    # Generate assertions if requested
    if generate_assertions:
        write_assertions(scenario, pattern_dir, verbose)


def write_assertions(
    scenario: PatternScenario,
    pattern_dir: Path,
    verbose: bool = False,
) -> None:
    """Generate and write assertion YAML files."""
    columns = list(scenario.t0.columns)

    if scenario.pattern == "snapshot":
        assertions = create_snapshot_assertions(
            row_count=len(scenario.t0),
            columns=columns,
            first_row_values={"record_id": scenario.t0.iloc[0]["record_id"]},
        )
    elif scenario.pattern == "incremental_append":
        t1_rows = len(scenario.t1) if scenario.t1 is not None else 0
        assertions = create_incremental_append_assertions(
            new_rows_count=t1_rows,
            columns=columns,
        )
    elif scenario.pattern == "incremental_merge":
        changes = scenario.metadata.get("changes", {}).get("t1", {})
        assertions = create_incremental_merge_assertions(
            updated_count=changes.get("update_count", 0),
            inserted_count=changes.get("insert_count", 0),
            columns=columns,
        )
    elif scenario.pattern == "current_history":
        assertions = create_scd2_assertions(
            current_rows=scenario.metadata.get("expected_current_rows", len(scenario.t0)),
            history_rows=len(scenario.t0),
            columns=columns,
        )
    else:
        return

    assertions_path = pattern_dir / "assertions.yaml"
    assertions.to_yaml(assertions_path)
    if verbose:
        print(f"  Wrote {assertions_path}")


def generate_pattern(
    pattern: str,
    args: argparse.Namespace,
) -> PatternScenario:
    """Generate scenario for a specific pattern."""
    generator = PatternTestDataGenerator(
        seed=args.seed,
        base_rows=args.rows,
    )

    if pattern == "snapshot":
        return generator.generate_snapshot_scenario(
            rows=args.rows,
            include_replacement=True,
        )
    elif pattern == "incremental_append":
        return generator.generate_incremental_append_scenario(
            rows=args.rows,
            batches=args.batches,
            insert_rate=args.insert_rate,
        )
    elif pattern == "incremental_merge":
        return generator.generate_incremental_merge_scenario(
            rows=args.rows,
            batches=args.batches,
            update_rate=args.update_rate,
            insert_rate=args.insert_rate,
        )
    elif pattern == "current_history":
        return generator.generate_scd2_scenario(
            entities=args.entities,
            changes_per_entity=args.changes_per_entity,
        )
    else:
        raise ValueError(f"Unknown pattern: {pattern}")


def main() -> int:
    """Main entry point."""
    args = parse_args()

    patterns = (
        ["snapshot", "incremental_append", "incremental_merge", "current_history"]
        if args.all
        else [args.pattern]
    )

    if args.dry_run:
        print("DRY RUN - would generate the following:")
        print(f"  Output directory: {args.output}")
        print(f"  Format: {args.format}")
        print(f"  Patterns: {', '.join(patterns)}")
        print(f"  Rows per T0: {args.rows}")
        print(f"  Batches: {args.batches}")
        print(f"  Seed: {args.seed}")
        if "incremental_merge" in patterns:
            print(f"  Update rate: {args.update_rate}")
            print(f"  Insert rate: {args.insert_rate}")
        if "current_history" in patterns:
            print(f"  Entities: {args.entities}")
            print(f"  Changes per entity: {args.changes_per_entity}")
        return 0

    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)

    print(f"Generating pattern test data to {args.output}")
    print(f"  Seed: {args.seed}")
    print(f"  Format: {args.format}")
    print()

    for pattern in patterns:
        print(f"Generating {pattern.upper()} scenario...")

        scenario = generate_pattern(pattern, args)

        write_scenario(
            scenario,
            args.output,
            format=args.format,
            generate_assertions=args.generate_assertions,
            verbose=args.verbose,
        )

        # Print summary
        batch_summary = ", ".join(
            f"{name}={len(df)}" for name, df in scenario.batches.items()
        )
        print(f"  Generated: {batch_summary} rows")

    print()
    print(f"Done! Data written to {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
