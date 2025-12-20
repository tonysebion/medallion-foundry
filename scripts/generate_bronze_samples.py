#!/usr/bin/env python
"""CLI script to generate Bronze sample data for all load patterns.

This script generates canonical Bronze samples with proper metadata and checksums
for testing, debugging, and documentation. Data is generated on-demand and can
be verified against expected checksums.

Examples:
    # Generate all patterns (default)
    python scripts/generate_bronze_samples.py --all

    # Generate specific pattern
    python scripts/generate_bronze_samples.py --pattern snapshot

    # Generate with custom row count
    python scripts/generate_bronze_samples.py --all --rows 500

    # Verify existing samples match expected checksums
    python scripts/generate_bronze_samples.py --verify

    # Clean and regenerate
    python scripts/generate_bronze_samples.py --all --clean
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipelines.lib.bronze import BronzeOutputMetadata, LoadPattern, SourceType  # noqa: E402
from pipelines.lib.checksum import verify_checksum_manifest, write_checksum_manifest  # noqa: E402
from tests.pattern_verification.pattern_data.generators import (  # noqa: E402
    PatternTestDataGenerator,
)

# Output directory structure:
# sampledata/bronze_samples/sample={pattern}/dt={date}/
#   ├── chunk_0.parquet
#   ├── _metadata.json
#   └── _checksums.json

DEFAULT_OUTPUT_DIR = project_root / "sampledata" / "bronze_samples"
DEFAULT_SEED = 42
DEFAULT_ROWS = 500

# Pattern configurations
PATTERN_CONFIGS = {
    "snapshot": {
        "batches": 2,  # T0 + replacement T1
        "description": "Full data replacement each run",
    },
    "incremental_append": {
        "batches": 4,  # T0, T1, T2, T3
        "insert_rate": 0.1,
        "description": "Append new records only (insert-only CDC)",
    },
    "incremental_merge": {
        "batches": 4,  # T0, T1, T2, T3
        "update_rate": 0.2,
        "insert_rate": 0.1,
        "description": "Upsert by primary key (updates + inserts)",
    },
    "current_history": {
        "entities": 200,
        "changes_per_entity": 3,
        "description": "SCD Type 2 with current/history split",
    },
}

PATTERN_LOAD_MAP = {
    "snapshot": LoadPattern.FULL_SNAPSHOT,
    "incremental_append": LoadPattern.INCREMENTAL_APPEND,
    "incremental_merge": LoadPattern.CDC,
    "current_history": LoadPattern.CDC,
}


def _infer_columns(batch_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Infer column metadata from a DataFrame."""
    return [
        {
            "name": col,
            "type": str(batch_df[col].dtype),
        }
        for col in batch_df.columns
    ]


def write_metadata(
    out_dir: Path,
    load_pattern: str,
    batch_name: str,
    batch_df: pd.DataFrame,
    run_date: date,
    scenario_metadata: Dict[str, Any],
    seed: int,
) -> Path:
    """Write metadata file for the Bronze batch."""
    columns = _infer_columns(batch_df)
    metadata = BronzeOutputMetadata(
        row_count=len(batch_df),
        columns=columns,
        written_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        run_date=run_date.isoformat(),
        data_files=["chunk_0.parquet"],
        extra={
            "system": "synthetic",
            "entity": f"pattern_{load_pattern}",
            "source_type": SourceType.FILE_PARQUET.value,
            "load_pattern": PATTERN_LOAD_MAP.get(load_pattern, LoadPattern.CDC).value,
        },
    )

    metadata_dict = metadata.to_dict()
    if scenario_metadata:
        metadata_dict["pattern_info"] = scenario_metadata.get(load_pattern) or scenario_metadata
    metadata_dict["seed"] = seed

    metadata_path = out_dir / "_metadata.json"
    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_dict, f, indent=2)

    return metadata_path


def generate_pattern_samples(
    pattern: str,
    output_dir: Path,
    rows: int,
    seed: int,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Generate sample data for a specific pattern.

    Returns:
        Dictionary with generation summary including paths and row counts.
    """
    generator = PatternTestDataGenerator(seed=seed, base_rows=rows)
    config = PATTERN_CONFIGS[pattern]

    # Generate scenario based on pattern
    if pattern == "snapshot":
        scenario = generator.generate_snapshot_scenario(
            rows=rows,
            include_replacement=True,
        )
    elif pattern == "incremental_append":
        scenario = generator.generate_incremental_append_scenario(
            rows=rows,
            batches=config["batches"],
            insert_rate=config["insert_rate"],
        )
    elif pattern == "incremental_merge":
        scenario = generator.generate_incremental_merge_scenario(
            rows=rows,
            batches=config["batches"],
            update_rate=config["update_rate"],
            insert_rate=config["insert_rate"],
        )
    elif pattern == "current_history":
        scenario = generator.generate_scd2_scenario(
            entities=config["entities"],
            changes_per_entity=config["changes_per_entity"],
        )
    else:
        raise ValueError(f"Unknown pattern: {pattern}")

    # Create pattern directory
    pattern_dir = output_dir / f"sample={pattern}"
    pattern_dir.mkdir(parents=True, exist_ok=True)

    summary: Dict[str, Any] = {
        "pattern": pattern,
        "description": config["description"],
        "seed": seed,
        "batches": {},
    }

    # Write each batch to its own date partition
    base_date = date(2024, 1, 15)
    for batch_name, batch_df in scenario.batches.items():
        # Calculate batch date (T0 = base, T1 = base+1, etc.)
        batch_num = int(batch_name[1]) if batch_name[0] == "t" else 0
        batch_date = date(
            base_date.year,
            base_date.month,
            base_date.day + batch_num,
        )

        # Create date partition directory
        batch_dir = pattern_dir / f"dt={batch_date.isoformat()}"
        batch_dir.mkdir(parents=True, exist_ok=True)

        # Write parquet file
        parquet_path = batch_dir / "chunk_0.parquet"
        batch_df.to_parquet(parquet_path, index=False)

        if verbose:
            print(f"  Wrote {parquet_path} ({len(batch_df)} rows)")

        # Write metadata
        metadata_path = write_metadata(
            out_dir=batch_dir,
            load_pattern=pattern,
            batch_name=batch_name,
            batch_df=batch_df,
            run_date=batch_date,
            scenario_metadata=scenario.metadata,
            seed=seed,
        )

        if verbose:
            print(f"  Wrote {metadata_path}")

        # Write checksums
        checksum_path = write_checksum_manifest(
            out_dir=batch_dir,
            files=[parquet_path],
            row_count=len(batch_df),
            extra_metadata={
                "batch_name": batch_name,
                "load_pattern": pattern,
            },
        )

        if verbose:
            print(f"  Wrote {checksum_path}")

        summary["batches"][batch_name] = {
            "path": str(batch_dir.relative_to(output_dir)),
            "date": batch_date.isoformat(),
            "row_count": len(batch_df),
            "files": {
                "parquet": str(parquet_path.relative_to(output_dir)),
                "metadata": str(metadata_path.relative_to(output_dir)),
                "checksums": str(checksum_path.relative_to(output_dir)),
            },
        }

    # Write pattern-level manifest
    manifest_path = pattern_dir / "_manifest.json"
    manifest = {
        "pattern": pattern,
        "description": config["description"],
        "seed": seed,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "batches": list(scenario.batches.keys()),
        "total_rows": sum(len(df) for df in scenario.batches.values()),
        "scenario_metadata": scenario.metadata,
    }
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)

    if verbose:
        print(f"  Wrote {manifest_path}")

    summary["manifest_path"] = str(manifest_path.relative_to(output_dir))
    summary["total_rows"] = manifest["total_rows"]

    return summary


def verify_samples(output_dir: Path, verbose: bool = False) -> Dict[str, Any]:
    """Verify existing samples have valid checksums.

    Returns:
        Dictionary with verification results for each pattern.
    """
    results: Dict[str, Any] = {
        "valid": True,
        "patterns": {},
    }

    for pattern_dir in output_dir.glob("sample=*"):
        pattern = pattern_dir.name.replace("sample=", "")
        pattern_results: Dict[str, Any] = {
            "valid": True,
            "batches": {},
        }

        # Check each date partition
        for batch_dir in pattern_dir.glob("dt=*"):
            batch_date = batch_dir.name.replace("dt=", "")
            verification = verify_checksum_manifest(batch_dir)

            pattern_results["batches"][batch_date] = {
                "valid": verification.valid,
                "missing_files": verification.missing_files,
                "mismatched_files": verification.mismatched_files,
            }

            if not verification.valid:
                pattern_results["valid"] = False
                results["valid"] = False

        results["patterns"][pattern] = pattern_results

        if verbose:
            status = "PASS" if pattern_results["valid"] else "FAIL"
            print(f"  [{status}] {pattern}: {len(pattern_results['batches'])} batches")

    return results


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate Bronze sample data for all load patterns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--pattern",
        choices=list(PATTERN_CONFIGS.keys()),
        help="Generate data for specific pattern only",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate data for all patterns",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help=f"Number of rows for T0 batch (default: {DEFAULT_ROWS})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for reproducibility (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify existing samples match expected checksums",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing samples before generating",
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
    if not args.pattern and not args.all and not args.verify:
        parser.error("Either --pattern, --all, or --verify must be specified")

    return args


def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Handle verify mode
    if args.verify:
        print(f"Verifying samples in {args.output}")
        if not args.output.exists():
            print(f"ERROR: Output directory does not exist: {args.output}")
            return 1

        results = verify_samples(args.output, verbose=args.verbose)

        if results["valid"]:
            print("\n[PASS] All samples verified successfully")
            return 0
        else:
            print("\n[FAIL] Verification failed")
            if args.verbose:
                print(json.dumps(results, indent=2))
            return 1

    # Determine patterns to generate
    patterns = list(PATTERN_CONFIGS.keys()) if args.all else [args.pattern]

    if args.dry_run:
        print("DRY RUN - would generate the following:")
        print(f"  Output directory: {args.output}")
        print(f"  Patterns: {', '.join(patterns)}")
        print(f"  Rows per T0: {args.rows}")
        print(f"  Seed: {args.seed}")
        for pattern in patterns:
            config = PATTERN_CONFIGS[pattern]
            print(f"\n  {pattern.upper()}:")
            print(f"    {config['description']}")
            if "batches" in config:
                print(f"    Batches: {config['batches']}")
            if "entities" in config:
                print(f"    Entities: {config['entities']}")
        return 0

    # Clean if requested
    if args.clean and args.output.exists():
        print(f"Cleaning {args.output}")
        for pattern in patterns:
            pattern_dir = args.output / f"sample={pattern}"
            if pattern_dir.exists():
                shutil.rmtree(pattern_dir)
                if args.verbose:
                    print(f"  Removed {pattern_dir}")

    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)

    print(f"Generating Bronze samples to {args.output}")
    print(f"  Seed: {args.seed}")
    print(f"  Base rows: {args.rows}")
    print()

    summaries: Dict[str, Any] = {}

    for pattern in patterns:
        print(f"Generating {pattern.upper()} samples...")

        summary = generate_pattern_samples(
            pattern=pattern,
            output_dir=args.output,
            rows=args.rows,
            seed=args.seed,
            verbose=args.verbose,
        )

        summaries[pattern] = summary

        batch_summary = ", ".join(
            f"{name}={info['row_count']}"
            for name, info in summary["batches"].items()
        )
        print(f"  Generated: {batch_summary} rows (total: {summary['total_rows']})")

    # Write overall manifest
    overall_manifest_path = args.output / "_manifest.json"
    overall_manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "seed": args.seed,
        "base_rows": args.rows,
        "patterns": {
            pattern: {
                "description": PATTERN_CONFIGS[pattern]["description"],
                "total_rows": summaries[pattern]["total_rows"],
                "batches": list(summaries[pattern]["batches"].keys()),
            }
            for pattern in patterns
        },
    }
    with overall_manifest_path.open("w", encoding="utf-8") as f:
        json.dump(overall_manifest, f, indent=2)

    print()
    print(f"Done! Samples written to {args.output}")
    print(f"  Manifest: {overall_manifest_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
