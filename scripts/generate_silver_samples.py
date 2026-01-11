#!/usr/bin/env python
"""CLI script to generate Silver sample data from Bronze samples.

This script processes Bronze sample data through the Silver processing pipeline
to generate canonical Silver outputs for each pattern. Supports different
entity kinds (EVENT, STATE) and history modes (SCD1, SCD2).

Examples:
    # Generate Silver samples for all Bronze patterns
    python scripts/generate_silver_samples.py --all

    # Generate Silver for specific Bronze pattern
    python scripts/generate_silver_samples.py --pattern snapshot

    # Generate with different entity kinds
    python scripts/generate_silver_samples.py --pattern incremental_merge --entity-kind state --history-mode scd2

    # Include deduplication scenarios
    python scripts/generate_silver_samples.py --all --with-duplicates

    # Verify existing Silver samples
    python scripts/generate_silver_samples.py --verify
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd  # noqa: E402

from pipelines.lib.checksum import verify_checksum_manifest, write_checksum_manifest  # noqa: E402
from pipelines.lib.io import SilverOutputMetadata  # noqa: E402
from tests.synthetic_data import DuplicateInjector, DuplicateConfig  # noqa: E402

# Output directory structure:
# sampledata/silver_samples/sample={pattern}_{entity_kind}_{history_mode}/dt={date}/
#   ├── chunk_0.parquet
#   ├── _metadata.json
#   └── _checksums.json

DEFAULT_BRONZE_DIR = project_root / "sampledata" / "bronze_samples"
DEFAULT_OUTPUT_DIR = project_root / "sampledata" / "silver_samples"
DEFAULT_SEED = 42

# Silver processing configurations for each Bronze pattern
#
# PATTERN COVERAGE MATRIX (Bronze × Silver EntityKind × HistoryMode):
# ============================================================================
# | Bronze Pattern      | EVENT          | STATE (SCD1)   | STATE (SCD2)    |
# |---------------------|----------------|----------------|-----------------|
# | FULL_SNAPSHOT       | pattern1 ✓     | pattern8 ✓     | pattern9 ✓      |
# | INCREMENTAL_APPEND  | pattern2 ✓     | pattern6 ✓     | pattern7 ✓      |
# | CDC (incr_merge)    | pattern10 ✓    | pattern4 ✓     | pattern5 ✓      |
# | CDC (curr_history)  | (use pattern10)| (use pattern4) | pattern3 ✓      |
# ============================================================================
#
# All 9 logical Bronze→Silver combinations are covered (3 Bronze × 3 Silver modes).
# Note: current_history is a specialized CDC variant for pre-built SCD2 sources.
#
SILVER_CONFIGS = {
    # =========================================================================
    # FULL_SNAPSHOT Bronze Pattern (complete data replacement each extraction)
    # =========================================================================
    #
    # pattern1: snapshot_events - Treat each snapshot as immutable event log
    "pattern1_full_events": {
        "bronze_pattern": "snapshot",
        "entity_kind": "event",
        "history_mode": None,
        "input_mode": "replace_daily",
        "description": "Full periodic snapshots - each batch is a complete view",
        "unique_columns": ["record_id"],
        "event_ts_column": "created_at",
    },
    # pattern8: snapshot_state_scd1 - Keep only latest snapshot as current state
    "pattern8_snapshot_state_scd1": {
        "bronze_pattern": "snapshot",
        "entity_kind": "state",
        "history_mode": "scd1",
        "input_mode": None,
        "description": "Full snapshot as current state - overwrites previous, no history",
        "unique_columns": ["record_id"],
        "change_ts_column": "created_at",
    },
    # pattern9: snapshot_state_scd2 - Track changes between snapshots with history
    "pattern9_snapshot_state_scd2": {
        "bronze_pattern": "snapshot",
        "entity_kind": "state",
        "history_mode": "scd2",
        "input_mode": None,
        "description": "Full snapshot with SCD2 history - tracks changes between snapshots",
        "unique_columns": ["record_id"],
        "change_ts_column": "created_at",
    },
    # =========================================================================
    # INCREMENTAL_APPEND Bronze Pattern (append-only new records)
    # =========================================================================
    #
    # pattern2: incremental_events - Append-only event log
    "pattern2_cdc_events": {
        "bronze_pattern": "incremental_append",
        "entity_kind": "event",
        "history_mode": None,
        "input_mode": "append_log",
        "description": "Incremental events - append new events, no deduplication",
        "unique_columns": ["record_id"],
        "event_ts_column": "created_at",
    },
    # pattern6: incremental_state_scd1 - Dedupe incremental to latest state
    "pattern6_hybrid_incremental_point": {
        "bronze_pattern": "incremental_append",
        "entity_kind": "state",
        "history_mode": "scd1",
        "input_mode": None,
        "description": "Incremental append treated as state - latest only",
        "unique_columns": ["record_id"],
        "change_ts_column": "updated_at",
    },
    # pattern7: incremental_state_scd2 - Build history from incremental appends
    "pattern7_hybrid_incremental_cumulative": {
        "bronze_pattern": "incremental_append",
        "entity_kind": "state",
        "history_mode": "scd2",
        "input_mode": None,
        "description": "Incremental append as state with full history",
        "unique_columns": ["record_id"],
        "change_ts_column": "updated_at",
    },
    # =========================================================================
    # CDC Bronze Pattern (change data capture with I/U/D operations)
    # =========================================================================
    #
    # pattern10: cdc_events - Treat CDC operations as immutable event log
    "pattern10_cdc_events": {
        "bronze_pattern": "incremental_merge",
        "entity_kind": "event",
        "history_mode": None,
        "input_mode": "append_log",
        "description": "CDC operations as events - each I/U/D is an immutable event",
        "unique_columns": ["record_id"],
        "event_ts_column": "updated_at",
    },
    # pattern4: cdc_state_scd1 - Apply CDC to get current state only
    "pattern4_hybrid_cdc_point": {
        "bronze_pattern": "incremental_merge",
        "entity_kind": "state",
        "history_mode": "scd1",
        "input_mode": None,
        "description": "CDC with deduplication - latest version per key",
        "unique_columns": ["record_id"],
        "change_ts_column": "updated_at",
    },
    # pattern5: cdc_state_scd2 - Build full history from CDC stream
    "pattern5_hybrid_cdc_cumulative": {
        "bronze_pattern": "incremental_merge",
        "entity_kind": "state",
        "history_mode": "scd2",
        "input_mode": None,
        "description": "CDC with full history tracking",
        "unique_columns": ["record_id"],
        "change_ts_column": "updated_at",
    },
    # =========================================================================
    # SPECIALIZED: current_history Bronze Pattern (pre-built SCD2 source)
    # =========================================================================
    #
    # pattern3: Pre-built SCD2 from source system with effective dates
    "pattern3_scd_state": {
        "bronze_pattern": "current_history",
        "entity_kind": "state",
        "history_mode": "scd2",
        "input_mode": None,
        "description": "SCD Type 2 state - full history with effective dates",
        "unique_columns": ["entity_id"],
        "change_ts_column": "effective_from",
    },
}


def _infer_columns(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Infer column metadata from a DataFrame."""
    return [
        {
            "name": col,
            "type": str(df[col].dtype),
            "nullable": bool(df[col].isnull().any()),
        }
        for col in df.columns
    ]


def _normalize_history_mode(config_value: Optional[str], entity_kind: str) -> str:
    """Normalize history mode names to Silver conventions."""
    if entity_kind == "event":
        return "current_only"
    if config_value == "scd2":
        return "full_history"
    return "current_only"


def write_silver_metadata(
    out_dir: Path,
    silver_config: Dict[str, Any],
    batch_name: str,
    df: pd.DataFrame,
    run_date: date,
    processing_info: Dict[str, Any],
    view: Optional[str] = None,
) -> Path:
    """Write metadata file for the Silver batch."""
    history_mode = _normalize_history_mode(
        silver_config.get("history_mode"),
        silver_config["entity_kind"],
    )
    change_ts = silver_config.get("change_ts_column") or silver_config.get(
        "event_ts_column", "updated_at"
    )

    metadata = SilverOutputMetadata(
        row_count=len(df),
        columns=_infer_columns(df),
        written_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        entity_kind=silver_config["entity_kind"],
        history_mode=history_mode,
        unique_columns=silver_config["unique_columns"],
        last_updated_column=change_ts,
        source_path=str(out_dir.parent),
        pipeline_name=f"silver_synthetic_{silver_config['bronze_pattern']}_ingest",
        run_date=run_date.isoformat(),
        format="parquet",
        compression="snappy",
        data_files=["chunk_0.parquet"],
    )

    metadata_dict = metadata.to_dict()
    metadata_dict["processing_info"] = processing_info
    if view:
        metadata_dict["view"] = view

    metadata_path = out_dir / "_metadata.json"
    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_dict, f, indent=2)

    return metadata_path


def load_bronze_batch(
    bronze_dir: Path, pattern: str, batch_date: str
) -> Optional[pd.DataFrame]:
    """Load a Bronze batch from the sample data directory."""
    batch_path = bronze_dir / f"sample={pattern}" / f"dt={batch_date}"
    parquet_file = batch_path / "chunk_0.parquet"

    if not parquet_file.exists():
        return None

    return pd.read_parquet(parquet_file)


def process_event_pattern(
    dfs: List[pd.DataFrame],
    config: Dict[str, Any],
    input_mode: str,
) -> pd.DataFrame:
    """Process data as EVENT entity kind.

    Events are immutable facts - either replace daily or append to log.
    """
    event_ts = config.get("event_ts_column", "created_at")

    if input_mode == "replace_daily":
        # For replace_daily, just use the latest batch
        return dfs[-1].copy() if dfs else pd.DataFrame()

    elif input_mode == "append_log":
        # For append_log, concatenate all batches
        if not dfs:
            return pd.DataFrame()
        combined = pd.concat(dfs, ignore_index=True)
        # Sort by event timestamp
        if event_ts in combined.columns:
            combined = combined.sort_values(event_ts)
        return combined

    return dfs[-1].copy() if dfs else pd.DataFrame()


def process_state_pattern(
    dfs: List[pd.DataFrame],
    config: Dict[str, Any],
    history_mode: str,
) -> Dict[str, pd.DataFrame]:
    """Process data as STATE entity kind.

    State entities have current view and optionally history.
    Returns dict with 'current' and optionally 'history' DataFrames.
    """
    unique_columns = config["unique_columns"]
    change_ts = config.get("change_ts_column", "updated_at")

    if not dfs:
        return {"current": pd.DataFrame()}

    # Combine all batches
    combined = pd.concat(dfs, ignore_index=True)

    if history_mode == "scd1":
        # SCD1: Keep only latest version per key (overwrite)
        if change_ts in combined.columns:
            combined = combined.sort_values(change_ts)
        current = combined.drop_duplicates(subset=unique_columns, keep="last")
        return {"current": current}

    elif history_mode == "scd2":
        # SCD2: Build full history with effective dates
        if change_ts in combined.columns:
            combined = combined.sort_values(unique_columns + [change_ts])

        # Add SCD2 columns
        combined = combined.copy()
        combined["_version"] = combined.groupby(unique_columns).cumcount() + 1

        # Mark current records (last per key)
        combined["_is_current"] = False
        last_idx = combined.groupby(unique_columns)[change_ts].idxmax()
        combined.loc[last_idx, "_is_current"] = True

        # Build current view (just latest records)
        current = combined[combined["_is_current"]].drop(
            columns=["_is_current", "_version"]
        )

        # Build history view (all records with version info)
        history = combined.copy()

        return {"current": current, "history": history}

    return {"current": combined}


def inject_duplicates_if_requested(
    df: pd.DataFrame,
    config: Dict[str, Any],
    seed: int,
) -> pd.DataFrame:
    """Inject duplicates into DataFrame for deduplication testing."""
    if df.empty:
        return df

    unique_columns = config["unique_columns"]
    change_ts = config.get("change_ts_column") or config.get(
        "event_ts_column", "updated_at"
    )

    # Determine mutable columns (columns that can change in near-duplicates)
    mutable_cols = [
        c
        for c in df.columns
        if c not in unique_columns and "amount" in c.lower() or "status" in c.lower()
    ]

    dup_config = DuplicateConfig(
        exact_duplicate_rate=0.05,
        near_duplicate_rate=0.03,
        out_of_order_rate=0.02,
        seed=seed,
    )

    injector = DuplicateInjector(config=dup_config)

    # Inject all types of duplicates
    result = injector.inject_all_duplicate_types(
        df=df,
        key_columns=unique_columns,
        mutable_columns=mutable_cols if mutable_cols else None,
        timestamp_column=change_ts if change_ts in df.columns else None,
    )

    return result


def generate_silver_samples(
    silver_pattern: str,
    bronze_dir: Path,
    output_dir: Path,
    with_duplicates: bool = False,
    seed: int = DEFAULT_SEED,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Generate Silver sample data for a specific pattern configuration.

    Returns:
        Dictionary with generation summary.
    """
    config = SILVER_CONFIGS[silver_pattern]
    bronze_pattern = config["bronze_pattern"]

    # Create output directory
    pattern_dir = output_dir / f"sample={silver_pattern}"
    pattern_dir.mkdir(parents=True, exist_ok=True)

    summary: Dict[str, Any] = {
        "silver_pattern": silver_pattern,
        "bronze_pattern": bronze_pattern,
        "description": config["description"],
        "entity_kind": config["entity_kind"],
        "history_mode": config.get("history_mode"),
        "batches": {},
    }

    # Find all Bronze batches for this pattern
    bronze_pattern_dir = bronze_dir / f"sample={bronze_pattern}"
    if not bronze_pattern_dir.exists():
        if verbose:
            print(
                f"  WARNING: Bronze pattern directory not found: {bronze_pattern_dir}"
            )
        return summary

    batch_dirs = sorted(bronze_pattern_dir.glob("dt=*"))
    if not batch_dirs:
        if verbose:
            print(f"  WARNING: No Bronze batches found in {bronze_pattern_dir}")
        return summary

    # Load all Bronze batches
    bronze_dfs: List[pd.DataFrame] = []
    batch_dates: List[str] = []
    for batch_dir in batch_dirs:
        batch_date = batch_dir.name.replace("dt=", "")
        df = load_bronze_batch(bronze_dir, bronze_pattern, batch_date)
        if df is not None:
            bronze_dfs.append(df)
            batch_dates.append(batch_date)

    if not bronze_dfs:
        if verbose:
            print(f"  WARNING: Could not load any Bronze data for {bronze_pattern}")
        return summary

    # Inject duplicates if requested (for dedup testing)
    if with_duplicates:
        bronze_dfs = [
            inject_duplicates_if_requested(df, config, seed) for df in bronze_dfs
        ]

    # Process according to entity kind
    entity_kind = config["entity_kind"]
    processing_info: Dict[str, Any] = {
        "bronze_batches_processed": len(bronze_dfs),
        "bronze_dates": batch_dates,
        "duplicates_injected": with_duplicates,
    }

    history_mode_value = _normalize_history_mode(
        config.get("history_mode"),
        entity_kind,
    )

    if entity_kind == "event":
        input_mode = config.get("input_mode", "append_log")
        result_df = process_event_pattern(bronze_dfs, config, input_mode)

        # Write single output
        output_date = batch_dates[-1]  # Use last batch date
        batch_dir = pattern_dir / f"dt={output_date}"
        batch_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = batch_dir / "chunk_0.parquet"
        result_df.to_parquet(parquet_path, index=False)

        if verbose:
            print(f"  Wrote {parquet_path} ({len(result_df)} rows)")

        # Write metadata and checksums
        metadata_path = write_silver_metadata(
            out_dir=batch_dir,
            silver_config=config,
            batch_name=f"silver_{output_date}",
            df=result_df,
            run_date=date.fromisoformat(output_date),
            processing_info={**processing_info, "view": "event"},
        )
        checksum_path = write_checksum_manifest(
            out_dir=batch_dir,
            files=[parquet_path],
            entity_kind=config["entity_kind"],
            history_mode=history_mode_value,
            row_count=len(result_df),
            extra_metadata={"view": "event"},
        )

        if verbose:
            print(f"  Wrote {metadata_path}")
            print(f"  Wrote {checksum_path}")

        summary["batches"][output_date] = {
            "path": str(batch_dir.relative_to(output_dir)),
            "row_count": len(result_df),
        }
        summary["total_rows"] = len(result_df)

    elif entity_kind == "state":
        history_mode = config.get("history_mode", "scd1")
        result = process_state_pattern(bronze_dfs, config, history_mode)

        output_date = batch_dates[-1]
        total_rows = 0

        # Write current view
        current_df = result["current"]
        current_dir = pattern_dir / f"dt={output_date}" / "view=current"
        current_dir.mkdir(parents=True, exist_ok=True)

        current_parquet = current_dir / "chunk_0.parquet"
        current_df.to_parquet(current_parquet, index=False)

        if verbose:
            print(f"  Wrote {current_parquet} ({len(current_df)} rows)")

        write_silver_metadata(
            out_dir=current_dir,
            silver_config=config,
            batch_name=f"silver_current_{output_date}",
            df=current_df,
            run_date=date.fromisoformat(output_date),
            processing_info={**processing_info, "view": "current"},
            view="current",
        )
        write_checksum_manifest(
            out_dir=current_dir,
            files=[current_parquet],
            entity_kind=config["entity_kind"],
            history_mode=history_mode_value,
            row_count=len(current_df),
            extra_metadata={"view": "current"},
        )

        summary["batches"][f"{output_date}/current"] = {
            "path": str(current_dir.relative_to(output_dir)),
            "row_count": len(current_df),
        }
        total_rows += len(current_df)

        # Write history view if SCD2
        if "history" in result:
            history_df = result["history"]
            history_dir = pattern_dir / f"dt={output_date}" / "view=history"
            history_dir.mkdir(parents=True, exist_ok=True)

            history_parquet = history_dir / "chunk_0.parquet"
            history_df.to_parquet(history_parquet, index=False)

            if verbose:
                print(f"  Wrote {history_parquet} ({len(history_df)} rows)")

            write_silver_metadata(
                out_dir=history_dir,
                silver_config=config,
                batch_name=f"silver_history_{output_date}",
                df=history_df,
                run_date=date.fromisoformat(output_date),
                processing_info={**processing_info, "view": "history"},
                view="history",
            )
            write_checksum_manifest(
                out_dir=history_dir,
                files=[history_parquet],
                entity_kind=config["entity_kind"],
                history_mode=history_mode_value,
                row_count=len(history_df),
                extra_metadata={"view": "history"},
            )

            summary["batches"][f"{output_date}/history"] = {
                "path": str(history_dir.relative_to(output_dir)),
                "row_count": len(history_df),
            }
            total_rows += len(history_df)

        summary["total_rows"] = total_rows

    # Write pattern-level manifest
    manifest_path = pattern_dir / "_manifest.json"
    manifest = {
        "silver_pattern": silver_pattern,
        "bronze_pattern": bronze_pattern,
        "description": config["description"],
        "entity_kind": config["entity_kind"],
        "history_mode": config.get("history_mode"),
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "batches": list(summary["batches"].keys()),
        "total_rows": summary.get("total_rows", 0),
        "duplicates_injected": with_duplicates,
    }
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)

    if verbose:
        print(f"  Wrote {manifest_path}")

    summary["manifest_path"] = str(manifest_path.relative_to(output_dir))

    return summary


def verify_samples(output_dir: Path, verbose: bool = False) -> Dict[str, Any]:
    """Verify existing Silver samples have valid checksums."""
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

        for checksums_path in pattern_dir.rglob("_checksums.json"):
            batch_dir = checksums_path.parent
            relative_path = str(batch_dir.relative_to(pattern_dir))

            verification = verify_checksum_manifest(batch_dir)
            pattern_results["batches"][relative_path] = {
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
            print(f"  [{status}] {pattern}: {len(pattern_results['batches'])} outputs")

    return results


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate Silver sample data from Bronze samples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--pattern",
        choices=list(SILVER_CONFIGS.keys()),
        help="Generate Silver for specific pattern configuration",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate Silver for all pattern configurations",
    )
    parser.add_argument(
        "--bronze-dir",
        type=Path,
        default=DEFAULT_BRONZE_DIR,
        help=f"Bronze samples directory (default: {DEFAULT_BRONZE_DIR})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--with-duplicates",
        action="store_true",
        help="Inject duplicates for deduplication testing",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for duplicate injection (default: {DEFAULT_SEED})",
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
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    if not args.pattern and not args.all and not args.verify:
        parser.error("Either --pattern, --all, or --verify must be specified")

    return args


def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Handle verify mode
    if args.verify:
        print(f"Verifying Silver samples in {args.output}")
        if not args.output.exists():
            print(f"ERROR: Output directory does not exist: {args.output}")
            return 1

        results = verify_samples(args.output, verbose=args.verbose)

        if results["valid"]:
            print("\n[PASS] All Silver samples verified successfully")
            return 0
        else:
            print("\n[FAIL] Verification failed")
            if args.verbose:
                print(json.dumps(results, indent=2))
            return 1

    # Check Bronze samples exist
    if not args.bronze_dir.exists():
        print(f"ERROR: Bronze samples directory not found: {args.bronze_dir}")
        print("Run 'python scripts/generate_bronze_samples.py --all' first")
        return 1

    # Determine patterns to generate
    patterns = list(SILVER_CONFIGS.keys()) if args.all else [args.pattern]

    if args.dry_run:
        print("DRY RUN - would generate the following:")
        print(f"  Bronze source: {args.bronze_dir}")
        print(f"  Output directory: {args.output}")
        print(f"  Patterns: {len(patterns)}")
        print(f"  With duplicates: {args.with_duplicates}")
        for pattern in patterns:
            config = SILVER_CONFIGS[pattern]
            print(f"\n  {pattern}:")
            print(f"    Bronze: {config['bronze_pattern']}")
            print(f"    Entity: {config['entity_kind']}")
            print(f"    History: {config.get('history_mode', 'N/A')}")
            print(f"    {config['description']}")
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

    print(f"Generating Silver samples to {args.output}")
    print(f"  Bronze source: {args.bronze_dir}")
    print(f"  With duplicates: {args.with_duplicates}")
    print()

    summaries: Dict[str, Any] = {}

    for pattern in patterns:
        config = SILVER_CONFIGS[pattern]
        print(
            f"Generating {pattern} ({config['entity_kind']}, {config.get('history_mode', 'N/A')})..."
        )

        summary = generate_silver_samples(
            silver_pattern=pattern,
            bronze_dir=args.bronze_dir,
            output_dir=args.output,
            with_duplicates=args.with_duplicates,
            seed=args.seed,
            verbose=args.verbose,
        )

        summaries[pattern] = summary

        if "total_rows" in summary:
            print(f"  Generated: {summary['total_rows']} total rows")
        else:
            print("  WARNING: No data generated")

    # Write overall manifest
    overall_manifest_path = args.output / "_manifest.json"
    overall_manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "bronze_source": str(args.bronze_dir),
        "with_duplicates": args.with_duplicates,
        "seed": args.seed,
        "patterns": {
            pattern: {
                "description": SILVER_CONFIGS[pattern]["description"],
                "entity_kind": SILVER_CONFIGS[pattern]["entity_kind"],
                "history_mode": SILVER_CONFIGS[pattern].get("history_mode"),
                "total_rows": summaries[pattern].get("total_rows", 0),
            }
            for pattern in patterns
        },
    }
    with overall_manifest_path.open("w", encoding="utf-8") as f:
        json.dump(overall_manifest, f, indent=2)

    print()
    print(f"Done! Silver samples written to {args.output}")
    print(f"  Manifest: {overall_manifest_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
