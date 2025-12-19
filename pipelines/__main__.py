"""CLI entry point for running pipelines.

Usage:
    python -m pipelines claims.header --date 2025-01-15
    python -m pipelines claims.header:bronze --date 2025-01-15
    python -m pipelines claims.header:silver --date 2025-01-15
    python -m pipelines claims.header --date 2025-01-15 --dry-run

Pipeline naming convention:
    - Pipelines are Python modules in the pipelines/ directory
    - Use dot notation: claims.header -> pipelines/claims/header.py
    - Append :bronze or :silver to run only that layer
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipelines.lib.connections import close_all_connections


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for pipeline execution."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def discover_pipelines() -> List[Dict[str, Any]]:
    """Discover available pipelines in the pipelines directory.

    Returns:
        List of dicts with pipeline info: name, path, has_bronze, has_silver
    """
    pipelines_dir = Path(__file__).parent
    pipelines: List[Dict[str, Any]] = []

    # Skip these directories/files
    skip = {"lib", "templates", "examples", "__pycache__", "__init__.py", "__main__.py"}

    for py_file in pipelines_dir.rglob("*.py"):
        # Get relative path from pipelines dir
        rel_path = py_file.relative_to(pipelines_dir)

        # Skip lib, templates, examples, and special files
        if any(part in skip for part in rel_path.parts):
            continue

        # Skip __init__.py and __main__.py
        if py_file.name.startswith("_"):
            continue

        # Convert path to module name: claims/header.py -> claims.header
        module_name = str(rel_path.with_suffix("")).replace("\\", ".").replace("/", ".")

        # Try to load and inspect the module
        try:
            full_path = f"pipelines.{module_name}"
            module = importlib.import_module(full_path)

            has_bronze = hasattr(module, "run_bronze") or hasattr(module, "bronze")
            has_silver = hasattr(module, "run_silver") or hasattr(module, "silver")
            has_run = hasattr(module, "run")

            # Get docstring if available
            doc = module.__doc__.strip().split("\n")[0] if module.__doc__ else ""

            pipelines.append({
                "name": module_name,
                "path": str(rel_path),
                "has_bronze": has_bronze,
                "has_silver": has_silver,
                "has_run": has_run,
                "description": doc,
            })
        except Exception:
            # Skip modules that can't be loaded
            pass

    return sorted(pipelines, key=lambda p: p["name"])


def list_pipelines() -> None:
    """Print list of available pipelines."""
    pipelines = discover_pipelines()

    if not pipelines:
        print("No pipelines found.")
        print()
        print("To create a pipeline, add a Python file to the pipelines/ directory.")
        print("See pipelines/templates/ for examples.")
        return

    print("Available pipelines:")
    print()

    # Calculate column widths
    max_name = max(len(p["name"]) for p in pipelines)
    max_name = max(max_name, 10)  # Minimum width

    # Print header
    print(f"  {'Name':<{max_name}}  {'Layers':<15}  Description")
    print(f"  {'-' * max_name}  {'-' * 15}  {'-' * 40}")

    for p in pipelines:
        layers = []
        if p["has_bronze"]:
            layers.append("bronze")
        if p["has_silver"]:
            layers.append("silver")
        layers_str = ", ".join(layers) if layers else "run only"

        desc = p["description"][:40] if p["description"] else ""

        print(f"  {p['name']:<{max_name}}  {layers_str:<15}  {desc}")

    print()
    print("Usage:")
    print("  python -m pipelines <name> --date YYYY-MM-DD")
    print("  python -m pipelines <name>:bronze --date YYYY-MM-DD  # Bronze only")
    print("  python -m pipelines <name>:silver --date YYYY-MM-DD  # Silver only")


def parse_pipeline_spec(spec: str) -> tuple[str, Optional[str]]:
    """Parse pipeline specification into module path and optional layer.

    Args:
        spec: Pipeline specification like "claims.header" or "claims.header:bronze"

    Returns:
        Tuple of (module_path, layer) where layer is "bronze", "silver", or None
    """
    if ":" in spec:
        module_path, layer = spec.rsplit(":", 1)
        if layer not in ("bronze", "silver"):
            print(f"Error: Invalid layer '{layer}'. Must be 'bronze' or 'silver'.")
            sys.exit(1)
        return module_path, layer
    return spec, None


def load_pipeline_module(module_path: str) -> Any:
    """Load a pipeline module by path.

    Args:
        module_path: Dot-separated path like "claims.header"

    Returns:
        Loaded module
    """
    # Convert dots to module path: claims.header -> pipelines.claims.header
    full_path = f"pipelines.{module_path.replace('.', '_')}"

    # Also try with dots preserved for nested directories
    try:
        return importlib.import_module(full_path)
    except ModuleNotFoundError:
        # Try with dots as subdirectories
        full_path_nested = f"pipelines.{module_path}"
        try:
            return importlib.import_module(full_path_nested)
        except ModuleNotFoundError:
            print(f"Error: Pipeline module not found: {module_path}")
            print(f"  Tried: {full_path}")
            print(f"  Tried: {full_path_nested}")
            print()
            print("Make sure the pipeline file exists at one of:")
            print(f"  pipelines/{module_path.replace('.', '_')}.py")
            print(f"  pipelines/{module_path.replace('.', '/')}.py")
            sys.exit(1)


def run_pipeline(
    module: Any,
    layer: Optional[str],
    run_date: str,
    dry_run: bool = False,
    target_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Run a pipeline module.

    Args:
        module: Loaded pipeline module
        layer: Optional layer to run ("bronze", "silver", or None for both)
        run_date: Date for this pipeline run
        dry_run: If True, validate but don't execute
        target_override: Override target path for local development

    Returns:
        Pipeline result dictionary
    """
    kwargs: Dict[str, Any] = {"dry_run": dry_run}
    if target_override:
        kwargs["target_override"] = target_override

    if layer == "bronze":
        if hasattr(module, "run_bronze"):
            return module.run_bronze(run_date, **kwargs)
        elif hasattr(module, "bronze"):
            return module.bronze.run(run_date, **kwargs)
        else:
            print("Error: Pipeline has no 'run_bronze' function or 'bronze' object")
            sys.exit(1)

    elif layer == "silver":
        if hasattr(module, "run_silver"):
            return module.run_silver(run_date, **kwargs)
        elif hasattr(module, "silver"):
            return module.silver.run(run_date, **kwargs)
        else:
            print("Error: Pipeline has no 'run_silver' function or 'silver' object")
            sys.exit(1)

    else:
        # Run full pipeline
        if hasattr(module, "run"):
            return module.run(run_date, **kwargs)
        else:
            print("Error: Pipeline has no 'run' function")
            print("  Define a run(run_date: str) function in your pipeline")
            sys.exit(1)


def explain_pipeline(module: Any, layer: Optional[str], run_date: str) -> None:
    """Explain what the pipeline would do without executing.

    Shows configuration details and execution plan.
    """
    print()
    print("=" * 60)
    print("PIPELINE EXPLANATION")
    print("=" * 60)
    print(f"Run Date: {run_date}")
    print(f"Layer: {layer or 'all (bronze → silver)'}")
    print()

    # Check for bronze configuration
    bronze = getattr(module, "bronze", None)
    if bronze and (layer is None or layer == "bronze"):
        print("BRONZE LAYER:")
        print("-" * 40)
        print(f"  System:       {bronze.system}")
        print(f"  Entity:       {bronze.entity}")
        print(f"  Source Type:  {bronze.source_type.value}")
        print(f"  Source Path:  {bronze.source_path or '(from database)'}")
        print(f"  Target Path:  {bronze.target_path}")
        print(f"  Load Pattern: {bronze.load_pattern.value}")
        if bronze.watermark_column:
            print(f"  Watermark:    {bronze.watermark_column}")
        print()

    # Check for silver configuration
    silver = getattr(module, "silver", None)
    if silver and (layer is None or layer == "silver"):
        print("SILVER LAYER:")
        print("-" * 40)
        print(f"  Source Path:  {silver.source_path}")
        print(f"  Target Path:  {silver.target_path}")
        print(f"  Natural Keys: {', '.join(silver.natural_keys)}")
        print(f"  Change Col:   {silver.change_timestamp}")
        print(f"  Entity Kind:  {silver.entity_kind.value}")
        print(f"  History Mode: {silver.history_mode.value}")
        if silver.attributes:
            print(f"  Attributes:   {', '.join(silver.attributes)}")
        print()

    # Show execution flow
    print("EXECUTION FLOW:")
    print("-" * 40)
    if layer == "bronze":
        print("  1. Extract data from source")
        print("  2. Add Bronze metadata columns")
        print("  3. Write to Bronze target path")
        print("  4. Generate _metadata.json and _checksums.json")
    elif layer == "silver":
        print("  1. Read Bronze data from source path")
        print("  2. Deduplicate by natural keys")
        print("  3. Apply history mode (SCD1/SCD2)")
        print("  4. Write to Silver target path")
        print("  5. Generate _metadata.json and _checksums.json")
    else:
        print("  1. BRONZE: Extract data from source")
        print("  2. BRONZE: Add metadata, write parquet + artifacts")
        print("  3. SILVER: Read Bronze data")
        print("  4. SILVER: Deduplicate and apply history mode")
        print("  5. SILVER: Write parquet + artifacts")
    print()
    print("=" * 60)


def check_pipeline(module: Any, layer: Optional[str], run_date: str) -> None:
    """Validate pipeline configuration and connectivity.

    Performs pre-flight checks without executing the pipeline.
    """
    from pipelines.lib.validate import (
        validate_bronze_source,
        validate_silver_entity,
        format_validation_report,
        ValidationSeverity,
    )

    print()
    print("=" * 60)
    print("PIPELINE VALIDATION")
    print("=" * 60)

    all_issues = []
    has_errors = False

    # Check bronze configuration
    bronze = getattr(module, "bronze", None)
    if bronze and (layer is None or layer == "bronze"):
        print(f"\nBronze: {bronze.system}.{bronze.entity}")
        print("-" * 40)

        issues = validate_bronze_source(bronze)
        all_issues.extend(issues)

        if issues:
            print(format_validation_report(issues))
            if any(i.severity == ValidationSeverity.ERROR for i in issues):
                has_errors = True
        else:
            print("  Configuration: OK")

        # Check source connectivity
        print("  Connectivity: ", end="")
        try:
            connectivity_ok = _check_bronze_connectivity(bronze)
            if connectivity_ok:
                print("OK")
            else:
                print("FAILED (see above)")
                has_errors = True
        except Exception as e:
            print(f"FAILED - {e}")
            has_errors = True

    # Check silver configuration
    silver = getattr(module, "silver", None)
    if silver and (layer is None or layer == "silver"):
        print(f"\nSilver: {silver.target_path}")
        print("-" * 40)

        issues = validate_silver_entity(silver)
        all_issues.extend(issues)

        if issues:
            print(format_validation_report(issues))
            if any(i.severity == ValidationSeverity.ERROR for i in issues):
                has_errors = True
        else:
            print("  Configuration: OK")

        # Check source path exists (Bronze output)
        print("  Source Data:  ", end="")
        source_path = silver.source_path.format(run_date=run_date)
        if _check_path_exists(source_path):
            print("OK")
        else:
            print(f"NOT FOUND at {source_path}")
            print("  (Run Bronze layer first to generate source data)")

    print()
    print("=" * 60)

    if has_errors:
        print("RESULT: FAILED - Fix errors above before running")
        sys.exit(1)
    else:
        print("RESULT: PASSED - Pipeline is ready to run")


def _check_bronze_connectivity(bronze: Any) -> bool:
    """Check connectivity to Bronze source."""
    from pipelines.lib.bronze import SourceType
    from pipelines.lib.env import expand_options

    if bronze.source_type in (SourceType.DATABASE_MSSQL, SourceType.DATABASE_POSTGRES):
        # For databases, try to connect
        from pipelines.lib.connections import get_connection

        opts = expand_options(bronze.options)
        connection_name = opts.get(
            "connection_name", f"{bronze.system}_{bronze.entity}"
        )
        try:
            con = get_connection(connection_name, bronze.source_type, opts)
            # Try a simple query to verify connection
            con.list_tables()
            return True
        except Exception as e:
            print(f"FAILED - Cannot connect to database: {e}")
            return False

    elif bronze.source_type in (
        SourceType.FILE_CSV,
        SourceType.FILE_PARQUET,
        SourceType.FILE_SPACE_DELIMITED,
        SourceType.FILE_FIXED_WIDTH,
    ):
        # For files, check if path exists
        source_path = bronze.source_path
        if "{run_date}" in source_path:
            print("(skipped - path contains {run_date} template)")
            return True
        return _check_path_exists(source_path)

    elif bronze.source_type == SourceType.API_REST:
        # For APIs, just check URL format
        print("(skipped - API connectivity checked at runtime)")
        return True

    return True


def _check_path_exists(path: str) -> bool:
    """Check if a path exists (local or cloud)."""
    from pathlib import Path

    if path.startswith("s3://"):
        try:
            import fsspec

            fs = fsspec.filesystem("s3")
            return fs.exists(path)
        except Exception:
            return False
    elif path.startswith(("abfss://", "wasbs://")):
        try:
            import fsspec

            fs = fsspec.filesystem("abfs")
            return fs.exists(path)
        except Exception:
            return False
    else:
        # Local path - handle glob patterns
        if "*" in path:
            import glob

            return len(glob.glob(path)) > 0
        return Path(path).exists()


def test_connection_command(connection_name: str, args: Any) -> None:
    """Test database connection.

    Usage: python -m pipelines test-connection <name> --host <host> --database <db>
    """
    import os
    from pipelines.lib.bronze import SourceType
    from pipelines.lib.connections import get_connection

    print()
    print("=" * 60)
    print(f"TESTING CONNECTION: {connection_name}")
    print("=" * 60)

    # Build options from args or environment
    options: Dict[str, Any] = {}

    # Check for host/database in remaining args or env vars
    host = os.environ.get(f"{connection_name.upper()}_HOST")
    database = os.environ.get(f"{connection_name.upper()}_DATABASE")
    db_type = os.environ.get(f"{connection_name.upper()}_TYPE", "mssql")

    # Parse additional args (simple key=value parsing)
    remaining = sys.argv[3:] if len(sys.argv) > 3 else []
    for arg in remaining:
        if arg.startswith("--host="):
            host = arg.split("=", 1)[1]
        elif arg.startswith("--database="):
            database = arg.split("=", 1)[1]
        elif arg.startswith("--type="):
            db_type = arg.split("=", 1)[1]
        elif arg == "--host" and remaining.index(arg) + 1 < len(remaining):
            host = remaining[remaining.index(arg) + 1]
        elif arg == "--database" and remaining.index(arg) + 1 < len(remaining):
            database = remaining[remaining.index(arg) + 1]
        elif arg == "--type" and remaining.index(arg) + 1 < len(remaining):
            db_type = remaining[remaining.index(arg) + 1]

    if not host:
        print("ERROR: No host specified.")
        print(f"  Set {connection_name.upper()}_HOST environment variable")
        print("  Or use: --host <hostname>")
        sys.exit(1)

    if not database:
        print("ERROR: No database specified.")
        print(f"  Set {connection_name.upper()}_DATABASE environment variable")
        print("  Or use: --database <database>")
        sys.exit(1)

    options["host"] = host
    options["database"] = database

    # Determine source type
    source_type = (
        SourceType.DATABASE_POSTGRES
        if db_type.lower() in ("postgres", "postgresql", "pg")
        else SourceType.DATABASE_MSSQL
    )

    print(f"  Host:     {host}")
    print(f"  Database: {database}")
    print(f"  Type:     {source_type.value}")
    print()
    print("Connecting...", end=" ")

    try:
        con = get_connection(connection_name, source_type, options)
        tables = con.list_tables()
        print("OK")
        print()
        print(f"Found {len(tables)} tables")
        if tables and len(tables) <= 10:
            print("Tables:")
            for table in sorted(tables)[:10]:
                print(f"  - {table}")
        elif tables:
            print(f"First 10 tables: {', '.join(sorted(tables)[:10])}")
        print()
        print("=" * 60)
        print("RESULT: CONNECTION SUCCESSFUL")
    except Exception as e:
        print("FAILED")
        print()
        print(f"Error: {e}")
        print()
        print("=" * 60)
        print("RESULT: CONNECTION FAILED")
        sys.exit(1)


def print_result(result: Dict[str, Any], pipeline_spec: str) -> None:
    """Print pipeline result in a readable format."""
    print()
    print("=" * 60)
    print(f"Pipeline: {pipeline_spec}")
    print("=" * 60)

    if result.get("dry_run"):
        print("DRY RUN - No data was written")
        return

    if result.get("skipped"):
        print(f"SKIPPED - {result.get('reason', 'unknown reason')}")
        return

    # Bronze results
    if "bronze" in result:
        bronze = result["bronze"]
        print(f"Bronze: {bronze.get('row_count', 0)} rows")
        if bronze.get("target"):
            print(f"  Target: {bronze['target']}")

    # Silver results
    if "silver" in result:
        silver = result["silver"]
        print(f"Silver: {silver.get('row_count', 0)} rows")
        if silver.get("target"):
            print(f"  Target: {silver['target']}")

    # Single layer results
    if "row_count" in result and "bronze" not in result and "silver" not in result:
        print(f"Rows: {result['row_count']}")
        if result.get("target"):
            print(f"Target: {result['target']}")

    # Timing
    if "_elapsed_seconds" in result:
        print(f"Elapsed: {result['_elapsed_seconds']:.2f}s")

    print("=" * 60)


def main() -> None:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Run Bronze/Silver data pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # List available pipelines
    python -m pipelines --list

    # Run full pipeline (Bronze -> Silver)
    python -m pipelines claims.header --date 2025-01-15

    # Run only Bronze extraction
    python -m pipelines claims.header:bronze --date 2025-01-15

    # Run only Silver curation
    python -m pipelines claims.header:silver --date 2025-01-15

    # Validate configuration and connectivity
    python -m pipelines claims.header --date 2025-01-15 --check

    # Show what pipeline would do without executing
    python -m pipelines claims.header --date 2025-01-15 --explain

    # Dry run (validate without executing)
    python -m pipelines claims.header --date 2025-01-15 --dry-run

    # Local development (override target paths)
    python -m pipelines claims.header --date 2025-01-15 --target ./local_output/

    # Test database connection
    python -m pipelines test-connection claims_db --host myserver.com --database ClaimsDB
        """,
    )

    parser.add_argument(
        "pipeline",
        nargs="?",
        help="Pipeline name (e.g., claims.header, retail.orders:bronze)",
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        dest="list_pipelines",
        help="List available pipelines",
    )
    parser.add_argument(
        "--date",
        help="Run date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without executing",
    )
    parser.add_argument(
        "--target",
        dest="target_override",
        help="Override target path (for local development)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate pipeline configuration without running (checks connectivity)",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Show what the pipeline would do without executing",
    )

    args = parser.parse_args()

    # Handle --list flag
    if args.list_pipelines:
        list_pipelines()
        return

    # Handle test-connection command
    if args.pipeline and args.pipeline.startswith("test-connection"):
        parts = args.pipeline.split()
        if len(parts) < 2:
            print("Usage: python -m pipelines test-connection <connection_name>")
            print("  Options: --host, --database, --type (mssql|postgres)")
            sys.exit(1)
        connection_name = parts[1] if len(parts) > 1 else "default"
        test_connection_command(connection_name, args)
        return

    # Validate required arguments for running a pipeline
    if not args.pipeline:
        parser.error("Pipeline name is required (or use --list to see available pipelines)")

    if not args.date:
        parser.error("--date is required when running a pipeline")

    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger("pipelines")

    # Parse pipeline specification
    module_path, layer = parse_pipeline_spec(args.pipeline)

    logger.info(
        "Running pipeline: %s (layer=%s, date=%s)",
        module_path,
        layer or "all",
        args.date,
    )

    # Load and run pipeline
    try:
        module = load_pipeline_module(module_path)

        # Handle --explain: show what would run without executing
        if args.explain:
            explain_pipeline(module, layer, args.date)
            return

        # Handle --check: validate configuration and connectivity
        if args.check:
            check_pipeline(module, layer, args.date)
            return

        result = run_pipeline(
            module,
            layer,
            args.date,
            dry_run=args.dry_run,
            target_override=args.target_override,
        )

        print_result(result, args.pipeline)

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(130)

    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        print(f"\nError: {e}")
        sys.exit(1)

    finally:
        # Clean up connections
        close_all_connections()


if __name__ == "__main__":
    main()
