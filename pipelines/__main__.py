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

    # Run full pipeline (Bronze → Silver)
    python -m pipelines claims.header --date 2025-01-15

    # Run only Bronze extraction
    python -m pipelines claims.header:bronze --date 2025-01-15

    # Run only Silver curation
    python -m pipelines claims.header:silver --date 2025-01-15

    # Dry run (validate without executing)
    python -m pipelines claims.header --date 2025-01-15 --dry-run

    # Local development (override target paths)
    python -m pipelines claims.header --date 2025-01-15 --target ./local_output/
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

    args = parser.parse_args()

    # Handle --list flag
    if args.list_pipelines:
        list_pipelines()
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
