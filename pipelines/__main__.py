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
from pipelines.lib._path_utils import storage_path_exists


def setup_logging(
    verbose: bool = False,
    json_format: bool = False,
    log_file: Optional[str] = None,
) -> None:
    """Configure logging for pipeline execution.

    Args:
        verbose: Enable debug-level logging
        json_format: Use JSON output format (for log aggregation)
        log_file: Optional file path to write logs to
    """
    from pipelines.lib.observability import setup_logging as _setup_logging

    _setup_logging(verbose=verbose, json_format=json_format, log_file=log_file)


logger = logging.getLogger(__name__)


def discover_pipelines() -> List[Dict[str, Any]]:
    """Discover available pipelines in the pipelines directory.

    Returns:
        List of dicts with pipeline info: name, path, has_bronze, has_silver

    Note:
        Import errors are logged as warnings rather than silently swallowed,
        so developers can diagnose why a pipeline isn't appearing in --list.
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
        except ImportError as e:
            # Log import errors so developers know why a pipeline isn't listed
            logger.warning(
                "Failed to import pipeline '%s' from %s: %s",
                module_name,
                rel_path,
                e,
            )
        except Exception as e:
            # Log unexpected errors with more detail
            logger.warning(
                "Unexpected error loading pipeline '%s' from %s: %s (%s)",
                module_name,
                rel_path,
                e,
                type(e).__name__,
            )

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
    print("  python -m pipelines.create              # Launch interactive pipeline creator")
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
    return storage_path_exists(path)


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


def generate_sample_command(pipeline_name: str, rows: int = 100, output_dir: str = "./sample_data") -> None:
    """Generate sample data for testing a pipeline.

    Creates synthetic data matching the pipeline's expected schema.

    Usage:
        python -m pipelines generate-sample claims.header --rows 1000
    """
    import pandas as pd
    import random
    import string
    from datetime import datetime, timedelta

    print()
    print("=" * 60)
    print(f"GENERATING SAMPLE DATA: {pipeline_name}")
    print("=" * 60)
    print(f"Rows: {rows}")
    print(f"Output: {output_dir}")
    print()

    # Try to load the pipeline module to understand its schema
    module_path, _ = parse_pipeline_spec(pipeline_name)
    try:
        full_path = f"pipelines.{module_path.replace('.', '_')}"
        module = importlib.import_module(full_path)
        bronze = getattr(module, "bronze", None)
        if bronze:
            system = bronze.system
            entity = bronze.entity
        else:
            system = module_path.split(".")[0] if "." in module_path else module_path
            entity = module_path.split(".")[-1] if "." in module_path else "data"
    except ModuleNotFoundError:
        try:
            full_path_nested = f"pipelines.{module_path}"
            module = importlib.import_module(full_path_nested)
            bronze = getattr(module, "bronze", None)
            if bronze:
                system = bronze.system
                entity = bronze.entity
            else:
                system = module_path.split(".")[0] if "." in module_path else module_path
                entity = module_path.split(".")[-1] if "." in module_path else "data"
        except ModuleNotFoundError:
            # Pipeline doesn't exist yet - use name-based defaults
            print(f"Note: Pipeline '{pipeline_name}' not found, generating generic sample data.")
            print()
            system = module_path.split(".")[0] if "." in module_path else module_path
            entity = module_path.split(".")[-1] if "." in module_path else "data"

    # Generate synthetic data
    def random_string(length: int = 8) -> str:
        return "".join(random.choices(string.ascii_letters, k=length))

    def random_date(start_year: int = 2024) -> str:
        start = datetime(start_year, 1, 1)
        offset = random.randint(0, 365)
        return (start + timedelta(days=offset)).strftime("%Y-%m-%d")

    def random_timestamp() -> str:
        return f"{random_date()} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"

    # Create sample data
    data = {
        "id": list(range(1, rows + 1)),
        f"{entity}_name": [f"{entity.title()}_{random_string(6)}" for _ in range(rows)],
        "status": [random.choice(["active", "inactive", "pending"]) for _ in range(rows)],
        "amount": [round(random.uniform(10, 10000), 2) for _ in range(rows)],
        "created_at": [random_timestamp() for _ in range(rows)],
        "updated_at": [random_timestamp() for _ in range(rows)],
    }

    df = pd.DataFrame(data)

    # Write to output
    from pathlib import Path

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Write as CSV and Parquet
    csv_file = output_path / f"{system}_{entity}_sample.csv"
    parquet_file = output_path / f"{system}_{entity}_sample.parquet"

    df.to_csv(csv_file, index=False)
    df.to_parquet(parquet_file, index=False)

    print(f"Generated {rows} sample rows")
    print(f"CSV: {csv_file}")
    print(f"Parquet: {parquet_file}")
    print()
    print("Columns generated:")
    for col in df.columns:
        print(f"  - {col} ({df[col].dtype})")
    print()
    print("=" * 60)


def generate_all_samples_command() -> None:
    """Generate sample data for all examples.

    This runs the existing sample data generation scripts to create
    test data that the example pipelines can use.

    Usage:
        python -m pipelines generate-samples
    """
    import subprocess

    print()
    print("=" * 60)
    print("GENERATING SAMPLE DATA FOR EXAMPLES")
    print("=" * 60)
    print()

    scripts_dir = Path(__file__).parent.parent / "scripts"

    # Look for sample generation scripts
    bronze_script = scripts_dir / "generate_bronze_samples.py"
    silver_script = scripts_dir / "generate_silver_samples.py"

    scripts_found = []
    if bronze_script.exists():
        scripts_found.append(("Bronze samples", bronze_script))
    if silver_script.exists():
        scripts_found.append(("Silver samples", silver_script))

    if not scripts_found:
        print("No sample generation scripts found in scripts/ directory.")
        print()
        print("You can generate sample data manually with:")
        print("  python -m pipelines generate-sample <pipeline_name> --rows 100")
        print()
        return

    for name, script in scripts_found:
        print(f"Generating {name}...")
        try:
            result = subprocess.run(
                [sys.executable, str(script)],
                capture_output=True,
                text=True,
                cwd=str(Path(__file__).parent.parent),
            )
            if result.returncode == 0:
                print(f"  {name} generated successfully")
            else:
                print(f"  Warning: {name} generation had issues")
                if result.stderr:
                    for line in result.stderr.strip().split("\n")[:5]:
                        print(f"    {line}")
        except Exception as e:
            print(f"  Error running {script.name}: {e}")

    print()
    print("Sample data generation complete!")
    print()
    print("You can now run example pipelines:")
    print("  python -m pipelines examples.retail_orders --date 2025-01-15")
    print()
    print("=" * 60)


def new_pipeline_command(pipeline_name: str, source_type: str = "file_csv") -> None:
    """Create a new pipeline from template.

    Usage:
        python -m pipelines new claims.header --source-type database_mssql
    """
    from pathlib import Path

    print()
    print("=" * 60)
    print(f"CREATING NEW PIPELINE: {pipeline_name}")
    print("=" * 60)
    print(f"Source Type: {source_type}")
    print()

    # Parse pipeline name
    parts = pipeline_name.split(".")
    if len(parts) == 2:
        system, entity = parts
        filename = f"{system}_{entity}.py"
        dir_path = Path("pipelines")
    else:
        system = parts[0]
        entity = parts[-1]
        filename = f"{entity}.py"
        dir_path = Path("pipelines") / "/".join(parts[:-1])

    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / filename

    if file_path.exists():
        print(f"ERROR: Pipeline already exists at {file_path}")
        sys.exit(1)

    # Determine template based on source type
    source_type_map = {
        "file_csv": "SourceType.FILE_CSV",
        "file_parquet": "SourceType.FILE_PARQUET",
        "database_mssql": "SourceType.DATABASE_MSSQL",
        "database_postgres": "SourceType.DATABASE_POSTGRES",
        "api_rest": "SourceType.API_REST",
    }

    source_type_enum = source_type_map.get(source_type.lower(), "SourceType.FILE_CSV")

    # Generate template
    template = f'''"""Pipeline: {system}.{entity}

Bronze extraction and Silver curation for {entity} data.
"""

from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode


# =============================================================================
# BRONZE LAYER - Raw extraction
# =============================================================================

bronze = BronzeSource(
    system="{system}",
    entity="{entity}",
    source_type={source_type_enum},
    source_path="./source_data/{system}/{entity}.csv",  # <-- CHANGE THIS
    target_path="./bronze/system={{system}}/entity={{entity}}/dt={{run_date}}/",
    load_pattern=LoadPattern.FULL_SNAPSHOT,
    options={{
        # Add source-specific options here
    }},
)


# =============================================================================
# SILVER LAYER - Curation
# =============================================================================

silver = SilverEntity(
    source_path="./bronze/system={system}/entity={entity}/dt={{run_date}}/*.parquet",
    target_path="./silver/{entity}/",
    natural_keys=["id"],  # <-- CHANGE THIS: columns that uniquely identify a record
    change_timestamp="updated_at",  # <-- CHANGE THIS: when the record was last modified
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.CURRENT_ONLY,
)


# =============================================================================
# PIPELINE ENTRY POINT
# =============================================================================

def run(run_date: str, *, dry_run: bool = False, target_override: str = None):
    """Run the full Bronze -> Silver pipeline."""
    bronze_result = bronze.run(run_date, dry_run=dry_run, target_override=target_override)
    silver_result = silver.run(run_date, dry_run=dry_run, target_override=target_override)
    return {{"bronze": bronze_result, "silver": silver_result}}


def run_bronze(run_date: str, *, dry_run: bool = False, target_override: str = None):
    """Run only the Bronze layer."""
    return bronze.run(run_date, dry_run=dry_run, target_override=target_override)


def run_silver(run_date: str, *, dry_run: bool = False, target_override: str = None):
    """Run only the Silver layer."""
    return silver.run(run_date, dry_run=dry_run, target_override=target_override)
'''

    file_path.write_text(template)

    print(f"Created: {file_path}")
    print()
    print("Next steps:")
    print(f"  1. Edit {file_path}")
    print("  2. Update source_path to your data location")
    print("  3. Update natural_keys and change_timestamp")
    print(f"  4. Test: python -m pipelines {pipeline_name} --date 2025-01-15 --dry-run")
    print()
    print("=" * 60)


def inspect_source_command(source_path: str = None, file_type: str = None) -> None:
    """Inspect a data source and suggest pipeline configuration.

    Usage:
        python -m pipelines inspect-source --file ./data.csv
        python -m pipelines inspect-source --file ./data.parquet
    """
    import pandas as pd
    from pathlib import Path

    print()
    print("=" * 60)
    print("SOURCE INSPECTION")
    print("=" * 60)

    if not source_path:
        print("ERROR: --file is required")
        print("Usage: python -m pipelines inspect-source --file ./data.csv")
        sys.exit(1)

    path = Path(source_path)
    if not path.exists():
        print(f"ERROR: File not found: {source_path}")
        sys.exit(1)

    print(f"File: {source_path}")
    print()

    # Read the file
    try:
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path, nrows=1000)
        elif path.suffix.lower() in (".parquet", ".pq"):
            df = pd.read_parquet(path)
        elif path.suffix.lower() == ".json":
            df = pd.read_json(path, lines=True, nrows=1000)
        else:
            print(f"Unsupported file type: {path.suffix}")
            print("Supported: .csv, .parquet, .json")
            sys.exit(1)
    except Exception as e:
        print(f"ERROR reading file: {e}")
        sys.exit(1)

    print("SCHEMA:")
    print("-" * 40)
    for col in df.columns:
        dtype = df[col].dtype
        null_count = df[col].isna().sum()
        unique_count = df[col].nunique()
        print(f"  {col:<25} {str(dtype):<15} nulls={null_count:<5} unique={unique_count}")
    print()

    print("STATISTICS:")
    print("-" * 40)
    print(f"  Rows: {len(df)}")
    print(f"  Columns: {len(df.columns)}")
    print()

    # Suggest configuration
    print("SUGGESTED CONFIGURATION:")
    print("-" * 40)

    # Find potential primary keys
    pk_candidates = []
    for col in df.columns:
        if df[col].nunique() == len(df) and df[col].isna().sum() == 0:
            pk_candidates.append(col)

    if pk_candidates:
        print(f"  natural_keys = {pk_candidates[:2]}")
    else:
        print("  natural_keys = [???]  # No unique columns found")

    # Find potential timestamp columns
    timestamp_cols = []
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower() or "updated" in col.lower():
            timestamp_cols.append(col)
        elif df[col].dtype == "datetime64[ns]":
            timestamp_cols.append(col)

    if timestamp_cols:
        print(f"  change_timestamp = \"{timestamp_cols[0]}\"")
    else:
        print("  change_timestamp = \"???\"  # No timestamp column found")

    # Suggest source type
    source_type = {
        ".csv": "SourceType.FILE_CSV",
        ".parquet": "SourceType.FILE_PARQUET",
        ".pq": "SourceType.FILE_PARQUET",
        ".json": "# JSON not directly supported - convert to CSV/Parquet first",
    }.get(path.suffix.lower(), "SourceType.FILE_CSV")
    print(f"  source_type = {source_type}")

    print()
    print("=" * 60)


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


def print_welcome_message() -> None:
    """Print friendly welcome message when run with no arguments."""
    print()
    print("Bronze-Foundry: Medallion Architecture Pipeline Framework")
    print("=" * 58)
    print()
    print("Quick Start:")
    print("  python -m pipelines --list              List available pipelines")
    print("  python -m pipelines <name> --date YYYY-MM-DD    Run a pipeline")
    print("  python -m pipelines.create              Launch interactive wizard")
    print()
    print("Common Commands:")
    print("  python -m pipelines new <name> --source-type file_csv")
    print("                                          Create new pipeline from template")
    print("  python -m pipelines generate-samples    Generate sample data for examples")
    print("  python -m pipelines inspect-source --file <path>")
    print("                                          Analyze a data file")
    print("  python -m pipelines test-connection <name> --host <host>")
    print("                                          Test database connectivity")
    print()
    print("Options:")
    print("  --dry-run     Validate without executing")
    print("  --check       Pre-flight connectivity checks")
    print("  --explain     Show what pipeline would do")
    print("  --target      Override output path (local dev)")
    print()
    print("Try an Example:")
    print("  python -m pipelines generate-samples")
    print("  python -m pipelines examples.retail_orders --date 2025-01-15")
    print()
    print("Documentation: python -m pipelines --help")
    print()


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

    # Create a new pipeline from template
    python -m pipelines new claims.header --source-type database_mssql

    # Generate sample test data
    python -m pipelines generate-sample claims.header --rows 1000 --output ./test_data/

    # Inspect a data source and get configuration suggestions
    python -m pipelines inspect-source --file ./data/input.csv
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
        "--json-log",
        action="store_true",
        help="Output logs in JSON format (for log aggregation systems)",
    )
    parser.add_argument(
        "--log-file",
        help="Write logs to a file in addition to console",
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
    parser.add_argument(
        "--rows",
        type=int,
        default=100,
        help="Number of rows for generate-sample command (default: 100)",
    )
    parser.add_argument(
        "--output",
        default="./sample_data",
        help="Output directory for generate-sample command (default: ./sample_data)",
    )
    parser.add_argument(
        "--source-type",
        default="file_csv",
        help="Source type for new command (file_csv, file_parquet, database_mssql, database_postgres, api_rest)",
    )
    parser.add_argument(
        "--file",
        dest="inspect_file",
        help="File path for inspect-source command",
    )
    parser.add_argument(
        "extra_args",
        nargs="*",
        help=argparse.SUPPRESS,  # Hidden - for command arguments
    )

    args = parser.parse_args()

    # Handle --list flag
    if args.list_pipelines:
        list_pipelines()
        return

    # Handle special commands
    if args.pipeline:
        # Handle test-connection command
        if args.pipeline == "test-connection":
            # Get connection name from extra_args
            if not args.extra_args:
                print("Usage: python -m pipelines test-connection <connection_name>")
                print("  Options: --host, --database, --type (mssql|postgres)")
                sys.exit(1)
            connection_name = args.extra_args[0]
            test_connection_command(connection_name, args)
            return

        # Handle generate-sample command
        if args.pipeline == "generate-sample":
            if not args.extra_args:
                print("Usage: python -m pipelines generate-sample <pipeline_name>")
                print("  Options: --rows (default 100), --output (default ./sample_data)")
                sys.exit(1)
            pipeline_name = args.extra_args[0]
            generate_sample_command(pipeline_name, rows=args.rows, output_dir=args.output)
            return

        # Handle new command
        if args.pipeline == "new":
            if not args.extra_args:
                print("Usage: python -m pipelines new <pipeline_name>")
                print("  Options: --source-type (file_csv, file_parquet, database_mssql, database_postgres, api_rest)")
                sys.exit(1)
            pipeline_name = args.extra_args[0]
            new_pipeline_command(pipeline_name, source_type=args.source_type)
            return

        # Handle inspect-source command
        if args.pipeline == "inspect-source":
            if not args.inspect_file:
                print("Usage: python -m pipelines inspect-source --file <path>")
                print("  Supported formats: .csv, .parquet, .json")
                sys.exit(1)
            inspect_source_command(source_path=args.inspect_file)
            return

        # Handle generate-samples command (shortcut for generating all sample data)
        if args.pipeline == "generate-samples":
            generate_all_samples_command()
            return

    # Validate required arguments for running a pipeline
    if not args.pipeline:
        # Show friendly welcome message when run with no arguments
        print_welcome_message()
        return

    if not args.date:
        parser.error("--date is required when running a pipeline")

    # Setup logging
    setup_logging(
        verbose=args.verbose,
        json_format=args.json_log,
        log_file=args.log_file,
    )
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
