"""CLI entrypoint for bronze-foundry.

This file wires together:

- Config loading and validation
- Extractor selection
- Bronze writing
- Storage backend upload
- Incremental cursor update
- Parallel execution (optional)

The implementations in `core` and `extractors`
are intentionally modular so they can be extended or customized.
"""

import sys
import argparse
import logging
import datetime as dt
from pathlib import Path
from typing import List

from core.config import load_config, build_relative_path
from core.runner import run_extract
from core.parallel import run_parallel_extracts
from core.logging_config import setup_logging
from core.storage import get_storage_backend

__version__ = "1.0.0"

logger = logging.getLogger(__name__)


def list_storage_backends() -> List[str]:
    """Return list of available storage backends."""
    return ["s3", "azure", "gcs", "local"]


def validate_configs(config_paths: List[str], dry_run: bool = False) -> int:
    """
    Validate configuration files.
    
    Args:
        config_paths: List of config file paths
        dry_run: If True, don't actually run extraction
    
    Returns:
        0 if all configs valid, 1 if any invalid
    """
    all_valid = True
    
    for config_path in config_paths:
        try:
            logger.info(f"Validating config: {config_path}")
            cfg = load_config(config_path)
            
            # Additional validation checks
            source = cfg["source"]
            logger.info(f"  ✓ System: {source['system']}, Table: {source['table']}, Type: {source.get('type', 'api')}")
            
            # Check storage backend configuration
            platform = cfg["platform"]
            storage_backend = platform["bronze"].get("storage_backend", "s3")
            logger.info(f"  ✓ Storage backend: {storage_backend}")
            
            # Test storage backend initialization (dry-run mode)
            if dry_run:
                try:
                    backend = get_storage_backend(platform)
                    logger.info(f"  ✓ Storage backend initialized: {backend.get_backend_type()}")
                except Exception as e:
                    logger.warning(f"  ⚠ Storage backend validation failed: {e}")
            
            logger.info(f"  ✓ Config valid: {config_path}")
            
        except Exception as e:
            logger.error(f"  ✗ Config invalid: {config_path}")
            logger.error(f"    Error: {e}")
            all_valid = False
    
    return 0 if all_valid else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bronze-Foundry: Config-driven data extraction framework",
        epilog="For detailed documentation, see: https://github.com/bronze-foundry/bronze-foundry"
    )
    
    # Core arguments
    parser.add_argument(
        "--config",
        help="Path to YAML config file(s). Use comma-separated list for multiple configs."
    )
    parser.add_argument(
        "--date",
        help="Logical run date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=1,
        help="Number of parallel workers for multi-config extraction (default: 1 = sequential)"
    )
    
    # New flags
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and connections without running extraction"
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration files (no connection tests)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG level) logging"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress all output except errors"
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"bronze-foundry {__version__}",
        help="Show version and exit"
    )
    parser.add_argument(
        "--list-backends",
        action="store_true",
        help="List available storage backends and exit"
    )
    parser.add_argument(
        "--log-format",
        choices=["human", "json", "simple"],
        default=None,
        help="Log format (default: human). Can also set via BRONZE_LOG_FORMAT env var"
    )
    
    args = parser.parse_args()
    
    # Handle simple info commands
    if args.list_backends:
        backends = list_storage_backends()
        print("Available storage backends:")
        for backend in backends:
            print(f"  - {backend}")
        print("\nNote: Azure, GCS require additional dependencies. See INSTALLATION.md")
        return 0
    
    # Configure logging based on flags
    log_level = logging.DEBUG if args.verbose else logging.ERROR if args.quiet else logging.INFO
    setup_logging(level=log_level, format_type=args.log_format, use_colors=True)
    
    # Require --config for extraction operations
    if not args.config:
        parser.error("--config is required (unless using --list-backends or --version)")
    
    # Parse config paths
    config_paths = [p.strip() for p in args.config.split(",")]
    
    # Handle validation-only mode
    if args.validate_only:
        logger.info("Running configuration validation (no connection tests)")
        return validate_configs(config_paths, dry_run=False)
    
    # Handle dry-run mode
    if args.dry_run:
        logger.info("Running in dry-run mode (validation + connection tests, no extraction)")
        return validate_configs(config_paths, dry_run=True)
    
    # Load all configs
    configs = []
    for config_path in config_paths:
        try:
            cfg = load_config(config_path)
            configs.append(cfg)
        except Exception as e:
            logger.error(f"Failed to load config {config_path}: {e}", exc_info=True)
            return 1
    
    if not configs:
        logger.error("No valid configs loaded")
        return 1

    # Determine run date
    if args.date:
        run_date = dt.date.fromisoformat(args.date)
    else:
        run_date = dt.date.today()

    # Get local output base from first config (or use default)
    local_output_base = Path(configs[0]["source"]["run"].get("local_output_dir", "./output"))

    # Run extraction(s)
    if len(configs) == 1:
        # Single config - run directly
        relative_path = build_relative_path(configs[0], run_date)
        return run_extract(configs[0], run_date, local_output_base, relative_path)
    else:
        # Multiple configs - run in parallel if workers > 1
        logger.info(f"Running {len(configs)} configs with {args.parallel_workers} workers")
        results = run_parallel_extracts(configs, run_date, local_output_base, args.parallel_workers)
        
        # Return non-zero if any failed
        failed = sum(1 for _, status, _ in results if status != 0)
        return 1 if failed > 0 else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        logger.error(f"Fatal error: {exc}", exc_info=True)
        sys.exit(1)
