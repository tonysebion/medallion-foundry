"""Configuration migration utilities for medallion-foundry.

Helps migrate from dict-based configs to Pydantic-based typed configs.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

import yaml

from core.config.typed_models import RootConfig


def migrate_dict_to_typed(config_dict: Dict[str, Any]) -> RootConfig:
    """Convert dict-based config to typed Pydantic model.

    Args:
        config_dict: Dictionary configuration

    Returns:
        Typed RootConfig model

    Raises:
        ValidationError: If config doesn't match schema
    """
    return RootConfig.model_validate(config_dict)


def validate_config_file(config_path: Path) -> tuple[bool, list[str]]:
    """Validate a YAML config file against the Pydantic schema.

    Args:
        config_path: Path to YAML config file

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config_dict = yaml.safe_load(f)

        # Try to parse with Pydantic
        RootConfig.model_validate(config_dict)
        return True, []

    except Exception as exc:
        return False, [str(exc)]


def print_migration_report(config_path: Path) -> None:
    """Print a migration report for a config file.

    Args:
        config_path: Path to YAML config file
    """
    is_valid, errors = validate_config_file(config_path)

    print("\n" + "=" * 70)
    print(f"Config Migration Report: {config_path}")
    print("=" * 70 + "\n")

    if is_valid:
        print("✅ Config is valid and ready for typed Pydantic models!")
        print("\nNo migration needed - config already matches the schema.")
    else:
        print("❌ Config has validation errors:")
        for i, error in enumerate(errors, 1):
            print(f"\n  {i}. {error}")

        print("\n" + "=" * 70)
        print("Migration Recommendations:")
        print("=" * 70)
        print(
            """
1. Review the errors above
2. Update your config YAML to match the schema
3. Common issues:
   - Missing required fields (platform.bronze, source.system, source.table)
   - Incorrect field names (e.g., 'base_url' instead of 'url')
   - Wrong types (e.g., string instead of integer)
4. Run this script again to verify

For schema reference, see: docs/framework/reference/CONFIG_REFERENCE.md
"""
        )


def main() -> int:
    """CLI entrypoint for config migration."""
    if len(sys.argv) < 2:
        print("Usage: python -m core.config.migrate <config.yaml>")
        print("\nValidates a config file against the typed Pydantic schema.")
        return 1

    config_path = Path(sys.argv[1])

    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        return 1

    print_migration_report(config_path)

    is_valid, _ = validate_config_file(config_path)
    return 0 if is_valid else 1


if __name__ == "__main__":
    sys.exit(main())
