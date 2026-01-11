#!/usr/bin/env python
"""Migration script to update YAML pipelines from old field names to new ones.

Renames:
  - natural_keys -> unique_columns (Silver)
  - change_timestamp -> last_updated_column (Silver)
  - watermark_column -> incremental_column (Bronze)

Usage:
    # Preview changes (no files modified)
    python scripts/migrate_pipelines.py ./pipelines --dry-run

    # Apply changes with backup
    python scripts/migrate_pipelines.py ./pipelines

    # Migrate a single file
    python scripts/migrate_pipelines.py ./my_pipeline.yaml
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple


# Field renames: (old_name, new_name)
RENAMES = [
    ("natural_keys", "unique_columns"),
    ("change_timestamp", "last_updated_column"),
    ("watermark_column", "incremental_column"),
]


def find_yaml_files(path: Path) -> List[Path]:
    """Find all YAML files in a directory (recursively) or return single file."""
    if path.is_file():
        if path.suffix in (".yaml", ".yml"):
            return [path]
        return []

    yaml_files = list(path.glob("**/*.yaml")) + list(path.glob("**/*.yml"))
    return sorted(yaml_files)


def migrate_content(content: str) -> Tuple[str, List[str]]:
    """Migrate field names in YAML content.

    Returns:
        Tuple of (migrated_content, list_of_changes_made)
    """
    changes = []
    new_content = content

    for old_name, new_name in RENAMES:
        # Match the field as a YAML key (with colon after)
        # Handles: "  natural_keys:" and "natural_keys:"
        pattern = rf"(\s*)({old_name})(\s*:)"
        replacement = rf"\1{new_name}\3"

        matches = re.findall(pattern, new_content)
        if matches:
            new_content = re.sub(pattern, replacement, new_content)
            changes.append(f"{old_name} -> {new_name}")

    return new_content, changes


def migrate_file(
    file_path: Path,
    dry_run: bool = False,
    create_backup: bool = True,
) -> Tuple[bool, List[str]]:
    """Migrate a single YAML file.

    Returns:
        Tuple of (was_modified, list_of_changes)
    """
    content = file_path.read_text(encoding="utf-8")
    new_content, changes = migrate_content(content)

    if not changes:
        return False, []

    if dry_run:
        return True, changes

    # Create backup
    if create_backup:
        backup_path = file_path.with_suffix(
            f".yaml.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        shutil.copy2(file_path, backup_path)

    # Write updated content
    file_path.write_text(new_content, encoding="utf-8")

    return True, changes


def main():
    parser = argparse.ArgumentParser(
        description="Migrate YAML pipeline configurations to new field names.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Preview changes (dry run)
    python scripts/migrate_pipelines.py ./pipelines --dry-run

    # Apply changes
    python scripts/migrate_pipelines.py ./pipelines

    # Migrate without backup
    python scripts/migrate_pipelines.py ./pipelines --no-backup
""",
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to YAML file or directory containing YAML files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying files",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Don't create backup files before modifying",
    )

    args = parser.parse_args()

    if not args.path.exists():
        print(f"Error: Path does not exist: {args.path}", file=sys.stderr)
        sys.exit(1)

    yaml_files = find_yaml_files(args.path)

    if not yaml_files:
        print(f"No YAML files found in: {args.path}")
        sys.exit(0)

    print(f"Found {len(yaml_files)} YAML file(s)")
    print()

    if args.dry_run:
        print("DRY RUN - No files will be modified")
        print("=" * 50)
        print()

    modified_count = 0

    for file_path in yaml_files:
        was_modified, changes = migrate_file(
            file_path,
            dry_run=args.dry_run,
            create_backup=not args.no_backup,
        )

        if was_modified:
            modified_count += 1
            rel_path = (
                file_path.relative_to(args.path.parent)
                if args.path.is_dir()
                else file_path.name
            )
            action = "Would update" if args.dry_run else "Updated"
            print(f"{action}: {rel_path}")
            for change in changes:
                print(f"  - {change}")
            print()

    # Summary
    print("=" * 50)
    if args.dry_run:
        print(f"Would modify {modified_count} of {len(yaml_files)} file(s)")
    else:
        print(f"Modified {modified_count} of {len(yaml_files)} file(s)")
        if modified_count > 0 and not args.no_backup:
            print("Backup files created with .bak.YYYYMMDD_HHMMSS extension")


if __name__ == "__main__":
    main()
