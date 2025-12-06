#!/usr/bin/env python3
"""Validate that imports respect the layer hierarchy.

Layer Hierarchy (lower cannot import from higher):
  Layer 0: primitives/     - No core/ dependencies allowed
  Layer 1: infrastructure/ - Can only import from primitives/
  Layer 2: pipeline/       - Can import from primitives/, infrastructure/
  Layer 3: adapters/       - Can import from primitives/, infrastructure/, pipeline/
  Layer 4: orchestration/  - Can import from all above

Usage:
  python scripts/check_layer_imports.py
  python scripts/check_layer_imports.py --verbose
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple


LAYER_HIERARCHY: Dict[str, int] = {
    "primitives": 0,
    "infrastructure": 1,
    "pipeline": 2,
    "adapters": 3,
    "orchestration": 4,
}

LAYER_CAN_IMPORT: Dict[str, Set[str]] = {
    "primitives": set(),
    "infrastructure": {"primitives"},
    "pipeline": {"primitives", "infrastructure"},
    "adapters": {"primitives", "infrastructure", "pipeline"},
    "orchestration": {"primitives", "infrastructure", "pipeline", "adapters"},
}


def get_layer_from_path(file_path: Path) -> str | None:
    """Extract the layer name from a file path under core/."""
    parts = file_path.parts
    try:
        core_idx = parts.index("core")
        if core_idx + 1 < len(parts):
            layer = parts[core_idx + 1]
            if layer in LAYER_HIERARCHY:
                return layer
    except ValueError:
        pass
    return None


def get_layer_from_import(import_path: str) -> str | None:
    """Extract the layer name from an import path like 'core.primitives.foo'."""
    parts = import_path.split(".")
    if len(parts) >= 2 and parts[0] == "core":
        layer = parts[1]
        if layer in LAYER_HIERARCHY:
            return layer
    return None


def extract_imports(file_path: Path) -> List[str]:
    """Extract all 'from core...' imports from a Python file."""
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"  Warning: Could not parse {file_path}: {e}")
        return []

    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("core."):
                imports.append(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("core."):
                    imports.append(alias.name)
    return imports


def check_file(file_path: Path, verbose: bool = False) -> List[Tuple[str, str, str, str]]:
    """Check a single file for layer violations.

    Returns list of (file_path, from_layer, to_layer, import_path) tuples.
    """
    violations = []
    from_layer = get_layer_from_path(file_path)

    if from_layer is None:
        return violations

    allowed = LAYER_CAN_IMPORT.get(from_layer, set())
    imports = extract_imports(file_path)

    for import_path in imports:
        to_layer = get_layer_from_import(import_path)
        if to_layer is None:
            continue

        # Same layer is always OK
        if to_layer == from_layer:
            continue

        # Check if import is allowed
        if to_layer not in allowed:
            violations.append((str(file_path), from_layer, to_layer, import_path))

    return violations


def main() -> int:
    parser = argparse.ArgumentParser(description="Check layer import hierarchy")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    core_dir = Path(__file__).parent.parent / "core"
    if not core_dir.exists():
        print(f"Error: core directory not found at {core_dir}")
        return 1

    all_violations: List[Tuple[str, str, str, str]] = []
    files_checked = 0

    for py_file in core_dir.rglob("*.py"):
        if py_file.name.startswith("__"):
            continue
        files_checked += 1
        violations = check_file(py_file, verbose=args.verbose)
        all_violations.extend(violations)

    if args.verbose:
        print(f"Checked {files_checked} files")
        print()

    if all_violations:
        print("LAYER VIOLATIONS FOUND:")
        print("=" * 60)
        for file_path, from_layer, to_layer, import_path in all_violations:
            rel_path = Path(file_path).relative_to(core_dir.parent)
            print(f"  {rel_path}")
            print(f"    Layer {from_layer} (L{LAYER_HIERARCHY[from_layer]}) -> {to_layer} (L{LAYER_HIERARCHY[to_layer]})")
            print(f"    Import: {import_path}")
            print()
        print(f"Total violations: {len(all_violations)}")
        return 1
    else:
        print(f"OK - No layer violations found ({files_checked} files checked)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
