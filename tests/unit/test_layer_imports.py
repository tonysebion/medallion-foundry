"""Test that imports respect the layer hierarchy.

Layer Hierarchy (lower cannot import from higher):
  Layer 0: foundation/      - No core/ dependencies allowed
  Layer 1: platform/        - Can only import from foundation/
  Layer 2: infrastructure/  - Can import from foundation/, platform/
  Layer 3: domain/          - Can import from foundation/, platform/, infrastructure/
  Layer 4: orchestration/   - Can import from all above

This ensures architectural integrity and prevents circular dependencies.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pytest


LAYER_HIERARCHY: Dict[str, int] = {
    "foundation": 0,      # L0: Zero-dependency building blocks
    "platform": 1,        # L1: Cross-cutting platform services
    "infrastructure": 2,  # L2: Core infrastructure services
    "domain": 3,          # L3: Business domain logic
    "orchestration": 4,   # L4: Job execution
}

LAYER_CAN_IMPORT: Dict[str, Set[str]] = {
    "foundation": set(),
    "platform": {"foundation"},
    "infrastructure": {"foundation", "platform"},
    "domain": {"foundation", "platform", "infrastructure"},
    "orchestration": {"foundation", "platform", "infrastructure", "domain"},
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
    except (SyntaxError, UnicodeDecodeError):
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


def check_file(file_path: Path) -> List[Tuple[str, str, str, str]]:
    """Check a single file for layer violations.

    Returns list of (file_path, from_layer, to_layer, import_path) tuples.
    """
    violations: list[tuple[str, str, str, str]] = []
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


def collect_all_violations() -> List[Tuple[str, str, str, str]]:
    """Collect all layer violations across the core/ directory."""
    repo_root = Path(__file__).resolve().parents[2]
    core_dir = repo_root / "core"
    all_violations = []

    for py_file in core_dir.rglob("*.py"):
        if py_file.name.startswith("__"):
            continue
        violations = check_file(py_file)
        all_violations.extend(violations)

    return all_violations


class TestLayerImports:
    """Test suite for layer import hierarchy enforcement."""

    def test_no_layer_violations(self) -> None:
        """Verify no files violate the layer import hierarchy."""
        violations: list[tuple[str, str, str, str]] = collect_all_violations()

        if violations:
            msg_parts = ["Layer import violations found:\n"]
            for file_path, from_layer, to_layer, import_path in violations:
                rel_path = Path(file_path).relative_to(Path(__file__).parent.parent)
                msg_parts.append(
                    f"  {rel_path}: {from_layer} (L{LAYER_HIERARCHY[from_layer]}) "
                    f"-> {to_layer} (L{LAYER_HIERARCHY[to_layer]})\n"
                    f"    Import: {import_path}\n"
                )
            pytest.fail("".join(msg_parts))

    def test_layer_hierarchy_is_complete(self) -> None:
        """Verify the layer hierarchy defines all expected layers."""
        expected_layers = {"foundation", "platform", "infrastructure", "domain", "orchestration"}
        assert set(LAYER_HIERARCHY.keys()) == expected_layers

    def test_layer_can_import_is_consistent(self) -> None:
        """Verify LAYER_CAN_IMPORT is consistent with the hierarchy."""
        for layer, allowed in LAYER_CAN_IMPORT.items():
            layer_level = LAYER_HIERARCHY[layer]
            for allowed_layer in allowed:
                allowed_level = LAYER_HIERARCHY[allowed_layer]
                assert allowed_level < layer_level, (
                    f"{layer} (L{layer_level}) allows importing from "
                    f"{allowed_layer} (L{allowed_level}) which is not lower"
                )

    def test_core_directory_exists(self) -> None:
        """Verify the core directory exists for testing."""
        repo_root = Path(__file__).resolve().parents[2]
        core_dir = repo_root / "core"
        assert core_dir.exists(), f"Core directory not found at {core_dir}"
        assert core_dir.is_dir(), f"{core_dir} is not a directory"
