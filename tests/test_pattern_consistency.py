"""Test pattern consistency across the codebase.

This module enforces consistent patterns across the bronze-foundry codebase:
- All enums should have normalize(), describe(), choices() methods
- Key dataclasses should have to_dict() and from_dict() methods
- All __init__.py files should have explicit __all__ exports

These tests ensure architectural consistency is maintained as the codebase evolves.
"""

from __future__ import annotations

import ast
import inspect
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Set, Type

import pytest


# =============================================================================
# Test: RichEnumMixin Pattern
# =============================================================================


class TestRichEnumPattern:
    """Ensure key enums follow the RichEnumMixin pattern."""

    REQUIRED_ENUMS = [
        # Primitives layer
        "core.foundation.primitives.patterns.LoadPattern",
        "core.foundation.primitives.models.SilverModel",
        "core.foundation.state.watermark.WatermarkType",
        # Config layer
        "core.config.models.root.StorageBackend",
        "core.config.models.root.SourceType",
        "core.config.models.root.DataClassification",
        "core.config.models.enums.EntityKind",
        "core.config.models.enums.HistoryMode",
        "core.config.models.enums.InputMode",
        "core.config.models.enums.DeleteMode",
        "core.config.models.enums.SchemaMode",
        "core.platform.resilience.late_data.LateDataMode",
        # Pipeline layer
        "core.runtime.metadata.Layer",
        "core.runtime.metadata.RunStatus",
        # Adapters layer
        "core.domain.adapters.schema.evolution.SchemaEvolutionMode",
        "core.domain.adapters.quality.rules.RuleLevel",
    ]

    def _import_class(self, dotted_path: str) -> Type:
        """Import a class from a dotted path."""
        module_path, class_name = dotted_path.rsplit(".", 1)
        module = __import__(module_path, fromlist=[class_name])
        return getattr(module, class_name)

    def test_enums_have_choices_method(self) -> None:
        """Every key enum should have a choices() classmethod."""
        for enum_path in self.REQUIRED_ENUMS:
            enum_cls = self._import_class(enum_path)
            assert hasattr(enum_cls, "choices"), (
                f"{enum_path} missing choices() classmethod"
            )
            choices = enum_cls.choices()
            assert isinstance(choices, list), (
                f"{enum_path}.choices() should return a list"
            )
            assert len(choices) > 0, (
                f"{enum_path}.choices() should not be empty"
            )

    def test_enums_have_normalize_method(self) -> None:
        """Every key enum should have a normalize() classmethod."""
        for enum_path in self.REQUIRED_ENUMS:
            enum_cls = self._import_class(enum_path)
            assert hasattr(enum_cls, "normalize"), (
                f"{enum_path} missing normalize() classmethod"
            )
            # Test that normalize works with a valid value
            choices = enum_cls.choices()
            result = enum_cls.normalize(choices[0])
            assert isinstance(result, enum_cls), (
                f"{enum_path}.normalize() should return an enum member"
            )

    def test_enums_have_describe_method(self) -> None:
        """Every key enum should have a describe() method."""
        for enum_path in self.REQUIRED_ENUMS:
            enum_cls = self._import_class(enum_path)
            assert hasattr(enum_cls, "describe"), (
                f"{enum_path} missing describe() method"
            )
            # Test that describe works on a member
            member = list(enum_cls)[0]
            description = member.describe()
            assert isinstance(description, str), (
                f"{enum_path}.describe() should return a string"
            )
            assert len(description) > 0, (
                f"{enum_path}.describe() should not be empty"
            )


# =============================================================================
# Test: Serializable Dataclass Pattern
# =============================================================================


class TestSerializableDataclassPattern:
    """Ensure key dataclasses have to_dict() and from_dict() methods."""

    # Dataclasses that should have to_dict() and from_dict() methods
    # Note: RunMetadata uses load_run_metadata() for specialized file loading
    REQUIRED_DATACLASSES = [
        # Primitives layer
        "core.foundation.state.watermark.Watermark",
        "core.foundation.state.manifest.FileEntry",
        "core.foundation.state.manifest.FileManifest",
        # Resilience layer
        "core.platform.resilience.retry.RetryPolicy",
        "core.platform.resilience.circuit_breaker.CircuitBreaker",
        "core.platform.resilience.late_data.LateDataConfig",
        # Pipeline layer
        "core.runtime.context.RunContext",
    ]

    def _import_class(self, dotted_path: str) -> Type:
        """Import a class from a dotted path."""
        module_path, class_name = dotted_path.rsplit(".", 1)
        module = __import__(module_path, fromlist=[class_name])
        return getattr(module, class_name)

    def test_dataclasses_have_to_dict(self) -> None:
        """Every key dataclass should have to_dict() method."""
        for dc_path in self.REQUIRED_DATACLASSES:
            dc_cls = self._import_class(dc_path)
            assert hasattr(dc_cls, "to_dict"), (
                f"{dc_path} missing to_dict() method"
            )

    def test_dataclasses_have_from_dict(self) -> None:
        """Every key dataclass should have from_dict() classmethod."""
        for dc_path in self.REQUIRED_DATACLASSES:
            dc_cls = self._import_class(dc_path)
            assert hasattr(dc_cls, "from_dict"), (
                f"{dc_path} missing from_dict() classmethod"
            )


# =============================================================================
# Test: Extractor Registry Pattern
# =============================================================================


class TestExtractorRegistryPattern:
    """Ensure extractors are properly registered."""

    def test_registry_has_expected_types(self) -> None:
        """The extractor registry should have all expected source types."""
        from core.infrastructure.io.extractors.base import EXTRACTOR_REGISTRY
        # Force import of extractor modules to trigger registration
        from core.domain.adapters.extractors import api_extractor  # noqa: F401
        from core.domain.adapters.extractors import db_extractor  # noqa: F401
        from core.domain.adapters.extractors import db_multi_extractor  # noqa: F401
        from core.domain.adapters.extractors import file_extractor  # noqa: F401

        expected_types = {"api", "db", "db_multi", "file"}
        registered_types = set(EXTRACTOR_REGISTRY.keys())

        assert expected_types <= registered_types, (
            f"Missing registered extractors: {expected_types - registered_types}"
        )

    def test_registered_extractors_subclass_base(self) -> None:
        """All registered extractors should subclass BaseExtractor."""
        from core.infrastructure.io.extractors.base import BaseExtractor, EXTRACTOR_REGISTRY
        # Force import of extractor modules
        from core.domain.adapters.extractors import api_extractor  # noqa: F401
        from core.domain.adapters.extractors import db_extractor  # noqa: F401
        from core.domain.adapters.extractors import db_multi_extractor  # noqa: F401
        from core.domain.adapters.extractors import file_extractor  # noqa: F401

        for source_type, extractor_cls in EXTRACTOR_REGISTRY.items():
            assert issubclass(extractor_cls, BaseExtractor), (
                f"Extractor for '{source_type}' does not subclass BaseExtractor"
            )


# =============================================================================
# Test: __init__.py Export Pattern
# =============================================================================


class TestInitExportPattern:
    """Ensure all __init__.py files have explicit __all__ exports."""

    CORE_PACKAGES = [
        "core/foundation",
        "core/foundation/primitives",
        "core/foundation/state",
        "core/foundation/catalog",
        "core/infrastructure",
        "core/infrastructure/resilience",
        "core/infrastructure/storage",
        "core/infrastructure/config",
        "core/pipeline",
        "core/pipeline/runtime",
        "core/pipeline/bronze",
        "core/pipeline/silver",
        "core/domain/adapters",
        "core/domain/adapters/extractors",
        "core/domain/adapters/schema",
        "core/domain/adapters/quality",
        "core/domain/adapters/polybase",
        "core/orchestration",
        "core/orchestration/runner",
    ]

    def test_init_files_have_explicit_all(self) -> None:
        """Every core __init__.py should have explicit __all__ list."""
        project_root = Path(__file__).parent.parent

        for package_path in self.CORE_PACKAGES:
            init_file = project_root / package_path / "__init__.py"
            if not init_file.exists():
                continue  # Skip packages that don't exist yet

            source = init_file.read_text(encoding="utf-8")
            try:
                tree = ast.parse(source, filename=str(init_file))
            except SyntaxError:
                pytest.fail(f"Syntax error in {init_file}")

            has_all = False
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id == "__all__":
                            has_all = True
                            break

            assert has_all, (
                f"{init_file} missing explicit __all__ export list"
            )

    def test_no_lazy_getattr_in_init(self) -> None:
        """No __init__.py should use __getattr__ for lazy loading."""
        project_root = Path(__file__).parent.parent

        for package_path in self.CORE_PACKAGES:
            init_file = project_root / package_path / "__init__.py"
            if not init_file.exists():
                continue

            source = init_file.read_text(encoding="utf-8")
            try:
                tree = ast.parse(source, filename=str(init_file))
            except SyntaxError:
                continue

            has_getattr = False
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == "__getattr__":
                    has_getattr = True
                    break

            assert not has_getattr, (
                f"{init_file} should not use __getattr__ for lazy loading. "
                "Use explicit imports and lazy imports within functions if needed."
            )
