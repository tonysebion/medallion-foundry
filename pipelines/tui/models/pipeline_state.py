"""Main pipeline state container for the TUI.

This class provides a UI-agnostic representation of pipeline configuration
that can be tested without Textual dependencies. It tracks field values,
their sources, and handles validation using the same runtime helpers as
pipeline execution.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pipelines.tui.models.field_value import FieldSource, FieldValue
from pipelines.tui.models.field_metadata import (
    ALL_BRONZE_FIELDS,
    ALL_SILVER_FIELDS,
    SENSITIVE_FIELDS,
    get_dynamic_required_fields,
    get_visible_fields,
)


@dataclass
class PipelineState:
    """UI-agnostic state for pipeline configuration.

    This class holds all the configuration state for a pipeline, tracks
    where each value came from (default, parent, or local), and provides
    methods for validation and serialization.

    Attributes:
        name: Pipeline name (optional, defaults to filename)
        description: Pipeline description
        extends: Path to parent config file (for inheritance)
        bronze: Dictionary of bronze field values
        silver: Dictionary of silver field values
        mode: "create" for new pipelines, "edit" for existing
        view_mode: "beginner" shows essential fields, "advanced" shows all
        yaml_path: Path to the YAML file being edited (if any)
        parent_config: Raw parent config dict (for reference)
        loaded_env_files: List of loaded .env file paths
        available_env_vars: Set of environment variable names available
    """

    name: str = ""
    description: str = ""
    extends: str | None = None

    bronze: dict[str, FieldValue] = field(default_factory=dict)
    silver: dict[str, FieldValue] = field(default_factory=dict)

    mode: Literal["create", "edit"] = "create"
    view_mode: Literal["beginner", "advanced"] = "beginner"
    yaml_path: Path | None = None

    parent_config: dict[str, Any] | None = None
    loaded_env_files: list[str] = field(default_factory=list)
    available_env_vars: set[str] = field(default_factory=set)

    @classmethod
    def from_schema_defaults(cls) -> "PipelineState":
        """Create a new state with all fields initialized from schema defaults.

        This is used when creating a new pipeline from scratch.
        """
        state = cls()

        # Initialize bronze fields
        for field_name in ALL_BRONZE_FIELDS:
            state.bronze[field_name] = FieldValue(
                name=field_name,
                value=_get_schema_default("bronze", field_name),
                source=FieldSource.DEFAULT,
                is_sensitive=field_name in SENSITIVE_FIELDS,
            )

        # Initialize silver fields
        for field_name in ALL_SILVER_FIELDS:
            state.silver[field_name] = FieldValue(
                name=field_name,
                value=_get_schema_default("silver", field_name),
                source=FieldSource.DEFAULT,
                is_sensitive=field_name in SENSITIVE_FIELDS,
            )

        # Populate available env vars from current environment
        state.available_env_vars = set(os.environ.keys())

        return state

    @classmethod
    def from_yaml(cls, path: Path) -> "PipelineState":
        """Load state from a YAML file with inheritance resolution.

        Args:
            path: Path to the YAML configuration file

        Returns:
            PipelineState populated from the YAML file
        """
        import yaml

        from pipelines.tui.utils.inheritance import (
            identify_field_sources,
            load_with_inheritance,
        )

        # Load the config with parent inheritance resolved
        merged_config, parent_config = load_with_inheritance(path)

        # Load the child config separately to identify field sources
        with open(path, encoding="utf-8") as f:
            child_config = yaml.safe_load(f) or {}

        # Identify which fields came from parent vs child
        if parent_config:
            field_sources = identify_field_sources(
                parent_config, child_config, merged_config
            )
        else:
            field_sources = {}

        state = cls()
        state.yaml_path = path
        state.mode = "edit"

        # Set top-level fields
        state.name = merged_config.get("name", "")
        state.description = merged_config.get("description", "")
        state.extends = merged_config.get("extends")
        state.parent_config = parent_config

        # Populate bronze fields
        bronze_config = merged_config.get("bronze", {})
        for field_name in ALL_BRONZE_FIELDS:
            value = _get_nested_value(bronze_config, field_name)
            source_key = f"bronze.{field_name}"

            if source_key in field_sources:
                source = (
                    FieldSource.PARENT
                    if field_sources[source_key] == "parent"
                    else FieldSource.LOCAL
                )
            elif value is not None and value != "":
                source = FieldSource.LOCAL
            else:
                source = FieldSource.DEFAULT

            parent_value = None
            if parent_config:
                parent_bronze = parent_config.get("bronze", {})
                parent_value = _get_nested_value(parent_bronze, field_name)

            state.bronze[field_name] = FieldValue(
                name=field_name,
                value=value if value is not None else "",
                source=source,
                is_sensitive=field_name in SENSITIVE_FIELDS,
                parent_value=parent_value,
            )

        # Populate silver fields
        silver_config = merged_config.get("silver", {})
        for field_name in ALL_SILVER_FIELDS:
            value = _get_nested_value(silver_config, field_name)
            source_key = f"silver.{field_name}"

            if source_key in field_sources:
                source = (
                    FieldSource.PARENT
                    if field_sources[source_key] == "parent"
                    else FieldSource.LOCAL
                )
            elif value is not None and value != "":
                source = FieldSource.LOCAL
            else:
                source = FieldSource.DEFAULT

            parent_value = None
            if parent_config:
                parent_silver = parent_config.get("silver", {})
                parent_value = _get_nested_value(parent_silver, field_name)

            state.silver[field_name] = FieldValue(
                name=field_name,
                value=value if value is not None else "",
                source=source,
                is_sensitive=field_name in SENSITIVE_FIELDS,
                parent_value=parent_value,
            )

        # Populate available env vars
        state.available_env_vars = set(os.environ.keys())

        return state

    def get_bronze_value(self, field_name: str) -> Any:
        """Get the current value of a bronze field."""
        if field_name in self.bronze:
            return self.bronze[field_name].value
        return None

    def get_silver_value(self, field_name: str) -> Any:
        """Get the current value of a silver field."""
        if field_name in self.silver:
            return self.silver[field_name].value
        return None

    def set_bronze_value(self, field_name: str, value: Any) -> None:
        """Set a bronze field value (marks it as local)."""
        if field_name not in self.bronze:
            self.bronze[field_name] = FieldValue(
                name=field_name,
                is_sensitive=field_name in SENSITIVE_FIELDS,
            )
        self.bronze[field_name].set_local_value(value)

    def set_silver_value(self, field_name: str, value: Any) -> None:
        """Set a silver field value (marks it as local)."""
        if field_name not in self.silver:
            self.silver[field_name] = FieldValue(
                name=field_name,
                is_sensitive=field_name in SENSITIVE_FIELDS,
            )
        self.silver[field_name].set_local_value(value)

    def get_required_fields(self, section: str) -> list[str]:
        """Get dynamically required fields based on current state.

        Args:
            section: "bronze" or "silver"

        Returns:
            List of field names that are currently required
        """
        required = get_dynamic_required_fields(self)
        return required.get(section, [])

    def get_visible_fields(self, section: str) -> list[str]:
        """Get fields visible in current view mode.

        Args:
            section: "bronze" or "silver"

        Returns:
            List of field names that should be shown
        """
        return get_visible_fields(section, self.view_mode, self)

    def validate(self) -> list[str]:
        """Validate all fields and return list of errors.

        Returns:
            List of validation error messages (empty if valid)
        """
        from pipelines.tui.utils.schema_reader import validate_field_type

        errors: list[str] = []

        # Get currently required fields
        bronze_required = self.get_required_fields("bronze")
        silver_required = self.get_required_fields("silver")

        # Validate bronze fields
        for field_name in bronze_required:
            field_val = self.bronze.get(field_name)
            if not field_val or not field_val.value:
                errors.append(f"Bronze: {field_name} is required")

        for field_name, field_val in self.bronze.items():
            if field_val.value:
                error = validate_field_type("bronze", field_name, field_val.value)
                if error:
                    errors.append(f"Bronze: {error}")

        # Validate silver fields
        for field_name in silver_required:
            field_val = self.silver.get(field_name)
            if not field_val or not field_val.value:
                errors.append(f"Silver: {field_name} is required")

        for field_name, field_val in self.silver.items():
            if field_val.value:
                error = validate_field_type("silver", field_name, field_val.value)
                if error:
                    errors.append(f"Silver: {error}")

        # Check mutually exclusive fields
        has_attributes = bool(self.get_silver_value("attributes"))
        has_exclude = bool(self.get_silver_value("exclude_columns"))
        if has_attributes and has_exclude:
            errors.append(
                "Silver: Cannot specify both 'attributes' and 'exclude_columns'"
            )

        return errors

    def to_config_dict(self) -> dict[str, Any]:
        """Convert state to a configuration dictionary.

        Only includes fields that have non-empty values.
        """
        config: dict[str, Any] = {}

        if self.name:
            config["name"] = self.name
        if self.description:
            config["description"] = self.description
        if self.extends:
            config["extends"] = self.extends

        # Bronze section
        bronze: dict[str, Any] = {}
        for field_name, field_val in self.bronze.items():
            if field_val.value not in (None, "", []):
                # Handle nested fields (auth.*, pagination.*)
                if "." in field_name:
                    parts = field_name.split(".", 1)
                    if parts[0] not in bronze:
                        bronze[parts[0]] = {}
                    bronze[parts[0]][parts[1]] = field_val.value
                else:
                    bronze[field_name] = field_val.value

        if bronze:
            config["bronze"] = bronze

        # Silver section
        silver: dict[str, Any] = {}
        for field_name, field_val in self.silver.items():
            if field_val.value not in (None, "", []):
                silver[field_name] = field_val.value

        if silver:
            config["silver"] = silver

        return config

    def to_yaml(self) -> str:
        """Generate YAML string from current state.

        Uses the existing yaml_generator to ensure consistent output format.
        """
        from pipelines.tui.utils.yaml_generator import generate_yaml

        config = self.to_config_dict()
        return generate_yaml(config)

    def load_env_file(self, path: str) -> set[str]:
        """Load environment variables from a .env file.

        Uses the same env loading as pipeline runtime.

        Args:
            path: Path to .env file

        Returns:
            Set of newly discovered variable names
        """
        from pipelines.lib.env import load_env_file

        # Load the file (adds to os.environ)
        load_env_file(path, override=False)

        # Track which file we loaded
        if path not in self.loaded_env_files:
            self.loaded_env_files.append(path)

        # Update available vars
        new_vars = set(os.environ.keys()) - self.available_env_vars
        self.available_env_vars = set(os.environ.keys())

        return new_vars

    def discover_env_files(self) -> list[Path]:
        """Discover .env files in common locations.

        Looks in:
        - Current working directory
        - Project root (if yaml_path is set)
        - environments/ directory

        Returns:
            List of discovered .env file paths
        """
        discovered: list[Path] = []

        # Check current directory
        cwd = Path.cwd()
        for pattern in [".env", "*.env", ".env.*"]:
            discovered.extend(cwd.glob(pattern))

        # Check project root if we have a yaml_path
        if self.yaml_path:
            project_root = self.yaml_path.parent
            while project_root.parent != project_root:
                # Look for common project markers
                if (project_root / ".git").exists() or (
                    project_root / "pyproject.toml"
                ).exists():
                    for pattern in [".env", "*.env"]:
                        discovered.extend(project_root.glob(pattern))
                    break
                project_root = project_root.parent

        # Check environments/ directory
        env_dirs = [
            cwd / "environments",
            Path(__file__).parent.parent.parent.parent / "environments",
        ]
        for env_dir in env_dirs:
            if env_dir.exists():
                discovered.extend(env_dir.glob("*.env"))
                discovered.extend(env_dir.glob(".env*"))

        # Deduplicate while preserving order
        seen: set[Path] = set()
        unique: list[Path] = []
        for p in discovered:
            resolved = p.resolve()
            if resolved not in seen and resolved.is_file():
                seen.add(resolved)
                unique.append(resolved)

        return unique

    def set_parent_config(self, path: Path) -> None:
        """Set a parent configuration for inheritance.

        Args:
            path: Path to parent YAML file
        """
        from pipelines.tui.utils.inheritance import load_with_inheritance

        self.extends = str(path)

        # Load parent config
        parent_merged, _ = load_with_inheritance(path)
        self.parent_config = parent_merged

        # Update field values with parent values
        parent_bronze = parent_merged.get("bronze", {})
        for field_name, value in _flatten_config(parent_bronze).items():
            if field_name in self.bronze:
                field_val = self.bronze[field_name]
                if field_val.source == FieldSource.DEFAULT:
                    field_val.value = value
                    field_val.source = FieldSource.PARENT
                field_val.parent_value = value
            else:
                self.bronze[field_name] = FieldValue(
                    name=field_name,
                    value=value,
                    source=FieldSource.PARENT,
                    parent_value=value,
                    is_sensitive=field_name in SENSITIVE_FIELDS,
                )

        parent_silver = parent_merged.get("silver", {})
        for field_name, value in _flatten_config(parent_silver).items():
            if field_name in self.silver:
                field_val = self.silver[field_name]
                if field_val.source == FieldSource.DEFAULT:
                    field_val.value = value
                    field_val.source = FieldSource.PARENT
                field_val.parent_value = value
            else:
                self.silver[field_name] = FieldValue(
                    name=field_name,
                    value=value,
                    source=FieldSource.PARENT,
                    parent_value=value,
                    is_sensitive=field_name in SENSITIVE_FIELDS,
                )


def _get_schema_default(section: str, field_name: str) -> Any:
    """Get the default value for a field from the schema."""
    from pipelines.tui.utils.schema_reader import get_field_metadata

    metadata = get_field_metadata(section, field_name)
    default = metadata.get("default")
    return default if default is not None else ""


def _get_nested_value(config: dict[str, Any], field_name: str) -> Any:
    """Get a value from config, handling nested fields like auth.token."""
    if "." in field_name:
        parts = field_name.split(".", 1)
        nested = config.get(parts[0], {})
        if isinstance(nested, dict):
            return nested.get(parts[1])
        return None

    # Handle fields that might be in nested structures
    # Map flat field names to their nested locations
    nested_mappings = {
        "auth_type": ("auth", "auth_type"),
        "token": ("auth", "token"),
        "api_key": ("auth", "api_key"),
        "api_key_header": ("auth", "api_key_header"),
        "username": ("auth", "username"),
        "password": ("auth", "password"),
        "pagination_strategy": ("pagination", "strategy"),
        "page_size": ("pagination", "page_size"),
        "offset_param": ("pagination", "offset_param"),
        "limit_param": ("pagination", "limit_param"),
        "page_param": ("pagination", "page_param"),
        "page_size_param": ("pagination", "page_size_param"),
        "cursor_param": ("pagination", "cursor_param"),
        "cursor_path": ("pagination", "cursor_path"),
        "max_pages": ("pagination", "max_pages"),
        "max_records": ("pagination", "max_records"),
    }

    if field_name in nested_mappings:
        parent_key, child_key = nested_mappings[field_name]
        nested = config.get(parent_key, {})
        if isinstance(nested, dict):
            return nested.get(child_key)

    return config.get(field_name)


def _flatten_config(config: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten nested config into flat field names."""
    result: dict[str, Any] = {}

    for key, value in config.items():
        if isinstance(value, dict) and key in ("auth", "pagination", "options"):
            for nested_key, nested_value in value.items():
                flat_key = f"{key}_{nested_key}" if key != "options" else nested_key
                result[flat_key] = nested_value
        else:
            result[key] = value

    return result
