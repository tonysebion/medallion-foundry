"""Consolidated pipeline editor screen.

This screen replaces the multi-screen wizard with a single, unified editor
for creating and editing pipeline YAML configurations.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Horizontal, ScrollableContainer, Vertical
from textual.screen import Screen
from textual.widgets import Button, Collapsible, Footer, Header, Static

from pipelines.tui.models import PipelineState, FieldSource
from pipelines.tui.utils.schema_reader import get_enum_options, get_field_help_text
from pipelines.tui.widgets import (
    EnumSelect,
    FileBrowserModal,
    EnvVarPickerModal,
    HelpPanel,
    InheritableInput,
    ListInput,
)


class PipelineEditorScreen(Screen):
    """Unified screen for creating and editing pipelines.

    Layout:
    - Top bar: Mode toggle (Beginner/Advanced), Add Parent, Save button
    - Left: Scrollable form with collapsible Bronze/Silver sections
    - Right: YAML preview + Help panel

    Features:
    - Single screen for all configuration
    - Dynamic field visibility based on source_type, load_pattern
    - Visual inheritance indicators (italic blue + badge)
    - Env var picker for sensitive fields
    - Beginner mode (essential fields) / Advanced mode (all fields)
    """

    BINDINGS = [
        ("ctrl+s", "save", "Save"),
        ("ctrl+b", "toggle_mode", "Toggle Mode"),
        ("escape", "maybe_exit", "Back"),
    ]

    DEFAULT_CSS = """
    PipelineEditorScreen {
        layout: grid;
        grid-size: 2 1;
        grid-columns: 3fr 2fr;
    }

    PipelineEditorScreen .main-panel {
        height: 100%;
    }

    PipelineEditorScreen .side-panel {
        height: 100%;
        border-left: solid $primary;
    }

    PipelineEditorScreen .mode-bar {
        dock: top;
        height: 3;
        background: $surface;
        padding: 0 1;
        border-bottom: solid $primary;
    }

    PipelineEditorScreen .mode-bar Button {
        margin-right: 1;
    }

    PipelineEditorScreen .mode-bar .spacer {
        width: 1fr;
    }

    PipelineEditorScreen .form-container {
        height: 1fr;
        padding: 1 2;
    }

    PipelineEditorScreen .section-title {
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
        margin-top: 1;
    }

    PipelineEditorScreen .yaml-preview {
        height: 1fr;
        border: tall $surface;
        padding: 1;
        background: $surface;
    }

    PipelineEditorScreen .preview-title {
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
    }

    PipelineEditorScreen .help-container {
        height: auto;
        max-height: 40%;
        border-top: solid $primary;
    }

    PipelineEditorScreen .hidden {
        display: none;
    }

    PipelineEditorScreen .advanced-only {
        /* Shown only in advanced mode */
    }

    PipelineEditorScreen .field-group {
        margin-bottom: 1;
        padding: 1;
        border: round $surface;
    }

    PipelineEditorScreen .field-group-title {
        color: $text-muted;
        text-style: italic;
        margin-bottom: 1;
    }
    """

    def __init__(
        self,
        state: PipelineState | None = None,
        yaml_path: Path | None = None,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the pipeline editor.

        Args:
            state: Existing pipeline state (for edit mode)
            yaml_path: Path to YAML file being edited
            name: Screen name
            id: Screen ID
            classes: CSS classes
        """
        super().__init__(name=name, id=id, classes=classes)
        self.state = state or PipelineState.from_schema_defaults()
        self.yaml_path = yaml_path or self.state.yaml_path

        # Track which env picker is active
        self._active_env_field: str | None = None

    def compose(self) -> ComposeResult:
        """Compose the editor layout."""
        yield Header()

        # Main panel (left side)
        with Vertical(classes="main-panel"):
            # Mode bar
            with Horizontal(classes="mode-bar"):
                yield Button(
                    "Beginner",
                    id="btn-beginner",
                    variant="primary" if self.state.view_mode == "beginner" else "default",
                )
                yield Button(
                    "Advanced",
                    id="btn-advanced",
                    variant="primary" if self.state.view_mode == "advanced" else "default",
                )
                yield Static("", classes="spacer")
                yield Button("Add Parent Config", id="btn-add-parent", variant="default")
                yield Button("Save", id="btn-save", variant="success")

            # Form container
            with ScrollableContainer(classes="form-container"):
                # Metadata section
                yield self._compose_metadata_section()

                # Bronze section
                yield self._compose_bronze_section()

                # Silver section
                yield self._compose_silver_section()

        # Side panel (right side)
        with Vertical(classes="side-panel"):
            yield Static("YAML Preview", classes="preview-title")
            yield Static("", id="yaml-preview", classes="yaml-preview")

            with Vertical(classes="help-container"):
                yield HelpPanel(title="Field Help")

        yield Footer()

    def _compose_metadata_section(self) -> ComposeResult:
        """Compose the metadata section."""
        yield Static("Pipeline Metadata", classes="section-title")

        yield InheritableInput(
            field_name="name",
            label="Pipeline Name",
            placeholder="my_pipeline",
            help_text="A unique name for this pipeline (optional, defaults to filename)",
            default=self.state.name,
            source=self._get_source("name"),
        )

        yield InheritableInput(
            field_name="description",
            label="Description",
            placeholder="Brief description of this pipeline",
            default=self.state.description,
            source=self._get_source("description"),
        )

        if self.state.extends:
            yield Static(
                f"Inheriting from: {self.state.extends}",
                classes="field-group-title",
            )

    def _compose_bronze_section(self) -> ComposeResult:
        """Compose the Bronze layer section."""
        with Collapsible(title="Bronze Layer (Data Extraction)", collapsed=False):
            # Required fields (always visible)
            yield Static("Required Fields", classes="field-group-title")

            yield self._bronze_field("system", required=True)
            yield self._bronze_field("entity", required=True)

            yield EnumSelect(
                field_name="source_type",
                label="Source Type",
                options=get_enum_options("bronze", "source_type"),
                required=True,
                help_text="Type of data source",
                default=self.state.get_bronze_value("source_type") or "",
            )

            # File source section
            with Vertical(id="file-section", classes="field-group"):
                yield Static("File Source Options", classes="field-group-title")
                yield self._bronze_field("source_path")

            # Database source section
            with Vertical(id="database-section", classes="field-group hidden"):
                yield Static("Database Connection", classes="field-group-title")
                yield self._bronze_field("host", is_sensitive=True)
                yield self._bronze_field("database")
                yield self._bronze_field("query")
                yield self._bronze_field("connection")

            # API source section (flat fields as requested)
            with Vertical(id="api-section", classes="field-group hidden"):
                yield Static("API Configuration", classes="field-group-title")
                yield self._bronze_field("base_url")
                yield self._bronze_field("endpoint")
                yield self._bronze_field("data_path")
                yield self._bronze_field("requests_per_second")
                yield self._bronze_field("timeout")
                yield self._bronze_field("max_retries")

                yield Static("Authentication", classes="field-group-title")
                yield EnumSelect(
                    field_name="auth_type",
                    label="Auth Type",
                    options=[
                        ("none", "None"),
                        ("bearer", "Bearer Token"),
                        ("api_key", "API Key"),
                        ("basic", "Basic Auth"),
                    ],
                    help_text="Authentication method for API",
                    default=self.state.get_bronze_value("auth_type") or "none",
                )

                # Auth fields (conditionally visible)
                with Vertical(id="auth-bearer", classes="hidden"):
                    yield self._bronze_field("token", is_sensitive=True)

                with Vertical(id="auth-apikey", classes="hidden"):
                    yield self._bronze_field("api_key", is_sensitive=True)
                    yield self._bronze_field("api_key_header")

                with Vertical(id="auth-basic", classes="hidden"):
                    yield self._bronze_field("username", is_sensitive=True)
                    yield self._bronze_field("password", is_sensitive=True, password=True)

                yield Static("Pagination", classes="field-group-title")
                yield EnumSelect(
                    field_name="pagination_strategy",
                    label="Pagination Strategy",
                    options=[
                        ("none", "None (single request)"),
                        ("offset", "Offset/Limit"),
                        ("page", "Page Number"),
                        ("cursor", "Cursor-based"),
                    ],
                    help_text="How to paginate through API results",
                    default=self.state.get_bronze_value("pagination_strategy") or "none",
                )

                # Pagination fields (conditionally visible)
                with Vertical(id="pagination-common", classes="hidden"):
                    yield self._bronze_field("page_size")
                    yield self._bronze_field("max_pages")
                    yield self._bronze_field("max_records")

                with Vertical(id="pagination-offset", classes="hidden"):
                    yield self._bronze_field("offset_param")
                    yield self._bronze_field("limit_param")

                with Vertical(id="pagination-page", classes="hidden"):
                    yield self._bronze_field("page_param")
                    yield self._bronze_field("page_size_param")

                with Vertical(id="pagination-cursor", classes="hidden"):
                    yield self._bronze_field("cursor_param")
                    yield self._bronze_field("cursor_path")

            # Load pattern section
            yield Static("Load Pattern", classes="section-title")
            yield EnumSelect(
                field_name="load_pattern",
                label="Load Pattern",
                options=get_enum_options("bronze", "load_pattern"),
                help_text="How to load data from source",
                default=self.state.get_bronze_value("load_pattern") or "full_snapshot",
            )

            with Vertical(id="incremental-section", classes="hidden"):
                yield self._bronze_field("watermark_column")
                yield self._bronze_field("watermark_param")
                yield self._bronze_field("full_refresh_days")

            # Advanced fields (only in advanced mode)
            with Vertical(id="bronze-advanced", classes="advanced-only hidden"):
                yield Static("Advanced Options", classes="section-title")
                yield self._bronze_field("target_path")
                yield self._bronze_field("chunk_size")
                yield self._bronze_field("partition_by")
                yield self._bronze_field("write_checksums")
                yield self._bronze_field("write_metadata")

    def _compose_silver_section(self) -> ComposeResult:
        """Compose the Silver layer section."""
        with Collapsible(title="Silver Layer (Data Curation)", collapsed=False):
            # Required fields
            yield Static("Required Fields", classes="field-group-title")

            yield ListInput(
                field_name="natural_keys",
                label="Natural Keys",
                placeholder="e.g., order_id, customer_id",
                help_text="Column(s) that uniquely identify a record",
                required=True,
                default=self._list_to_str(self.state.get_silver_value("natural_keys")),
            )

            yield self._silver_field("change_timestamp", required=True)

            # Entity configuration
            yield EnumSelect(
                field_name="entity_kind",
                label="Entity Kind",
                options=get_enum_options("silver", "entity_kind"),
                help_text="Type of entity (state = dimension, event = fact)",
                default=self.state.get_silver_value("entity_kind") or "state",
            )

            yield EnumSelect(
                field_name="history_mode",
                label="History Mode",
                options=get_enum_options("silver", "history_mode"),
                help_text="How to handle historical changes",
                default=self.state.get_silver_value("history_mode") or "current_only",
            )

            # Column selection
            yield Static("Column Selection", classes="section-title")
            yield ListInput(
                field_name="attributes",
                label="Include Columns",
                placeholder="e.g., name, email, status (leave empty for all)",
                help_text="Columns to include (cannot use with exclude)",
                default=self._list_to_str(self.state.get_silver_value("attributes")),
            )

            yield ListInput(
                field_name="exclude_columns",
                label="Exclude Columns",
                placeholder="e.g., internal_id, temp_field",
                help_text="Columns to exclude (cannot use with include)",
                default=self._list_to_str(self.state.get_silver_value("exclude_columns")),
            )

            # Advanced fields
            with Vertical(id="silver-advanced", classes="advanced-only hidden"):
                yield Static("Advanced Options", classes="section-title")
                yield self._silver_field("source_path")
                yield self._silver_field("target_path")
                yield ListInput(
                    field_name="partition_by",
                    label="Partition By",
                    placeholder="e.g., year, month",
                    default=self._list_to_str(self.state.get_silver_value("partition_by")),
                )
                yield EnumSelect(
                    field_name="parquet_compression",
                    label="Compression",
                    options=get_enum_options("silver", "parquet_compression"),
                    default=self.state.get_silver_value("parquet_compression") or "snappy",
                )
                yield EnumSelect(
                    field_name="validate_source",
                    label="Validate Source",
                    options=get_enum_options("silver", "validate_source"),
                    help_text="How to validate Bronze checksums",
                    default=self.state.get_silver_value("validate_source") or "skip",
                )

    def _bronze_field(
        self,
        field_name: str,
        required: bool = False,
        is_sensitive: bool = False,
        password: bool = False,
    ) -> InheritableInput:
        """Create a bronze field input."""
        field_val = self.state.bronze.get(field_name)
        value = field_val.value if field_val else ""
        source = field_val.source.value if field_val else "default"
        parent_value = field_val.parent_value if field_val else ""

        return InheritableInput(
            field_name=f"bronze_{field_name}",
            label=field_name.replace("_", " ").title(),
            help_text=get_field_help_text("bronze", field_name),
            default=str(value) if value else "",
            source=source,
            parent_value=str(parent_value) if parent_value else "",
            required=required,
            is_sensitive=is_sensitive,
            password=password,
        )

    def _silver_field(
        self,
        field_name: str,
        required: bool = False,
        is_sensitive: bool = False,
    ) -> InheritableInput:
        """Create a silver field input."""
        field_val = self.state.silver.get(field_name)
        value = field_val.value if field_val else ""
        source = field_val.source.value if field_val else "default"
        parent_value = field_val.parent_value if field_val else ""

        return InheritableInput(
            field_name=f"silver_{field_name}",
            label=field_name.replace("_", " ").title(),
            help_text=get_field_help_text("silver", field_name),
            default=str(value) if value else "",
            source=source,
            parent_value=str(parent_value) if parent_value else "",
            required=required,
            is_sensitive=is_sensitive,
        )

    def _get_source(self, field_name: str) -> str:
        """Get the source for a top-level field."""
        if self.state.extends and hasattr(self.state, "parent_config"):
            if self.state.parent_config and field_name in self.state.parent_config:
                return "parent"
        return "local" if getattr(self.state, field_name, None) else "default"

    def _list_to_str(self, value: Any) -> str:
        """Convert a list value to comma-separated string."""
        if isinstance(value, list):
            return ", ".join(str(v) for v in value)
        if value:
            return str(value)
        return ""

    def on_mount(self) -> None:
        """Initialize the editor when mounted."""
        self._update_yaml_preview()
        self._apply_view_mode()
        self._update_conditional_sections()

    def on_enum_select_changed(self, event: EnumSelect.Changed) -> None:
        """Handle enum selection changes."""
        field_name = event.enum_select.field_name

        if field_name == "source_type":
            self._update_source_sections(event.value)
        elif field_name == "load_pattern":
            self._update_load_sections(event.value)
        elif field_name == "auth_type":
            self._update_auth_sections(event.value)
        elif field_name == "pagination_strategy":
            self._update_pagination_sections(event.value)

        self._update_state_from_field(field_name, event.value)
        self._update_yaml_preview()

    def on_inheritable_input_changed(self, event: InheritableInput.Changed) -> None:
        """Handle input changes."""
        field_name = event.inheritable_input.field_name
        self._update_state_from_field(field_name, event.value)
        self._update_yaml_preview()

        # Update help panel
        help_panel = self.query_one(HelpPanel)
        section = "bronze" if field_name.startswith("bronze_") else "silver"
        actual_field = field_name.replace("bronze_", "").replace("silver_", "")
        help_panel.update_help(section, actual_field)

    def on_inheritable_input_env_var_requested(
        self, event: InheritableInput.EnvVarRequested
    ) -> None:
        """Handle env var button clicks."""
        self._active_env_field = event.inheritable_input.field_name
        env_files = self.state.discover_env_files()

        def on_select(reference: str) -> None:
            if self._active_env_field:
                try:
                    widget = self.query_one(
                        f"#{self._active_env_field}_input",
                        InheritableInput,
                    )
                    widget.insert_env_var(reference)
                except Exception:
                    pass
            self._active_env_field = None

        self.app.push_screen(
            EnvVarPickerModal(
                env_files=env_files,
                on_select=on_select,
            )
        )

    def on_list_input_changed(self, event: ListInput.Changed) -> None:
        """Handle list input changes."""
        field_name = event.list_input.field_name
        self._update_state_from_field(field_name, event.values)
        self._update_yaml_preview()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        button_id = event.button.id

        if button_id == "btn-beginner":
            self.state.view_mode = "beginner"
            self._apply_view_mode()
        elif button_id == "btn-advanced":
            self.state.view_mode = "advanced"
            self._apply_view_mode()
        elif button_id == "btn-add-parent":
            self._show_parent_browser()
        elif button_id == "btn-save":
            self._save_pipeline()

    def _update_source_sections(self, source_type: str) -> None:
        """Show/hide sections based on source_type."""
        file_section = self.query_one("#file-section")
        database_section = self.query_one("#database-section")
        api_section = self.query_one("#api-section")

        # Hide all
        file_section.add_class("hidden")
        database_section.add_class("hidden")
        api_section.add_class("hidden")

        # Show relevant
        if source_type.startswith("file_"):
            file_section.remove_class("hidden")
        elif source_type.startswith("database_"):
            database_section.remove_class("hidden")
        elif source_type == "api_rest":
            api_section.remove_class("hidden")

    def _update_load_sections(self, load_pattern: str) -> None:
        """Show/hide load pattern sections."""
        incremental_section = self.query_one("#incremental-section")

        if load_pattern in ("incremental", "incremental_append", "cdc"):
            incremental_section.remove_class("hidden")
        else:
            incremental_section.add_class("hidden")

    def _update_auth_sections(self, auth_type: str) -> None:
        """Show/hide auth sections."""
        bearer = self.query_one("#auth-bearer")
        apikey = self.query_one("#auth-apikey")
        basic = self.query_one("#auth-basic")

        bearer.add_class("hidden")
        apikey.add_class("hidden")
        basic.add_class("hidden")

        if auth_type == "bearer":
            bearer.remove_class("hidden")
        elif auth_type == "api_key":
            apikey.remove_class("hidden")
        elif auth_type == "basic":
            basic.remove_class("hidden")

    def _update_pagination_sections(self, strategy: str) -> None:
        """Show/hide pagination sections."""
        common = self.query_one("#pagination-common")
        offset = self.query_one("#pagination-offset")
        page = self.query_one("#pagination-page")
        cursor = self.query_one("#pagination-cursor")

        common.add_class("hidden")
        offset.add_class("hidden")
        page.add_class("hidden")
        cursor.add_class("hidden")

        if strategy != "none":
            common.remove_class("hidden")

            if strategy == "offset":
                offset.remove_class("hidden")
            elif strategy == "page":
                page.remove_class("hidden")
            elif strategy == "cursor":
                cursor.remove_class("hidden")

    def _update_conditional_sections(self) -> None:
        """Update all conditional sections based on current state."""
        source_type = self.state.get_bronze_value("source_type") or ""
        load_pattern = self.state.get_bronze_value("load_pattern") or "full_snapshot"
        auth_type = self.state.get_bronze_value("auth_type") or "none"
        pagination = self.state.get_bronze_value("pagination_strategy") or "none"

        self._update_source_sections(source_type)
        self._update_load_sections(load_pattern)
        self._update_auth_sections(auth_type)
        self._update_pagination_sections(pagination)

    def _apply_view_mode(self) -> None:
        """Apply beginner/advanced mode styling."""
        beginner_btn = self.query_one("#btn-beginner", Button)
        advanced_btn = self.query_one("#btn-advanced", Button)

        if self.state.view_mode == "beginner":
            beginner_btn.variant = "primary"
            advanced_btn.variant = "default"
            for elem in self.query(".advanced-only"):
                elem.add_class("hidden")
        else:
            beginner_btn.variant = "default"
            advanced_btn.variant = "primary"
            for elem in self.query(".advanced-only"):
                elem.remove_class("hidden")

    def _update_state_from_field(self, field_name: str, value: Any) -> None:
        """Update state from a field change."""
        if field_name.startswith("bronze_"):
            actual_name = field_name.replace("bronze_", "")
            self.state.set_bronze_value(actual_name, value)
        elif field_name.startswith("silver_"):
            actual_name = field_name.replace("silver_", "")
            self.state.set_silver_value(actual_name, value)
        elif field_name in ("name", "description"):
            setattr(self.state, field_name, value)
        else:
            # Handle enum selects without prefix
            if field_name in ("source_type", "load_pattern", "auth_type", "pagination_strategy"):
                self.state.set_bronze_value(field_name, value)
            elif field_name in ("entity_kind", "history_mode", "parquet_compression", "validate_source"):
                self.state.set_silver_value(field_name, value)

    def _update_yaml_preview(self) -> None:
        """Update the YAML preview panel."""
        try:
            yaml_content = self.state.to_yaml()
            preview = self.query_one("#yaml-preview", Static)
            preview.update(yaml_content)
        except Exception as e:
            preview = self.query_one("#yaml-preview", Static)
            preview.update(f"Error generating YAML: {e}")

    def _show_parent_browser(self) -> None:
        """Show file browser for parent config selection."""
        def on_select(path: Path) -> None:
            self.state.set_parent_config(path)
            self._update_yaml_preview()
            self.notify(f"Parent config loaded: {path.name}")

        self.app.push_screen(
            FileBrowserModal(
                extensions=[".yaml", ".yml"],
                title="Select Parent Configuration",
                on_select=on_select,
            )
        )

    def _save_pipeline(self) -> None:
        """Save the pipeline configuration."""
        # Validate first
        errors = self.state.validate()
        if errors:
            error_text = "\n".join(errors)
            self.notify(f"Validation errors:\n{error_text}", severity="error")
            return

        # Determine save path
        if self.yaml_path:
            save_path = self.yaml_path
        else:
            # Generate default path
            name = self.state.name or "new_pipeline"
            save_path = Path.cwd() / "pipelines" / f"{name}.yaml"

        # Ensure directory exists
        save_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the file
        try:
            yaml_content = self.state.to_yaml()
            save_path.write_text(yaml_content, encoding="utf-8")
            self.notify(f"Saved to: {save_path}", severity="information")
            self.yaml_path = save_path
            self.state.yaml_path = save_path
        except Exception as e:
            self.notify(f"Failed to save: {e}", severity="error")

    def action_save(self) -> None:
        """Save keyboard shortcut handler."""
        self._save_pipeline()

    def action_toggle_mode(self) -> None:
        """Toggle view mode keyboard shortcut handler."""
        if self.state.view_mode == "beginner":
            self.state.view_mode = "advanced"
        else:
            self.state.view_mode = "beginner"
        self._apply_view_mode()

    def action_maybe_exit(self) -> None:
        """Handle escape key - confirm before exiting if changes made."""
        # For now, just go back
        self.app.pop_screen()
