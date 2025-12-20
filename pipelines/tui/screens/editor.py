"""Editor screen for modifying existing YAML configurations."""

from __future__ import annotations

from pathlib import Path
from typing import Any


from textual.app import ComposeResult
from textual.containers import Horizontal, ScrollableContainer, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Static, TabbedContent, TabPane

from pipelines.tui.utils.inheritance import load_with_inheritance, identify_field_sources
from pipelines.tui.utils.yaml_generator import generate_yaml
from pipelines.tui.utils.schema_reader import get_enum_options, get_field_metadata
from pipelines.tui.widgets.validated_input import ValidatedInput
from pipelines.tui.widgets.enum_select import EnumSelect
from pipelines.tui.widgets.list_input import ListInput
from pipelines.tui.widgets.help_panel import HelpPanel


class EditorScreen(Screen):
    """Screen for editing existing YAML pipeline configurations.

    Features:
    - Load and parse existing YAML files
    - Support for parent-child inheritance (shows inherited values)
    - Tabbed interface for Bronze and Silver sections
    - Real-time YAML preview
    - Validation before save
    """

    BINDINGS = [
        ("escape", "go_back", "Back"),
        ("ctrl+s", "save", "Save"),
    ]

    DEFAULT_CSS = """
    EditorScreen {
        layout: grid;
        grid-size: 2 1;
        grid-columns: 2fr 1fr;
    }

    EditorScreen .main-panel {
        height: 100%;
    }

    EditorScreen .side-panel {
        height: 100%;
        border-left: solid $primary;
    }

    EditorScreen .header-bar {
        height: 3;
        padding: 1;
        background: $surface;
        border-bottom: solid $primary;
    }

    EditorScreen .file-path {
        color: $text-muted;
    }

    EditorScreen TabbedContent {
        height: 100%;
    }

    EditorScreen .tab-content {
        padding: 1;
    }

    EditorScreen .yaml-preview-title {
        text-style: bold;
        padding: 1;
        border-bottom: solid $primary;
    }

    EditorScreen .yaml-preview {
        padding: 1;
        background: $surface;
    }

    EditorScreen .button-row {
        height: auto;
        align: center middle;
        padding: 1;
        dock: bottom;
        border-top: solid $primary;
    }

    EditorScreen Button {
        margin: 0 1;
    }

    EditorScreen .inherited-badge {
        color: $text-muted;
        text-style: italic;
    }

    EditorScreen .save-status {
        padding: 1;
    }
    """

    def __init__(
        self,
        yaml_path: str,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the editor screen.

        Args:
            yaml_path: Path to the YAML file to edit
            name: Screen name
            id: Screen ID
            classes: CSS classes
        """
        super().__init__(name=name, id=id, classes=classes)
        self.yaml_path = Path(yaml_path)
        self.config: dict[str, Any] = {}
        self.parent_config: dict[str, Any] | None = None
        self.field_sources: dict[str, str] = {}

        self._load_config()

    def _load_config(self) -> None:
        """Load the configuration file."""
        try:
            self.config, self.parent_config = load_with_inheritance(self.yaml_path)

            if self.parent_config:
                # Identify which fields are inherited
                from pipelines.tui.utils.inheritance import deep_merge

                merged = deep_merge(self.parent_config, self.config)
                self.field_sources = identify_field_sources(
                    self.parent_config, self.config, merged
                )
        except Exception as e:
            self.config = {}
            self.notify(f"Error loading config: {e}", severity="error")

    def compose(self) -> ComposeResult:
        """Compose the editor layout."""
        yield Header()

        # Main editing panel
        with Vertical(classes="main-panel"):
            # File info header
            with Horizontal(classes="header-bar"):
                yield Static(f"Editing: [bold]{self.yaml_path.name}[/bold]")
                yield Static(f" ({self.yaml_path})", classes="file-path")

            # Tabbed content for Bronze/Silver
            with TabbedContent():
                with TabPane("Bronze", id="bronze-tab"):
                    with ScrollableContainer(classes="tab-content"):
                        yield from self._compose_bronze_fields()

                with TabPane("Silver", id="silver-tab"):
                    with ScrollableContainer(classes="tab-content"):
                        yield from self._compose_silver_fields()

                with TabPane("Metadata", id="metadata-tab"):
                    with ScrollableContainer(classes="tab-content"):
                        yield from self._compose_metadata_fields()

            # Button row
            with Horizontal(classes="button-row"):
                yield Button("Cancel", id="btn_cancel", variant="default")
                yield Button("Save", id="btn_save", variant="primary")
            yield Static("", classes="save-status", id="save_status")

        # Side panel - YAML preview and help
        with Vertical(classes="side-panel"):
            yield Static("YAML Preview", classes="yaml-preview-title")
            with ScrollableContainer(classes="yaml-preview"):
                yield Static(
                    self._generate_preview(),
                    id="yaml_preview_content",
                )

            yield HelpPanel(title="Field Help")

        yield Footer()

    def _compose_metadata_fields(self) -> ComposeResult:
        """Compose metadata fields (name, description)."""
        yield Static("[b]Pipeline Metadata[/b]")

        yield ValidatedInput(
            field_name="name",
            label="Pipeline Name",
            required=False,
            default=self.config.get("name", ""),
            help_text="Optional name for the pipeline",
        )

        yield ValidatedInput(
            field_name="description",
            label="Description",
            required=False,
            default=self.config.get("description", ""),
            help_text="Brief description of what this pipeline does",
        )

        if self.parent_config:
            yield Static(
                f"\n[dim]This pipeline extends: {self.config.get('extends', 'parent')}[/dim]",
                classes="inherited-badge",
            )

    def _compose_bronze_fields(self) -> ComposeResult:
        """Compose Bronze configuration fields."""
        bronze = self.config.get("bronze", {})

        yield Static("[b]Bronze Layer Configuration[/b]")

        if not bronze:
            yield Static("[dim]No Bronze configuration in this file.[/dim]")
            return

        # Basic fields
        yield ValidatedInput(
            field_name="bronze_system",
            label="System",
            required=True,
            default=bronze.get("system", ""),
            help_text=get_field_metadata("bronze", "system").get("description", ""),
        )

        yield ValidatedInput(
            field_name="bronze_entity",
            label="Entity",
            required=True,
            default=bronze.get("entity", ""),
            help_text=get_field_metadata("bronze", "entity").get("description", ""),
        )

        yield EnumSelect(
            field_name="bronze_source_type",
            label="Source Type",
            options=get_enum_options("bronze", "source_type"),
            required=True,
            default=bronze.get("source_type", ""),
        )

        yield ValidatedInput(
            field_name="bronze_source_path",
            label="Source Path",
            required=False,
            default=bronze.get("source_path", ""),
            help_text="Path to source file or connection string",
        )

        # Database fields
        yield ValidatedInput(
            field_name="bronze_host",
            label="Host",
            required=False,
            default=bronze.get("host", ""),
        )

        yield ValidatedInput(
            field_name="bronze_database",
            label="Database",
            required=False,
            default=bronze.get("database", ""),
        )

        # Load pattern
        yield EnumSelect(
            field_name="bronze_load_pattern",
            label="Load Pattern",
            options=get_enum_options("bronze", "load_pattern"),
            required=False,
            default=bronze.get("load_pattern", "full_snapshot"),
        )

        yield ValidatedInput(
            field_name="bronze_watermark_column",
            label="Watermark Column",
            required=False,
            default=bronze.get("watermark_column", ""),
            help_text="Column for incremental loads",
        )

    def _compose_silver_fields(self) -> ComposeResult:
        """Compose Silver configuration fields."""
        silver = self.config.get("silver", {})

        yield Static("[b]Silver Layer Configuration[/b]")

        if not silver:
            yield Static("[dim]No Silver configuration in this file.[/dim]")
            return

        # Key fields
        natural_keys = silver.get("natural_keys", [])
        if isinstance(natural_keys, str):
            natural_keys = [natural_keys]

        yield ListInput(
            field_name="silver_natural_keys",
            label="Natural Keys",
            required=True,
            default=natural_keys,
            help_text=get_field_metadata("silver", "natural_keys").get("description", ""),
        )

        yield ValidatedInput(
            field_name="silver_change_timestamp",
            label="Change Timestamp",
            required=True,
            default=silver.get("change_timestamp", ""),
            help_text=get_field_metadata("silver", "change_timestamp").get("description", ""),
        )

        # Entity type
        yield EnumSelect(
            field_name="silver_entity_kind",
            label="Entity Kind",
            options=get_enum_options("silver", "entity_kind"),
            required=False,
            default=silver.get("entity_kind", "state"),
        )

        yield EnumSelect(
            field_name="silver_history_mode",
            label="History Mode",
            options=get_enum_options("silver", "history_mode"),
            required=False,
            default=silver.get("history_mode", "current_only"),
        )

        # Attributes
        yield ListInput(
            field_name="silver_attributes",
            label="Attributes (optional)",
            required=False,
            default=silver.get("attributes", []),
            help_text="Columns to include (leave empty for all)",
        )

        yield ListInput(
            field_name="silver_exclude_columns",
            label="Exclude Columns (optional)",
            required=False,
            default=silver.get("exclude_columns", []),
            help_text="Columns to exclude",
        )

        # Output options
        yield EnumSelect(
            field_name="silver_parquet_compression",
            label="Compression",
            options=get_enum_options("silver", "parquet_compression"),
            required=False,
            default=silver.get("parquet_compression", "snappy"),
        )

        yield EnumSelect(
            field_name="silver_validate_source",
            label="Source Validation",
            options=get_enum_options("silver", "validate_source"),
            required=False,
            default=silver.get("validate_source", "skip"),
        )

    def _generate_preview(self) -> str:
        """Generate YAML preview from current form state."""
        try:
            yaml_content = generate_yaml(self.config, include_comments=True)
            return f"```yaml\n{yaml_content}\n```"
        except Exception as e:
            return f"Error generating preview: {e}"

    def _update_preview(self) -> None:
        """Update the YAML preview with current form values."""
        # Collect current form data
        new_config = self._collect_form_data()

        # Generate new preview
        try:
            yaml_content = generate_yaml(new_config, include_comments=True)
            preview = self.query_one("#yaml_preview_content", Static)
            preview.update(f"```yaml\n{yaml_content}\n```")
        except Exception:
            pass

    def _collect_form_data(self) -> dict[str, Any]:
        """Collect all form data into config structure."""
        config: dict[str, Any] = {}

        # Collect metadata
        for widget in self.query(ValidatedInput):
            if widget.field_name == "name" and widget.value:
                config["name"] = widget.value
            elif widget.field_name == "description" and widget.value:
                config["description"] = widget.value

        # Collect Bronze
        bronze: dict[str, Any] = {}
        for widget in self.query(ValidatedInput):
            if widget.field_name.startswith("bronze_"):
                key = widget.field_name.replace("bronze_", "")
                if widget.value:
                    bronze[key] = widget.value

        for widget in self.query(EnumSelect):
            if widget.field_name.startswith("bronze_"):
                key = widget.field_name.replace("bronze_", "")
                if widget.value:
                    bronze[key] = widget.value

        if bronze:
            config["bronze"] = bronze

        # Collect Silver
        silver: dict[str, Any] = {}
        for widget in self.query(ValidatedInput):
            if widget.field_name.startswith("silver_"):
                key = widget.field_name.replace("silver_", "")
                if widget.value:
                    silver[key] = widget.value

        for widget in self.query(EnumSelect):
            if widget.field_name.startswith("silver_"):
                key = widget.field_name.replace("silver_", "")
                if widget.value:
                    silver[key] = widget.value

        for widget in self.query(ListInput):
            if widget.field_name.startswith("silver_"):
                key = widget.field_name.replace("silver_", "")
                if widget.values:
                    silver[key] = widget.values

        if silver:
            config["silver"] = silver

        return config

    def on_validated_input_changed(self, event: ValidatedInput.Changed) -> None:
        """Handle input changes."""
        self._update_preview()

        # Update help panel
        help_panel = self.query_one(HelpPanel)
        field = event.validated_input.field_name
        if field.startswith("bronze_"):
            help_panel.update_help("bronze", field.replace("bronze_", ""))
        elif field.startswith("silver_"):
            help_panel.update_help("silver", field.replace("silver_", ""))

    def on_enum_select_changed(self, event: EnumSelect.Changed) -> None:
        """Handle select changes."""
        self._update_preview()

    def on_list_input_changed(self, event: ListInput.Changed) -> None:
        """Handle list input changes."""
        self._update_preview()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "btn_cancel":
            self.action_go_back()
        elif event.button.id == "btn_save":
            self._save_config()

    def action_go_back(self) -> None:
        """Go back to welcome screen."""
        self.app.pop_screen()

    def action_save(self) -> None:
        """Save the configuration."""
        self._save_config()

    def _save_config(self) -> None:
        """Save the configuration to file."""
        new_config = self._collect_form_data()

        # Preserve extends if it existed
        if "extends" in self.config:
            new_config["extends"] = self.config["extends"]

        try:
            yaml_content = generate_yaml(
                new_config,
                include_comments=True,
                parent_path=self.config.get("extends"),
            )
            self.yaml_path.write_text(yaml_content, encoding="utf-8")

            status = self.query_one("#save_status", Static)
            status.update(f"[green]Saved to {self.yaml_path}[/green]")

            self.notify(f"Saved to {self.yaml_path}", severity="information")
        except Exception as e:
            status = self.query_one("#save_status", Static)
            status.update(f"[red]Error: {e}[/red]")
            self.notify(f"Error saving: {e}", severity="error")
