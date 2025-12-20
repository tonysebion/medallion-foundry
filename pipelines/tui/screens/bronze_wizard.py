"""Bronze configuration wizard screen."""

from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.containers import Horizontal, ScrollableContainer, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Static

from pipelines.tui.utils.schema_reader import get_enum_options, get_field_metadata
from pipelines.tui.widgets.validated_input import ValidatedInput
from pipelines.tui.widgets.enum_select import EnumSelect
from pipelines.tui.widgets.help_panel import HelpPanel, WizardProgress


class BronzeWizardScreen(Screen):
    """Step-by-step Bronze layer configuration.

    Guides users through configuring:
    - Basic info (system, entity, description)
    - Source type and source-specific options
    - Load pattern and incremental settings
    """

    BINDINGS = [
        ("escape", "go_back", "Back"),
        ("ctrl+n", "next_step", "Next"),
        ("ctrl+p", "prev_step", "Previous"),
    ]

    DEFAULT_CSS = """
    BronzeWizardScreen {
        layout: grid;
        grid-size: 3 1;
        grid-columns: 1fr 2fr 1fr;
    }

    BronzeWizardScreen .sidebar {
        height: 100%;
        padding: 1;
    }

    BronzeWizardScreen .main-content {
        height: 100%;
        border-left: solid $primary;
        border-right: solid $primary;
    }

    BronzeWizardScreen .form-container {
        padding: 1 2;
    }

    BronzeWizardScreen .section-title {
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
        border-bottom: solid $primary;
        padding-bottom: 1;
    }

    BronzeWizardScreen .button-row {
        height: auto;
        align: center middle;
        margin-top: 2;
        padding: 1;
        dock: bottom;
    }

    BronzeWizardScreen Button {
        margin: 0 1;
    }

    BronzeWizardScreen .conditional-section {
        margin-top: 1;
        padding: 1;
        border: round $secondary;
    }

    BronzeWizardScreen .conditional-section.hidden {
        display: none;
    }
    """

    def __init__(
        self,
        parent_path: str | None = None,
        existing_config: dict[str, Any] | None = None,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the bronze wizard.

        Args:
            parent_path: Path to parent config (for child pipelines)
            existing_config: Existing configuration (for edit mode)
            name: Screen name
            id: Screen ID
            classes: CSS classes
        """
        super().__init__(name=name, id=id, classes=classes)
        self.parent_path = parent_path
        self.existing_config = existing_config or {}
        self.form_data: dict[str, Any] = {}

        # Load parent config if specified
        if parent_path:
            from pipelines.tui.utils.inheritance import load_with_inheritance
            from pathlib import Path

            merged, parent = load_with_inheritance(Path(parent_path))
            self.parent_config = parent
            self.form_data = merged.get("bronze", {})
        else:
            self.parent_config = None

    def compose(self) -> ComposeResult:
        """Compose the bronze wizard layout."""
        yield Header()

        # Left sidebar - progress
        with Vertical(classes="sidebar"):
            yield WizardProgress(
                total_steps=4,
                step_names=["Basic Info", "Bronze Config", "Silver Config", "Review"],
            )
            yield Static("")  # Spacer

        # Main content area
        with Vertical(classes="main-content"):
            yield Static("Bronze Layer Configuration", classes="section-title")

            with ScrollableContainer(classes="form-container"):
                # Basic info section
                yield Static("[b]Basic Information[/b]")

                yield ValidatedInput(
                    field_name="name",
                    label="Pipeline Name",
                    required=True,
                    placeholder="my_pipeline",
                    help_text="A unique name for this pipeline",
                    default=self.form_data.get("name", ""),
                )

                yield ValidatedInput(
                    field_name="description",
                    label="Description",
                    required=False,
                    placeholder="Brief description of this pipeline",
                    default=self.form_data.get("description", ""),
                )

                yield ValidatedInput(
                    field_name="system",
                    label="System",
                    required=True,
                    placeholder="e.g., retail, crm, erp",
                    help_text=get_field_metadata("bronze", "system").get("description", ""),
                    default=self.form_data.get("system", ""),
                )

                yield ValidatedInput(
                    field_name="entity",
                    label="Entity",
                    required=True,
                    placeholder="e.g., orders, customers",
                    help_text=get_field_metadata("bronze", "entity").get("description", ""),
                    default=self.form_data.get("entity", ""),
                )

                # Source type section
                yield Static("\n[b]Source Configuration[/b]")

                yield EnumSelect(
                    field_name="source_type",
                    label="Source Type",
                    options=get_enum_options("bronze", "source_type"),
                    required=True,
                    help_text=get_field_metadata("bronze", "source_type").get("description", ""),
                    default=self.form_data.get("source_type", ""),
                )

                # Conditional: File source path
                with Vertical(classes="conditional-section", id="file_section"):
                    yield Static("[dim]File Source Options[/dim]")
                    yield ValidatedInput(
                        field_name="source_path",
                        label="Source Path",
                        required=False,
                        placeholder="./data/{entity}_{run_date}.csv",
                        help_text="Path to source file. Use {run_date} for date substitution.",
                        default=self.form_data.get("source_path", ""),
                    )

                # Conditional: Database connection
                with Vertical(classes="conditional-section hidden", id="database_section"):
                    yield Static("[dim]Database Connection[/dim]")
                    yield ValidatedInput(
                        field_name="host",
                        label="Host",
                        required=False,
                        placeholder="${DB_HOST}",
                        help_text="Database host or environment variable",
                        default=self.form_data.get("host", ""),
                    )
                    yield ValidatedInput(
                        field_name="database",
                        label="Database",
                        required=False,
                        placeholder="MyDatabase",
                        default=self.form_data.get("database", ""),
                    )
                    yield ValidatedInput(
                        field_name="query",
                        label="Custom Query",
                        required=False,
                        placeholder="SELECT * FROM table WHERE ...",
                        help_text="Optional custom SQL query",
                        default=self.form_data.get("query", ""),
                    )

                # Load pattern section
                yield Static("\n[b]Load Pattern[/b]")

                yield EnumSelect(
                    field_name="load_pattern",
                    label="Load Pattern",
                    options=get_enum_options("bronze", "load_pattern"),
                    required=False,
                    help_text=get_field_metadata("bronze", "load_pattern").get("description", ""),
                    default=self.form_data.get("load_pattern", "full_snapshot"),
                )

                # Conditional: Incremental options
                with Vertical(classes="conditional-section hidden", id="incremental_section"):
                    yield Static("[dim]Incremental Options[/dim]")
                    yield ValidatedInput(
                        field_name="watermark_column",
                        label="Watermark Column",
                        required=False,
                        placeholder="updated_at",
                        help_text="Column to track for incremental loads",
                        default=self.form_data.get("watermark_column", ""),
                    )
                    yield ValidatedInput(
                        field_name="full_refresh_days",
                        label="Full Refresh Days",
                        required=False,
                        placeholder="7",
                        help_text="Force full refresh every N days (optional)",
                        default=str(self.form_data.get("full_refresh_days", "")),
                    )

                # Advanced options
                yield Static("\n[b]Advanced Options[/b]")
                yield ValidatedInput(
                    field_name="chunk_size",
                    label="Chunk Size",
                    required=False,
                    placeholder="100000",
                    help_text="Rows per chunk for large datasets (optional)",
                    default=str(self.form_data.get("chunk_size", "")),
                )

            # Button row
            with Horizontal(classes="button-row"):
                yield Button("Cancel", id="btn_cancel", variant="default")
                yield Button("Next: Silver Config →", id="btn_next", variant="primary")

        # Right sidebar - help panel
        with Vertical(classes="sidebar"):
            yield HelpPanel(title="Field Help")

        yield Footer()

    def on_mount(self) -> None:
        """Handle screen mount."""
        # Set initial progress
        progress = self.query_one(WizardProgress)
        progress.set_step(2)  # Step 2: Bronze Config

        # Update conditional sections based on existing values
        if self.form_data.get("source_type"):
            self._update_source_sections(self.form_data["source_type"])
        if self.form_data.get("load_pattern"):
            self._update_load_sections(self.form_data["load_pattern"])

    def on_enum_select_changed(self, event: EnumSelect.Changed) -> None:
        """Handle enum selection changes."""
        if event.enum_select.field_name == "source_type":
            self._update_source_sections(event.value)
        elif event.enum_select.field_name == "load_pattern":
            self._update_load_sections(event.value)

    def _update_source_sections(self, source_type: str) -> None:
        """Show/hide source-specific sections based on source type."""
        file_section = self.query_one("#file_section")
        database_section = self.query_one("#database_section")

        if source_type.startswith("file_"):
            file_section.remove_class("hidden")
            database_section.add_class("hidden")
        elif source_type.startswith("database_"):
            file_section.add_class("hidden")
            database_section.remove_class("hidden")
        else:
            # API or other
            file_section.remove_class("hidden")
            database_section.add_class("hidden")

    def _update_load_sections(self, load_pattern: str) -> None:
        """Show/hide load pattern-specific sections."""
        incremental_section = self.query_one("#incremental_section")

        if load_pattern in ("incremental", "incremental_append", "cdc"):
            incremental_section.remove_class("hidden")
        else:
            incremental_section.add_class("hidden")

    def on_validated_input_changed(self, event: ValidatedInput.Changed) -> None:
        """Update help panel when input is focused."""
        help_panel = self.query_one(HelpPanel)
        help_panel.update_help("bronze", event.validated_input.field_name)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "btn_cancel":
            self.action_go_back()
        elif event.button.id == "btn_next":
            self._go_to_silver()

    def action_go_back(self) -> None:
        """Go back to previous screen."""
        self.app.pop_screen()

    def _collect_form_data(self) -> dict[str, Any]:
        """Collect all form data from widgets."""
        data: dict[str, Any] = {}

        # Collect from ValidatedInput widgets
        for widget in self.query(ValidatedInput):
            if widget.value:
                # Convert numeric fields
                if widget.field_name in ("chunk_size", "full_refresh_days"):
                    try:
                        data[widget.field_name] = int(widget.value)
                    except ValueError:
                        pass
                else:
                    data[widget.field_name] = widget.value

        # Collect from EnumSelect widgets
        for widget in self.query(EnumSelect):
            if widget.value:
                data[widget.field_name] = widget.value

        return data

    def _validate_form(self) -> list[str]:
        """Validate all form fields.

        Returns list of error messages.
        """
        errors: list[str] = []

        for widget in self.query(ValidatedInput):
            if not widget.validate():
                errors.append(widget.error_message)

        for widget in self.query(EnumSelect):
            if not widget.validate():
                errors.append(f"{widget.label_text} is required")

        return errors

    def _go_to_silver(self) -> None:
        """Validate and proceed to Silver configuration."""
        errors = self._validate_form()
        if errors:
            self.notify("\n".join(errors), severity="error")
            return

        # Collect form data
        form_data = self._collect_form_data()

        # Store in app
        self.app.bronze_config = form_data

        # Navigate to Silver wizard
        from pipelines.tui.screens.silver_wizard import SilverWizardScreen

        self.app.push_screen(SilverWizardScreen(bronze_config=form_data))
