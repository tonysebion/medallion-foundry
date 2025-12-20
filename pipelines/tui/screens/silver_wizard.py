"""Silver configuration wizard screen."""

from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.containers import Horizontal, ScrollableContainer, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Static

from pipelines.tui.utils.schema_reader import get_enum_options, get_field_metadata
from pipelines.tui.widgets.validated_input import ValidatedInput
from pipelines.tui.widgets.enum_select import EnumSelect
from pipelines.tui.widgets.list_input import ListInput
from pipelines.tui.widgets.help_panel import HelpPanel, WizardProgress


class SilverWizardScreen(Screen):
    """Step-by-step Silver layer configuration.

    Guides users through configuring:
    - Natural keys and change timestamp
    - Entity kind and history mode
    - Attributes and output options
    """

    BINDINGS = [
        ("escape", "go_back", "Back"),
        ("ctrl+n", "next_step", "Next"),
        ("ctrl+p", "prev_step", "Previous"),
    ]

    DEFAULT_CSS = """
    SilverWizardScreen {
        layout: grid;
        grid-size: 3 1;
        grid-columns: 1fr 2fr 1fr;
    }

    SilverWizardScreen .sidebar {
        height: 100%;
        padding: 1;
    }

    SilverWizardScreen .main-content {
        height: 100%;
        border-left: solid $primary;
        border-right: solid $primary;
    }

    SilverWizardScreen .form-container {
        padding: 1 2;
    }

    SilverWizardScreen .section-title {
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
        border-bottom: solid $primary;
        padding-bottom: 1;
    }

    SilverWizardScreen .button-row {
        height: auto;
        align: center middle;
        margin-top: 2;
        padding: 1;
        dock: bottom;
    }

    SilverWizardScreen Button {
        margin: 0 1;
    }

    SilverWizardScreen .info-box {
        padding: 1;
        margin: 1 0;
        border: round $secondary;
        background: $surface;
    }

    SilverWizardScreen .history-section {
        margin-top: 1;
    }

    SilverWizardScreen .history-section.hidden {
        display: none;
    }
    """

    def __init__(
        self,
        bronze_config: dict[str, Any] | None = None,
        existing_config: dict[str, Any] | None = None,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the silver wizard.

        Args:
            bronze_config: Bronze configuration from previous step
            existing_config: Existing Silver configuration (for edit mode)
            name: Screen name
            id: Screen ID
            classes: CSS classes
        """
        super().__init__(name=name, id=id, classes=classes)
        self.bronze_config = bronze_config or {}
        self.existing_config = existing_config or {}

    def compose(self) -> ComposeResult:
        """Compose the silver wizard layout."""
        yield Header()

        # Left sidebar - progress
        with Vertical(classes="sidebar"):
            yield WizardProgress(
                total_steps=4,
                step_names=["Basic Info", "Bronze Config", "Silver Config", "Review"],
            )

            # Show bronze summary
            if self.bronze_config:
                yield Static("\n[b]Bronze Summary[/b]", classes="info-title")
                yield Static(
                    f"System: {self.bronze_config.get('system', 'N/A')}\n"
                    f"Entity: {self.bronze_config.get('entity', 'N/A')}\n"
                    f"Source: {self.bronze_config.get('source_type', 'N/A')}",
                    classes="info-box",
                )

        # Main content area
        with Vertical(classes="main-content"):
            yield Static("Silver Layer Configuration", classes="section-title")

            with ScrollableContainer(classes="form-container"):
                # Key fields section
                yield Static("[b]Key Configuration[/b]")

                yield ListInput(
                    field_name="natural_keys",
                    label="Natural Keys",
                    required=True,
                    min_items=1,
                    placeholder="id, customer_id",
                    help_text=get_field_metadata("silver", "natural_keys").get(
                        "description", "Column(s) that uniquely identify a record"
                    ),
                    default=self._get_list_default("natural_keys"),
                )

                yield ValidatedInput(
                    field_name="change_timestamp",
                    label="Change Timestamp",
                    required=True,
                    placeholder="updated_at",
                    help_text=get_field_metadata("silver", "change_timestamp").get(
                        "description", "Column containing when the record was last modified"
                    ),
                    default=self.existing_config.get("change_timestamp", ""),
                )

                # Entity type section
                yield Static("\n[b]Entity Type[/b]")

                yield EnumSelect(
                    field_name="entity_kind",
                    label="Entity Kind",
                    options=get_enum_options("silver", "entity_kind"),
                    required=False,
                    help_text=get_field_metadata("silver", "entity_kind").get(
                        "description", ""
                    ),
                    default=self.existing_config.get("entity_kind", "state"),
                )

                yield Static(
                    "[dim]State[/dim] = Dimension (slowly changing, e.g., customers)\n"
                    "[dim]Event[/dim] = Fact (immutable, e.g., orders, clicks)",
                    classes="info-box",
                )

                # History mode section
                yield Static("\n[b]History Mode[/b]")

                yield EnumSelect(
                    field_name="history_mode",
                    label="History Mode",
                    options=get_enum_options("silver", "history_mode"),
                    required=False,
                    help_text=get_field_metadata("silver", "history_mode").get(
                        "description", ""
                    ),
                    default=self.existing_config.get("history_mode", "current_only"),
                )

                with Vertical(classes="history-section", id="history_info"):
                    yield Static(
                        "[dim]Current Only (SCD1)[/dim] = Keep only the latest version\n"
                        "[dim]Full History (SCD2)[/dim] = Keep all versions with "
                        "effective_from, effective_to, is_current columns",
                        classes="info-box",
                    )

                # Attribute selection
                yield Static("\n[b]Attribute Selection[/b]")

                yield ListInput(
                    field_name="attributes",
                    label="Include Columns (optional)",
                    required=False,
                    placeholder="name, email, status",
                    help_text="Specific columns to include. Leave empty for all columns.",
                    default=self._get_list_default("attributes"),
                )

                yield ListInput(
                    field_name="exclude_columns",
                    label="Exclude Columns (optional)",
                    required=False,
                    placeholder="internal_id, temp_field",
                    help_text="Columns to exclude. Cannot be used with Include Columns.",
                    default=self._get_list_default("exclude_columns"),
                )

                # Output options
                yield Static("\n[b]Output Options[/b]")

                yield EnumSelect(
                    field_name="parquet_compression",
                    label="Compression",
                    options=get_enum_options("silver", "parquet_compression"),
                    required=False,
                    help_text="Parquet compression codec",
                    default=self.existing_config.get("parquet_compression", "snappy"),
                )

                yield EnumSelect(
                    field_name="validate_source",
                    label="Source Validation",
                    options=get_enum_options("silver", "validate_source"),
                    required=False,
                    help_text="How to validate Bronze source checksums",
                    default=self.existing_config.get("validate_source", "skip"),
                )

            # Button row
            with Horizontal(classes="button-row"):
                yield Button("← Back: Bronze", id="btn_back", variant="default")
                yield Button("Next: Review →", id="btn_next", variant="primary")

        # Right sidebar - help panel
        with Vertical(classes="sidebar"):
            yield HelpPanel(title="Field Help")

        yield Footer()

    def _get_list_default(self, field_name: str) -> list[str]:
        """Get default list value from existing config."""
        value = self.existing_config.get(field_name, [])
        if isinstance(value, str):
            return [value]
        return list(value) if value else []

    def on_mount(self) -> None:
        """Handle screen mount."""
        # Set progress to step 3
        progress = self.query_one(WizardProgress)
        progress.set_step(3)

    def on_validated_input_changed(self, event: ValidatedInput.Changed) -> None:
        """Update help panel when input changes."""
        help_panel = self.query_one(HelpPanel)
        help_panel.update_help("silver", event.validated_input.field_name)

    def on_list_input_changed(self, event: ListInput.Changed) -> None:
        """Update help panel when list input changes."""
        help_panel = self.query_one(HelpPanel)
        help_panel.update_help("silver", event.list_input.field_name)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "btn_back":
            self.action_go_back()
        elif event.button.id == "btn_next":
            self._go_to_summary()

    def action_go_back(self) -> None:
        """Go back to Bronze wizard."""
        self.app.pop_screen()

    def _collect_form_data(self) -> dict[str, Any]:
        """Collect all form data from widgets."""
        data: dict[str, Any] = {}

        # Collect from ValidatedInput widgets
        for widget in self.query(ValidatedInput):
            if widget.value:
                data[widget.field_name] = widget.value

        # Collect from EnumSelect widgets
        for widget in self.query(EnumSelect):
            if widget.value:
                data[widget.field_name] = widget.value

        # Collect from ListInput widgets
        for widget in self.query(ListInput):
            if widget.values:
                data[widget.field_name] = widget.values

        return data

    def _validate_form(self) -> list[str]:
        """Validate all form fields.

        Returns list of error messages.
        """
        errors: list[str] = []

        for widget in self.query(ValidatedInput):
            if not widget.validate():
                errors.append(widget.error_message)

        for widget in self.query(ListInput):
            if not widget.validate():
                errors.append(widget.error_message)

        # Check mutually exclusive fields
        form_data = self._collect_form_data()
        if form_data.get("attributes") and form_data.get("exclude_columns"):
            errors.append("Cannot specify both 'attributes' and 'exclude_columns'")

        return errors

    def _go_to_summary(self) -> None:
        """Validate and proceed to Summary screen."""
        errors = self._validate_form()
        if errors:
            self.notify("\n".join(errors), severity="error")
            return

        # Collect form data
        silver_config = self._collect_form_data()

        # Store in app
        self.app.silver_config = silver_config

        # Navigate to Summary screen
        from pipelines.tui.screens.summary import SummaryScreen

        self.app.push_screen(
            SummaryScreen(
                bronze_config=self.bronze_config,
                silver_config=silver_config,
            )
        )
