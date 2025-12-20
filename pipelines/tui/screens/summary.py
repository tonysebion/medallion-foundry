"""Summary and save screen."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Horizontal, ScrollableContainer, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Input, Static

from pipelines.tui.utils.yaml_generator import generate_yaml
from pipelines.tui.widgets.help_panel import WizardProgress


class SummaryScreen(Screen):
    """Final review screen before saving.

    Shows:
    - YAML preview of the generated configuration
    - Validation summary
    - Save path input
    - Save/Cancel buttons
    """

    BINDINGS = [
        ("escape", "go_back", "Back"),
        ("ctrl+s", "save", "Save"),
    ]

    DEFAULT_CSS = """
    SummaryScreen {
        layout: grid;
        grid-size: 2 1;
        grid-columns: 1fr 1fr;
    }

    SummaryScreen .left-panel {
        height: 100%;
        border-right: solid $primary;
    }

    SummaryScreen .right-panel {
        height: 100%;
    }

    SummaryScreen .panel-title {
        text-style: bold;
        color: $primary;
        padding: 1;
        border-bottom: solid $primary;
    }

    SummaryScreen .yaml-preview {
        padding: 1;
        background: $surface;
    }

    SummaryScreen .yaml-content {
        padding: 1;
    }

    SummaryScreen .config-summary {
        padding: 1;
    }

    SummaryScreen .summary-section {
        margin-bottom: 1;
        padding: 1;
        border: round $secondary;
    }

    SummaryScreen .summary-title {
        text-style: bold;
        margin-bottom: 1;
    }

    SummaryScreen .save-section {
        padding: 1;
        margin-top: 1;
        border-top: solid $primary;
    }

    SummaryScreen .save-label {
        margin-bottom: 1;
    }

    SummaryScreen Input {
        width: 100%;
        margin-bottom: 1;
    }

    SummaryScreen .button-row {
        height: auto;
        align: center middle;
        margin-top: 1;
    }

    SummaryScreen Button {
        margin: 0 1;
    }

    SummaryScreen .success-message {
        color: $success;
        padding: 1;
        border: round $success;
        margin: 1;
    }

    SummaryScreen .error-message {
        color: $error;
        padding: 1;
        border: round $error;
        margin: 1;
    }

    SummaryScreen .validation-ok {
        color: $success;
    }

    SummaryScreen .validation-error {
        color: $error;
    }
    """

    def __init__(
        self,
        bronze_config: dict[str, Any],
        silver_config: dict[str, Any],
        parent_path: str | None = None,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the summary screen.

        Args:
            bronze_config: Bronze configuration from wizard
            silver_config: Silver configuration from wizard
            parent_path: Path to parent config (for child pipelines)
            name: Screen name
            id: Screen ID
            classes: CSS classes
        """
        super().__init__(name=name, id=id, classes=classes)
        self.bronze_config = bronze_config
        self.silver_config = silver_config
        self.parent_path = parent_path

        # Build full config
        self.config = self._build_config()
        self.yaml_content = generate_yaml(
            self.config,
            parent_path=parent_path,
        )

    def _build_config(self) -> dict[str, Any]:
        """Build the full configuration dictionary."""
        config: dict[str, Any] = {}

        # Top-level fields
        if self.bronze_config.get("name"):
            config["name"] = self.bronze_config["name"]
        if self.bronze_config.get("description"):
            config["description"] = self.bronze_config["description"]

        # Bronze section (exclude top-level fields)
        bronze = {
            k: v
            for k, v in self.bronze_config.items()
            if k not in ("name", "description")
        }
        if bronze:
            config["bronze"] = bronze

        # Silver section
        if self.silver_config:
            config["silver"] = self.silver_config

        return config

    def compose(self) -> ComposeResult:
        """Compose the summary screen layout."""
        yield Header()

        # Left panel - YAML preview
        with Vertical(classes="left-panel"):
            yield Static("Generated YAML", classes="panel-title")
            with ScrollableContainer(classes="yaml-preview"):
                yield Static(
                    f"```yaml\n{self.yaml_content}\n```",
                    classes="yaml-content",
                    id="yaml_display",
                )

        # Right panel - Summary and save
        with Vertical(classes="right-panel"):
            yield WizardProgress(
                total_steps=4,
                step_names=["Basic Info", "Bronze Config", "Silver Config", "Review"],
            )

            with ScrollableContainer(classes="config-summary"):
                yield Static("Configuration Summary", classes="panel-title")

                # Bronze summary
                with Vertical(classes="summary-section"):
                    yield Static("[b]Bronze Layer[/b]", classes="summary-title")
                    yield Static(self._format_bronze_summary())

                # Silver summary
                with Vertical(classes="summary-section"):
                    yield Static("[b]Silver Layer[/b]", classes="summary-title")
                    yield Static(self._format_silver_summary())

                # Validation summary
                with Vertical(classes="summary-section"):
                    yield Static("[b]Validation[/b]", classes="summary-title")
                    yield Static(
                        self._format_validation_summary(),
                        id="validation_summary",
                    )

            # Save section
            with Vertical(classes="save-section"):
                yield Static("Save Configuration", classes="save-label")

                default_path = self._get_default_path()
                yield Input(
                    value=default_path,
                    placeholder="./pipelines/my_pipeline.yaml",
                    id="save_path_input",
                )

                yield Static("", id="save_message")

                with Horizontal(classes="button-row"):
                    yield Button("← Back", id="btn_back", variant="default")
                    yield Button("Save", id="btn_save", variant="primary")

        yield Footer()

    def on_mount(self) -> None:
        """Handle screen mount."""
        progress = self.query_one(WizardProgress)
        progress.set_step(4)

    def _format_bronze_summary(self) -> str:
        """Format bronze config summary."""
        lines = []
        bc = self.bronze_config

        lines.append(f"System: {bc.get('system', 'N/A')}")
        lines.append(f"Entity: {bc.get('entity', 'N/A')}")
        lines.append(f"Source Type: {bc.get('source_type', 'N/A')}")

        if bc.get("source_path"):
            lines.append(f"Source Path: {bc['source_path']}")
        if bc.get("host"):
            lines.append(f"Host: {bc['host']}")
        if bc.get("load_pattern"):
            lines.append(f"Load Pattern: {bc['load_pattern']}")
        if bc.get("watermark_column"):
            lines.append(f"Watermark: {bc['watermark_column']}")

        return "\n".join(lines)

    def _format_silver_summary(self) -> str:
        """Format silver config summary."""
        lines = []
        sc = self.silver_config

        keys = sc.get("natural_keys", [])
        if isinstance(keys, list):
            lines.append(f"Natural Keys: {', '.join(keys)}")
        else:
            lines.append(f"Natural Keys: {keys}")

        lines.append(f"Change Timestamp: {sc.get('change_timestamp', 'N/A')}")
        lines.append(f"Entity Kind: {sc.get('entity_kind', 'state')}")
        lines.append(f"History Mode: {sc.get('history_mode', 'current_only')}")

        if sc.get("attributes"):
            lines.append(f"Attributes: {', '.join(sc['attributes'])}")
        if sc.get("exclude_columns"):
            lines.append(f"Excluded: {', '.join(sc['exclude_columns'])}")

        return "\n".join(lines)

    def _format_validation_summary(self) -> str:
        """Format validation results."""
        errors = self._validate_config()

        if not errors:
            return "[green]✓ Configuration is valid[/green]"

        lines = ["[red]Validation errors:[/red]"]
        for error in errors:
            lines.append(f"  • {error}")
        return "\n".join(lines)

    def _validate_config(self) -> list[str]:
        """Validate the full configuration.

        Returns list of error messages.
        """
        errors: list[str] = []

        # Check required bronze fields
        bc = self.bronze_config
        if not bc.get("system"):
            errors.append("Bronze: system is required")
        if not bc.get("entity"):
            errors.append("Bronze: entity is required")
        if not bc.get("source_type"):
            errors.append("Bronze: source_type is required")

        # Check required silver fields
        sc = self.silver_config
        if not sc.get("natural_keys"):
            errors.append("Silver: natural_keys is required")
        if not sc.get("change_timestamp"):
            errors.append("Silver: change_timestamp is required")

        # Check mutually exclusive fields
        if sc.get("attributes") and sc.get("exclude_columns"):
            errors.append("Silver: cannot specify both attributes and exclude_columns")

        return errors

    def _get_default_path(self) -> str:
        """Generate default save path based on config."""
        name = self.bronze_config.get("name", "")
        if not name:
            system = self.bronze_config.get("system", "my")
            entity = self.bronze_config.get("entity", "pipeline")
            name = f"{system}_{entity}"

        return f"./pipelines/{name}.yaml"

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "btn_back":
            self.action_go_back()
        elif event.button.id == "btn_save":
            self._save_config()

    def action_go_back(self) -> None:
        """Go back to Silver wizard."""
        self.app.pop_screen()

    def _save_config(self) -> None:
        """Save the configuration to file."""
        # Validate first
        errors = self._validate_config()
        if errors:
            self._show_message("\n".join(errors), is_error=True)
            return

        # Get save path
        path_input = self.query_one("#save_path_input", Input)
        save_path = path_input.value.strip()

        if not save_path:
            self._show_message("Please enter a save path", is_error=True)
            return

        path = Path(save_path)

        # Add .yaml extension if missing
        if path.suffix.lower() not in (".yaml", ".yml"):
            path = path.with_suffix(".yaml")

        # Create parent directories
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            self._show_message(f"Cannot create directory: {e}", is_error=True)
            return

        # Write file
        try:
            path.write_text(self.yaml_content, encoding="utf-8")
            self._show_message(f"Saved to: {path}", is_error=False)

            # Show run instructions
            self.notify(
                f"Pipeline saved! Run with:\n"
                f"python -m pipelines {path} --date 2025-01-15",
                title="Success",
                severity="information",
            )
        except OSError as e:
            self._show_message(f"Error saving file: {e}", is_error=True)

    def _show_message(self, message: str, is_error: bool) -> None:
        """Display a status message."""
        msg_widget = self.query_one("#save_message", Static)
        if is_error:
            msg_widget.update(f"[red]{message}[/red]")
        else:
            msg_widget.update(f"[green]{message}[/green]")
