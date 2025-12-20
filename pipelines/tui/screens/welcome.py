"""Welcome screen for mode selection."""

from __future__ import annotations

from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Center, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Static


class WelcomeScreen(Screen):
    """Initial screen for selecting operation mode.

    Options:
    - Create new pipeline
    - Edit existing pipeline
    - Create child pipeline (inheriting from parent)
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("n", "new_pipeline", "New"),
        ("e", "edit_pipeline", "Edit"),
        ("c", "child_pipeline", "Child"),
    ]

    DEFAULT_CSS = """
    WelcomeScreen {
        align: center middle;
    }

    WelcomeScreen .welcome-container {
        width: 60;
        height: auto;
        padding: 2;
        border: round $primary;
        background: $surface;
    }

    WelcomeScreen .title {
        text-align: center;
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
    }

    WelcomeScreen .subtitle {
        text-align: center;
        color: $text-muted;
        margin-bottom: 2;
    }

    WelcomeScreen .button-container {
        align: center middle;
        height: auto;
    }

    WelcomeScreen Button {
        width: 40;
        margin: 1;
    }

    WelcomeScreen .description {
        text-align: center;
        color: $text-muted;
        margin-top: 2;
    }
    """

    def compose(self) -> ComposeResult:
        """Compose the welcome screen layout."""
        yield Header()
        with Center():
            with Vertical(classes="welcome-container"):
                yield Static(
                    "Bronze-Foundry Pipeline Configurator",
                    classes="title",
                )
                yield Static(
                    "Create and edit pipeline YAML configurations",
                    classes="subtitle",
                )

                with Vertical(classes="button-container"):
                    yield Button(
                        "Create New Pipeline",
                        id="btn_new",
                        variant="primary",
                    )
                    yield Button(
                        "Edit Existing Pipeline",
                        id="btn_edit",
                        variant="default",
                    )
                    yield Button(
                        "Create Child Pipeline",
                        id="btn_child",
                        variant="default",
                    )

                yield Static(
                    "Press [b]N[/b] for New, [b]E[/b] for Edit, [b]C[/b] for Child, [b]Q[/b] to Quit",
                    classes="description",
                )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "btn_new":
            self.action_new_pipeline()
        elif event.button.id == "btn_edit":
            self.action_edit_pipeline()
        elif event.button.id == "btn_child":
            self.action_child_pipeline()

    def action_new_pipeline(self) -> None:
        """Start creating a new pipeline."""
        from pipelines.tui.screens.bronze_wizard import BronzeWizardScreen

        self.app.push_screen(BronzeWizardScreen())

    def action_edit_pipeline(self) -> None:
        """Open file picker to edit existing pipeline."""
        # For now, push to a file input screen
        # TODO: Implement proper file picker
        self.app.push_screen(FilePickerScreen(mode="edit"))

    def action_child_pipeline(self) -> None:
        """Open file picker to select parent for child pipeline."""
        self.app.push_screen(FilePickerScreen(mode="child"))

    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()


class FilePickerScreen(Screen):
    """Simple file path input screen.

    TODO: Replace with proper file browser widget.
    """

    BINDINGS = [
        ("escape", "go_back", "Back"),
        ("enter", "submit", "Submit"),
    ]

    DEFAULT_CSS = """
    FilePickerScreen {
        align: center middle;
    }

    FilePickerScreen .picker-container {
        width: 70;
        height: auto;
        padding: 2;
        border: round $primary;
        background: $surface;
    }

    FilePickerScreen .title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    FilePickerScreen .instruction {
        color: $text-muted;
        margin-bottom: 1;
    }

    FilePickerScreen Input {
        width: 100%;
        margin-bottom: 1;
    }

    FilePickerScreen .error {
        color: $error;
        margin-bottom: 1;
    }

    FilePickerScreen .button-row {
        height: auto;
        align: center middle;
    }

    FilePickerScreen Button {
        margin: 0 1;
    }
    """

    def __init__(
        self,
        mode: str = "edit",
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the file picker.

        Args:
            mode: "edit" for editing, "child" for creating child pipeline
            name: Screen name
            id: Screen ID
            classes: CSS classes
        """
        super().__init__(name=name, id=id, classes=classes)
        self.mode = mode

    def compose(self) -> ComposeResult:
        """Compose the file picker layout."""
        from textual.widgets import Input
        from textual.containers import Horizontal

        yield Header()
        with Center():
            with Vertical(classes="picker-container"):
                if self.mode == "edit":
                    yield Static("Edit Existing Pipeline", classes="title")
                    yield Static(
                        "Enter the path to the YAML configuration file:",
                        classes="instruction",
                    )
                else:
                    yield Static("Create Child Pipeline", classes="title")
                    yield Static(
                        "Enter the path to the parent YAML configuration file:",
                        classes="instruction",
                    )

                yield Input(
                    placeholder="./pipelines/my_pipeline.yaml",
                    id="file_path_input",
                )
                yield Static("", classes="error", id="error_message")

                with Horizontal(classes="button-row"):
                    yield Button("Cancel", id="btn_cancel", variant="default")
                    yield Button("Continue", id="btn_continue", variant="primary")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "btn_cancel":
            self.action_go_back()
        elif event.button.id == "btn_continue":
            self.action_submit()

    def action_go_back(self) -> None:
        """Go back to welcome screen."""
        self.app.pop_screen()

    def action_submit(self) -> None:
        """Submit the file path."""
        from textual.widgets import Input

        input_widget = self.query_one("#file_path_input", Input)
        file_path = input_widget.value.strip()

        if not file_path:
            self._show_error("Please enter a file path")
            return

        path = Path(file_path)
        if not path.exists():
            self._show_error(f"File not found: {file_path}")
            return

        if path.suffix.lower() not in (".yaml", ".yml"):
            self._show_error("File must be a YAML file (.yaml or .yml)")
            return

        if self.mode == "edit":
            from pipelines.tui.screens.editor import EditorScreen

            self.app.switch_screen(EditorScreen(yaml_path=str(path)))
        else:
            # Child mode - go to bronze wizard with parent
            from pipelines.tui.screens.bronze_wizard import BronzeWizardScreen

            self.app.switch_screen(BronzeWizardScreen(parent_path=str(path)))

    def _show_error(self, message: str) -> None:
        """Display an error message."""
        error_widget = self.query_one("#error_message", Static)
        error_widget.update(message)
