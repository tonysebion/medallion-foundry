"""Welcome screen for mode selection."""

from __future__ import annotations

from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Center, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Static

from pipelines.tui.models import PipelineState
from pipelines.tui.widgets import FileBrowserModal


class WelcomeScreen(Screen):
    """Initial screen for selecting operation mode.

    Simplified to two options:
    - Create new pipeline (opens editor with defaults)
    - Edit existing pipeline (opens file browser, then editor)

    Note: "Create Child Pipeline" is now available as "Add Parent Config"
    within the pipeline editor itself.
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("n", "new_pipeline", "New"),
        ("e", "edit_pipeline", "Edit"),
    ]

    DEFAULT_CSS = """
    WelcomeScreen {
        align: center middle;
    }

    WelcomeScreen .welcome-container {
        width: 70;
        height: auto;
        padding: 2 3;
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

    WelcomeScreen .tagline {
        text-align: center;
        color: $success;
        text-style: italic;
        margin-bottom: 2;
    }

    WelcomeScreen .button-container {
        align: center middle;
        height: auto;
    }

    WelcomeScreen Button {
        width: 50;
        margin: 1;
    }

    WelcomeScreen .description {
        text-align: center;
        color: $text-muted;
        margin-top: 2;
    }

    WelcomeScreen .features {
        margin-top: 2;
        padding: 1;
        border: round $secondary;
    }

    WelcomeScreen .features-title {
        text-style: bold;
        color: $secondary;
        margin-bottom: 1;
    }

    WelcomeScreen .feature-item {
        color: $text;
        margin-left: 2;
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
                yield Static(
                    "Making medallion data pipelines accessible to everyone",
                    classes="tagline",
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

                yield Static(
                    "Press [b]N[/b] for New, [b]E[/b] for Edit, [b]Q[/b] to Quit",
                    classes="description",
                )

                # Features highlight
                with Vertical(classes="features"):
                    yield Static("What's New:", classes="features-title")
                    yield Static("- Beginner/Advanced mode toggle", classes="feature-item")
                    yield Static("- Visual inheritance indicators", classes="feature-item")
                    yield Static("- Environment variable picker for secrets", classes="feature-item")
                    yield Static("- Full API configuration support", classes="feature-item")
                    yield Static("- Real-time YAML preview", classes="feature-item")

        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "btn_new":
            self.action_new_pipeline()
        elif event.button.id == "btn_edit":
            self.action_edit_pipeline()

    def action_new_pipeline(self) -> None:
        """Start creating a new pipeline.

        Opens the consolidated editor with default state.
        """
        from pipelines.tui.screens.pipeline_editor import PipelineEditorScreen

        state = PipelineState.from_schema_defaults()
        self.app.push_screen(PipelineEditorScreen(state=state))

    def action_edit_pipeline(self) -> None:
        """Open file browser to select existing pipeline.

        Uses the new FileBrowserModal for real filesystem navigation.
        """
        def on_file_selected(path: Path) -> None:
            from pipelines.tui.screens.pipeline_editor import PipelineEditorScreen

            try:
                state = PipelineState.from_yaml(path)
                self.app.push_screen(PipelineEditorScreen(state=state, yaml_path=path))
            except Exception as e:
                self.notify(f"Failed to load pipeline: {e}", severity="error")

        # Start from pipelines directory if it exists
        start_path = Path.cwd() / "pipelines"
        if not start_path.exists():
            start_path = Path.cwd()

        self.app.push_screen(
            FileBrowserModal(
                extensions=[".yaml", ".yml"],
                title="Select Pipeline Configuration",
                start_path=start_path,
                on_select=on_file_selected,
            )
        )

    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()
