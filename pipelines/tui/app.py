"""Main Textual application for pipeline configuration.

This is the entry point for the TUI. It manages screen navigation
and holds shared application state.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual.app import App
from textual.binding import Binding

from pipelines.tui.screens.welcome import WelcomeScreen


class PipelineConfigApp(App):
    """Textual TUI for creating and editing pipeline YAML configs.

    Features:
    - Multi-step wizard for creating new pipelines
    - Edit mode for modifying existing YAML files
    - Parent-child inheritance support
    - Real-time validation and contextual help
    """

    TITLE = "Bronze-Foundry Pipeline Configurator"
    SUB_TITLE = "Create and edit pipeline YAML configurations"
    CSS_PATH = "styles.tcss"

    BINDINGS = [
        Binding("q", "quit", "Quit", show=True),
        Binding("escape", "back", "Back", show=False),
        Binding("f1", "help", "Help", show=True),
        Binding("ctrl+s", "save", "Save", show=False),
    ]

    def __init__(
        self,
        mode: str = "create",
        yaml_path: str | None = None,
        parent_path: str | None = None,
    ) -> None:
        """Initialize the application.

        Args:
            mode: "create" for new pipeline, "edit" for existing
            yaml_path: Path to YAML file (for edit mode)
            parent_path: Path to parent config (for child pipelines)
        """
        super().__init__()
        self.mode = mode
        self.yaml_path = yaml_path
        self.parent_path = parent_path

        # Shared state for wizard
        self.bronze_config: dict[str, Any] = {}
        self.silver_config: dict[str, Any] = {}

    def on_mount(self) -> None:
        """Handle app mount - show initial screen."""
        from pipelines.tui.models import PipelineState
        from pipelines.tui.screens.pipeline_editor import PipelineEditorScreen

        if self.mode == "edit" and self.yaml_path:
            # Go directly to editor with loaded state
            state = PipelineState.from_yaml(Path(self.yaml_path))
            self.push_screen(PipelineEditorScreen(state=state, yaml_path=Path(self.yaml_path)))
        elif self.mode == "create" and self.parent_path:
            # Create new pipeline with parent inheritance
            state = PipelineState.from_schema_defaults()
            state.set_parent_config(Path(self.parent_path))
            self.push_screen(PipelineEditorScreen(state=state))
        else:
            # Show welcome screen
            self.push_screen(WelcomeScreen())

    def action_back(self) -> None:
        """Go back to previous screen."""
        if len(self.screen_stack) > 1:
            self.pop_screen()

    def action_help(self) -> None:
        """Show help information."""
        self.notify(
            "Keyboard shortcuts:\n"
            "  Q - Quit\n"
            "  Escape - Go back\n"
            "  Tab - Next field\n"
            "  Shift+Tab - Previous field\n"
            "  Enter - Submit/Continue\n"
            "  Ctrl+S - Save (in summary screen)",
            title="Help",
            severity="information",
        )

    def action_quit(self) -> None:
        """Quit the application."""
        self.exit()


def run_create_wizard(parent_path: str | None = None) -> None:
    """Run the TUI wizard for creating a new pipeline.

    Args:
        parent_path: Optional path to parent config for child pipeline
    """
    app = PipelineConfigApp(mode="create", parent_path=parent_path)
    app.run()


def run_editor(yaml_path: str) -> None:
    """Run the TUI editor for an existing pipeline.

    Args:
        yaml_path: Path to the YAML configuration file
    """
    if not Path(yaml_path).exists():
        raise FileNotFoundError(f"Configuration file not found: {yaml_path}")

    app = PipelineConfigApp(mode="edit", yaml_path=yaml_path)
    app.run()


def main() -> None:
    """Main entry point for the TUI application."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Bronze-Foundry Pipeline Configuration TUI",
    )
    parser.add_argument(
        "yaml_path",
        nargs="?",
        help="Path to YAML file to edit",
    )
    parser.add_argument(
        "--extends",
        "-e",
        help="Parent config path for creating child pipeline",
    )
    args = parser.parse_args()

    if args.yaml_path:
        run_editor(args.yaml_path)
    elif args.extends:
        run_create_wizard(parent_path=args.extends)
    else:
        run_create_wizard()


if __name__ == "__main__":
    main()
