"""Context-sensitive help panel widget."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.reactive import reactive
from textual.widgets import Static

from pipelines.tui.utils.schema_reader import get_field_help_text


class HelpPanel(Vertical):
    """Panel showing contextual help for the focused field.

    Updates automatically when different form fields are focused.
    Displays field descriptions, examples, and defaults from JSON Schema.
    """

    DEFAULT_CSS = """
    HelpPanel {
        height: auto;
        min-height: 5;
        border: round $primary;
        padding: 1;
        margin: 1;
        background: $surface;
    }

    HelpPanel .help-title {
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
    }

    HelpPanel .help-content {
        color: $text;
    }

    HelpPanel .help-empty {
        color: $text-muted;
        text-style: italic;
    }
    """

    current_field: reactive[str] = reactive("")
    current_section: reactive[str] = reactive("")

    def __init__(
        self,
        *,
        title: str = "Help",
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the help panel.

        Args:
            title: Panel title
            id: Widget ID
            classes: CSS classes
        """
        super().__init__(id=id, classes=classes)
        self.title = title

    def compose(self) -> ComposeResult:
        """Compose the widget layout."""
        yield Static(f"[b]{self.title}[/b]", classes="help-title")
        yield Static(
            "Select a field to see help.",
            classes="help-empty",
            id="help_content",
        )

    def update_help(self, section: str, field: str) -> None:
        """Update help content for a specific field.

        Args:
            section: "bronze" or "silver"
            field: Field name
        """
        self.current_section = section
        self.current_field = field

        content_widget = self.query_one("#help_content", Static)

        if not field:
            content_widget.update("Select a field to see help.")
            content_widget.set_class(True, "help-empty")
            content_widget.set_class(False, "help-content")
            return

        help_text = get_field_help_text(section, field)

        if help_text:
            content_widget.update(help_text)
            content_widget.set_class(False, "help-empty")
            content_widget.set_class(True, "help-content")
        else:
            content_widget.update(f"No help available for {field}.")
            content_widget.set_class(True, "help-empty")
            content_widget.set_class(False, "help-content")

    def show_custom_help(self, title: str, content: str) -> None:
        """Show custom help content.

        Args:
            title: Help title
            content: Help content text
        """
        title_widget = self.query_one(".help-title", Static)
        title_widget.update(f"[b]{title}[/b]")

        content_widget = self.query_one("#help_content", Static)
        content_widget.update(content)
        content_widget.set_class(False, "help-empty")
        content_widget.set_class(True, "help-content")

    def clear(self) -> None:
        """Clear help content."""
        self.current_field = ""
        self.current_section = ""

        title_widget = self.query_one(".help-title", Static)
        title_widget.update(f"[b]{self.title}[/b]")

        content_widget = self.query_one("#help_content", Static)
        content_widget.update("Select a field to see help.")
        content_widget.set_class(True, "help-empty")
        content_widget.set_class(False, "help-content")


class WizardProgress(Static):
    """Progress indicator for multi-step wizard.

    Shows current step and total steps with visual progress.
    """

    DEFAULT_CSS = """
    WizardProgress {
        height: 3;
        padding: 0 1;
        background: $surface;
        border-bottom: solid $primary;
    }

    WizardProgress .progress-text {
        text-align: center;
        width: 100%;
    }

    WizardProgress .progress-bar {
        height: 1;
        background: $surface-darken-1;
    }

    WizardProgress .progress-fill {
        height: 1;
        background: $primary;
    }
    """

    current_step: reactive[int] = reactive(1)
    total_steps: reactive[int] = reactive(4)
    step_names: reactive[list[str]] = reactive(list, init=False)

    def __init__(
        self,
        total_steps: int = 4,
        step_names: list[str] | None = None,
        *,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the progress indicator.

        Args:
            total_steps: Total number of wizard steps
            step_names: Optional names for each step
            id: Widget ID
            classes: CSS classes
        """
        super().__init__(id=id, classes=classes)
        self.total_steps = total_steps
        self.step_names = step_names or [
            "Basic Info",
            "Bronze Config",
            "Silver Config",
            "Review",
        ]

    def render(self) -> str:
        """Render the progress indicator."""
        step_name = (
            self.step_names[self.current_step - 1]
            if self.current_step <= len(self.step_names)
            else ""
        )

        # Create progress bar
        width = 40
        filled = int((self.current_step / self.total_steps) * width)
        bar = "█" * filled + "░" * (width - filled)

        return (
            f"Step {self.current_step} of {self.total_steps}: {step_name}\n"
            f"[{bar}]"
        )

    def set_step(self, step: int) -> None:
        """Set the current step.

        Args:
            step: Step number (1-indexed)
        """
        self.current_step = max(1, min(step, self.total_steps))
        self.refresh()

    def next_step(self) -> None:
        """Advance to the next step."""
        if self.current_step < self.total_steps:
            self.current_step += 1
            self.refresh()

    def prev_step(self) -> None:
        """Go back to the previous step."""
        if self.current_step > 1:
            self.current_step -= 1
            self.refresh()
