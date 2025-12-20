"""Input widget with real-time validation and help text."""

from __future__ import annotations

from typing import Callable

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Input, Label, Static


class ValidatedInput(Vertical):
    """Input field with label, validation, and error display.

    Provides real-time validation feedback as the user types.

    Attributes:
        value: Current input value
        is_valid: Whether current value passes validation
        error_message: Current validation error (empty if valid)
    """

    DEFAULT_CSS = """
    ValidatedInput {
        height: auto;
        margin-bottom: 1;
    }

    ValidatedInput .field-label {
        color: $text;
        margin-bottom: 0;
    }

    ValidatedInput Input {
        width: 100%;
    }

    ValidatedInput Input.-invalid {
        border: tall $error;
    }

    ValidatedInput .error-text {
        color: $error;
        height: 1;
        margin-top: 0;
    }

    ValidatedInput .help-text {
        color: $text-muted;
        height: auto;
        margin-top: 0;
    }
    """

    value: reactive[str] = reactive("")
    is_valid: reactive[bool] = reactive(True)
    error_message: reactive[str] = reactive("")

    class Changed(Message):
        """Posted when the input value changes."""

        def __init__(self, validated_input: "ValidatedInput", value: str) -> None:
            super().__init__()
            self.validated_input = validated_input
            self.value = value

    class Validated(Message):
        """Posted when validation completes."""

        def __init__(
            self, validated_input: "ValidatedInput", is_valid: bool, error: str
        ) -> None:
            super().__init__()
            self.validated_input = validated_input
            self.is_valid = is_valid
            self.error = error

    def __init__(
        self,
        field_name: str,
        label: str | None = None,
        *,
        validator: Callable[[str], str | None] | None = None,
        required: bool = False,
        help_text: str = "",
        placeholder: str = "",
        default: str = "",
        password: bool = False,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the validated input.

        Args:
            field_name: Internal field name for form data
            label: Display label (defaults to field_name)
            validator: Function that returns error message or None if valid
            required: Whether field is required
            help_text: Help text shown below input
            placeholder: Placeholder text in input
            default: Default value
            password: Whether to mask input
            id: Widget ID
            classes: CSS classes
        """
        super().__init__(id=id, classes=classes)
        self.field_name = field_name
        self.label_text = label or field_name.replace("_", " ").title()
        self.validator = validator
        self.required = required
        self.help_text = help_text
        self.placeholder = placeholder
        self.default = default
        self.password = password

        # Initialize reactive values
        self.value = default

    def compose(self) -> ComposeResult:
        """Compose the widget layout."""
        # Add asterisk to label for required fields
        label_display = f"{self.label_text} [red]*[/red]" if self.required else self.label_text
        yield Label(label_display, classes="field-label")
        yield Input(
            value=self.default,
            placeholder=self.placeholder,
            password=self.password,
            id=f"{self.field_name}_input",
        )
        yield Static("", classes="error-text", id=f"{self.field_name}_error")
        if self.help_text:
            yield Static(self.help_text, classes="help-text")

    def on_input_changed(self, event: Input.Changed) -> None:
        """Handle input changes with validation."""
        self.value = event.value
        self._validate()
        self.post_message(self.Changed(self, event.value))

    def _validate(self) -> None:
        """Run validation and update state."""
        error: str | None = None

        # Check required
        if self.required and not self.value.strip():
            error = f"{self.label_text} is required"

        # Run custom validator
        if error is None and self.validator and self.value:
            error = self.validator(self.value)

        # Update state
        self.is_valid = error is None
        self.error_message = error or ""

        # Update UI
        error_widget = self.query_one(f"#{self.field_name}_error", Static)
        error_widget.update(self.error_message)

        input_widget = self.query_one(f"#{self.field_name}_input", Input)
        input_widget.set_class(not self.is_valid, "-invalid")

        self.post_message(self.Validated(self, self.is_valid, self.error_message))

    def validate(self) -> bool:
        """Trigger validation and return result."""
        self._validate()
        return self.is_valid

    def set_value(self, value: str) -> None:
        """Set the input value programmatically."""
        input_widget = self.query_one(f"#{self.field_name}_input", Input)
        input_widget.value = value
        self.value = value
        self._validate()

    def focus_input(self) -> None:
        """Focus the input field."""
        input_widget = self.query_one(f"#{self.field_name}_input", Input)
        input_widget.focus()
