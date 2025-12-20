"""Input widget with inheritance visual indicator."""

from __future__ import annotations

from typing import Any, Callable, Literal

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Button, Input, Label, Static


class InheritableInput(Vertical):
    """Input field with inheritance visualization.

    Extends the basic input pattern to show:
    - Italic blue text + [inherited] badge for inherited values
    - Normal text for local values
    - "Reset to inherited" action when value differs from parent

    Attributes:
        value: Current input value
        source: Where this value came from (default/parent/local)
        parent_value: The value from parent config (for reset)
        is_valid: Whether current value passes validation
        error_message: Current validation error (empty if valid)
    """

    DEFAULT_CSS = """
    InheritableInput {
        height: auto;
        margin-bottom: 1;
    }

    InheritableInput .field-row {
        height: auto;
    }

    InheritableInput .field-label {
        color: $text;
        margin-bottom: 0;
        width: auto;
    }

    InheritableInput .required-marker {
        color: $error;
        margin-left: 0;
    }

    InheritableInput .inherited-badge {
        color: $primary;
        text-style: italic;
        margin-left: 1;
        background: $surface;
        padding: 0 1;
    }

    InheritableInput Input {
        width: 1fr;
    }

    InheritableInput Input.inherited {
        color: $primary;
        text-style: italic;
    }

    InheritableInput Input.-invalid {
        border: tall $error;
    }

    InheritableInput .error-text {
        color: $error;
        height: 1;
        margin-top: 0;
    }

    InheritableInput .help-text {
        color: $text-muted;
        height: auto;
        margin-top: 0;
    }

    InheritableInput .action-button {
        min-width: 8;
        margin-left: 1;
    }

    InheritableInput .env-button {
        min-width: 6;
        margin-left: 1;
    }

    InheritableInput .input-row {
        height: auto;
    }
    """

    value: reactive[str] = reactive("")
    source: reactive[Literal["default", "parent", "local"]] = reactive("default")
    is_valid: reactive[bool] = reactive(True)
    error_message: reactive[str] = reactive("")

    class Changed(Message):
        """Posted when the input value changes."""

        def __init__(self, widget: "InheritableInput", value: str) -> None:
            super().__init__()
            self.inheritable_input = widget
            self.value = value

    class Validated(Message):
        """Posted when validation completes."""

        def __init__(
            self,
            widget: "InheritableInput",
            is_valid: bool,
            error: str,
        ) -> None:
            super().__init__()
            self.inheritable_input = widget
            self.is_valid = is_valid
            self.error = error

    class EnvVarRequested(Message):
        """Posted when user clicks the env var button."""

        def __init__(self, widget: "InheritableInput") -> None:
            super().__init__()
            self.inheritable_input = widget

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
        source: Literal["default", "parent", "local"] = "default",
        parent_value: str = "",
        password: bool = False,
        is_sensitive: bool = False,
        show_env_button: bool | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the inheritable input.

        Args:
            field_name: Internal field name for form data
            label: Display label (defaults to field_name)
            validator: Function that returns error message or None if valid
            required: Whether field is required
            help_text: Help text shown below input
            placeholder: Placeholder text in input
            default: Default value
            source: Where the value came from (default/parent/local)
            parent_value: Value from parent config (for reset functionality)
            password: Whether to mask input
            is_sensitive: Whether this is a sensitive field (shows env var button)
            show_env_button: Override for showing env var button
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
        self.parent_value = parent_value
        self.password = password
        self.is_sensitive = is_sensitive

        # Show env button if sensitive or explicitly requested
        if show_env_button is not None:
            self.show_env_button = show_env_button
        else:
            self.show_env_button = is_sensitive

        # Initialize reactive values
        self.value = default
        self.source = source

    def compose(self) -> ComposeResult:
        """Compose the widget layout."""
        # Label row with inheritance badge
        with Horizontal(classes="field-row"):
            yield Label(self.label_text, classes="field-label")
            if self.required:
                yield Static("*", classes="required-marker")
            yield Static("", classes="inherited-badge", id=f"{self.field_name}_badge")

        # Input row with optional buttons
        with Horizontal(classes="input-row"):
            yield Input(
                value=self.default,
                placeholder=self.placeholder,
                password=self.password,
                id=f"{self.field_name}_input",
            )
            if self.show_env_button:
                yield Button(
                    "${...}",
                    id=f"{self.field_name}_env_btn",
                    classes="env-button",
                    variant="default",
                )
            yield Button(
                "Reset",
                id=f"{self.field_name}_reset_btn",
                classes="action-button",
                variant="default",
                disabled=True,
            )

        # Error text
        yield Static("", classes="error-text", id=f"{self.field_name}_error")

        # Help text
        if self.help_text:
            yield Static(self.help_text, classes="help-text")

    def on_mount(self) -> None:
        """Initialize styling based on source."""
        self._update_inheritance_display()

    def watch_source(self, new_source: str) -> None:
        """React to source changes."""
        self._update_inheritance_display()

    def _update_inheritance_display(self) -> None:
        """Update visual indicators based on source."""
        try:
            input_widget = self.query_one(f"#{self.field_name}_input", Input)
            badge = self.query_one(f"#{self.field_name}_badge", Static)
            reset_btn = self.query_one(f"#{self.field_name}_reset_btn", Button)
        except Exception:
            return  # Widget not yet mounted

        if self.source == "parent":
            input_widget.add_class("inherited")
            badge.update("[inherited]")
            # Enable reset if current value differs from parent
            reset_btn.disabled = self.value == self.parent_value
        else:
            input_widget.remove_class("inherited")
            badge.update("")
            # Enable reset if parent value exists and differs
            reset_btn.disabled = not self.parent_value or self.value == self.parent_value

    def on_input_changed(self, event: Input.Changed) -> None:
        """Handle input changes with validation."""
        if event.input.id != f"{self.field_name}_input":
            return

        self.value = event.value

        # If value differs from parent, mark as local
        if self.source == "parent" and event.value != self.parent_value:
            self.source = "local"

        self._validate()
        self._update_inheritance_display()
        self.post_message(self.Changed(self, event.value))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == f"{self.field_name}_reset_btn":
            self.action_reset_to_inherited()
        elif event.button.id == f"{self.field_name}_env_btn":
            self.post_message(self.EnvVarRequested(self))

    def action_reset_to_inherited(self) -> None:
        """Reset field to its inherited parent value."""
        if self.parent_value:
            self.set_value(self.parent_value)
            self.source = "parent"
            self._update_inheritance_display()

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
        try:
            error_widget = self.query_one(f"#{self.field_name}_error", Static)
            error_widget.update(self.error_message)

            input_widget = self.query_one(f"#{self.field_name}_input", Input)
            input_widget.set_class(not self.is_valid, "-invalid")
        except Exception:
            pass  # Widget not yet mounted

        self.post_message(self.Validated(self, self.is_valid, self.error_message))

    def validate(self) -> bool:
        """Trigger validation and return result."""
        self._validate()
        return self.is_valid

    def set_value(self, value: str, source: Literal["default", "parent", "local"] | None = None) -> None:
        """Set the input value programmatically."""
        try:
            input_widget = self.query_one(f"#{self.field_name}_input", Input)
            input_widget.value = value
        except Exception:
            pass

        self.value = value
        if source:
            self.source = source
        self._validate()
        self._update_inheritance_display()

    def set_parent_value(self, value: str) -> None:
        """Set the parent value for inheritance."""
        self.parent_value = value

        # If currently at default, inherit the parent value
        if self.source == "default" and value:
            self.set_value(value, "parent")

        self._update_inheritance_display()

    def focus_input(self) -> None:
        """Focus the input field."""
        try:
            input_widget = self.query_one(f"#{self.field_name}_input", Input)
            input_widget.focus()
        except Exception:
            pass

    def insert_env_var(self, reference: str) -> None:
        """Insert an environment variable reference."""
        self.set_value(reference, "local")
