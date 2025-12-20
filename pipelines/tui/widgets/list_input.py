"""Input widget for comma-separated list values."""

from __future__ import annotations

from typing import Callable

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Input, Label, Static


class ListInput(Vertical):
    """Input field for comma-separated lists.

    Used for fields like natural_keys, attributes, etc.
    Parses comma-separated input into a list of values.

    Attributes:
        values: Current list of values
        is_valid: Whether current input passes validation
    """

    DEFAULT_CSS = """
    ListInput {
        height: auto;
        margin-bottom: 1;
    }

    ListInput .field-label {
        color: $text;
        margin-bottom: 0;
    }

    ListInput .field-label.required::after {
        content: " *";
        color: $error;
    }

    ListInput Input {
        width: 100%;
    }

    ListInput Input.-invalid {
        border: tall $error;
    }

    ListInput .error-text {
        color: $error;
        height: 1;
        margin-top: 0;
    }

    ListInput .help-text {
        color: $text-muted;
        height: auto;
        margin-top: 0;
    }

    ListInput .item-count {
        color: $text-muted;
        text-style: italic;
        margin-top: 0;
    }
    """

    values: reactive[list[str]] = reactive(list, init=False)
    is_valid: reactive[bool] = reactive(True)
    error_message: reactive[str] = reactive("")

    class Changed(Message):
        """Posted when the list values change."""

        def __init__(self, list_input: "ListInput", values: list[str]) -> None:
            super().__init__()
            self.list_input = list_input
            self.values = values

    def __init__(
        self,
        field_name: str,
        label: str | None = None,
        *,
        validator: Callable[[list[str]], str | None] | None = None,
        required: bool = False,
        min_items: int = 0,
        help_text: str = "",
        placeholder: str = "item1, item2, item3",
        default: list[str] | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the list input.

        Args:
            field_name: Internal field name for form data
            label: Display label (defaults to field_name)
            validator: Function that validates the list
            required: Whether at least one item is required
            min_items: Minimum number of items required
            help_text: Help text shown below input
            placeholder: Placeholder text in input
            default: Default list of values
            id: Widget ID
            classes: CSS classes
        """
        super().__init__(id=id, classes=classes)
        self.field_name = field_name
        self.label_text = label or field_name.replace("_", " ").title()
        self.validator = validator
        self.required = required
        self.min_items = min_items if min_items > 0 else (1 if required else 0)
        self.help_text = help_text
        self.placeholder = placeholder
        self.default = default or []

        # Initialize reactive values
        self.values = list(self.default)

    def compose(self) -> ComposeResult:
        """Compose the widget layout."""
        label_classes = "field-label required" if self.required else "field-label"
        yield Label(self.label_text, classes=label_classes)

        default_str = ", ".join(self.default) if self.default else ""
        yield Input(
            value=default_str,
            placeholder=self.placeholder,
            id=f"{self.field_name}_input",
        )
        yield Static("", classes="error-text", id=f"{self.field_name}_error")
        yield Static("", classes="item-count", id=f"{self.field_name}_count")
        if self.help_text:
            yield Static(self.help_text, classes="help-text")

    def on_input_changed(self, event: Input.Changed) -> None:
        """Handle input changes."""
        # Parse comma-separated values
        raw = event.value.strip()
        if raw:
            self.values = [item.strip() for item in raw.split(",") if item.strip()]
        else:
            self.values = []

        self._validate()
        self._update_count()
        self.post_message(self.Changed(self, self.values))

    def _validate(self) -> None:
        """Run validation and update state."""
        error: str | None = None

        # Check minimum items
        if len(self.values) < self.min_items:
            if self.min_items == 1:
                error = f"{self.label_text} requires at least one item"
            else:
                error = f"{self.label_text} requires at least {self.min_items} items"

        # Run custom validator
        if error is None and self.validator and self.values:
            error = self.validator(self.values)

        # Update state
        self.is_valid = error is None
        self.error_message = error or ""

        # Update UI
        error_widget = self.query_one(f"#{self.field_name}_error", Static)
        error_widget.update(self.error_message)

        input_widget = self.query_one(f"#{self.field_name}_input", Input)
        input_widget.set_class(not self.is_valid, "-invalid")

    def _update_count(self) -> None:
        """Update the item count display."""
        count_widget = self.query_one(f"#{self.field_name}_count", Static)
        count = len(self.values)
        if count == 0:
            count_widget.update("")
        elif count == 1:
            count_widget.update("1 item")
        else:
            count_widget.update(f"{count} items")

    def validate(self) -> bool:
        """Trigger validation and return result."""
        self._validate()
        return self.is_valid

    def set_values(self, values: list[str]) -> None:
        """Set the list values programmatically."""
        self.values = list(values)
        input_widget = self.query_one(f"#{self.field_name}_input", Input)
        input_widget.value = ", ".join(values)
        self._validate()
        self._update_count()

    def focus_input(self) -> None:
        """Focus the input field."""
        input_widget = self.query_one(f"#{self.field_name}_input", Input)
        input_widget.focus()
