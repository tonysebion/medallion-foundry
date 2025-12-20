"""Dropdown select widget for enum values."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Label, Select, Static


class EnumSelect(Vertical):
    """Dropdown select for enum/choice fields.

    Displays a labeled dropdown with options from JSON Schema enums
    or custom option lists.

    Attributes:
        value: Currently selected value
    """

    DEFAULT_CSS = """
    EnumSelect {
        height: auto;
        margin-bottom: 1;
    }

    EnumSelect .field-label {
        color: $text;
        margin-bottom: 0;
    }

    EnumSelect .field-label.required::after {
        content: " *";
        color: $error;
    }

    EnumSelect Select {
        width: 100%;
    }

    EnumSelect .help-text {
        color: $text-muted;
        height: auto;
        margin-top: 0;
    }
    """

    value: reactive[str] = reactive("")

    class Changed(Message):
        """Posted when selection changes."""

        def __init__(self, enum_select: "EnumSelect", value: str) -> None:
            super().__init__()
            self.enum_select = enum_select
            self.value = value

    def __init__(
        self,
        field_name: str,
        options: list[tuple[str, str]],
        label: str | None = None,
        *,
        required: bool = False,
        help_text: str = "",
        default: str | None = None,
        allow_blank: bool = True,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the enum select.

        Args:
            field_name: Internal field name for form data
            options: List of (value, display_label) tuples
            label: Display label (defaults to field_name)
            required: Whether selection is required
            help_text: Help text shown below select
            default: Default selected value
            allow_blank: Whether to include a blank option
            id: Widget ID
            classes: CSS classes
        """
        super().__init__(id=id, classes=classes)
        self.field_name = field_name
        self.label_text = label or field_name.replace("_", " ").title()
        self.options = options
        self.required = required
        self.help_text = help_text
        self.default = default
        self.allow_blank = allow_blank

        # Set initial value
        if default:
            self.value = default
        elif options and not allow_blank:
            self.value = options[0][0]

    def compose(self) -> ComposeResult:
        """Compose the widget layout."""
        label_classes = "field-label required" if self.required else "field-label"
        yield Label(self.label_text, classes=label_classes)

        # Build options list
        select_options: list[tuple[str, str]] = []
        if self.allow_blank:
            select_options.append(("", "-- Select --"))
        select_options.extend(self.options)

        yield Select(
            options=[(label, value) for value, label in select_options],
            value=self.default if self.default else Select.BLANK,
            allow_blank=self.allow_blank,
            id=f"{self.field_name}_select",
        )

        if self.help_text:
            yield Static(self.help_text, classes="help-text")

    def on_select_changed(self, event: Select.Changed) -> None:
        """Handle selection changes."""
        # Select.Changed.value can be Select.BLANK or the actual value
        if event.value == Select.BLANK:
            self.value = ""
        else:
            self.value = str(event.value)
        self.post_message(self.Changed(self, self.value))

    def set_value(self, value: str) -> None:
        """Set the selected value programmatically."""
        select_widget = self.query_one(f"#{self.field_name}_select", Select)
        if value:
            select_widget.value = value
        else:
            select_widget.value = Select.BLANK
        self.value = value

    def set_options(self, options: list[tuple[str, str]]) -> None:
        """Update the available options."""
        self.options = options
        select_widget = self.query_one(f"#{self.field_name}_select", Select)

        # Rebuild options
        select_options: list[tuple[str, str]] = []
        if self.allow_blank:
            select_options.append(("-- Select --", ""))
        select_options.extend([(label, value) for value, label in options])

        select_widget.set_options(select_options)

    def validate(self) -> bool:
        """Check if selection is valid (required field has value)."""
        if self.required and not self.value:
            return False
        return True
