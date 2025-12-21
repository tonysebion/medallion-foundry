"""Full-screen prompt_toolkit application for pipeline configuration.

This provides a single-screen editor where all settings are visible and
the user can navigate freely using Tab/Shift+Tab, arrow keys, or mouse.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from prompt_toolkit import Application
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import (
    Layout,
    HSplit,
    VSplit,
    Window,
    FormattedTextControl,
    BufferControl,
    ScrollablePane,
)
from prompt_toolkit.layout.containers import ScrollOffsets
from prompt_toolkit.layout.dimension import Dimension as D
from prompt_toolkit.layout.controls import UIControl, UIContent
from prompt_toolkit.mouse_events import MouseEvent, MouseEventType
from prompt_toolkit.widgets import (
    Frame,
    TextArea,
)
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML, FormattedText

from pipelines.tui.models import PipelineState
from pipelines.tui.models.field_metadata import get_dynamic_required_fields


# Application style
STYLE = Style.from_dict({
    "title": "bold bg:#005f87 #ffffff",
    "section": "bold #00af00",
    "section-header": "bold underline #00af00",
    "field-label": "#d7d700",
    "field-label.required": "#d7d700 bold",
    "field-value": "bg:#262626 #ffffff",
    "field-value.focused": "bg:#303030 #ffffff",
    "field.focused": "bg:#303030",
    "field-input": "bg:#1e1e1e #ffffff",
    "field-input.focused": "bg:#2a2a2a #ffffff",
    "field-input.inherited": "bg:#1e1e1e #6699ff italic",  # Inherited value - blue italic
    "field-input.overridden": "bg:#1e1e1e #ff9900",  # Overridden value - orange
    "field-input.invalid": "bg:#3a1515 #ff6666",  # Invalid value - red tint
    "field-input.invalid-focused": "bg:#4a2020 #ff6666",  # Invalid focused - darker red
    "field-help": "#808080 italic",
    "field-help.inline": "#6a9955 italic",
    "inherited": "italic #6699ff",
    "inherited-badge": "#6699ff",
    "override-badge": "#ff9900",
    "sensitive": "#d700af",
    "error": "bold #ff0000",
    "success": "bold #00ff00",
    "help": "#808080 italic",
    "help-panel": "bg:#1c1c1c #a0a0a0",
    "yaml-preview": "bg:#1c1c1c #d0d0d0",
    "status-bar": "bg:#005f87 #ffffff",
    "button": "bg:#404040 #ffffff",
    "button.focused": "bg:#0087af #ffffff bold",
    "button.hover": "bg:#005f87 #ffffff bold",
    "dialog": "bg:#1c1c1c",
    "dialog.body": "bg:#262626",
    "dialog shadow": "bg:#000000",
    "clickable": "underline",
    "clickable.hover": "bold underline #00d7ff",
    # Dropdown styles
    "dropdown": "#808080",
    "dropdown.selected": "bold #00ff00",
    "dropdown.hover": "bg:#404040 #ffffff",
    "dropdown.focused": "#d0d0d0",
    "dropdown.inherited": "#6699ff italic",  # Inherited dropdown value
    # Section styles
    "section.hover": "bold underline #00d7ff",
    # Error panel
    "error-panel": "bg:#1c1c1c #ff6666",
    "error-panel.ok": "bg:#1c1c1c #00ff00",
    "error-panel.header": "bg:#1c1c1c #ff6666 bold",
    # Warning panel styles
    "warning-panel": "bg:#1c1c1c #ffcc00",
    "warning-panel.header": "bg:#1c1c1c #ffcc00 bold",
    # File browser styles
    "file-browser": "bg:#262626",
    "file-browser.header": "bold #00d7ff",
    "file-browser.divider": "#404040",
    "file-browser.dir": "#87afff",
    "file-browser.file": "#d0d0d0",
    "file-browser.selected": "bg:#005f87 #ffffff bold",
    "file-browser.hover": "bg:#303030 #ffffff",
})


class BaseFileBrowserControl(UIControl):
    """Base class for interactive file browser controls.

    Subclasses override _filter_file() to customize which files are shown,
    and _get_file_icon() to customize the file icon.
    """

    # Override in subclasses
    HEADER_ICON = "📂"
    FILE_ICON = "📄"

    def __init__(
        self,
        on_select: Callable[[Path], None],
        on_cancel: Callable[[], None],
        initial_path: Path | None = None,
    ):
        self.on_select = on_select
        self.on_cancel = on_cancel
        self.current_path = initial_path or Path.cwd()
        self.items: list[Path] = []
        self.selected_idx = 0
        self._hover_idx: int | None = None
        self._refresh_items()

    def _filter_file(self, item: Path) -> bool:
        """Return True if this file should be shown. Override in subclasses."""
        return True

    def _get_file_display(self, item: Path) -> str:
        """Get display string for a file. Override for custom formatting."""
        return f"{self.FILE_ICON} {item.name}"

    def _get_header_lines(self) -> list[tuple[str, str]]:
        """Get header lines. Override to add extra header content."""
        return []

    def _refresh_items(self) -> None:
        """Refresh the file/folder list."""
        self.items = []
        try:
            # Add parent directory option
            if self.current_path.parent != self.current_path:
                self.items.append(self.current_path.parent)

            # Get directories first, then filtered files
            dirs = []
            files = []
            for item in sorted(self.current_path.iterdir()):
                if item.is_dir() and not item.name.startswith('.'):
                    dirs.append(item)
                elif item.is_file() and self._filter_file(item):
                    files.append(item)

            self.items.extend(dirs)
            self.items.extend(files)
        except PermissionError:
            pass
        self.selected_idx = 0

    def _get_header_offset(self) -> int:
        """Number of header lines before items. Override if adding extra headers."""
        return 2  # Icon + path line, then divider

    def create_content(self, width: int, height: int) -> UIContent:
        header = f"  {self.HEADER_ICON} {self.current_path}"
        header_offset = self._get_header_offset()

        def get_line(i: int) -> list[tuple[str, str]]:
            if i == 0:
                return [("class:file-browser.header bold", header[:width])]
            if i == 1:
                return [("class:file-browser.divider", "─" * (width - 2))]

            item_idx = i - header_offset
            if item_idx >= len(self.items):
                return []

            item = self.items[item_idx]
            is_selected = item_idx == self.selected_idx
            is_hovered = item_idx == self._hover_idx

            # Build display name
            if item == self.current_path.parent:
                display = "📁 .."
            elif item.is_dir():
                display = f"📁 {item.name}/"
            else:
                display = self._get_file_display(item)

            # Truncate if needed
            display = display[:width - 4]

            # Determine style
            if is_selected:
                return [("class:file-browser.selected", f" ▸ {display}")]
            elif is_hovered:
                return [("class:file-browser.hover", f"   {display}")]
            elif item.is_dir():
                return [("class:file-browser.dir", f"   {display}")]
            else:
                return [("class:file-browser.file", f"   {display}")]

        return UIContent(get_line=get_line, line_count=len(self.items) + header_offset)

    def mouse_handler(self, mouse_event: MouseEvent) -> None:
        item_idx = mouse_event.position.y - self._get_header_offset()
        if 0 <= item_idx < len(self.items):
            if mouse_event.event_type == MouseEventType.MOUSE_UP:
                self.selected_idx = item_idx
                self._activate_selected()
            elif mouse_event.event_type == MouseEventType.MOUSE_MOVE:
                self._hover_idx = item_idx
        else:
            self._hover_idx = None

    def _activate_selected(self) -> None:
        """Activate the currently selected item."""
        if 0 <= self.selected_idx < len(self.items):
            item = self.items[self.selected_idx]
            if item.is_dir():
                self.current_path = item
                self._refresh_items()
            else:
                self.on_select(item)

    def is_focusable(self) -> bool:
        return True

    def get_key_bindings(self) -> KeyBindings:
        kb = KeyBindings()

        @kb.add("up")
        def move_up(event: Any) -> None:
            if self.items:
                self.selected_idx = (self.selected_idx - 1) % len(self.items)

        @kb.add("down")
        def move_down(event: Any) -> None:
            if self.items:
                self.selected_idx = (self.selected_idx + 1) % len(self.items)

        @kb.add("enter")
        def select(event: Any) -> None:
            self._activate_selected()

        @kb.add("escape")
        def cancel(event: Any) -> None:
            self.on_cancel()

        @kb.add("backspace")
        def go_up(event: Any) -> None:
            if self.current_path.parent != self.current_path:
                self.current_path = self.current_path.parent
                self._refresh_items()

        return kb


class FileBrowserControl(BaseFileBrowserControl):
    """File browser for selecting YAML configuration files."""

    HEADER_ICON = "📂"
    FILE_ICON = "📄"

    def _filter_file(self, item: Path) -> bool:
        return item.suffix.lower() in ('.yaml', '.yml')


class EnvFileBrowserControl(BaseFileBrowserControl):
    """File browser for selecting .env files."""

    HEADER_ICON = "🔐"
    FILE_ICON = "🔐"

    def __init__(
        self,
        on_select: Callable[[Path], None],
        on_cancel: Callable[[], None],
        initial_path: Path | None = None,
        discovered_files: list[Path] | None = None,
    ):
        self._discovered_files = discovered_files or []
        super().__init__(on_select, on_cancel, initial_path)

    def _filter_file(self, item: Path) -> bool:
        return (
            item.name == ".env"
            or item.name.endswith(".env")
            or item.name.startswith(".env.")
        )

    def _get_file_display(self, item: Path) -> str:
        is_discovered = item.resolve() in [p.resolve() for p in self._discovered_files]
        star = "★ " if is_discovered else ""
        return f"{self.FILE_ICON} {star}{item.name}"

    def _get_header_offset(self) -> int:
        # Extra line for discovered hint if present
        return 3 if self._discovered_files else 2

    def create_content(self, width: int, height: int) -> UIContent:
        header = f"  {self.HEADER_ICON} {self.current_path}"
        discovered_hint = ""
        if self._discovered_files:
            discovered_hint = f"  ({len(self._discovered_files)} found in project)"

        header_offset = self._get_header_offset()

        def get_line(i: int) -> list[tuple[str, str]]:
            if i == 0:
                return [("class:file-browser.header bold", header[:width])]
            if i == 1:
                if discovered_hint:
                    return [("class:field-help", discovered_hint[:width])]
                return [("class:file-browser.divider", "─" * (width - 2))]
            if i == 2 and discovered_hint:
                return [("class:file-browser.divider", "─" * (width - 2))]

            item_idx = i - header_offset
            if item_idx >= len(self.items):
                return []

            item = self.items[item_idx]
            is_selected = item_idx == self.selected_idx
            is_hovered = item_idx == self._hover_idx

            # Build display name
            if item == self.current_path.parent:
                display = "📁 .."
            elif item.is_dir():
                display = f"📁 {item.name}/"
            else:
                display = self._get_file_display(item)

            # Truncate if needed
            display = display[:width - 4]

            # Determine style
            if is_selected:
                return [("class:file-browser.selected", f" ▸ {display}")]
            elif is_hovered:
                return [("class:file-browser.hover", f"   {display}")]
            elif item.is_dir():
                return [("class:file-browser.dir", f"   {display}")]
            else:
                return [("class:file-browser.file", f"   {display}")]

        return UIContent(get_line=get_line, line_count=len(self.items) + header_offset)


class ClickableBufferControl(BufferControl):
    """BufferControl that notifies when it receives focus via click."""

    def __init__(
        self,
        buffer: Buffer,
        field_idx: int,
        on_focus: Callable[[int], None],
        **kwargs: Any,
    ):
        super().__init__(buffer=buffer, focusable=True, **kwargs)
        self.field_idx = field_idx
        self.on_focus = on_focus

    def mouse_handler(self, mouse_event: MouseEvent) -> None:
        # Update field index on click
        if mouse_event.event_type == MouseEventType.MOUSE_UP:
            self.on_focus(self.field_idx)
        # Let parent handle the rest (cursor positioning, selection, etc.)
        super().mouse_handler(mouse_event)


class ClickableButton(UIControl):
    """A clickable button control."""

    def __init__(
        self,
        text: str,
        handler: Callable[[], None],
        style: str = "class:button",
    ):
        self.text = text
        self.handler = handler
        self.style = style
        self._hover = False

    def create_content(self, width: int, height: int) -> UIContent:
        style = "class:button.hover" if self._hover else self.style

        def get_line(i: int) -> list[tuple[str, str]]:
            if i == 0:
                return [(style, f" {self.text} ")]
            return []

        return UIContent(get_line=get_line, line_count=1)

    def mouse_handler(self, mouse_event: MouseEvent) -> None:
        if mouse_event.event_type == MouseEventType.MOUSE_UP:
            self.handler()
        elif mouse_event.event_type == MouseEventType.MOUSE_MOVE:
            self._hover = True
        else:
            self._hover = False

    def is_focusable(self) -> bool:
        return True


class ClickableFieldLabel(UIControl):
    """A clickable field label that selects the field on click."""

    def __init__(
        self,
        label_parts: list[tuple[str, str]],
        field_idx: int,
        on_click: Callable[[int], None],
    ):
        self.label_parts = label_parts
        self.field_idx = field_idx
        self.on_click = on_click

    def create_content(self, width: int, height: int) -> UIContent:
        def get_line(i: int) -> list[tuple[str, str]]:
            if i == 0:
                return self.label_parts
            return []

        return UIContent(get_line=get_line, line_count=1)

    def mouse_handler(self, mouse_event: MouseEvent) -> None:
        if mouse_event.event_type == MouseEventType.MOUSE_UP:
            self.on_click(self.field_idx)

    def is_focusable(self) -> bool:
        return False


class ClickableSectionHeader(UIControl):
    """A clickable section header for collapsing/expanding sections."""

    def __init__(
        self,
        text: str,
        section: str,
        on_click: Callable[[str], None],
    ):
        self.text = text
        self.section = section
        self.on_click = on_click
        self._hover = False

    def create_content(self, width: int, height: int) -> UIContent:
        style = "class:section.hover" if self._hover else "class:section-header"

        def get_line(i: int) -> list[tuple[str, str]]:
            if i == 0:
                return [(style, f"── {self.text} ──")]
            return []

        return UIContent(get_line=get_line, line_count=1)

    def mouse_handler(self, mouse_event: MouseEvent) -> None:
        if mouse_event.event_type == MouseEventType.MOUSE_UP:
            self.on_click(self.section)
        elif mouse_event.event_type == MouseEventType.MOUSE_MOVE:
            self._hover = True
        else:
            self._hover = False

    def is_focusable(self) -> bool:
        return False


class DropdownMenu(UIControl):
    """A dropdown menu with click-to-expand/collapse behavior.

    - When collapsed: shows selected value with ▼ indicator
    - Click to expand and show all options
    - Click an option to select it and collapse
    - Up/Down arrows navigate options when expanded
    - Enter/Escape collapse the menu
    """

    def __init__(
        self,
        field: "Field",
        on_select: Callable[["Field", str], None],
        field_idx: int,
        on_focus: Callable[[int], None],
        on_collapse: Callable[[], None],
    ):
        self.field = field
        self.on_select = on_select
        self.field_idx = field_idx
        self.on_focus = on_focus
        self.on_collapse = on_collapse
        self._hover_idx: int | None = None
        self._expanded = False
        self._highlight_idx = 0  # Which option is highlighted for keyboard nav

    def get_min_width(self) -> int:
        """Calculate minimum width needed to display all options."""
        max_len = 0
        for _, txt in self.field.enum_options:
            max_len = max(max_len, len(txt))
        # Add space for indicator (● ) and padding
        return max_len + 4

    def create_content(self, width: int, height: int) -> UIContent:
        selected = self.field.buffer.text
        options = self.field.enum_options

        # Find current selected display text
        display_text = selected
        for i, (val, txt) in enumerate(options):
            if val == selected:
                display_text = txt
                self._highlight_idx = i
                break

        # Calculate width for consistent display
        min_width = self.get_min_width()

        # When collapsed, show only the selected value
        if not self._expanded:
            def get_line_collapsed(i: int) -> list[tuple[str, str]]:
                if i == 0:
                    text = display_text if display_text else "(select...)"
                    # Pad to minimum width for consistent appearance
                    padded = f"▼ {text}".ljust(min_width)
                    return [("class:dropdown", padded)]
                return []

            return UIContent(get_line=get_line_collapsed, line_count=1)

        # When expanded, show all options
        def get_line(i: int) -> list[tuple[str, str]]:
            if i >= len(options):
                return []

            val, txt = options[i]
            is_selected = val == selected
            is_highlighted = i == self._highlight_idx
            is_hovered = i == self._hover_idx

            # Build the line with consistent width
            parts = []

            # Selection/highlight indicator
            if is_selected:
                indicator = "● "
            elif is_highlighted:
                indicator = "▸ "
            else:
                indicator = "  "

            # Pad text to minimum width
            padded_txt = txt.ljust(min_width - 2)

            # Determine style based on state
            if is_hovered or is_highlighted:
                parts.append(("class:dropdown.hover", indicator + padded_txt))
            elif is_selected:
                parts.append(("class:dropdown.selected", indicator + padded_txt))
            else:
                parts.append(("class:dropdown.focused", indicator + padded_txt))

            return parts

        return UIContent(get_line=get_line, line_count=len(options))

    def mouse_handler(self, mouse_event: MouseEvent) -> None:
        if mouse_event.event_type == MouseEventType.MOUSE_UP:
            if not self._expanded:
                # Click on collapsed dropdown - expand it
                self._expanded = True
                self.on_focus(self.field_idx)
            else:
                # Click on expanded dropdown - select option if valid
                options = self.field.enum_options
                if mouse_event.position.y < len(options):
                    val, _ = options[mouse_event.position.y]
                    self._select_and_collapse(val)
                else:
                    # Clicked outside options - just collapse
                    self._expanded = False
                    self.on_collapse()
        elif mouse_event.event_type == MouseEventType.MOUSE_MOVE:
            if self._expanded:
                options = self.field.enum_options
                if mouse_event.position.y < len(options):
                    self._hover_idx = mouse_event.position.y
                else:
                    self._hover_idx = None

    def _select_and_collapse(self, value: str) -> None:
        """Select a value and collapse the dropdown."""
        self.field.buffer.text = value
        self._expanded = False
        self.on_select(self.field, value)

    def expand(self) -> None:
        """Expand the dropdown."""
        self._expanded = True
        # Set highlight to current selection
        current = self.field.buffer.text
        for i, (val, _) in enumerate(self.field.enum_options):
            if val == current:
                self._highlight_idx = i
                break

    def collapse(self) -> None:
        """Collapse the dropdown."""
        self._expanded = False

    def is_expanded(self) -> bool:
        """Check if dropdown is currently expanded."""
        return self._expanded

    def is_focusable(self) -> bool:
        return True

    def get_key_bindings(self) -> KeyBindings:
        """Key bindings for dropdown navigation."""
        kb = KeyBindings()

        @kb.add("up")
        def move_up(event: Any) -> None:
            if self._expanded:
                # Move highlight up
                options = self.field.enum_options
                self._highlight_idx = (self._highlight_idx - 1) % len(options)
            else:
                # Expand and highlight previous option
                self.expand()

        @kb.add("down")
        def move_down(event: Any) -> None:
            if self._expanded:
                # Move highlight down
                options = self.field.enum_options
                self._highlight_idx = (self._highlight_idx + 1) % len(options)
            else:
                # Expand and highlight next option
                self.expand()

        @kb.add("enter")
        def select_option(event: Any) -> None:
            if self._expanded:
                # Select highlighted option and collapse
                options = self.field.enum_options
                if 0 <= self._highlight_idx < len(options):
                    val, _ = options[self._highlight_idx]
                    self._select_and_collapse(val)
            else:
                # Expand the dropdown
                self.expand()

        @kb.add("escape")
        def cancel(event: Any) -> None:
            if self._expanded:
                self._expanded = False
                self.on_collapse()

        @kb.add("space")
        def toggle(event: Any) -> None:
            if self._expanded:
                # Select highlighted option
                options = self.field.enum_options
                if 0 <= self._highlight_idx < len(options):
                    val, _ = options[self._highlight_idx]
                    self._select_and_collapse(val)
            else:
                self.expand()

        return kb


class Field:
    """Represents an editable field in the form."""

    def __init__(
        self,
        name: str,
        label: str,
        section: str,
        *,
        required: bool = False,
        help_text: str = "",
        is_sensitive: bool = False,
        field_type: str = "text",  # text, enum, list, multiline
        enum_options: list[tuple[str, str]] | None = None,
        default: str = "",
        source: str = "default",
        visible_when: Callable[["PipelineConfigApp"], bool] | None = None,
        is_basic: bool = False,  # Show in Basic mode
        multiline: bool = False,  # Expand when focused (for SQL queries)
    ):
        self.name = name
        self.label = label
        self.section = section
        self.required = required
        self.help_text = help_text
        self.is_sensitive = is_sensitive
        self.field_type = field_type
        self.enum_options = enum_options or []
        self.default = default
        self.source = source
        self.visible_when = visible_when
        self.is_basic = is_basic
        self.multiline = multiline
        self.buffer = Buffer(name=name, multiline=multiline)
        self.buffer.text = default

    def is_visible(self, app: "PipelineConfigApp") -> bool:
        """Check if this field should be visible."""
        # Check conditional visibility first (e.g., source_type conditions)
        if self.visible_when is not None and not self.visible_when(app):
            return False

        # In basic mode, show basic fields AND required fields
        if not app.advanced_mode and not self.is_basic and not self.required:
            return False

        return True


class ConfirmDialog(UIControl):
    """A confirmation dialog with Yes/No options."""

    def __init__(
        self,
        message: str,
        on_confirm: Callable[[], None],
        on_cancel: Callable[[], None],
    ):
        self.message = message
        self.on_confirm = on_confirm
        self.on_cancel = on_cancel
        self.selected = 0  # 0 = Yes, 1 = No

    def create_content(self, width: int, height: int) -> UIContent:
        lines = [
            "  " + self.message,
            "",
            "  " + ("▸ Yes" if self.selected == 0 else "  Yes") + "    " +
            ("▸ No" if self.selected == 1 else "  No"),
        ]

        def get_line(i: int) -> list[tuple[str, str]]:
            if i < len(lines):
                if i == 2:
                    # Buttons line
                    yes_style = "class:button.focused" if self.selected == 0 else "class:button"
                    no_style = "class:button.focused" if self.selected == 1 else "class:button"
                    return [
                        ("", "  "),
                        (yes_style, " Yes "),
                        ("", "    "),
                        (no_style, " No "),
                    ]
                return [("class:dialog.body", lines[i])]
            return []

        return UIContent(get_line=get_line, line_count=3)

    def mouse_handler(self, mouse_event: MouseEvent) -> None:
        if mouse_event.event_type == MouseEventType.MOUSE_UP:
            if mouse_event.position.y == 2:  # Button row
                if mouse_event.position.x < 10:
                    self.on_confirm()
                else:
                    self.on_cancel()

    def is_focusable(self) -> bool:
        return True

    def get_key_bindings(self) -> KeyBindings:
        kb = KeyBindings()

        @kb.add("left")
        @kb.add("right")
        def toggle(event: Any) -> None:
            self.selected = 1 - self.selected

        @kb.add("enter")
        def select(event: Any) -> None:
            if self.selected == 0:
                self.on_confirm()
            else:
                self.on_cancel()

        @kb.add("escape")
        @kb.add("n")
        def cancel(event: Any) -> None:
            self.on_cancel()

        @kb.add("y")
        def confirm(event: Any) -> None:
            self.on_confirm()

        return kb


class PipelineConfigApp:
    """Full-screen pipeline configuration editor.

    Shows all settings on one screen with keyboard navigation.
    Tab/Shift+Tab to move between fields, Enter to edit enums,
    Ctrl+S to save, Ctrl+Q to quit.
    """

    def __init__(
        self,
        mode: str = "create",
        yaml_path: str | None = None,
        parent_path: str | None = None,
    ) -> None:
        self.mode = mode
        self.yaml_path = yaml_path
        self.parent_path = parent_path
        self.state: PipelineState | None = None
        self.fields: list[Field] = []
        self.current_field_idx = 0
        self.show_help = True
        self.status_message = ""
        self.app: Application | None = None
        self.advanced_mode = False  # Basic mode by default
        self.yaml_preview_area: TextArea | None = None
        self.file_browser_active = False
        self.file_browser: FileBrowserControl | None = None
        # Env file browser state
        self.env_browser_active = False
        self.env_browser: EnvFileBrowserControl | None = None
        # Section collapse state
        self.sections_collapsed: dict[str, bool] = {
            "metadata": False,
            "bronze": False,
            "silver": False,
        }
        self.validation_errors: list[str] = []
        self.validation_warnings: list[str] = []
        # Track unsaved changes
        self._has_unsaved_changes = False
        self._last_saved_yaml = ""
        # YAML preview panel collapse state
        self.yaml_preview_collapsed = False
        # Confirmation dialog state
        self._confirm_dialog: ConfirmDialog | None = None
        self._confirm_dialog_active = False
        # Undo/redo history: list of (field_name, old_value, new_value) tuples
        self._undo_stack: list[tuple[str, str, str]] = []
        self._redo_stack: list[tuple[str, str, str]] = []
        self._is_undoing = False  # Flag to prevent recording undo during undo/redo
        # Search/filter for fields
        self._search_text = ""
        self._search_buffer = Buffer(name="search")
        self._search_buffer.on_text_changed += self._on_search_changed
        # Persistent ScrollablePane to maintain scroll position across layout refreshes
        self._scrollable_pane: ScrollablePane | None = None

    def run(self) -> None:
        """Run the full-screen application."""
        # Initialize state
        if self.mode == "edit" and self.yaml_path:
            self.state = PipelineState.from_yaml(Path(self.yaml_path))
            self.status_message = f"Loaded: {self.yaml_path}"
        elif self.mode == "create" and self.parent_path:
            self.state = PipelineState.from_schema_defaults()
            self.state.set_parent_config(Path(self.parent_path))
            self.status_message = f"Inheriting from: {self.parent_path}"
        else:
            self.state = PipelineState.from_schema_defaults()
            self.status_message = "Creating new pipeline"

        # Create fields
        self._create_fields()

        # Build and run application
        self.app = Application(
            layout=self._create_layout(),
            key_bindings=self._create_bindings(),
            style=STYLE,
            full_screen=True,
            mouse_support=True,
        )
        self.app.run()

    def _create_fields(self) -> None:
        """Create all form fields."""
        # Metadata fields
        self.fields.extend([
            Field(
                "name", "Pipeline Name", "metadata",
                help_text="A unique name for this pipeline (optional)",
                default=self.state.name or "",
                is_basic=True,
            ),
            Field(
                "description", "Description", "metadata",
                help_text="Brief description of what this pipeline does",
                default=self.state.description or "",
                is_basic=True,
            ),
        ])

        # Bronze fields
        self.fields.extend([
            Field(
                "system", "System", "bronze",
                required=True,
                help_text="Source system name (e.g., retail, crm, erp)",
                default=self.state.get_bronze_value("system") or "",
                source=self._get_source("bronze", "system"),
                is_basic=True,
            ),
            Field(
                "entity", "Entity", "bronze",
                required=True,
                help_text="Entity/table name (e.g., orders, customers)",
                default=self.state.get_bronze_value("entity") or "",
                source=self._get_source("bronze", "entity"),
                is_basic=True,
            ),
            Field(
                "source_type", "Source Type", "bronze",
                required=True,
                field_type="enum",
                enum_options=[
                    ("file_csv", "CSV File"),
                    ("file_parquet", "Parquet File"),
                    ("file_json", "JSON File"),
                    ("file_jsonl", "JSON Lines"),
                    ("file_excel", "Excel File"),
                    ("file_fixed_width", "Fixed Width"),
                    ("file_space_delimited", "Space Delimited"),
                    ("database_mssql", "SQL Server"),
                    ("database_postgres", "PostgreSQL"),
                    ("database_mysql", "MySQL"),
                    ("database_db2", "DB2"),
                    ("api_rest", "REST API"),
                ],
                help_text="Type of data source",
                default=self.state.get_bronze_value("source_type") or "file_csv",
                is_basic=True,
            ),
            Field(
                "source_path", "Source Path", "bronze",
                help_text="Path to data file (supports {run_date})",
                default=self.state.get_bronze_value("source_path") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("file_"),
                is_basic=True,
            ),
            Field(
                "host", "Host", "bronze",
                help_text="Database hostname (use ${VAR} for env vars)",
                is_sensitive=True,
                default=self.state.get_bronze_value("host") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("database_"),
                is_basic=True,
            ),
            Field(
                "database", "Database", "bronze",
                help_text="Database name",
                default=self.state.get_bronze_value("database") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("database_"),
                is_basic=True,
            ),
            Field(
                "query", "Full Query", "bronze",
                required=True,
                help_text="SQL query for full snapshot (all records). Use {run_date} for date placeholders.",
                default=self.state.get_bronze_value("query") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("database_"),
                is_basic=True,
                multiline=True,
            ),
            Field(
                "incremental_query", "Incremental Query", "bronze",
                help_text="SQL query for incremental loads. Use {watermark} for last processed value. Falls back to full query if not provided.",
                default=self.state.get_bronze_value("incremental_query") or "",
                visible_when=lambda app: (
                    app._get_field_value("source_type").startswith("database_") and
                    app._get_field_value("load_pattern") in ("incremental", "cdc", "incremental_append")
                ),
                is_basic=True,
                multiline=True,
            ),
            Field(
                "db_username", "Username", "bronze",
                help_text="Windows auth: DOMAIN\\user. Leave empty for trusted connection.",
                is_sensitive=True,
                default=self.state.get_bronze_value("username") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("database_"),
                is_basic=True,
            ),
            Field(
                "db_password", "Password", "bronze",
                help_text="Windows auth password. Leave empty for trusted connection.",
                is_sensitive=True,
                default=self.state.get_bronze_value("password") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("database_"),
                is_basic=True,
            ),
            Field(
                "base_url", "Base URL", "bronze",
                required=True,
                help_text="API base URL (e.g., https://api.example.com)",
                default=self.state.get_bronze_value("base_url") or "",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
                is_basic=True,
            ),
            Field(
                "endpoint", "Endpoint", "bronze",
                required=True,
                help_text="API endpoint path (e.g., /v1/customers)",
                default=self.state.get_bronze_value("endpoint") or "",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
                is_basic=True,
            ),
            Field(
                "auth_type", "Authentication", "bronze",
                field_type="enum",
                enum_options=[
                    ("none", "None"),
                    ("bearer", "Bearer Token"),
                    ("api_key", "API Key"),
                    ("basic", "Basic Auth"),
                ],
                help_text="How to authenticate with the API",
                default=self.state.get_bronze_value("auth_type") or "none",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
                is_basic=True,
            ),
            Field(
                "token", "Token", "bronze",
                help_text="Bearer token (use ${VAR} for env vars)",
                is_sensitive=True,
                default=self.state.get_bronze_value("token") or "",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("auth_type") == "bearer"
                ),
                is_basic=True,
            ),
            Field(
                "api_key", "API Key", "bronze",
                help_text="API key value (use ${VAR} for env vars)",
                is_sensitive=True,
                default=self.state.get_bronze_value("api_key") or "",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("auth_type") == "api_key"
                ),
                is_basic=True,
            ),
            Field(
                "api_key_header", "API Key Header", "bronze",
                help_text="HTTP header name for API key (default: X-API-Key)",
                default=self.state.get_bronze_value("api_key_header") or "X-API-Key",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("auth_type") == "api_key"
                ),
                is_basic=True,
            ),
            Field(
                "api_username", "Username", "bronze",
                help_text="Basic auth username (use ${VAR} for env vars)",
                is_sensitive=True,
                default=self.state.get_bronze_value("username") or "",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("auth_type") == "basic"
                ),
                is_basic=True,
            ),
            Field(
                "api_password", "Password", "bronze",
                help_text="Basic auth password (use ${VAR} for env vars)",
                is_sensitive=True,
                default=self.state.get_bronze_value("password") or "",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("auth_type") == "basic"
                ),
                is_basic=True,
            ),
            # API data extraction
            Field(
                "data_path", "Data Path", "bronze",
                help_text="Path to records in JSON response (e.g., data.items, response.records)",
                default=self.state.get_bronze_value("data_path") or "",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
            ),
            # API pagination
            Field(
                "pagination_strategy", "Pagination", "bronze",
                field_type="enum",
                enum_options=[
                    ("none", "None (single request)"),
                    ("offset", "Offset/Limit"),
                    ("page", "Page Number"),
                    ("cursor", "Cursor-based"),
                ],
                help_text="How to paginate through API results",
                default=self.state.get_bronze_value("pagination_strategy") or "none",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
            ),
            Field(
                "page_size", "Page Size", "bronze",
                help_text="Number of records per page (default: 100)",
                default=str(self.state.get_bronze_value("page_size") or "100"),
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("pagination_strategy") not in ("none", "")
                ),
            ),
            Field(
                "cursor_path", "Cursor Path", "bronze",
                help_text="Path to next cursor in response (e.g., meta.next_cursor)",
                default=self.state.get_bronze_value("cursor_path") or "",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("pagination_strategy") == "cursor"
                ),
            ),
            Field(
                "max_pages", "Max Pages", "bronze",
                help_text="Maximum pages to fetch (leave empty for no limit)",
                default=str(self.state.get_bronze_value("max_pages") or ""),
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("pagination_strategy") not in ("none", "")
                ),
            ),
            Field(
                "max_records", "Max Records", "bronze",
                help_text="Maximum total records to fetch (leave empty for no limit)",
                default=str(self.state.get_bronze_value("max_records") or ""),
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("pagination_strategy") not in ("none", "")
                ),
            ),
            # Pagination parameter customization (advanced)
            Field(
                "offset_param", "Offset Param", "bronze",
                help_text="Query param name for offset (default: offset)",
                default=self.state.get_bronze_value("offset_param") or "offset",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("pagination_strategy") == "offset"
                ),
            ),
            Field(
                "limit_param", "Limit Param", "bronze",
                help_text="Query param name for limit (default: limit)",
                default=self.state.get_bronze_value("limit_param") or "limit",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("pagination_strategy") == "offset"
                ),
            ),
            Field(
                "page_param", "Page Param", "bronze",
                help_text="Query param name for page number (default: page)",
                default=self.state.get_bronze_value("page_param") or "page",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("pagination_strategy") == "page"
                ),
            ),
            Field(
                "page_size_param", "Page Size Param", "bronze",
                help_text="Query param name for page size (default: page_size)",
                default=self.state.get_bronze_value("page_size_param") or "page_size",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("pagination_strategy") == "page"
                ),
            ),
            Field(
                "cursor_param", "Cursor Param", "bronze",
                help_text="Query param name for cursor (default: cursor)",
                default=self.state.get_bronze_value("cursor_param") or "cursor",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("pagination_strategy") == "cursor"
                ),
            ),
            # API rate limiting and reliability
            Field(
                "requests_per_second", "Requests/Second", "bronze",
                help_text="Rate limit for API calls (default: unlimited). Recommended for production.",
                default=str(self.state.get_bronze_value("requests_per_second") or ""),
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
            ),
            Field(
                "timeout", "Timeout (seconds)", "bronze",
                help_text="Request timeout in seconds (default: 30, rarely changed)",
                default=str(self.state.get_bronze_value("timeout") or "30"),
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
            ),
            Field(
                "max_retries", "Max Retries", "bronze",
                help_text="Number of retry attempts on failure (default: 3, rarely changed)",
                default=str(self.state.get_bronze_value("max_retries") or "3"),
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
            ),
            # API custom headers and params
            Field(
                "headers", "Custom Headers", "bronze",
                help_text="Additional HTTP headers (JSON format: {\"X-Custom\": \"value\"})",
                default=self.state.get_bronze_value("headers") or "",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
                multiline=True,
            ),
            Field(
                "params", "Query Parameters", "bronze",
                help_text="Additional query params (JSON format: {\"include\": \"metadata\"})",
                default=self.state.get_bronze_value("params") or "",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
                multiline=True,
            ),
            # Load pattern - essential for understanding how the pipeline works
            Field(
                "load_pattern", "Load Pattern", "bronze",
                field_type="enum",
                enum_options=[
                    ("full_snapshot", "Full Snapshot (replace all each run)"),
                    ("incremental", "Incremental (append new via watermark)"),
                    ("cdc", "CDC (capture inserts, updates, deletes)"),
                ],
                help_text="How to load data from source",
                default=self.state.get_bronze_value("load_pattern") or "full_snapshot",
                is_basic=True,  # Always show - essential for understanding the pipeline
            ),
            Field(
                "watermark_column", "Watermark Column", "bronze",
                help_text="Column to track for incremental/CDC loads (e.g., updated_at, modified_date)",
                default=self.state.get_bronze_value("watermark_column") or "",
                visible_when=lambda app: app._get_field_value("load_pattern") in ("incremental", "cdc", "incremental_append"),
                is_basic=True,  # Show when load_pattern requires it
            ),
            Field(
                "watermark_param", "Watermark Param", "bronze",
                help_text="API query param for watermark value (e.g., since, updated_after)",
                default=self.state.get_bronze_value("watermark_param") or "",
                visible_when=lambda app: (
                    app._get_field_value("source_type") == "api_rest" and
                    app._get_field_value("load_pattern") in ("incremental", "cdc", "incremental_append")
                ),
            ),
            Field(
                "full_refresh_days", "Full Refresh Days", "bronze",
                help_text="Force full refresh every N days (leave empty for never)",
                default=str(self.state.get_bronze_value("full_refresh_days") or ""),
                visible_when=lambda app: app._get_field_value("load_pattern") in ("incremental", "cdc", "incremental_append"),
            ),
            # Output options (advanced)
            Field(
                "target_path", "Target Path", "bronze",
                help_text="Override output directory (default: ./bronze/system={system}/entity={entity}/dt={run_date}/)",
                default=self.state.get_bronze_value("target_path") or "",
            ),
            # CSV/Delimited file options
            Field(
                "csv_delimiter", "Delimiter", "bronze",
                help_text="Column delimiter (default: comma for CSV, space for space-delimited)",
                default=self.state.get_bronze_value("csv_delimiter") or "",
                visible_when=lambda app: app._get_field_value("source_type") in ("file_csv", "file_space_delimited"),
            ),
            Field(
                "csv_header", "Has Header", "bronze",
                field_type="enum",
                enum_options=[
                    ("true", "Yes - first row is column names"),
                    ("false", "No - no header row"),
                ],
                help_text="Whether file has a header row with column names",
                default=self.state.get_bronze_value("csv_header") or "true",
                visible_when=lambda app: app._get_field_value("source_type") in ("file_csv", "file_space_delimited"),
            ),
            Field(
                "csv_skip_rows", "Skip Rows", "bronze",
                help_text="Number of rows to skip at top of file (default: 0)",
                default=str(self.state.get_bronze_value("csv_skip_rows") or ""),
                visible_when=lambda app: app._get_field_value("source_type") in ("file_csv", "file_excel"),
            ),
            # Excel options
            Field(
                "sheet", "Sheet", "bronze",
                help_text="Sheet name or index (default: first sheet, index 0)",
                default=self.state.get_bronze_value("sheet") or "",
                visible_when=lambda app: app._get_field_value("source_type") == "file_excel",
            ),
            # JSON options
            Field(
                "flatten", "Flatten JSON", "bronze",
                field_type="enum",
                enum_options=[
                    ("false", "No - keep nested structure"),
                    ("true", "Yes - flatten nested objects"),
                ],
                help_text="Flatten nested JSON structures into columns",
                default=self.state.get_bronze_value("flatten") or "false",
                visible_when=lambda app: app._get_field_value("source_type") in ("file_json", "file_jsonl"),
            ),
            # Fixed-width options
            Field(
                "widths", "Column Widths", "bronze",
                help_text="Column widths (comma-separated, e.g., 10,20,15,30)",
                default=self._list_to_str(self.state.get_bronze_value("widths")),
                visible_when=lambda app: app._get_field_value("source_type") == "file_fixed_width",
            ),
            Field(
                "columns", "Column Names", "bronze",
                help_text="Column names (comma-separated, must match widths count)",
                default=self._list_to_str(self.state.get_bronze_value("columns")),
                visible_when=lambda app: app._get_field_value("source_type") in ("file_fixed_width", "file_space_delimited"),
            ),
        ])

        # Silver fields
        self.fields.extend([
            Field(
                "natural_keys", "Natural Keys", "silver",
                required=True,
                field_type="list",
                help_text="Columns that uniquely identify a record (comma-separated)",
                default=self._list_to_str(self.state.get_silver_value("natural_keys")),
                is_basic=True,
            ),
            Field(
                "change_timestamp", "Change Timestamp", "silver",
                required=True,
                help_text="Column that tracks when records changed",
                default=self.state.get_silver_value("change_timestamp") or "",
                is_basic=True,
            ),
            Field(
                "entity_kind", "Entity Kind", "silver",
                field_type="enum",
                enum_options=[
                    ("state", "State (dimension - customers, products, accounts)"),
                    ("event", "Event (fact - orders, transactions, logs)"),
                ],
                help_text="State=slowly changing entities, Event=immutable records",
                default=self.state.get_silver_value("entity_kind") or "state",
                is_basic=True,  # Essential for understanding the data model
            ),
            Field(
                "history_mode", "History Mode", "silver",
                field_type="enum",
                enum_options=[
                    ("current_only", "Current Only (SCD1 - keep latest)"),
                    ("full_history", "Full History (SCD2 - track all changes)"),
                ],
                help_text="SCD1=overwrite, SCD2=version history (state entities only)",
                default=self.state.get_silver_value("history_mode") or "current_only",
                is_basic=True,  # Essential for understanding history tracking
            ),
            # Column selection (mutually exclusive)
            Field(
                "column_mode", "Column Selection", "silver",
                field_type="enum",
                enum_options=[
                    ("all", "All Columns (include everything)"),
                    ("include", "Include Only (specify which columns to keep)"),
                    ("exclude", "Exclude (specify which columns to remove)"),
                ],
                help_text="Choose how to select columns for the Silver layer",
                default=self._get_column_mode_default(),
            ),
            Field(
                "attributes", "Include Columns", "silver",
                field_type="list",
                help_text="Columns to include (comma-separated). Only these will appear in Silver.",
                default=self._list_to_str(self.state.get_silver_value("attributes")),
                visible_when=lambda app: app._get_field_value("column_mode") == "include",
            ),
            Field(
                "exclude_columns", "Exclude Columns", "silver",
                field_type="list",
                help_text="Columns to exclude (comma-separated). All others will appear in Silver.",
                default=self._list_to_str(self.state.get_silver_value("exclude_columns")),
                visible_when=lambda app: app._get_field_value("column_mode") == "exclude",
            ),
            # Output options
            Field(
                "validate_source", "Validate Source", "silver",
                field_type="enum",
                enum_options=[
                    ("skip", "Skip (fastest, no validation)"),
                    ("warn", "Warn (log warning if checksums fail)"),
                    ("strict", "Strict (fail if checksums don't match)"),
                ],
                help_text="Validate Bronze checksums before processing. Default: skip (rarely changed)",
                default=self.state.get_silver_value("validate_source") or "skip",
            ),
            Field(
                "output_formats", "Output Format", "silver",
                field_type="enum",
                enum_options=[
                    ("parquet", "Parquet (recommended - typed, compressed, fast)"),
                    ("csv", "CSV (loses types, slower queries)"),
                    ("both", "Both (Parquet + CSV)"),
                ],
                help_text="Output file format. Parquet is strongly recommended.",
                default=self._get_output_format_default(),
            ),
            Field(
                "parquet_compression", "Compression", "silver",
                field_type="enum",
                enum_options=[
                    ("zstd", "ZSTD (recommended - best compression/speed balance)"),
                    ("snappy", "Snappy (fast but poor compression)"),
                    ("gzip", "GZIP (good compression, slower)"),
                    ("lz4", "LZ4 (very fast, moderate compression)"),
                    ("none", "None (no compression)"),
                ],
                help_text="Parquet compression. ZSTD recommended over Snappy for better compression.",
                default=self.state.get_silver_value("parquet_compression") or "snappy",
                visible_when=lambda app: app._get_field_value("output_formats") in ("parquet", "both"),
            ),
            # Output paths (advanced)
            Field(
                "silver_source_path", "Source Path", "silver",
                help_text="Override Bronze source path (default: auto-wired from Bronze)",
                default=self.state.get_silver_value("source_path") or "",
            ),
            Field(
                "silver_target_path", "Target Path", "silver",
                help_text="Override output directory (default: ./silver/{entity}/)",
                default=self.state.get_silver_value("target_path") or "",
            ),
            Field(
                "silver_partition_by", "Partition By", "silver",
                field_type="list",
                help_text="Columns to partition output by (comma-separated, advanced)",
                default=self._list_to_str(self.state.get_silver_value("partition_by")),
            ),
        ])

        # Add change handlers to all buffers for real-time updates
        for field in self.fields:
            # Store last value for undo tracking
            field._last_value = field.buffer.text
            field.buffer.on_text_changed += lambda buf, f=field: self._on_field_changed_with_undo(buf, f)

        # Set initial dynamic required fields
        self._update_field_requirements()

    def _update_field_requirements(self) -> None:
        """Update field required status based on current state values."""
        if not self.state:
            return

        # Sync current field values to state first
        self._sync_fields_to_state()

        # Get dynamic required fields
        dynamic_required = get_dynamic_required_fields(self.state)
        bronze_required = set(dynamic_required.get("bronze", []))
        silver_required = set(dynamic_required.get("silver", []))

        # Map TUI field names to state field names for dynamic requirements
        # The state uses "username" and "password" but TUI uses context-specific names
        source_type = self._get_field_value("source_type")

        for field in self.fields:
            if field.section == "bronze":
                # Map field names for username/password based on source type
                state_name = field.name
                if source_type and source_type.startswith("database_"):
                    if field.name == "db_username":
                        state_name = "username"
                    elif field.name == "db_password":
                        state_name = "password"
                elif source_type == "api_rest":
                    if field.name == "api_username":
                        state_name = "username"
                    elif field.name == "api_password":
                        state_name = "password"

                field.required = state_name in bronze_required
            elif field.section == "silver":
                field.required = field.name in silver_required

    def _on_field_changed_with_undo(self, buffer: Buffer, field: Field) -> None:
        """Handle field value changes with undo tracking."""
        # Skip if we're in the middle of an undo/redo operation
        if not self._is_undoing:
            old_value = getattr(field, '_last_value', '')
            new_value = buffer.text
            if old_value != new_value:
                # Record change for undo
                self._undo_stack.append((field.name, old_value, new_value))
                # Clear redo stack when new change is made
                self._redo_stack.clear()
                # Update last value
                field._last_value = new_value

        # Call the original handler
        self._on_field_changed(buffer)

    def _on_field_changed(self, buffer: Buffer) -> None:
        """Handle field value changes - update YAML preview and validation."""
        # Update field requirements based on new values
        self._update_field_requirements()

        # Track unsaved changes
        self._has_unsaved_changes = True

        # Refresh layout when app is running
        if self.app:
            self._refresh_layout()

    def _undo(self) -> None:
        """Undo the last field change."""
        if not self._undo_stack:
            self.status_message = "Nothing to undo"
            return

        field_name, old_value, new_value = self._undo_stack.pop()

        # Find the field and restore old value
        for field in self.fields:
            if field.name == field_name:
                self._is_undoing = True
                field.buffer.text = old_value
                field._last_value = old_value
                self._is_undoing = False
                # Push to redo stack
                self._redo_stack.append((field_name, old_value, new_value))
                self.status_message = f"Undone change to {field.label}"
                self._refresh_layout()
                return

        self.status_message = f"Field {field_name} not found"

    def _redo(self) -> None:
        """Redo the last undone field change."""
        if not self._redo_stack:
            self.status_message = "Nothing to redo"
            return

        field_name, old_value, new_value = self._redo_stack.pop()

        # Find the field and restore new value
        for field in self.fields:
            if field.name == field_name:
                self._is_undoing = True
                field.buffer.text = new_value
                field._last_value = new_value
                self._is_undoing = False
                # Push back to undo stack
                self._undo_stack.append((field_name, old_value, new_value))
                self.status_message = f"Redone change to {field.label}"
                self._refresh_layout()
                return

        self.status_message = f"Field {field_name} not found"

    def _is_field_valid(self, field: Field) -> tuple[bool, str]:
        """Check if a specific field is valid.

        Returns:
            Tuple of (is_valid, error_message)
        """
        from pipelines.tui.utils.schema_reader import validate_field_type

        value = field.buffer.text.strip()

        # Check required fields
        if field.required and not value:
            return False, f"{field.label} is required"

        # Check type validation if value is present
        if value:
            error = validate_field_type(field.section, field.name, value)
            if error:
                return False, error

        # Cross-field validation
        if field.name == "history_mode":
            entity_kind = self._get_field_value("entity_kind")
            if entity_kind == "event" and value == "full_history":
                return False, "Events are immutable - use 'Current Only' instead"

        if field.name == "cursor_path":
            pagination = self._get_field_value("pagination_strategy")
            if pagination == "cursor" and not value:
                return False, "Cursor path required for cursor pagination"

        return True, ""

    def _on_search_changed(self, buffer: Buffer) -> None:
        """Handle search text changes."""
        self._search_text = buffer.text.lower()
        self._refresh_layout()

    def _get_source(self, section: str, field_name: str) -> str:
        """Get the source of a field value."""
        fields = self.state.bronze if section == "bronze" else self.state.silver
        if field_name in fields:
            return fields[field_name].source.value
        return "default"

    def _get_field_source(self, field: Field) -> str:
        """Get the value source for a field: 'local', 'parent', 'override', or 'default'.

        - 'parent': Value comes from parent config and hasn't been changed
        - 'override': Value was inherited but user has overridden it
        - 'local': Value was set directly in this config (no parent)
        - 'default': Using schema default value
        """
        if field.section == "metadata":
            return "local" if field.buffer.text.strip() else "default"

        # Get state field info
        state_fields = self.state.bronze if field.section == "bronze" else self.state.silver

        # Map field names for db/api username/password
        state_name = field.name
        if field.name in ("db_username", "api_username"):
            state_name = "username"
        elif field.name in ("db_password", "api_password"):
            state_name = "password"

        if state_name not in state_fields:
            return "default"

        field_val = state_fields[state_name]
        source = field_val.source.value if hasattr(field_val.source, 'value') else str(field_val.source)

        # Check if this is an override (was parent but now has local value different from parent)
        if source == "local" and field_val.parent_value is not None:
            current_value = field.buffer.text.strip()
            parent_value = str(field_val.parent_value) if field_val.parent_value else ""
            if current_value and current_value != parent_value:
                return "override"

        return source

    def _get_field_value(self, name: str) -> str:
        """Get the current value of a field by name."""
        for field in self.fields:
            if field.name == name:
                return field.buffer.text
        return ""

    def _list_to_str(self, value: Any) -> str:
        """Convert a list to comma-separated string."""
        if isinstance(value, list):
            return ", ".join(str(v) for v in value)
        if value:
            return str(value)
        return ""

    def _get_column_mode_default(self) -> str:
        """Determine column_mode based on existing attributes/exclude_columns values."""
        if self.state.get_silver_value("attributes"):
            return "include"
        elif self.state.get_silver_value("exclude_columns"):
            return "exclude"
        return "all"

    def _get_output_format_default(self) -> str:
        """Determine output_formats default from state."""
        value = self.state.get_silver_value("output_formats")
        if isinstance(value, list):
            if "parquet" in value and "csv" in value:
                return "both"
            elif "csv" in value:
                return "csv"
        elif value == "csv":
            return "csv"
        return "parquet"

    def _get_visible_fields(self) -> list[Field]:
        """Get list of currently visible fields, filtered by search."""
        visible = [f for f in self.fields if f.is_visible(self)]

        # Apply search filter if there's search text
        if self._search_text and self.advanced_mode:
            visible = [
                f for f in visible
                if self._search_text in f.name.lower()
                or self._search_text in f.label.lower()
                or self._search_text in f.help_text.lower()
                or self._search_text in f.section.lower()
            ]

        return visible

    def _create_layout(self) -> Layout:
        """Create the application layout."""
        # Build form sections
        form_content = self._build_form_content()

        # Create or update the ScrollablePane for the form
        # We keep the same ScrollablePane instance to preserve scroll position
        form_hsplit = HSplit(form_content)
        if self._scrollable_pane is None:
            # First time - create with scroll offsets to keep focused field in middle
            self._scrollable_pane = ScrollablePane(
                form_hsplit,
                show_scrollbar=True,
                # Keep focused element in middle 50% of screen
                # Large offsets push the focused element toward the center
                scroll_offsets=ScrollOffsets(top=8, bottom=8),
            )
        else:
            # Update content while preserving scroll position
            self._scrollable_pane.content = form_hsplit

        # YAML preview
        yaml_preview = TextArea(
            text=self._generate_yaml_preview(),
            read_only=True,
            scrollbar=True,
            style="class:yaml-preview",
        )

        # Help panel
        help_panel = Window(
            content=FormattedTextControl(self._get_help_text),
            style="class:help-panel",
            height=D(min=3, max=6),
        )

        # Status bar
        status_bar = Window(
            content=FormattedTextControl(self._get_status_bar),
            style="class:status-bar",
            height=1,
        )

        # Title bar with clickable buttons
        title_label = Window(
            content=FormattedTextControl(
                HTML("<b>Bronze-Foundry Pipeline Configurator</b>  ")
            ),
            style="class:title",
            height=1,
            dont_extend_width=True,
        )

        # Mode toggle - show descriptive text and check if can switch
        can_switch_to_basic = self._can_switch_to_basic()
        if self.advanced_mode:
            if can_switch_to_basic:
                mode_text = "Advanced → Basic"
                mode_button = Window(
                    content=ClickableButton(f"📋 {mode_text}", self._toggle_mode),
                    style="class:title",
                    height=1,
                    width=22,
                )
            else:
                # Can't switch back - advanced-only fields have values
                mode_text = "Advanced (locked)"
                mode_button = Window(
                    content=FormattedTextControl(
                        FormattedText([("class:title", f"📋 {mode_text}")])
                    ),
                    style="class:title",
                    height=1,
                    width=22,
                )
        else:
            mode_text = "Basic → Advanced"
            mode_button = Window(
                content=ClickableButton(f"📋 {mode_text}", self._toggle_mode),
                style="class:title",
                height=1,
                width=22,
            )

        # Parent config button - shows "Set Parent" or "Clear Parent" based on state
        if self.state.extends:
            parent_button = Window(
                content=ClickableButton("📁 Clear Parent", self._clear_parent_config),
                style="class:title",
                height=1,
                width=D(min=16, max=20),
            )
        else:
            parent_button = Window(
                content=ClickableButton("📁 Set Parent", self._show_parent_browser),
                style="class:title",
                height=1,
                width=D(min=14, max=18),
            )

        # Env file button - shows "Set Env" or "Clear Env" based on state
        if self.state.loaded_env_files:
            env_count = len(self.state.loaded_env_files)
            env_button = Window(
                content=ClickableButton(f"🔐 Env ({env_count})", self._show_env_browser),
                style="class:title",
                height=1,
                width=D(min=12, max=16),
            )
        else:
            env_button = Window(
                content=ClickableButton("🔐 Set Env", self._show_env_browser),
                style="class:title",
                height=1,
                width=D(min=12, max=14),
            )

        # YAML preview toggle button
        yaml_toggle_text = "◀ YAML" if not self.yaml_preview_collapsed else "▶ YAML"
        yaml_toggle_button = Window(
            content=ClickableButton(yaml_toggle_text, self._toggle_yaml_preview),
            style="class:title",
            height=1,
            width=10,
        )

        load_button = Window(
            content=ClickableButton("📂 Load", self._show_file_browser),
            style="class:title",
            height=1,
            width=10,
        )

        save_button = Window(
            content=ClickableButton("💾 Save", self._save_config),
            style="class:title",
            height=1,
            width=10,
        )

        quit_button = Window(
            content=ClickableButton("❌ Quit", self._quit_app),
            style="class:title",
            height=1,
            width=10,
        )

        # Search bar (only in advanced mode)
        title_bar_items = [
            title_label,
            mode_button,
            Window(width=1, style="class:title"),
            parent_button,
            Window(width=1, style="class:title"),
            env_button,
            Window(width=1, style="class:title"),
            yaml_toggle_button,
            Window(width=1, style="class:title"),
            load_button,
            Window(width=1, style="class:title"),
            save_button,
            Window(width=1, style="class:title"),
            quit_button,
        ]

        if self.advanced_mode:
            # Add search bar
            search_label = Window(
                content=FormattedTextControl(HTML("  🔍 ")),
                style="class:title",
                height=1,
                width=4,
            )
            search_input = Window(
                content=BufferControl(buffer=self._search_buffer),
                style="class:field-input",
                height=1,
                width=D(min=10, max=20),
            )
            title_bar_items.extend([
                search_label,
                search_input,
            ])

        title_bar_items.append(
            Window(
                content=FormattedTextControl(HTML("  ")),
                style="class:title",
                height=1,
            )
        )

        title_bar = VSplit(title_bar_items, height=1)

        # Validation errors panel
        self._update_validation()
        errors_content = self._get_errors_content()

        # Build main content based on YAML preview collapsed state
        if self.yaml_preview_collapsed:
            # YAML preview hidden - form takes full width
            main_content = VSplit([
                # Left side: form (uses persistent ScrollablePane)
                Frame(
                    body=self._scrollable_pane,
                    title=self._get_editor_title(),
                    width=D(weight=1),
                ),
                # Right side: just validation errors (narrow)
                Frame(
                    body=Window(
                        content=FormattedTextControl(errors_content),
                        style="class:error-panel",
                        wrap_lines=True,  # Wrap long error messages
                    ),
                    title="Validation",
                    width=D(min=25, max=35),
                ),
            ])
        else:
            # Right side panel: YAML preview + validation errors
            # Calculate dynamic heights based on warnings count
            warning_count = len(self.validation_errors) + len(self.validation_warnings)
            validation_height = min(12, max(4, warning_count + 3))

            right_panel = HSplit([
                Frame(
                    body=yaml_preview,
                    title=self._get_yaml_preview_title(),
                    height=D(weight=1),  # Take remaining space after validation
                ),
                Frame(
                    body=Window(
                        content=FormattedTextControl(errors_content),
                        style="class:error-panel",
                        wrap_lines=True,  # Wrap long error messages
                    ),
                    title="Validation",
                    # Dynamic height based on errors/warnings, capped
                    height=D(min=4, max=validation_height),
                ),
            ])

            # Main layout: form on left, preview+errors on right
            # Form gets 65% width, YAML preview gets 35%
            main_content = VSplit([
                # Left side: form (uses persistent ScrollablePane)
                Frame(
                    body=self._scrollable_pane,
                    title=self._get_editor_title(),
                    width=D(weight=65),
                ),
                # Right side: preview + errors (smaller weight = less space)
                HSplit([right_panel], width=D(weight=35, min=40, max=60)),
            ])

        # Check if confirmation dialog is active
        if self._confirm_dialog_active and self._confirm_dialog:
            dialog_frame = Frame(
                body=Window(
                    content=self._confirm_dialog,
                    style="class:dialog.body",
                    height=3,
                ),
                title="⚠ Confirm",
                width=D(min=40, max=50),
                height=5,
            )
            dialog_centered = VSplit([
                Window(width=D(weight=1)),  # Left spacer
                dialog_frame,
                Window(width=D(weight=1)),  # Right spacer
            ])
            return Layout(
                HSplit([
                    title_bar,
                    Window(height=D(weight=1)),  # Top spacer
                    dialog_centered,
                    Window(height=D(weight=1)),  # Bottom spacer
                    status_bar,
                ])
            )

        # Check if file browser is active
        if self.file_browser_active and self.file_browser:
            # Create back button
            back_button = Window(
                content=ClickableButton(
                    "← Back to Editor",
                    self._close_file_browser,
                    style="class:button",
                ),
                height=1,
            )
            # Show file browser as overlay with back button
            file_browser_frame = Frame(
                body=HSplit([
                    Window(
                        content=self.file_browser,
                        style="class:file-browser",
                        height=D(min=12, max=22),  # Constrain height so button is visible
                    ),
                    Window(height=1),  # Spacer
                    back_button,
                ]),
                title="📂 Select YAML File  (Esc to cancel)",
                width=D(min=50, max=80),
                height=D(min=18, max=28),
            )
            # Center the file browser in a FloatContainer-like layout
            main_with_browser = VSplit([
                Window(width=D(weight=1)),  # Left spacer
                file_browser_frame,
                Window(width=D(weight=1)),  # Right spacer
            ])
            return Layout(
                HSplit([
                    title_bar,
                    Window(height=D(weight=1)),  # Top spacer
                    main_with_browser,
                    Window(height=D(weight=1)),  # Bottom spacer
                    status_bar,
                ])
            )

        # Check if env file browser is active
        if self.env_browser_active and self.env_browser:
            # Create back button
            env_back_button = Window(
                content=ClickableButton(
                    "← Back to Editor",
                    self._close_env_browser,
                    style="class:button",
                ),
                height=1,
            )
            # Show env file browser as overlay with back button
            env_browser_frame = Frame(
                body=HSplit([
                    Window(
                        content=self.env_browser,
                        style="class:file-browser",
                        height=D(min=12, max=22),  # Constrain height so button is visible
                    ),
                    Window(height=1),  # Spacer
                    env_back_button,
                ]),
                title="🔐 Select Environment File  (Esc to cancel)",
                width=D(min=50, max=80),
                height=D(min=18, max=28),
            )
            # Center the env browser in a FloatContainer-like layout
            main_with_browser = VSplit([
                Window(width=D(weight=1)),  # Left spacer
                env_browser_frame,
                Window(width=D(weight=1)),  # Right spacer
            ])
            return Layout(
                HSplit([
                    title_bar,
                    Window(height=D(weight=1)),  # Top spacer
                    main_with_browser,
                    Window(height=D(weight=1)),  # Bottom spacer
                    status_bar,
                ])
            )

        return Layout(
            HSplit([
                title_bar,
                main_content,
                help_panel,
                status_bar,
            ])
        )

    def _build_form_content(self) -> list:
        """Build the form content with all fields."""
        content = []
        current_section = None
        visible_fields = self._get_visible_fields()
        field_counter = 0

        section_titles = {
            "metadata": "Pipeline Metadata",
            "bronze": "Bronze Layer (Data Extraction)",
            "silver": "Silver Layer (Data Curation)",
        }

        for field in visible_fields:
            # Section header (collapsible for bronze and silver)
            if field.section != current_section:
                current_section = field.section
                is_collapsed = self.sections_collapsed.get(current_section, False)
                collapse_icon = "▶" if is_collapsed else "▼"

                # Create clickable section header
                section_header = ClickableSectionHeader(
                    f"{collapse_icon} {section_titles.get(current_section, current_section)}",
                    current_section,
                    self._toggle_section,
                )

                content.append(Window(height=1))  # Spacer
                content.append(
                    Window(
                        content=section_header,
                        height=1,
                        style="class:section",
                    )
                )
                content.append(Window(height=1))  # Spacer

            # Skip fields if section is collapsed
            if self.sections_collapsed.get(field.section, False):
                continue

            # Field row
            content.append(self._create_field_row(field, field_counter))
            field_counter += 1

        # Add padding at bottom for scrolling (half-screen worth of space)
        # This allows fields at the bottom to be scrolled to the middle of the screen
        for _ in range(15):  # ~15 lines of padding
            content.append(Window(height=1))

        return content

    def _create_field_row(self, field: Field, idx: int) -> HSplit:
        """Create a row for a single field with mouse support and inline help."""
        is_focused = idx == self.current_field_idx

        # Determine the field's value source from state
        field_source = self._get_field_source(field)

        # Label - clickable to select field
        label_parts = []
        if field.required:
            label_parts.append(("class:field-label.required", f"{field.label}* "))
        else:
            label_parts.append(("class:field-label", f"{field.label} "))

        # Check field validity
        is_valid, validation_error = self._is_field_valid(field)

        # Show inheritance/override badges and validation error
        if field_source == "parent":
            label_parts.append(("class:inherited-badge", "[inherited] "))
        elif field_source == "override":
            label_parts.append(("class:override-badge", "[override] "))
        if not is_valid:
            label_parts.append(("class:error", "[!] "))
        if field.is_sensitive:
            label_parts.append(("class:sensitive", "[${...}] "))

        # Value display with styling based on source and validity
        if not is_valid:
            value_style = "class:field-input.invalid-focused" if is_focused else "class:field-input.invalid"
        elif is_focused:
            value_style = "class:field-input.focused"
        elif field_source == "parent":
            value_style = "class:field-input.inherited"
        elif field_source == "override":
            value_style = "class:field-input.overridden"
        else:
            value_style = "class:field-input"

        if field.field_type == "enum":
            # Create or reuse dropdown with proper expand/collapse state
            if not hasattr(field, '_dropdown') or field._dropdown is None:
                dropdown = DropdownMenu(
                    field,
                    self._on_dropdown_select,
                    field_idx=idx,
                    on_focus=self._on_field_click,
                    on_collapse=self._refresh_layout,
                )
                field._dropdown = dropdown
            else:
                dropdown = field._dropdown
                # Update field_idx in case it changed
                dropdown.field_idx = idx

            # Height depends on whether dropdown is expanded
            is_expanded = dropdown.is_expanded()
            dropdown_height = len(field.enum_options) if is_expanded else 1

            # Calculate minimum width for dropdown
            min_width = dropdown.get_min_width()

            label = Window(
                content=ClickableFieldLabel(label_parts, idx, self._on_field_click),
                width=D(min=20, max=25),
                height=dropdown_height,
            )

            value = Window(
                content=dropdown,
                style=value_style,
                height=dropdown_height,
                width=D(min=min_width, weight=2),
            )
        else:
            # Determine height for multiline fields (expand when focused)
            if field.multiline and is_focused:
                # Expanded multiline - show 6 lines when focused for SQL editing
                field_height = 6
            elif field.multiline:
                # Collapsed multiline - show 2 lines to hint at content
                field_height = 2
            else:
                field_height = 1

            # Non-enum field - create label
            label = Window(
                content=ClickableFieldLabel(label_parts, idx, self._on_field_click),
                width=D(min=20, max=25),
                height=1,  # Label stays single line
            )

            # Text input with click handler to update focus
            buffer_control = ClickableBufferControl(
                buffer=field.buffer,
                field_idx=idx,
                on_focus=self._on_field_click,
            )
            value = Window(
                content=buffer_control,
                style=value_style,
                height=field_height,
                cursorline=is_focused,
                width=D(weight=3) if is_focused else D(weight=1),
                wrap_lines=field.multiline,  # Word wrap for multiline
            )

        # For multiline/focused fields, show help above the field instead of beside it
        # This prevents the help text from squeezing the field width
        if is_focused and (field.help_text or not is_valid):
            if not is_valid:
                help_content = FormattedText([("class:error", f"  ⚠ {validation_error}")])
            else:
                help_content = FormattedText([("class:field-help.inline", f"  💡 {field.help_text}")])

            help_line = Window(
                content=FormattedTextControl(help_content),
                height=1,
                wrap_lines=True,  # Wrap long help text
            )
            row = VSplit([label, value], padding=1)
            return HSplit([help_line, row], padding=0)
        else:
            row = VSplit([label, value], padding=1)
            return HSplit([row], padding=0)

    def _on_field_click(self, field_idx: int) -> None:
        """Handle click on a field label."""
        # Collapse any expanded dropdown EXCEPT the one we're clicking on
        self._collapse_all_dropdowns(except_field_idx=field_idx)
        self.current_field_idx = field_idx
        self._refresh_layout()

    def _collapse_all_dropdowns(self, except_field_idx: int | None = None) -> None:
        """Collapse all expanded dropdown menus.

        Args:
            except_field_idx: If provided, don't collapse the dropdown at this field index
        """
        visible_fields = self._get_visible_fields()
        for i, field in enumerate(visible_fields):
            if except_field_idx is not None and i == except_field_idx:
                continue  # Don't collapse the dropdown we're clicking on
            if field.field_type == "enum" and hasattr(field, '_dropdown') and field._dropdown:
                if field._dropdown.is_expanded():
                    field._dropdown.collapse()

    def _on_dropdown_select(self, field: Field, value: str) -> None:
        """Handle selection of a dropdown option."""
        old_value = field.buffer.text
        field.buffer.text = value

        # Only refresh if value changed (for conditional visibility)
        if old_value != value:
            self._refresh_layout()

    def _generate_yaml_preview(self) -> str:
        """Generate YAML preview from current field values."""
        self._sync_fields_to_state()
        try:
            orphaned = self._get_orphaned_fields()
            return self.state.to_yaml(orphaned_fields=orphaned)
        except Exception as e:
            return f"# Error generating YAML: {e}"

    def _get_orphaned_fields(self) -> list:
        """Detect fields that have values but are not currently visible.

        Returns:
            List of OrphanedField objects for fields with values that aren't active
        """
        from pipelines.tui.utils.yaml_generator import OrphanedField

        orphaned: list[OrphanedField] = []

        # Map of visibility conditions to human-readable reasons
        orphan_reasons = {
            # Pagination fields
            "cursor_path": "not used with current pagination strategy",
            "cursor_param": "not used with current pagination strategy",
            "offset_param": "not used with offset pagination",
            "limit_param": "not used with offset pagination",
            "page_param": "not used with page pagination",
            "page_size_param": "not used with page pagination",
            # Auth fields
            "token": "not used with current auth type",
            "api_key": "not used with current auth type",
            "api_key_header": "not used with current auth type",
            "username": "not used with current auth type",
            "password": "not used with current auth type",
            # Source type specific
            "source_path": "not used with current source type",
            "host": "not used with current source type",
            "database": "not used with current source type",
            "query": "not used with current source type",
            "base_url": "not used with current source type",
            "endpoint": "not used with current source type",
            # Column selection
            "attributes": "column mode is not 'include'",
            "exclude_columns": "column mode is not 'exclude'",
            # Load pattern
            "watermark_column": "not used with full_snapshot load pattern",
            "full_refresh_days": "not used with full_snapshot load pattern",
        }

        for field in self.fields:
            # Skip if field is visible
            if field.is_visible(self):
                continue

            # Skip if field has no value
            value = field.buffer.text.strip()
            if not value:
                continue

            # This field has a value but is not visible - it's orphaned
            reason = orphan_reasons.get(field.name, "not applicable with current settings")
            orphaned.append(OrphanedField(
                name=field.name,
                value=value,
                section=field.section,
                reason=reason,
            ))

        return orphaned

    def _sync_fields_to_state(self) -> None:
        """Sync field buffer values to state."""
        for field in self.fields:
            value = field.buffer.text.strip()

            if field.name == "name":
                self.state.name = value
            elif field.name == "description":
                self.state.description = value
            elif field.section == "bronze":
                # Map field names to state field names
                # Handle different username/password contexts (db vs api)
                state_name = field.name
                if field.name == "db_username":
                    state_name = "username"
                elif field.name == "db_password":
                    state_name = "password"
                elif field.name == "api_username":
                    state_name = "username"
                elif field.name == "api_password":
                    state_name = "password"

                if field.field_type == "list":
                    self.state.set_bronze_value(
                        state_name,
                        [v.strip() for v in value.split(",") if v.strip()]
                    )
                else:
                    self.state.set_bronze_value(state_name, value)
            elif field.section == "silver":
                if field.name == "natural_keys":
                    self.state.set_silver_value(
                        field.name,
                        [v.strip() for v in value.split(",") if v.strip()]
                    )
                else:
                    self.state.set_silver_value(field.name, value)

    def _get_help_text(self) -> FormattedText:
        """Get contextual help text for current field with beginner guidance."""
        visible_fields = self._get_visible_fields()
        if 0 <= self.current_field_idx < len(visible_fields):
            field = visible_fields[self.current_field_idx]
            help_parts = [("class:help", f"  {field.help_text}" if field.help_text else "")]

            # Add beginner-friendly guidance for key concepts
            beginner_guidance = self._get_beginner_guidance(field.name)
            if beginner_guidance:
                help_parts.append(("class:help", f"\n  💡 {beginner_guidance}"))

            if field.field_type == "enum":
                options = ", ".join(f"{v}" for v, _ in field.enum_options)
                help_parts.append(("class:help", f"\n  Options: {options}"))

            if field.is_sensitive:
                help_parts.append(("class:help", "\n  🔒 Security: Use ${ENV_VAR} to avoid hardcoding secrets"))

            return FormattedText(help_parts)
        return FormattedText([("class:help", "  Navigate with Tab/Shift+Tab, Enter to edit")])

    def _get_beginner_guidance(self, field_name: str) -> str:
        """Get beginner-friendly guidance for medallion architecture concepts."""
        guidance = {
            # Bronze layer concepts
            "system": "This groups related data (e.g., 'salesforce', 'erp'). Think of it as the data source name.",
            "entity": "The table or dataset name from your source (e.g., 'customers', 'orders').",
            "source_type": "Choose based on your data format. Most common: CSV for files, REST API for web services.",
            "load_pattern": (
                "Full Snapshot: Replace all data each run (simplest). "
                "Incremental: Only load new/changed records (faster for large datasets)."
            ),
            "watermark_column": (
                "A timestamp column (like 'updated_at') that helps identify new/changed records. "
                "Choose a column that changes whenever the record is modified."
            ),
            # Database-specific
            "host": "Server hostname or IP. For SQL Server, include instance name if needed (server\\instance).",
            "database": "The database name to connect to.",
            "query": (
                "The SQL query to extract data. Use {run_date} for date filtering. "
                "Example: SELECT * FROM Orders WHERE ModifiedDate >= '{run_date}'"
            ),
            "db_username": (
                "For Windows auth, use DOMAIN\\username format. "
                "Leave blank for trusted connection (uses current Windows credentials)."
            ),
            "db_password": "Password for the specified user. Leave blank for trusted connection.",
            # API-specific
            "base_url": "The root URL of the API without endpoints (e.g., https://api.company.com).",
            "endpoint": "The API path for your data (e.g., /v1/customers). Can include {placeholders}.",
            "data_path": (
                "If your API wraps data in nested JSON (e.g., {\"data\": {\"items\": [...]}}), "
                "use dot notation to reach it (data.items)."
            ),
            "pagination_strategy": (
                "How to fetch multiple pages of results. Offset=uses skip/limit, "
                "Page=uses page numbers, Cursor=uses next-page tokens."
            ),
            "cursor_path": "Where to find the next-page token in the API response (e.g., meta.next_cursor).",
            # Silver layer concepts
            "natural_keys": (
                "Column(s) that uniquely identify each record. For orders: 'order_id'. "
                "For line items: 'order_id, line_number' (comma-separated for composite keys)."
            ),
            "change_timestamp": (
                "A column showing when the source record last changed. "
                "This helps detect which records need updating."
            ),
            "entity_kind": (
                "State = things that change over time (customers, products, accounts). "
                "Event = immutable facts that happened (orders, transactions, log entries)."
            ),
            "history_mode": (
                "Current Only (SCD1) = keep only latest values, simpler and smaller. "
                "Full History (SCD2) = track all historical changes, needed for auditing."
            ),
        }
        return guidance.get(field_name, "")

    def _get_status_bar(self) -> FormattedText:
        """Get status bar content with keyboard shortcut hints."""
        visible = len(self._get_visible_fields())
        shortcuts = "Tab:Nav  Ctrl+S:Save  Ctrl+Z:Undo  Ctrl+Y:Redo  Ctrl+Q:Quit"
        # Add unsaved indicator
        unsaved = " *" if self._has_unsaved_changes else ""
        return FormattedText([
            ("class:status-bar", f"  {self.status_message}{unsaved}  │  {visible} fields  │  {shortcuts}  ")
        ])

    def _create_bindings(self) -> KeyBindings:
        """Create key bindings."""
        kb = KeyBindings()

        @kb.add("c-q")
        def quit_(event):
            """Quit with confirmation if unsaved changes."""
            self._quit_app()

        @kb.add("c-s")
        def save_(event):
            """Save configuration."""
            self._save_config()

        @kb.add("c-z")
        def undo_(event):
            """Undo last change."""
            self._undo()

        @kb.add("c-y")
        def redo_(event):
            """Redo last undone change."""
            self._redo()

        @kb.add("c-f")
        def search_(event):
            """Focus search field (in advanced mode)."""
            if self.advanced_mode:
                try:
                    self.app.layout.focus(self._search_buffer)
                    self.status_message = "Type to search fields"
                except ValueError:
                    pass

        @kb.add("escape")
        def clear_search_(event):
            """Clear search, collapse dropdowns, and return to first field."""
            # Always collapse any expanded dropdowns
            self._collapse_all_dropdowns()
            if self._search_text:
                self._search_buffer.text = ""
                self._search_text = ""
                self.current_field_idx = 0
            self._refresh_layout()

        @kb.add("tab")
        def next_field_(event):
            """Move to next field."""
            self._collapse_all_dropdowns()
            visible = self._get_visible_fields()
            if visible:
                self.current_field_idx = (self.current_field_idx + 1) % len(visible)
                self._refresh_layout()

        @kb.add("s-tab")
        def prev_field_(event):
            """Move to previous field."""
            self._collapse_all_dropdowns()
            visible = self._get_visible_fields()
            if visible:
                self.current_field_idx = (self.current_field_idx - 1) % len(visible)
                self._refresh_layout()

        @kb.add("up")
        def move_up_(event):
            """Move selection up in dropdown."""
            visible = self._get_visible_fields()
            if 0 <= self.current_field_idx < len(visible):
                field = visible[self.current_field_idx]
                if field.field_type == "enum":
                    current = field.buffer.text
                    options = [v for v, _ in field.enum_options]
                    if current in options:
                        idx = (options.index(current) - 1) % len(options)
                        field.buffer.text = options[idx]
                        self._refresh_layout()

        @kb.add("down")
        def move_down_(event):
            """Move selection down in dropdown."""
            visible = self._get_visible_fields()
            if 0 <= self.current_field_idx < len(visible):
                field = visible[self.current_field_idx]
                if field.field_type == "enum":
                    current = field.buffer.text
                    options = [v for v, _ in field.enum_options]
                    if current in options:
                        idx = (options.index(current) + 1) % len(options)
                        field.buffer.text = options[idx]
                        self._refresh_layout()

        @kb.add("enter")
        def edit_field_(event):
            """Confirm selection or enter edit mode."""
            # Enter now just confirms current selection (no action needed for enum)
            # For text fields, this is default behavior
            pass

        @kb.add("f5")
        def refresh_(event):
            """Refresh YAML preview."""
            self._refresh_layout()

        return kb

    def _focus_current_field(self) -> None:
        """Focus the current field."""
        visible = self._get_visible_fields()
        if 0 <= self.current_field_idx < len(visible):
            field = visible[self.current_field_idx]
            if self.app:
                # Only focus text fields (enum fields use ClickableEnumField, not BufferControl)
                if field.field_type != "enum":
                    try:
                        self.app.layout.focus(field.buffer)
                    except ValueError:
                        # Buffer not in layout, ignore
                        pass

    def _refresh_layout(self) -> None:
        """Refresh the layout to update preview and visibility."""
        if self.app:
            self.app.layout = self._create_layout()
            self._focus_current_field()

    def _save_config(self) -> None:
        """Save the configuration to file."""
        self._sync_fields_to_state()

        # Validate
        errors = self.state.validate()
        if errors:
            self.status_message = f"Errors: {'; '.join(errors[:2])}"
            return

        # Determine save path - filename should match name
        name = self.state.name
        if not name:
            # Derive name from system+entity if not provided
            system = self.state.get_bronze_value("system") or ""
            entity = self.state.get_bronze_value("entity") or ""
            if system and entity:
                name = f"{system}_{entity}"
            else:
                name = "new_pipeline"
            self.state.name = name

        if self.yaml_path:
            # Use existing path's directory but update filename if name changed
            existing_path = Path(self.yaml_path)
            if existing_path.stem != name:
                save_path = existing_path.parent / f"{name}.yaml"
            else:
                save_path = existing_path
        else:
            save_path = Path.cwd() / "pipelines" / f"{name}.yaml"

        # Save
        try:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            yaml_content = self.state.to_yaml()
            save_path.write_text(yaml_content, encoding="utf-8")
            self.status_message = f"Saved to: {save_path}"
            self.yaml_path = str(save_path)
            self._has_unsaved_changes = False
            self._last_saved_yaml = yaml_content
        except Exception as e:
            self.status_message = f"Save failed: {e}"

    def _quit_app(self) -> None:
        """Quit the application with confirmation if there are unsaved changes."""
        if self._has_unsaved_changes:
            self._show_confirm_dialog(
                "You have unsaved changes. Quit anyway?",
                self._force_quit,
                self._cancel_confirm_dialog,
            )
        else:
            self._force_quit()

    def _force_quit(self) -> None:
        """Force quit without checking for unsaved changes."""
        self._confirm_dialog_active = False
        self._confirm_dialog = None
        if self.app:
            self.app.exit()

    def _show_confirm_dialog(
        self,
        message: str,
        on_confirm: Callable[[], None],
        on_cancel: Callable[[], None],
    ) -> None:
        """Show a confirmation dialog."""
        self._confirm_dialog = ConfirmDialog(message, on_confirm, on_cancel)
        self._confirm_dialog_active = True
        self._refresh_layout()

    def _cancel_confirm_dialog(self) -> None:
        """Cancel the confirmation dialog."""
        self._confirm_dialog_active = False
        self._confirm_dialog = None
        self.status_message = "Cancelled"
        self._refresh_layout()

    def _can_switch_to_basic(self) -> bool:
        """Check if we can switch back to Basic mode.

        Returns False if any advanced-only field has a value set.
        """
        for field in self.fields:
            # Skip fields that are visible in basic mode
            if field.is_basic or field.required:
                continue

            # Check if this advanced-only field has a value
            value = field.buffer.text.strip()
            if value:
                return False

        return True

    def _toggle_mode(self) -> None:
        """Toggle between Basic and Advanced mode."""
        # Don't allow switching to basic if advanced fields have values
        if self.advanced_mode and not self._can_switch_to_basic():
            self.status_message = "Cannot switch to Basic: advanced fields have values"
            return

        self.advanced_mode = not self.advanced_mode
        self.current_field_idx = 0  # Reset to first field
        self.status_message = f"Switched to {'Advanced' if self.advanced_mode else 'Basic'} mode"
        self._refresh_layout()

    def _toggle_section(self, section: str) -> None:
        """Toggle collapse state of a section."""
        self.sections_collapsed[section] = not self.sections_collapsed.get(section, False)
        self._refresh_layout()

    def _toggle_yaml_preview(self) -> None:
        """Toggle YAML preview panel visibility."""
        self.yaml_preview_collapsed = not self.yaml_preview_collapsed
        self.status_message = "YAML preview " + ("hidden" if self.yaml_preview_collapsed else "shown")
        self._refresh_layout()

    def _clear_parent_config(self) -> None:
        """Clear the parent configuration (remove inheritance)."""
        self.state.extends = None
        self.state.parent_config = None

        # Reset field sources to local/default
        for field_name, field_val in self.state.bronze.items():
            if field_val.source.value == "parent":
                field_val.source = field_val.source.__class__("default")
            field_val.parent_value = None

        for field_name, field_val in self.state.silver.items():
            if field_val.source.value == "parent":
                field_val.source = field_val.source.__class__("default")
            field_val.parent_value = None

        self.status_message = "Parent config cleared"
        self._refresh_layout()

    def _get_editor_title(self) -> str:
        """Get the title for the editor frame (shows current file only)."""
        if self.yaml_path:
            return f"Configuration - {Path(self.yaml_path).name}"
        return "Configuration - New Pipeline"

    def _get_yaml_preview_title(self) -> str:
        """Get the title for the YAML preview (shows parent info if inherited)."""
        if self.state.extends:
            return f"YAML Preview (extends: {Path(self.state.extends).name})"
        return "YAML Preview"

    def _show_parent_browser(self) -> None:
        """Show file browser to select a parent YAML file for inheritance."""
        self.file_browser = FileBrowserControl(
            on_select=self._on_parent_selected,
            on_cancel=self._close_file_browser,
            initial_path=Path.cwd(),
        )
        self.file_browser_active = True
        self.status_message = "Select parent config: ↑↓ Navigate | Enter: Select | Esc: Cancel"
        self._refresh_layout()

    def _on_parent_selected(self, path: Path) -> None:
        """Handle parent file selection."""
        self.file_browser_active = False
        self.file_browser = None

        try:
            # Set the parent config
            self.state.set_parent_config(path)
            self.status_message = f"Inheriting from: {path.name}"

            # Refresh fields to show inherited values
            self.fields = []
            self._create_fields()
            self.current_field_idx = 0
            self._refresh_layout()
        except Exception as e:
            self.status_message = f"Failed to load parent: {e}"
            self._refresh_layout()

    def _show_file_browser(self) -> None:
        """Show interactive file browser to load a YAML file."""
        self.file_browser = FileBrowserControl(
            on_select=self._on_file_selected,
            on_cancel=self._close_file_browser,
            initial_path=Path.cwd(),
        )
        self.file_browser_active = True
        self.status_message = "↑↓: Navigate | Enter: Select | Backspace: Go up | Esc: Cancel"
        self._refresh_layout()

    def _on_file_selected(self, path: Path) -> None:
        """Handle file selection from browser."""
        self.file_browser_active = False
        self.file_browser = None
        self._load_yaml_file(path)

    def _close_file_browser(self) -> None:
        """Close the file browser without selecting."""
        self.file_browser_active = False
        self.file_browser = None
        self.status_message = "File selection cancelled"
        self._refresh_layout()

    def _show_env_browser(self) -> None:
        """Show file browser to select an environment file."""
        # Discover env files to highlight in the browser
        discovered = self.state.discover_env_files() if self.state else []
        self.env_browser = EnvFileBrowserControl(
            on_select=self._on_env_file_selected,
            on_cancel=self._close_env_browser,
            initial_path=Path.cwd(),
            discovered_files=discovered,
        )
        self.env_browser_active = True
        self.status_message = "Select .env file: ↑↓ Navigate | Enter: Select | Esc: Cancel"
        self._refresh_layout()

    def _on_env_file_selected(self, path: Path) -> None:
        """Handle env file selection."""
        self.env_browser_active = False
        self.env_browser = None

        try:
            # Load the env file
            new_vars = self.state.load_env_file(str(path))
            if new_vars:
                self.status_message = f"Loaded {path.name}: {len(new_vars)} new vars available"
            else:
                self.status_message = f"Loaded {path.name} (no new vars)"
            self._refresh_layout()
        except Exception as e:
            self.status_message = f"Failed to load env file: {e}"
            self._refresh_layout()

    def _close_env_browser(self) -> None:
        """Close the env file browser without selecting."""
        self.env_browser_active = False
        self.env_browser = None
        self.status_message = "Env file selection cancelled"
        self._refresh_layout()

    def _load_yaml_file(self, path: Path) -> None:
        """Load a YAML file into the editor."""
        try:
            self.state = PipelineState.from_yaml(path)
            self.yaml_path = str(path)
            self.mode = "edit"

            # Auto-populate name from filename if not already set
            if not self.state.name:
                # Remove extension to get pipeline name
                self.state.name = path.stem

            self.fields = []
            self._create_fields()
            self.current_field_idx = 0
            self.status_message = f"Loaded: {path.name}"
            self._refresh_layout()
        except Exception as e:
            self.status_message = f"Load failed: {e}"

    def _update_validation(self) -> None:
        """Update validation errors and warnings lists."""
        self._sync_fields_to_state()

        # Update dynamic required fields - delegate to _update_field_requirements
        self._update_field_requirements()

        self.validation_errors = self.state.validate() if self.state else []
        self.validation_warnings = self._get_best_practice_warnings()

    def _get_best_practice_warnings(self) -> list[str]:
        """Get best practice warnings based on current configuration."""
        warnings: list[str] = []

        # Check for snappy compression (recommend zstd instead)
        compression = self._get_field_value("parquet_compression")
        if compression == "snappy":
            warnings.append(
                "Compression: Snappy is fast but has poor compression ratio. "
                "Consider 'zstd' for better compression with similar speed."
            )

        # Check for CSV output (recommend parquet)
        output_format = self._get_field_value("output_formats")
        if output_format in ("csv", "both"):
            warnings.append(
                "Output format: CSV loses type information and is slower to query. "
                "Use Parquet unless downstream systems require CSV."
            )

        # Check for full_history mode with event entities (doesn't make sense)
        entity_kind = self._get_field_value("entity_kind")
        history_mode = self._get_field_value("history_mode")
        if entity_kind == "event" and history_mode == "full_history":
            warnings.append(
                "History mode: Events are immutable, so 'full_history' (SCD2) "
                "doesn't make sense. Consider 'current_only' for events."
            )

        # Check for undefined environment variables
        warnings.extend(self._get_env_var_warnings())

        return warnings

    def _get_env_var_warnings(self) -> list[str]:
        """Check for environment variable issues in field values."""
        import re
        warnings: list[str] = []
        env_var_pattern = re.compile(r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}')

        # Track fields using env vars and which vars are missing
        fields_with_env_vars = []
        missing_vars: set[str] = set()

        for field in self.fields:
            value = field.buffer.text
            if not value:
                continue

            matches = env_var_pattern.findall(value)
            if matches:
                fields_with_env_vars.append(field.name)
                for var_name in matches:
                    if var_name not in self.state.available_env_vars:
                        missing_vars.add(var_name)

        # Warn about missing env vars
        if missing_vars:
            if len(missing_vars) == 1:
                var_name = next(iter(missing_vars))
                warnings.append(
                    f"Environment: ${{{var_name}}} not found. "
                    "Use 'Set Env' button to load a .env file."
                )
            else:
                var_list = ", ".join(f"${{{v}}}" for v in sorted(missing_vars)[:3])
                if len(missing_vars) > 3:
                    var_list += f" (+{len(missing_vars) - 3} more)"
                warnings.append(
                    f"Environment: {var_list} not found. "
                    "Use 'Set Env' button to load a .env file."
                )

        # Subtle hint if using env vars but no env file loaded
        elif fields_with_env_vars and not self.state.loaded_env_files:
            warnings.append(
                "Hint: Using env vars but no .env file loaded. "
                "Consider using 'Set Env' to verify vars are defined."
            )

        return warnings

    def _get_errors_content(self) -> FormattedText:
        """Get formatted validation errors and warnings content."""
        parts: list[tuple[str, str]] = []

        # Show errors first
        if self.validation_errors:
            parts.append(("class:error-panel.header", "  Errors:\n"))
            for error in self.validation_errors[:3]:  # Show max 3 errors
                parts.append(("class:error-panel", f"    • {error}\n"))
            if len(self.validation_errors) > 3:
                parts.append(("class:error-panel", f"    ... and {len(self.validation_errors) - 3} more\n"))
            parts.append(("", "\n"))

        # Show warnings
        if self.validation_warnings:
            parts.append(("class:warning-panel.header", "  Warnings:\n"))
            for warning in self.validation_warnings[:3]:  # Show max 3 warnings
                parts.append(("class:warning-panel", f"    ⚠ {warning}\n"))
            if len(self.validation_warnings) > 3:
                parts.append(("class:warning-panel", f"    ... and {len(self.validation_warnings) - 3} more\n"))

        # Show success if no errors (warnings are ok)
        if not self.validation_errors:
            if self.validation_warnings:
                parts.insert(0, ("class:error-panel.ok", "  ✓ Valid (with warnings)\n\n"))
            else:
                parts.append(("class:error-panel.ok", "  ✓ No validation errors"))

        return FormattedText(parts)


def run_create_wizard(parent_path: str | None = None) -> None:
    """Run the TUI wizard for creating a new pipeline."""
    app = PipelineConfigApp(mode="create", parent_path=parent_path)
    app.run()


def run_editor(yaml_path: str) -> None:
    """Run the TUI editor for an existing pipeline."""
    if not Path(yaml_path).exists():
        raise FileNotFoundError(f"Configuration file not found: {yaml_path}")
    app = PipelineConfigApp(mode="edit", yaml_path=yaml_path)
    app.run()


def main() -> None:
    """Main entry point for the TUI application."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Bronze-Foundry Pipeline Configuration Editor",
    )
    parser.add_argument(
        "yaml_path",
        nargs="?",
        help="Path to YAML file to edit",
    )
    parser.add_argument(
        "--extends", "-e",
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
