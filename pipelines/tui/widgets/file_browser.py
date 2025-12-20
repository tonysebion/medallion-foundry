"""Filesystem browser widget for selecting YAML and .env files."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Button, Input, Label, ListItem, ListView, Static


class FileBrowser(Vertical):
    """Filesystem browser widget for selecting files.

    Features:
    - Directory navigation with breadcrumbs
    - File type filtering (.yaml, .yml, .env)
    - Keyboard navigation (Enter to select, Backspace to go up)
    - Recent files list (optional)

    Attributes:
        current_path: Currently displayed directory
        selected_file: Currently selected file path (if any)
        extensions: List of allowed file extensions
    """

    DEFAULT_CSS = """
    FileBrowser {
        height: 100%;
        border: round $primary;
        padding: 1;
    }

    FileBrowser .breadcrumbs {
        height: auto;
        background: $surface;
        padding: 0 1;
        margin-bottom: 1;
    }

    FileBrowser .breadcrumb-item {
        color: $primary;
    }

    FileBrowser .breadcrumb-sep {
        color: $text-muted;
        margin: 0 1;
    }

    FileBrowser .file-list {
        height: 1fr;
        border: tall $surface;
    }

    FileBrowser .file-item {
        height: auto;
        padding: 0 1;
    }

    FileBrowser .file-item.directory {
        color: $primary;
        text-style: bold;
    }

    FileBrowser .file-item.file {
        color: $text;
    }

    FileBrowser .file-item.yaml {
        color: $success;
    }

    FileBrowser .file-item.env {
        color: $warning;
    }

    FileBrowser .file-item.parent-dir {
        color: $text-muted;
    }

    FileBrowser .path-input-row {
        height: auto;
        margin-top: 1;
    }

    FileBrowser .path-input {
        width: 1fr;
    }

    FileBrowser .button-row {
        height: auto;
        margin-top: 1;
        align: right middle;
    }

    FileBrowser Button {
        margin-left: 1;
    }

    FileBrowser .empty-message {
        color: $text-muted;
        text-align: center;
        padding: 2;
    }
    """

    current_path: reactive[Path] = reactive(Path.cwd)
    selected_file: reactive[Path | None] = reactive(None)

    class FileSelected(Message):
        """Posted when a file is selected."""

        def __init__(self, path: Path) -> None:
            super().__init__()
            self.path = path

    class Cancelled(Message):
        """Posted when the user cancels file selection."""

        pass

    def __init__(
        self,
        start_path: Path | None = None,
        extensions: list[str] | None = None,
        title: str = "Select File",
        allow_new: bool = False,
        show_hidden: bool = False,
        on_select: Callable[[Path], None] | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the file browser.

        Args:
            start_path: Initial directory to display (defaults to cwd)
            extensions: Allowed file extensions (e.g., [".yaml", ".yml"])
            title: Title displayed at top of browser
            allow_new: Whether to allow entering a new filename
            show_hidden: Whether to show hidden files/directories
            on_select: Callback function when file is selected
            id: Widget ID
            classes: CSS classes
        """
        super().__init__(id=id, classes=classes)
        self.start_path = start_path or Path.cwd()
        self.extensions = extensions or [".yaml", ".yml"]
        self.title = title
        self.allow_new = allow_new
        self.show_hidden = show_hidden
        self.on_select_callback = on_select

        # Set initial path
        self.current_path = self.start_path.resolve()

    def compose(self) -> ComposeResult:
        """Compose the file browser layout."""
        yield Label(self.title, classes="title")

        # Breadcrumbs
        yield Horizontal(id="breadcrumbs", classes="breadcrumbs")

        # File list
        yield ListView(id="file-list", classes="file-list")

        # Path input (for direct entry or new file)
        with Horizontal(classes="path-input-row"):
            yield Input(
                placeholder="Enter path or select from list above",
                id="path-input",
                classes="path-input",
            )

        # Buttons
        with Horizontal(classes="button-row"):
            yield Button("Cancel", id="btn-cancel", variant="default")
            yield Button("Select", id="btn-select", variant="primary")

    def on_mount(self) -> None:
        """Initialize the browser when mounted."""
        self._refresh_view()

    def watch_current_path(self, new_path: Path) -> None:
        """React to path changes."""
        self._refresh_view()

    def _refresh_view(self) -> None:
        """Refresh the breadcrumbs and file list."""
        self._update_breadcrumbs()
        self._update_file_list()

    def _update_breadcrumbs(self) -> None:
        """Update the breadcrumb navigation."""
        breadcrumbs = self.query_one("#breadcrumbs", Horizontal)
        breadcrumbs.remove_children()

        parts = self.current_path.parts
        for i, part in enumerate(parts):
            # Create path up to this part
            partial_path = Path(*parts[: i + 1])

            # Breadcrumb button/label
            crumb = Static(part, classes="breadcrumb-item")
            crumb.data_path = str(partial_path)
            breadcrumbs.mount(crumb)

            # Separator (except for last)
            if i < len(parts) - 1:
                breadcrumbs.mount(Static("/", classes="breadcrumb-sep"))

    def _update_file_list(self) -> None:
        """Update the file list for current directory."""
        file_list = self.query_one("#file-list", ListView)
        file_list.clear()

        try:
            entries = sorted(
                self.current_path.iterdir(),
                key=lambda p: (not p.is_dir(), p.name.lower()),
            )
        except PermissionError:
            file_list.mount(
                ListItem(Static("Permission denied", classes="empty-message"))
            )
            return
        except FileNotFoundError:
            file_list.mount(
                ListItem(Static("Directory not found", classes="empty-message"))
            )
            return

        # Parent directory entry (if not at root)
        if self.current_path.parent != self.current_path:
            item = ListItem(
                Static(".. (parent directory)", classes="file-item parent-dir"),
                id="item-parent",
            )
            item.data_path = str(self.current_path.parent)
            item.data_is_dir = True
            file_list.mount(item)

        # Filter and display entries
        for entry in entries:
            # Skip hidden files unless requested
            if not self.show_hidden and entry.name.startswith("."):
                continue

            # For files, check extension filter
            if entry.is_file():
                if self.extensions and entry.suffix.lower() not in self.extensions:
                    continue

            # Determine styling
            if entry.is_dir():
                css_class = "file-item directory"
                display_name = f"[DIR] {entry.name}"
            elif entry.suffix.lower() in (".yaml", ".yml"):
                css_class = "file-item yaml"
                display_name = entry.name
            elif entry.suffix.lower() == ".env" or entry.name.startswith(".env"):
                css_class = "file-item env"
                display_name = entry.name
            else:
                css_class = "file-item file"
                display_name = entry.name

            item = ListItem(
                Static(display_name, classes=css_class),
                id=f"item-{entry.name}",
            )
            item.data_path = str(entry)
            item.data_is_dir = entry.is_dir()
            file_list.mount(item)

        if not file_list.children:
            file_list.mount(
                ListItem(
                    Static("No matching files found", classes="empty-message")
                )
            )

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Handle item selection in the file list."""
        item = event.item
        if not hasattr(item, "data_path"):
            return

        path = Path(item.data_path)

        if hasattr(item, "data_is_dir") and item.data_is_dir:
            # Navigate to directory
            self.current_path = path.resolve()
        else:
            # Select file
            self.selected_file = path
            self._update_path_input(path)

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        """Handle item highlight (for preview)."""
        item = event.item
        if item and hasattr(item, "data_path"):
            path = Path(item.data_path)
            if not (hasattr(item, "data_is_dir") and item.data_is_dir):
                self._update_path_input(path)

    def _update_path_input(self, path: Path) -> None:
        """Update the path input field."""
        path_input = self.query_one("#path-input", Input)
        path_input.value = str(path)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "btn-cancel":
            self.post_message(self.Cancelled())
        elif event.button.id == "btn-select":
            self._confirm_selection()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter key in path input."""
        if event.input.id == "path-input":
            self._confirm_selection()

    def _confirm_selection(self) -> None:
        """Confirm the current selection."""
        path_input = self.query_one("#path-input", Input)
        path_str = path_input.value.strip()

        if not path_str:
            self.notify("Please enter or select a file path", severity="warning")
            return

        path = Path(path_str)

        # Handle relative paths
        if not path.is_absolute():
            path = self.current_path / path

        path = path.resolve()

        # Validate the path
        if self.allow_new:
            # For new files, just check parent exists
            if not path.parent.exists():
                self.notify(f"Directory does not exist: {path.parent}", severity="error")
                return
        else:
            # For existing files, check file exists
            if not path.exists():
                self.notify(f"File not found: {path}", severity="error")
                return

            if path.is_dir():
                # Navigate to directory instead of selecting
                self.current_path = path
                return

        # Check extension
        if self.extensions and path.suffix.lower() not in self.extensions:
            allowed = ", ".join(self.extensions)
            self.notify(f"File must be one of: {allowed}", severity="error")
            return

        # Success - post message and call callback
        self.selected_file = path
        self.post_message(self.FileSelected(path))

        if self.on_select_callback:
            self.on_select_callback(path)

    def on_static_click(self, event: Static.Click) -> None:
        """Handle clicks on breadcrumb items."""
        if event.static.has_class("breadcrumb-item"):
            if hasattr(event.static, "data_path"):
                self.current_path = Path(event.static.data_path)

    def navigate_up(self) -> None:
        """Navigate to parent directory."""
        if self.current_path.parent != self.current_path:
            self.current_path = self.current_path.parent

    def navigate_to(self, path: Path) -> None:
        """Navigate to a specific directory."""
        if path.is_dir():
            self.current_path = path.resolve()
        elif path.is_file():
            self.current_path = path.parent.resolve()
            self.selected_file = path


class FileBrowserModal(Vertical):
    """Modal dialog version of FileBrowser.

    Use this when you want to show the file browser as a modal overlay.
    """

    DEFAULT_CSS = """
    FileBrowserModal {
        width: 80%;
        height: 80%;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }

    FileBrowserModal .modal-title {
        text-style: bold;
        color: $primary;
        text-align: center;
        margin-bottom: 1;
    }
    """

    def __init__(
        self,
        extensions: list[str] | None = None,
        title: str = "Select File",
        start_path: Path | None = None,
        on_select: Callable[[Path], None] | None = None,
        on_cancel: Callable[[], None] | None = None,
        id: str | None = None,
    ) -> None:
        super().__init__(id=id)
        self.extensions = extensions
        self.title = title
        self.start_path = start_path
        self.on_select_callback = on_select
        self.on_cancel_callback = on_cancel

    def compose(self) -> ComposeResult:
        yield Static(self.title, classes="modal-title")
        yield FileBrowser(
            extensions=self.extensions,
            start_path=self.start_path,
            title="",
            id="file-browser",
        )

    def on_file_browser_file_selected(self, event: FileBrowser.FileSelected) -> None:
        """Handle file selection."""
        if self.on_select_callback:
            self.on_select_callback(event.path)
        self.remove()

    def on_file_browser_cancelled(self, event: FileBrowser.Cancelled) -> None:
        """Handle cancellation."""
        if self.on_cancel_callback:
            self.on_cancel_callback()
        self.remove()
