"""Environment variable picker widget for sensitive fields."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Button, Input, Label, ListItem, ListView, Static

from pipelines.tui.widgets.file_browser import FileBrowser


class EnvVarPicker(Vertical):
    """Widget for selecting environment variables.

    Features:
    - Auto-discover .env files in project root and environments/ directory
    - Browse for additional .env files
    - List available environment variables
    - Insert ${VAR_NAME} reference
    - Show which vars are defined vs undefined

    Attributes:
        available_vars: Set of available environment variable names
        loaded_files: List of loaded .env file paths
    """

    DEFAULT_CSS = """
    EnvVarPicker {
        height: 100%;
        border: round $primary;
        padding: 1;
    }

    EnvVarPicker .section-title {
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
    }

    EnvVarPicker .env-file-list {
        height: auto;
        max-height: 6;
        border: tall $surface;
        margin-bottom: 1;
    }

    EnvVarPicker .env-file-item {
        height: auto;
        padding: 0 1;
        color: $text-muted;
    }

    EnvVarPicker .env-file-item.loaded {
        color: $success;
    }

    EnvVarPicker .var-list {
        height: 1fr;
        border: tall $surface;
    }

    EnvVarPicker .var-item {
        height: auto;
        padding: 0 1;
    }

    EnvVarPicker .var-item.defined {
        color: $success;
    }

    EnvVarPicker .var-item.undefined {
        color: $warning;
    }

    EnvVarPicker .search-row {
        height: auto;
        margin-bottom: 1;
    }

    EnvVarPicker .search-input {
        width: 1fr;
    }

    EnvVarPicker .button-row {
        height: auto;
        margin-top: 1;
        align: right middle;
    }

    EnvVarPicker Button {
        margin-left: 1;
    }

    EnvVarPicker .help-text {
        color: $text-muted;
        text-align: center;
        padding: 1;
    }

    EnvVarPicker .custom-var-row {
        height: auto;
        margin-top: 1;
    }
    """

    available_vars: reactive[set[str]] = reactive(set)
    filter_text: reactive[str] = reactive("")

    class VarSelected(Message):
        """Posted when an environment variable is selected."""

        def __init__(self, var_name: str, reference: str) -> None:
            super().__init__()
            self.var_name = var_name
            self.reference = reference  # "${VAR_NAME}"

    class Cancelled(Message):
        """Posted when the user cancels."""

        pass

    def __init__(
        self,
        env_files: list[Path] | None = None,
        show_browser: bool = True,
        title: str = "Select Environment Variable",
        on_select: Callable[[str], None] | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        """Initialize the environment variable picker.

        Args:
            env_files: Pre-discovered .env files to show
            show_browser: Whether to show the "Browse..." button
            title: Title for the picker
            on_select: Callback when a variable is selected
            id: Widget ID
            classes: CSS classes
        """
        super().__init__(id=id, classes=classes)
        self.env_files = env_files or []
        self.show_browser = show_browser
        self.title = title
        self.on_select_callback = on_select
        self.loaded_files: list[Path] = []

        # Initialize with current environment
        self.available_vars = set(os.environ.keys())

    def compose(self) -> ComposeResult:
        """Compose the picker layout."""
        yield Label(self.title, classes="section-title")

        # Environment file section
        yield Static("Loaded .env Files:", classes="section-title")
        yield ListView(id="env-file-list", classes="env-file-list")

        with Horizontal(classes="button-row"):
            yield Button("Browse for .env", id="btn-browse", variant="default")
            yield Button("Discover Files", id="btn-discover", variant="default")

        # Search/filter
        with Horizontal(classes="search-row"):
            yield Input(
                placeholder="Filter variables...",
                id="filter-input",
                classes="search-input",
            )

        # Variable list
        yield Static("Available Variables:", classes="section-title")
        yield ListView(id="var-list", classes="var-list")

        # Custom variable entry
        with Horizontal(classes="custom-var-row"):
            yield Input(
                placeholder="Or enter custom variable name",
                id="custom-var-input",
            )
            yield Button("Use", id="btn-use-custom", variant="default")

        # Action buttons
        with Horizontal(classes="button-row"):
            yield Button("Cancel", id="btn-cancel", variant="default")

    def on_mount(self) -> None:
        """Initialize when mounted."""
        # Load any pre-specified env files
        for env_file in self.env_files:
            self._load_env_file(env_file)

        self._update_file_list()
        self._update_var_list()

    def watch_filter_text(self, new_filter: str) -> None:
        """React to filter changes."""
        self._update_var_list()

    def _load_env_file(self, path: Path) -> None:
        """Load environment variables from a .env file."""
        if not path.exists():
            return

        try:
            from pipelines.lib.env import load_env_file

            load_env_file(str(path), override=False)
            if path not in self.loaded_files:
                self.loaded_files.append(path)

            # Update available vars
            self.available_vars = set(os.environ.keys())
            self.notify(f"Loaded: {path.name}", severity="information")

        except Exception as e:
            self.notify(f"Failed to load {path.name}: {e}", severity="error")

    def _update_file_list(self) -> None:
        """Update the list of .env files."""
        file_list = self.query_one("#env-file-list", ListView)
        file_list.clear()

        if not self.loaded_files and not self.env_files:
            file_list.mount(
                ListItem(Static("No .env files loaded", classes="help-text"))
            )
            return

        # Show discovered (not yet loaded) files
        for env_file in self.env_files:
            is_loaded = env_file in self.loaded_files
            css_class = "env-file-item loaded" if is_loaded else "env-file-item"
            status = " (loaded)" if is_loaded else ""

            item = ListItem(
                Static(f"{env_file.name}{status}", classes=css_class),
                id=f"file-{env_file.name}",
            )
            item.data_path = str(env_file)
            file_list.mount(item)

    def _update_var_list(self) -> None:
        """Update the list of environment variables."""
        var_list = self.query_one("#var-list", ListView)
        var_list.clear()

        # Get and filter variables
        filter_lower = self.filter_text.lower()
        filtered_vars = sorted(
            var
            for var in self.available_vars
            if filter_lower in var.lower()
        )

        if not filtered_vars:
            var_list.mount(
                ListItem(Static("No matching variables", classes="help-text"))
            )
            return

        # Show up to 100 variables to avoid performance issues
        for var_name in filtered_vars[:100]:
            # Check if variable is actually defined (not empty)
            value = os.environ.get(var_name, "")
            is_defined = bool(value)
            css_class = "var-item defined" if is_defined else "var-item undefined"

            # Show preview of value (masked for sensitive vars)
            if is_defined:
                if self._is_sensitive_var(var_name):
                    preview = "***"
                else:
                    preview = value[:20] + "..." if len(value) > 20 else value
                display = f"{var_name} = {preview}"
            else:
                display = f"{var_name} (undefined)"

            item = ListItem(
                Static(display, classes=css_class),
                id=f"var-{var_name}",
            )
            item.data_var_name = var_name
            var_list.mount(item)

        if len(filtered_vars) > 100:
            var_list.mount(
                ListItem(
                    Static(
                        f"...and {len(filtered_vars) - 100} more. Use filter to narrow.",
                        classes="help-text",
                    )
                )
            )

    def _is_sensitive_var(self, var_name: str) -> bool:
        """Check if a variable name suggests sensitive content."""
        sensitive_keywords = [
            "password",
            "secret",
            "key",
            "token",
            "credential",
            "auth",
            "api_key",
            "apikey",
        ]
        var_lower = var_name.lower()
        return any(kw in var_lower for kw in sensitive_keywords)

    def on_input_changed(self, event: Input.Changed) -> None:
        """Handle input changes."""
        if event.input.id == "filter-input":
            self.filter_text = event.value

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter key in inputs."""
        if event.input.id == "custom-var-input":
            self._use_custom_var()
        elif event.input.id == "filter-input":
            # Select first visible var if any
            var_list = self.query_one("#var-list", ListView)
            if var_list.children:
                first_item = var_list.children[0]
                if hasattr(first_item, "data_var_name"):
                    self._select_var(first_item.data_var_name)

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Handle item selection."""
        item = event.item

        # Check if it's a var item
        if hasattr(item, "data_var_name"):
            self._select_var(item.data_var_name)

        # Check if it's an env file item
        if hasattr(item, "data_path"):
            path = Path(item.data_path)
            if path not in self.loaded_files:
                self._load_env_file(path)
                self._update_file_list()
                self._update_var_list()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "btn-cancel":
            self.post_message(self.Cancelled())
        elif event.button.id == "btn-browse":
            self._show_file_browser()
        elif event.button.id == "btn-discover":
            self._discover_env_files()
        elif event.button.id == "btn-use-custom":
            self._use_custom_var()

    def _select_var(self, var_name: str) -> None:
        """Select a variable and post message."""
        reference = f"${{{var_name}}}"
        self.post_message(self.VarSelected(var_name, reference))

        if self.on_select_callback:
            self.on_select_callback(reference)

    def _use_custom_var(self) -> None:
        """Use the custom variable name from input."""
        custom_input = self.query_one("#custom-var-input", Input)
        var_name = custom_input.value.strip()

        if not var_name:
            self.notify("Please enter a variable name", severity="warning")
            return

        # Remove ${ } if user included them
        var_name = var_name.replace("${", "").replace("}", "").replace("$", "")

        if not var_name.replace("_", "").isalnum():
            self.notify(
                "Variable names should be alphanumeric with underscores",
                severity="warning",
            )
            return

        self._select_var(var_name)

    def _show_file_browser(self) -> None:
        """Show file browser for .env files."""
        # This would typically push a modal screen
        # For now, we'll just notify - the parent screen should handle this
        self.notify(
            "Use the file browser to select a .env file",
            severity="information",
        )

    def _discover_env_files(self) -> None:
        """Auto-discover .env files in common locations."""
        discovered: list[Path] = []

        # Check current directory
        cwd = Path.cwd()
        for pattern in [".env", "*.env", ".env.*"]:
            discovered.extend(cwd.glob(pattern))

        # Check environments/ directory
        env_dirs = [
            cwd / "environments",
            Path(__file__).parent.parent.parent.parent / "environments",
        ]
        for env_dir in env_dirs:
            if env_dir.exists():
                discovered.extend(env_dir.glob("*.env"))
                discovered.extend(env_dir.glob(".env*"))
                # Also check for .yaml files that might contain env references
                for yaml_file in env_dir.glob("*.yaml"):
                    if yaml_file.name not in ("README.md",):
                        # These are config files, not env files
                        pass

        # Deduplicate
        seen: set[Path] = set()
        unique: list[Path] = []
        for p in discovered:
            resolved = p.resolve()
            if resolved not in seen and resolved.is_file():
                seen.add(resolved)
                unique.append(resolved)

        # Update env_files list
        for path in unique:
            if path not in self.env_files:
                self.env_files.append(path)

        self._update_file_list()
        self.notify(f"Found {len(unique)} .env files", severity="information")

    def add_env_file(self, path: Path) -> None:
        """Add and load an .env file."""
        if path not in self.env_files:
            self.env_files.append(path)
        self._load_env_file(path)
        self._update_file_list()
        self._update_var_list()


class EnvVarPickerModal(Vertical):
    """Modal dialog version of EnvVarPicker."""

    DEFAULT_CSS = """
    EnvVarPickerModal {
        width: 60%;
        height: 80%;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }

    EnvVarPickerModal .modal-title {
        text-style: bold;
        color: $primary;
        text-align: center;
        margin-bottom: 1;
    }
    """

    def __init__(
        self,
        env_files: list[Path] | None = None,
        on_select: Callable[[str], None] | None = None,
        on_cancel: Callable[[], None] | None = None,
        id: str | None = None,
    ) -> None:
        super().__init__(id=id)
        self.env_files = env_files or []
        self.on_select_callback = on_select
        self.on_cancel_callback = on_cancel

    def compose(self) -> ComposeResult:
        yield Static("Select Environment Variable", classes="modal-title")
        yield EnvVarPicker(
            env_files=self.env_files,
            title="",
            id="env-picker",
        )

    def on_env_var_picker_var_selected(self, event: EnvVarPicker.VarSelected) -> None:
        """Handle variable selection."""
        if self.on_select_callback:
            self.on_select_callback(event.reference)
        self.remove()

    def on_env_var_picker_cancelled(self, event: EnvVarPicker.Cancelled) -> None:
        """Handle cancellation."""
        if self.on_cancel_callback:
            self.on_cancel_callback()
        self.remove()
