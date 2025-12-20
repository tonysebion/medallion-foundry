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


# Application style
STYLE = Style.from_dict({
    "title": "bold bg:#005f87 #ffffff",
    "section": "bold #00af00",
    "section-header": "bold underline #00af00",
    "field-label": "#d7d700",
    "field-label.required": "#d7d700 bold",
    "field-value": "#ffffff",
    "field.focused": "bg:#303030",
    "inherited": "italic #6699ff",
    "inherited-badge": "#6699ff",
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
})


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


class ClickableEnumField(UIControl):
    """A clickable enum field that cycles through options on click."""

    def __init__(
        self,
        field: "Field",
        on_click: Callable[["Field"], None],
    ):
        self.field = field
        self.on_click = on_click
        self._hover = False

    def create_content(self, width: int, height: int) -> UIContent:
        selected = self.field.buffer.text
        display_text = selected
        for val, txt in self.field.enum_options:
            if val == selected:
                display_text = txt
                break

        style = "class:clickable.hover" if self._hover else "class:field-value"

        def get_line(i: int) -> list[tuple[str, str]]:
            if i == 0:
                return [(style, f"[{display_text}] ▼")]
            return []

        return UIContent(get_line=get_line, line_count=1)

    def mouse_handler(self, mouse_event: MouseEvent) -> None:
        if mouse_event.event_type == MouseEventType.MOUSE_UP:
            self.on_click(self.field)
        elif mouse_event.event_type == MouseEventType.MOUSE_MOVE:
            self._hover = True
        else:
            self._hover = False

    def is_focusable(self) -> bool:
        return True


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
        field_type: str = "text",  # text, enum, list
        enum_options: list[tuple[str, str]] | None = None,
        default: str = "",
        source: str = "default",
        visible_when: Callable[["PipelineConfigApp"], bool] | None = None,
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
        self.buffer = Buffer(name=name)
        self.buffer.text = default

    def is_visible(self, app: "PipelineConfigApp") -> bool:
        """Check if this field should be visible."""
        if self.visible_when is None:
            return True
        return self.visible_when(app)


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
            ),
            Field(
                "description", "Description", "metadata",
                help_text="Brief description of what this pipeline does",
                default=self.state.description or "",
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
            ),
            Field(
                "entity", "Entity", "bronze",
                required=True,
                help_text="Entity/table name (e.g., orders, customers)",
                default=self.state.get_bronze_value("entity") or "",
                source=self._get_source("bronze", "entity"),
            ),
            Field(
                "source_type", "Source Type", "bronze",
                required=True,
                field_type="enum",
                enum_options=[
                    ("file_csv", "CSV File"),
                    ("file_parquet", "Parquet File"),
                    ("file_json", "JSON File"),
                    ("database_mssql", "SQL Server"),
                    ("database_postgres", "PostgreSQL"),
                    ("api_rest", "REST API"),
                ],
                help_text="Type of data source",
                default=self.state.get_bronze_value("source_type") or "file_csv",
            ),
            Field(
                "source_path", "Source Path", "bronze",
                help_text="Path to data file (supports {run_date})",
                default=self.state.get_bronze_value("source_path") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("file_"),
            ),
            Field(
                "host", "Host", "bronze",
                help_text="Database hostname (use ${VAR} for env vars)",
                is_sensitive=True,
                default=self.state.get_bronze_value("host") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("database_"),
            ),
            Field(
                "database", "Database", "bronze",
                help_text="Database name",
                default=self.state.get_bronze_value("database") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("database_"),
            ),
            Field(
                "query", "Query", "bronze",
                help_text="SQL query (use {run_date} for date filter)",
                default=self.state.get_bronze_value("query") or "",
                visible_when=lambda app: app._get_field_value("source_type").startswith("database_"),
            ),
            Field(
                "base_url", "Base URL", "bronze",
                required=True,
                help_text="API base URL (e.g., https://api.example.com)",
                default=self.state.get_bronze_value("base_url") or "",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
            ),
            Field(
                "endpoint", "Endpoint", "bronze",
                required=True,
                help_text="API endpoint path (e.g., /v1/customers)",
                default=self.state.get_bronze_value("endpoint") or "",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
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
                default=self.state.get_bronze_value("auth_type") or "none",
                visible_when=lambda app: app._get_field_value("source_type") == "api_rest",
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
            ),
            Field(
                "load_pattern", "Load Pattern", "bronze",
                field_type="enum",
                enum_options=[
                    ("full_snapshot", "Full Snapshot"),
                    ("incremental", "Incremental"),
                    ("cdc", "CDC"),
                ],
                help_text="How to load data from source",
                default=self.state.get_bronze_value("load_pattern") or "full_snapshot",
            ),
            Field(
                "watermark_column", "Watermark Column", "bronze",
                help_text="Column to track for incremental loads",
                default=self.state.get_bronze_value("watermark_column") or "",
                visible_when=lambda app: app._get_field_value("load_pattern") in ("incremental", "cdc"),
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
            ),
            Field(
                "change_timestamp", "Change Timestamp", "silver",
                required=True,
                help_text="Column that tracks when records changed",
                default=self.state.get_silver_value("change_timestamp") or "",
            ),
            Field(
                "entity_kind", "Entity Kind", "silver",
                field_type="enum",
                enum_options=[
                    ("state", "State (dimension)"),
                    ("event", "Event (fact)"),
                ],
                help_text="Type of entity",
                default=self.state.get_silver_value("entity_kind") or "state",
            ),
            Field(
                "history_mode", "History Mode", "silver",
                field_type="enum",
                enum_options=[
                    ("current_only", "Current Only"),
                    ("full_history", "Full History (SCD2)"),
                ],
                help_text="How to handle historical changes",
                default=self.state.get_silver_value("history_mode") or "current_only",
            ),
        ])

    def _get_source(self, section: str, field_name: str) -> str:
        """Get the source of a field value."""
        fields = self.state.bronze if section == "bronze" else self.state.silver
        if field_name in fields:
            return fields[field_name].source.value
        return "default"

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

    def _get_visible_fields(self) -> list[Field]:
        """Get list of currently visible fields."""
        return [f for f in self.fields if f.is_visible(self)]

    def _create_layout(self) -> Layout:
        """Create the application layout."""
        # Build form sections
        form_content = self._build_form_content()

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

        title_spacer = Window(
            content=FormattedTextControl(
                HTML("  │  Tab/Click: Navigate  │  Enter: Edit enum  ")
            ),
            style="class:title",
            height=1,
        )

        title_bar = VSplit([
            title_label,
            save_button,
            Window(width=1, style="class:title"),  # Spacer
            quit_button,
            title_spacer,
        ], height=1)

        # Main layout: form on left, preview on right
        main_content = VSplit([
            # Left side: form
            Frame(
                body=ScrollablePane(
                    HSplit(form_content),
                    show_scrollbar=True,
                ),
                title="Configuration",
                width=D(weight=3),
            ),
            # Right side: preview
            Frame(
                body=yaml_preview,
                title="YAML Preview",
                width=D(weight=2),
            ),
        ])

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

        for i, field in enumerate(visible_fields):
            # Section header
            if field.section != current_section:
                current_section = field.section
                section_titles = {
                    "metadata": "Pipeline Metadata",
                    "bronze": "Bronze Layer (Data Extraction)",
                    "silver": "Silver Layer (Data Curation)",
                }
                content.append(Window(height=1))  # Spacer
                content.append(
                    Window(
                        content=FormattedTextControl(
                            HTML(f"<section-header>── {section_titles.get(current_section, current_section)} ──</section-header>")
                        ),
                        height=1,
                        style="class:section",
                    )
                )
                content.append(Window(height=1))  # Spacer

            # Field row
            content.append(self._create_field_row(field, i))

        return content

    def _create_field_row(self, field: Field, idx: int) -> HSplit:
        """Create a row for a single field with mouse support."""
        # Label - clickable to select field
        label_parts = []
        if field.required:
            label_parts.append(("class:field-label.required", f"{field.label}* "))
        else:
            label_parts.append(("class:field-label", f"{field.label} "))

        if field.source == "parent":
            label_parts.append(("class:inherited-badge", "[inherited] "))
        if field.is_sensitive:
            label_parts.append(("class:sensitive", "[${...}] "))

        label = Window(
            content=ClickableFieldLabel(label_parts, idx, self._on_field_click),
            width=D(min=20, max=25),
            height=1,
        )

        # Value display
        if field.field_type == "enum":
            # Clickable enum that cycles through options
            value = Window(
                content=ClickableEnumField(field, self._on_enum_click),
                style="class:field-value",
                height=1,
            )
        else:
            # Text input - buffer control already handles clicks for focus
            value = Window(
                content=BufferControl(buffer=field.buffer),
                style="class:field-value",
                height=1,
                cursorline=False,
            )

        return HSplit([
            VSplit([label, value], padding=1),
        ], padding=0)

    def _on_field_click(self, field_idx: int) -> None:
        """Handle click on a field label."""
        self.current_field_idx = field_idx
        self._focus_current_field()

    def _on_enum_click(self, field: Field) -> None:
        """Handle click on an enum field - cycle to next option."""
        # Find the field index
        visible = self._get_visible_fields()
        for i, f in enumerate(visible):
            if f.name == field.name:
                self.current_field_idx = i
                break

        # Cycle through options
        current = field.buffer.text
        options = [v for v, _ in field.enum_options]
        if current in options:
            idx = (options.index(current) + 1) % len(options)
            field.buffer.text = options[idx]
        else:
            field.buffer.text = options[0] if options else ""

        # Refresh layout for conditional visibility
        self._refresh_layout()

    def _generate_yaml_preview(self) -> str:
        """Generate YAML preview from current field values."""
        self._sync_fields_to_state()
        try:
            return self.state.to_yaml()
        except Exception as e:
            return f"# Error generating YAML: {e}"

    def _sync_fields_to_state(self) -> None:
        """Sync field buffer values to state."""
        for field in self.fields:
            value = field.buffer.text.strip()

            if field.name == "name":
                self.state.name = value
            elif field.name == "description":
                self.state.description = value
            elif field.section == "bronze":
                if field.field_type == "list":
                    self.state.set_bronze_value(
                        field.name,
                        [v.strip() for v in value.split(",") if v.strip()]
                    )
                else:
                    self.state.set_bronze_value(field.name, value)
            elif field.section == "silver":
                if field.name == "natural_keys":
                    self.state.set_silver_value(
                        field.name,
                        [v.strip() for v in value.split(",") if v.strip()]
                    )
                else:
                    self.state.set_silver_value(field.name, value)

    def _get_help_text(self) -> FormattedText:
        """Get contextual help text for current field."""
        visible_fields = self._get_visible_fields()
        if 0 <= self.current_field_idx < len(visible_fields):
            field = visible_fields[self.current_field_idx]
            help_parts = [("class:help", f"  {field.help_text}" if field.help_text else "")]

            if field.field_type == "enum":
                options = ", ".join(f"{v}" for v, _ in field.enum_options)
                help_parts.append(("class:help", f"\n  Options: {options}"))

            if field.is_sensitive:
                help_parts.append(("class:help", "\n  Tip: Use ${ENV_VAR} to reference environment variables"))

            return FormattedText(help_parts)
        return FormattedText([("class:help", "  Navigate with Tab/Shift+Tab, Enter to edit")])

    def _get_status_bar(self) -> FormattedText:
        """Get status bar content."""
        visible = len(self._get_visible_fields())
        return FormattedText([
            ("class:status-bar", f"  {self.status_message}  │  Fields: {visible}  │  Mode: {self.mode}  ")
        ])

    def _create_bindings(self) -> KeyBindings:
        """Create key bindings."""
        kb = KeyBindings()

        @kb.add("c-q")
        def quit_(event):
            """Quit without saving."""
            event.app.exit()

        @kb.add("c-s")
        def save_(event):
            """Save configuration."""
            self._save_config()

        @kb.add("tab")
        def next_field_(event):
            """Move to next field."""
            visible = self._get_visible_fields()
            if visible:
                self.current_field_idx = (self.current_field_idx + 1) % len(visible)
                self._focus_current_field()

        @kb.add("s-tab")
        def prev_field_(event):
            """Move to previous field."""
            visible = self._get_visible_fields()
            if visible:
                self.current_field_idx = (self.current_field_idx - 1) % len(visible)
                self._focus_current_field()

        @kb.add("enter")
        def edit_field_(event):
            """Edit current field (cycle enum or enter edit mode)."""
            visible = self._get_visible_fields()
            if 0 <= self.current_field_idx < len(visible):
                field = visible[self.current_field_idx]
                if field.field_type == "enum":
                    # Cycle through options
                    current = field.buffer.text
                    options = [v for v, _ in field.enum_options]
                    if current in options:
                        idx = (options.index(current) + 1) % len(options)
                        field.buffer.text = options[idx]
                    else:
                        field.buffer.text = options[0] if options else ""
                    # Refresh layout for conditional visibility
                    self._refresh_layout()

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

        # Determine save path
        if self.yaml_path:
            save_path = Path(self.yaml_path)
        else:
            name = self.state.name or "new_pipeline"
            save_path = Path.cwd() / "pipelines" / f"{name}.yaml"

        # Save
        try:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            yaml_content = self.state.to_yaml()
            save_path.write_text(yaml_content, encoding="utf-8")
            self.status_message = f"Saved to: {save_path}"
            self.yaml_path = str(save_path)
        except Exception as e:
            self.status_message = f"Save failed: {e}"

    def _quit_app(self) -> None:
        """Quit the application."""
        if self.app:
            self.app.exit()


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
