"""Entry point for running the TUI as a module.

Usage:
    python -m pipelines.tui              # Create new pipeline
    python -m pipelines.tui config.yaml  # Edit existing pipeline
    python -m pipelines.tui --parent parent.yaml  # Create with inheritance
"""

from __future__ import annotations

import sys

from pipelines.tui.app import PipelineConfigApp


def main() -> None:
    """Run the TUI application."""
    args = sys.argv[1:]

    mode = "create"
    yaml_path = None
    parent_path = None

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--parent" and i + 1 < len(args):
            parent_path = args[i + 1]
            i += 2
        elif arg == "--help" or arg == "-h":
            print(__doc__)
            sys.exit(0)
        elif not arg.startswith("-"):
            # Positional argument is the YAML file to edit
            yaml_path = arg
            mode = "edit"
            i += 1
        else:
            print(f"Unknown argument: {arg}")
            print(__doc__)
            sys.exit(1)

    app = PipelineConfigApp(
        mode=mode,
        yaml_path=yaml_path,
        parent_path=parent_path,
    )
    app.run()


if __name__ == "__main__":
    main()
