"""TUI project settings loader.

Reads project-specific TUI configuration from .bronze-foundry.yaml in the
project root. This allows teams to customize default paths, env file locations,
and other TUI behavior per-project.

Example .bronze-foundry.yaml:
    tui:
      config_dir: ./config           # Where to save new pipeline configs
      env_dirs:                       # Where to look for .env files
        - ./environments
        - ./
      parent_config_dir: ./config    # Default location for parent configs
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class TUISettings:
    """TUI configuration settings."""

    # Where to save new pipeline configuration files
    config_dir: str = "./config"

    # Directories to search for .env files (in order)
    env_dirs: list[str] = field(default_factory=lambda: ["./environments", "./"])

    # Default directory for browsing parent configs
    parent_config_dir: str = "./config"

    # Default directory for browsing existing configs to edit
    edit_config_dir: str = "./config"

    @classmethod
    def load(cls, project_root: Path | None = None) -> "TUISettings":
        """Load settings from .bronze-foundry.yaml in project root.

        Args:
            project_root: Project root directory. Defaults to cwd.

        Returns:
            TUISettings with values from config file or defaults.
        """
        root = project_root or Path.cwd()
        config_path = root / ".bronze-foundry.yaml"

        if not config_path.exists():
            return cls()

        try:
            with open(config_path, encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}

            tui_config = config.get("tui", {})
            return cls(
                config_dir=tui_config.get("config_dir", cls.config_dir),
                env_dirs=tui_config.get("env_dirs", cls().env_dirs),
                parent_config_dir=tui_config.get("parent_config_dir", cls.parent_config_dir),
                edit_config_dir=tui_config.get("edit_config_dir", cls.edit_config_dir),
            )
        except Exception:
            # If config file is malformed, use defaults
            return cls()

    def get_config_dir(self, project_root: Path | None = None) -> Path:
        """Get absolute path to config directory."""
        root = project_root or Path.cwd()
        return (root / self.config_dir).resolve()

    def get_env_dirs(self, project_root: Path | None = None) -> list[Path]:
        """Get list of absolute paths to env file directories."""
        root = project_root or Path.cwd()
        return [(root / d).resolve() for d in self.env_dirs]

    def get_parent_config_dir(self, project_root: Path | None = None) -> Path:
        """Get absolute path to parent config directory."""
        root = project_root or Path.cwd()
        return (root / self.parent_config_dir).resolve()

    def get_edit_config_dir(self, project_root: Path | None = None) -> Path:
        """Get absolute path to edit config directory."""
        root = project_root or Path.cwd()
        return (root / self.edit_config_dir).resolve()


# Global settings instance (loaded on first access)
_settings: TUISettings | None = None


def get_settings(reload: bool = False) -> TUISettings:
    """Get the global TUI settings.

    Args:
        reload: Force reload from config file.

    Returns:
        TUISettings instance.
    """
    global _settings
    if _settings is None or reload:
        _settings = TUISettings.load()
    return _settings
