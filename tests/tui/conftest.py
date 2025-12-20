"""Shared fixtures for TUI tests."""

from __future__ import annotations

from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture
def tmp_yaml(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary YAML file for testing."""
    yaml_file = tmp_path / "test_pipeline.yaml"
    yaml_file.write_text(
        """
bronze:
  system: retail
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv

silver:
  natural_keys:
    - order_id
  change_timestamp: updated_at
""",
        encoding="utf-8",
    )
    yield yaml_file


@pytest.fixture
def tmp_parent_yaml(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a parent YAML file for inheritance testing."""
    parent_file = tmp_path / "parent.yaml"
    parent_file.write_text(
        """
bronze:
  system: retail
  entity: orders
  source_type: database_mssql
  host: ${DB_HOST}
  database: SalesDB

silver:
  natural_keys: order_id
  change_timestamp: LastModified
  history_mode: full_history
""",
        encoding="utf-8",
    )
    yield parent_file


@pytest.fixture
def tmp_child_yaml(tmp_path: Path, tmp_parent_yaml: Path) -> Generator[Path, None, None]:
    """Create a child YAML file that inherits from parent."""
    child_file = tmp_path / "child.yaml"
    child_file.write_text(
        f"""
extends: {tmp_parent_yaml.name}
bronze:
  entity: orders_filtered
  query: SELECT * FROM Orders WHERE Status = 'Active'
""",
        encoding="utf-8",
    )
    yield child_file


@pytest.fixture
def tmp_api_yaml(tmp_path: Path) -> Generator[Path, None, None]:
    """Create an API pipeline YAML for testing."""
    yaml_file = tmp_path / "api_pipeline.yaml"
    yaml_file.write_text(
        """
bronze:
  system: github
  entity: repos
  source_type: api_rest
  base_url: https://api.github.com
  endpoint: /users/anthropics/repos
  auth:
    auth_type: bearer
    token: ${GITHUB_TOKEN}
  pagination:
    strategy: page
    page_size: 30
    page_param: page
    page_size_param: per_page
  requests_per_second: 1.0

silver:
  natural_keys: id
  change_timestamp: updated_at
""",
        encoding="utf-8",
    )
    yield yaml_file


@pytest.fixture
def tmp_env_file(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary .env file for testing."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        """
# Database credentials
DB_HOST=localhost
DB_NAME=testdb
DB_USER=testuser
DB_PASSWORD=secret123

# API credentials
API_TOKEN=test_token_12345
GITHUB_TOKEN=ghp_test_token
""",
        encoding="utf-8",
    )
    yield env_file
