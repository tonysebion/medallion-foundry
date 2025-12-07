"""Unit tests for cursor state persistence helpers."""

import json
from datetime import date


from core.domain.adapters.extractors.cursor_state import (
    CursorStateManager,
    build_incremental_query,
)


def test_save_and_load_cursor(tmp_path):
    manager = CursorStateManager(state_dir=tmp_path)
    key = "source/table"
    assert manager.load_cursor(key) is None

    manager.save_cursor(key, "cursor-val", run_date=date(2025, 1, 2))
    stored = manager.load_cursor(key)
    assert stored == "cursor-val"

    contents = json.loads((tmp_path / "source_table_cursor.json").read_text())
    assert "last_run" in contents


def test_load_cursor_handles_invalid_json(tmp_path, caplog):
    manager = CursorStateManager(state_dir=tmp_path)
    path = tmp_path / "bad_cursor_cursor.json"
    path.write_text("not-json")

    caplog.set_level("WARNING", logger="core.domain.adapters.extractors.cursor_state")
    result = manager.load_cursor("bad_cursor")
    assert result is None
    assert "Failed to load cursor state" in caplog.text


def test_build_incremental_query_variants():
    assert build_incremental_query("SELECT * FROM table", "id", "100") == (
        "SELECT * FROM table\nWHERE id > ?"
    )
    assert build_incremental_query("SELECT * FROM table WHERE flag=1", "id", "100") == (
        "SELECT * FROM table WHERE flag=1\nAND id > ?"
    )
    assert build_incremental_query("SELECT * FROM table", None, "100") == "SELECT * FROM table"
    assert build_incremental_query("SELECT * FROM table", "id", None) == "SELECT * FROM table"
