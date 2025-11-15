"""Tests for load pattern utilities."""

import pytest

from core.patterns import LoadPattern


def test_normalize_handles_case():
    assert LoadPattern.normalize("FULL") == LoadPattern.FULL
    assert LoadPattern.normalize("cDc") == LoadPattern.CDC


def test_normalize_invalid_value():
    with pytest.raises(ValueError):
        LoadPattern.normalize("weekly")


def test_folder_and_prefix_helpers():
    pattern = LoadPattern.CURRENT_HISTORY
    assert pattern.chunk_prefix == "current-history"
    assert pattern.folder_name == "pattern=current_history"
