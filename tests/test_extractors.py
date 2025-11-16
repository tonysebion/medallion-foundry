"""Tests for base extractor interface."""

import pytest
from datetime import date
from typing import Dict, Any, List, Optional, Tuple

from core.extractors.base import BaseExtractor


class TestBaseExtractor:
    """Test base extractor abstract interface."""

    def test_cannot_instantiate_base(self):
        """Test that BaseExtractor cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseExtractor()

    def test_subclass_must_implement_fetch_records(self):
        """Test that subclasses must implement fetch_records."""

        class IncompleteExtractor(BaseExtractor):
            pass

        with pytest.raises(TypeError):
            IncompleteExtractor()

    def test_valid_subclass(self):
        """Test that valid subclass can be created."""

        class ValidExtractor(BaseExtractor):
            def fetch_records(
                self,
                cfg: Dict[str, Any],
                run_date: date,
            ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
                return [{"test": "data"}], None

        extractor = ValidExtractor()
        records, cursor = extractor.fetch_records({}, date.today())

        assert len(records) == 1
        assert records[0]["test"] == "data"
        assert cursor is None
