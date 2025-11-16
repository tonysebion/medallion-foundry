from __future__ import annotations

import pytest

from core.patterns import LoadPattern
from core.silver.models import SilverModel, resolve_profile


def test_silver_model_normalize_alias() -> None:
    assert SilverModel.normalize("scd1") == SilverModel.SCD_TYPE_1


def test_silver_model_normalize_invalid() -> None:
    with pytest.raises(ValueError):
        SilverModel.normalize("unknown")


def test_default_model_for_patterns() -> None:
    assert SilverModel.default_for_load_pattern(LoadPattern.CDC) == SilverModel.INCREMENTAL_MERGE
    assert SilverModel.default_for_load_pattern(LoadPattern.FULL) == SilverModel.PERIODIC_SNAPSHOT


def test_requires_dedupe_flags() -> None:
    assert SilverModel.SCD_TYPE_1.requires_dedupe is True
    assert SilverModel.PERIODIC_SNAPSHOT.requires_dedupe is False


def test_resolve_profile_aliases() -> None:
    assert resolve_profile("analytics") == SilverModel.SCD_TYPE_2
    assert resolve_profile("CDC_Delta") == SilverModel.INCREMENTAL_MERGE
    assert resolve_profile(None) is None
