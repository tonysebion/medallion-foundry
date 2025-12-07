"""Tests for primitive foundation mixins."""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import ClassVar, Dict

import pytest

from core.foundation.primitives.base import RichEnumMixin, SerializableMixin, _serialize_value


class SampleEnum(RichEnumMixin, str, Enum):
    FIRST = "first"
    SECOND = "second"


@dataclass
class SampleModel(SerializableMixin):
    name: str
    value: int
    path: Path


def test_rich_enum_choices_and_describe():
    choices = SampleEnum.choices()
    assert "first" in choices
    assert "second" in choices
    assert SampleEnum.normalize("FIRST") == SampleEnum.FIRST
    assert SampleEnum.FIRST.describe() == "first"
    with pytest.raises(ValueError):
        SampleEnum.normalize("unknown")


def test_serializable_mixin_round_trip(tmp_path):
    model = SampleModel(name="test", value=5, path=Path(tmp_path / "file.txt"))
    data = model.to_dict()
    assert data["path"].endswith("file.txt")
    restored = SampleModel.from_dict(data)
    assert restored.name == model.name
    assert restored.value == model.value
    assert str(restored.path) == str(model.path)


def test_serialize_value_supports_nested():
    nested = {"enum": SampleEnum.SECOND, "items": [Path("a"), {"inner": 5}]}
    serialized = _serialize_value(nested)
    assert serialized["enum"] == "second"
    assert serialized["items"][0] == "a"
    assert serialized["items"][1]["inner"] == 5
