"""Unit tests for Bronze reference metadata handling."""

from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

import pytest

from core.io import write_batch_metadata
from scripts.generate_sample_data import (
    HYBRID_DELTA_DAYS,
    HYBRID_REFERENCE_INITIAL,
    HYBRID_REFERENCE_SECOND,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
HYBRID_DIR = REPO_ROOT / "docs" / "examples" / "data" / "bronze_samples"
HYBRID_COMBOS = [
    ("hybrid_cdc_point", "point_in_time"),
    ("hybrid_cdc_cumulative", "cumulative"),
    ("hybrid_incremental_point", "point_in_time"),
    ("hybrid_incremental_cumulative", "cumulative"),
]


def _write_metadata_with_reference(
    tmp_path: Path, run_date: dt.date, reference_mode: dict[str, object]
) -> Path:
    path = (
        tmp_path
        / f"system=retail_demo/table=orders/pattern=full/dt={run_date.isoformat()}"
    )
    path.mkdir(parents=True, exist_ok=True)
    metadata = {
        "record_count": 100,
        "chunk_count": 2,
        "run_date": run_date.isoformat(),
        "reference_mode": reference_mode,
        "load_pattern": "full",
    }
    write_batch_metadata(path, record_count=100, chunk_count=2, extra_metadata=metadata)
    return path


@pytest.fixture(scope="module", autouse=True)
def ensure_hybrid_samples() -> None:
    subprocess.run(
        [sys.executable, "scripts/generate_sample_data.py"],
        cwd=REPO_ROOT,
        check=True,
    )
    yield


def test_reference_then_cdc_delta(tmp_path: Path) -> None:
    ref_path = _write_metadata_with_reference(
        tmp_path,
        dt.date(2025, 11, 13),
        {"role": "reference", "cadence_days": 7, "delta_patterns": ["cdc"]},
    )
    delta_path = _write_metadata_with_reference(
        tmp_path,
        dt.date(2025, 11, 14),
        {"role": "delta", "delta_patterns": ["cdc"]},
    )
    ref_meta = json.loads((ref_path / "_metadata.json").read_text())
    delta_meta = json.loads((delta_path / "_metadata.json").read_text())
    assert ref_meta["reference_mode"]["role"] == "reference"
    assert delta_meta["reference_mode"]["role"] == "delta"
    assert delta_meta["reference_mode"]["delta_patterns"] == ["cdc"]


def test_reference_then_incremental_delta(tmp_path: Path) -> None:
    ref_path = _write_metadata_with_reference(
        tmp_path,
        dt.date(2025, 11, 13),
        {
            "role": "reference",
            "cadence_days": 7,
            "delta_patterns": ["incremental_merge"],
        },
    )
    delta_path = _write_metadata_with_reference(
        tmp_path,
        dt.date(2025, 11, 14),
        {"role": "delta", "delta_patterns": ["incremental_merge"]},
    )
    ref_meta = json.loads((ref_path / "_metadata.json").read_text())
    delta_meta = json.loads((delta_path / "_metadata.json").read_text())
    assert "incremental_merge" in ref_meta["reference_mode"]["delta_patterns"]
    assert delta_meta["reference_mode"]["role"] == "delta"


def test_hybrid_samples_cover_delta_sequence() -> None:
    for combo_name, delta_mode in HYBRID_COMBOS:
        base = (
            HYBRID_DIR
            / combo_name
            / "system=retail_demo"
            / "table=orders"
            / f"pattern={combo_name}"
        )
        for ref_date in (HYBRID_REFERENCE_INITIAL, HYBRID_REFERENCE_SECOND):
            reference_meta_path = (
                base / f"dt={ref_date.isoformat()}" / "reference" / "_metadata.json"
            )
            assert reference_meta_path.exists()
            reference_meta = json.loads(reference_meta_path.read_text())
            assert (
                reference_meta["reference_mode"]["reference_run_date"]
                == ref_date.isoformat()
            )
            assert reference_meta["reference_mode"]["delta_mode"] == delta_mode
        for offset in range(1, HYBRID_DELTA_DAYS + 1):
            delta_date = HYBRID_REFERENCE_INITIAL + dt.timedelta(days=offset)
            delta_meta_path = (
                base / f"dt={delta_date.isoformat()}" / "delta" / "_metadata.json"
            )
            assert delta_meta_path.exists()
            delta_meta = json.loads(delta_meta_path.read_text())
            assert delta_meta["reference_mode"]["role"] == "delta"
            assert delta_meta["reference_mode"]["delta_mode"] == delta_mode
            if delta_date > HYBRID_REFERENCE_SECOND:
                expected_reference = HYBRID_REFERENCE_SECOND.isoformat()
            else:
                expected_reference = HYBRID_REFERENCE_INITIAL.isoformat()
            assert (
                delta_meta["reference_mode"]["reference_run_date"] == expected_reference
            )
