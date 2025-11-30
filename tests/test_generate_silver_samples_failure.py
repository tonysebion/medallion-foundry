"""Verify generate_silver_samples reports task failures when uplift fails."""

from __future__ import annotations

import argparse
import pytest
from pathlib import Path

from scripts import generate_silver_samples as g  # type: ignore[attr-defined]
from core.config.loader import load_config_with_env


def test_generate_silver_samples_reports_failure(monkeypatch) -> None:
    dataset, env_config = load_config_with_env(
        Path("docs/examples/configs/patterns/pattern_cdc.yaml")
    )
    pattern_config = g.PatternConfig(
        path=Path("docs/examples/configs/patterns/pattern_cdc.yaml"),
        pattern_folder="pattern_cdc_events",
        silver_model="incremental_merge",
        domain=dataset.domain or dataset.system or "default",
        entity=dataset.entity,
        dataset=dataset,
        env_config=env_config,
    )

    partition = g.BronzePartition(
        pattern="pattern_cdc_events",
        run_date="2025-11-13",
        s3_bucket="mdf",
        s3_prefix="bronze_samples/system=retail_demo/table=orders/pattern=pattern_cdc_events/dt=2025-11-13/",
    )

    monkeypatch.setattr(
        g,
        "_discover_pattern_configs",
        lambda: {"pattern_cdc_events": [pattern_config]},
    )
    monkeypatch.setattr(g, "_list_all_s3_partitions", lambda _: [partition])
    monkeypatch.setattr(
        g,
        "_generate_for_partition",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        g,
        "parse_args",
        lambda: argparse.Namespace(
            formats="parquet",
            limit=1,
            workers=1,
            artifact_writer="transactional",
            use_locks=False,
        ),
    )

    with pytest.raises(RuntimeError, match="One or more Silver generation tasks failed"):
        g.main()
