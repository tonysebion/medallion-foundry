from core.runtime.metadata import Layer, build_run_metadata


def _base_config():
    return {
        "source": {
            "system": "testing",
            "table": "events",
            "run": {},
        },
        "platform": {"bronze": {}},
    }


def test_build_run_metadata_uses_root_schema_evolution_mode():
    cfg = _base_config()
    cfg["schema_evolution"] = {"mode": "ignore_unknown"}

    metadata = build_run_metadata(cfg, Layer.BRONZE, run_id="run-1")

    assert metadata.schema_evolution_mode == "ignore_unknown"


def test_build_run_metadata_reads_source_run_schema_evolution_when_root_missing():
    cfg = _base_config()
    cfg["source"]["run"]["schema_evolution"] = {"mode": "allow_new_nullable"}

    metadata = build_run_metadata(cfg, Layer.BRONZE, run_id="run-2")

    assert metadata.schema_evolution_mode == "allow_new_nullable"


def test_build_run_metadata_defaults_schema_evolution_mode_to_strict():
    cfg = _base_config()

    metadata = build_run_metadata(cfg, Layer.BRONZE, run_id="run-3")

    assert metadata.schema_evolution_mode == "strict"
