import sys
from pathlib import Path


def _write_min_config(tmp_path: Path) -> Path:
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(
        """
platform:
  bronze:
    storage_backend: local
    output_dir: ./output
source:
  system: demo
  table: users
  type: api
  api:
    url: https://example.com
  run:
    load_pattern: full
    local_output_dir: ./output
        """.strip(),
        encoding="utf-8",
    )
    return cfg


def test_bronze_validate_only_exits_zero(tmp_path, monkeypatch):
    cfg_path = _write_min_config(tmp_path)
    # Import late to avoid global argparse on import
    import bronze_extract as be

    argv = [
        "bronze-extract",
        "--validate-only",
        "--config",
        str(cfg_path),
        "--log-format",
        "json",
        "--quiet",
    ]
    monkeypatch.setenv("BRONZE_LOG_FORMAT", "json")
    monkeypatch.setattr(sys, "argv", argv)

    rc = be.main()
    assert rc == 0


def test_bronze_list_backends_prints_and_exits_zero(monkeypatch):
    import io
    import bronze_extract as be

    fake_out = io.StringIO()
    monkeypatch.setattr(sys, "argv", ["bronze-extract", "--list-backends"])
    monkeypatch.setattr(sys, "stdout", fake_out)

    rc = be.main()
    out = fake_out.getvalue()
    assert rc == 0
    assert "Available storage backends:" in out


def test_silver_parser_supports_json_logs(monkeypatch):
    import silver_extract as se

    # Using --validate-only requires --config; supply a minimal temp file
    # but we only exercise parser + setup_logging path here via dry-run flags.
    parser = se.build_parser()
    args = parser.parse_args(
        [
            "--log-format",
            "json",
            "--validate-only",
            "--config",
            "tests/does_not_exist.yaml",
        ]
    )
    # setup_logging is called inside main(); here we just assert arg surface exists
    assert getattr(args, "log_format", None) == "json"
