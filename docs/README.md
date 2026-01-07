# Bronze Foundry Documentation

**From source → Bronze → Silver with one Python package.**

## Quick Start

```bash
# Install
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac
pip install -e .

# List pipelines
python -m pipelines --list

# Run example
python -m pipelines ./pipelines/examples/retail_orders.yaml --date 2025-01-15

# Validate before running
python -m pipelines ./pipelines/examples/retail_orders.yaml --date 2025-01-15 --dry-run
```

## Commands

| Task | Command |
|------|---------|
| List pipelines | `python -m pipelines --list` |
| Run pipeline | `python -m pipelines <module> --date YYYY-MM-DD` |
| Bronze only | `python -m pipelines <module>:bronze --date YYYY-MM-DD` |
| Silver only | `python -m pipelines <module>:silver --date YYYY-MM-DD` |
| Dry run | Add `--dry-run` |
| Check connectivity | Add `--check` |
| Show plan | Add `--explain` |
| Override target | Add `--target ./local/` |
| Interactive creator | `python -m pipelines.create` |
| Test connection | `python -m pipelines test-connection db --host ... --database ...` |
| Inspect file | `python -m pipelines inspect-source --file ./data.csv` |

## Documentation

| Guide | Description |
|-------|-------------|
| [GETTING_STARTED.md](GETTING_STARTED.md) | Pipeline primer, source types, entity kinds |
| [ADVANCED_PATTERNS.md](ADVANCED_PATTERNS.md) | CDC presets, delete modes, SCD2, curate helpers |
| [OPERATIONS.md](OPERATIONS.md) | Testing, troubleshooting, error handling |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Storage backends, path structure, PolyBase |
| [ROADMAP.md](ROADMAP.md) | Future features |

## System Requirements

- Python 3.9+
- Storage: Local filesystem, S3, Azure (ADLS, Blob)
- Credentials: AWS env vars or Azure credentials

## Sample Data

Generate test fixtures:

```bash
python scripts/generate_bronze_samples.py --all
python scripts/generate_silver_samples.py --all
```

## Help

1. `python -m pipelines --list` - confirm pipeline is discoverable
2. `--dry-run --check` - validate before writing
3. See [OPERATIONS.md](OPERATIONS.md) for troubleshooting
