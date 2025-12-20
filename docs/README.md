# medallion-foundry documentation

This directory powers the on-site documentation, but it is also the canonical source of truth for new users exploring the framework locally.

## Start points

- `docs/index.md` — curated roadmap (beginner, owner, developer tracks) plus links to the most common tasks.
- `docs/pipelines/GETTING_STARTED.md` — primer for authoring and running pipelines through `python -m pipelines`.
- `pipelines/QUICKREF.md` — CLI cheatsheet with flags, helpers, and troubleshooting tips.
- `docs/scripts/README.md` — how to regenerate `sampledata/` fixtures with the current tooling.

## Quick sanity checks

- List every discovered pipeline: `python -m pipelines --list`
- Explain or validate before running: use `--explain`, `--dry-run`, or `--check`
- Build new pipelines interactively: `python -m pipelines.create` or `python -m pipelines new my.system --source-type database_mssql`
- Generate sample fixtures: `python scripts/generate_bronze_samples.py --all` and `python scripts/generate_silver_samples.py --all`

## Keeping the docs fresh

1. Align any conceptual content with `pipelines/lib` (Bronze/Silver dataclasses, runner helpers, resiliant utilities, etc.).
2. Update sample paths if `sampledata/` or `pipelines/examples/` moves.
3. Regenerate `docs/pipelines` guides when new CLI flags are added (for example, `inspect-source`, `generate-sample`, `test-connection`).

The rest of the directory includes usage guides, operational playbooks, and framework references that link back to these core entry points.
