# Scripted fixtures

The only curated scripts in `scripts/` now generate canonical sample datasets and test scenarios for the Bronze → Silver workflow. Run them after installing the package (`pip install -e .`) so the imports resolve, then point your pipelines at `sampledata/bronze_samples` or `sampledata/silver_samples`.

## `generate_bronze_samples.py`

Creates Bronze partitions for every load pattern (snapshot, incremental append, incremental merge, current history). Each batch writes `_metadata.json`, `_checksums.json`, and the Parquet chunk under:

```
sampledata/bronze_samples/sample=<pattern>/dt=<date>/
```

**Common flags**

- `--all` (default) — rebuilds every supported pattern.
- `--pattern <name>` — regenerate a single scenario (`snapshot`, `incremental_append`, `incremental_merge`, `current_history`).
- `--rows <N>` — control the base row count for T0.
- `--verify` — compare existing files to the expected checksums and fail if they drift.
- `--clean` — delete the output directory before regenerating.

**Examples**

```bash
python scripts/generate_bronze_samples.py --all --rows 500 --clean
python scripts/generate_bronze_samples.py --pattern snapshot --verify
```

## `generate_silver_samples.py`

Operationalizes Bronze fixtures into curated Silver outputs with the right `entity_kind`, `history_mode`, and `natural_keys`. The script writes:

```
sampledata/silver_samples/sample=<pattern>_<kind>_<history>/dt=<date>/
```

**Key options**

- `--all` (default) — regenerate every cataloged Silver scenario.
- `--pattern <pattern_name>` — limit the run to one Bronze pattern. The generator knows how to map each pattern to the correct Silver model.
- `--entity-kind` / `--history-mode` — override the defaults when experimenting with SCD1 versus SCD2, or event versus state semantics.
- `--with-duplicates` — inject intentional duplicates so you can verify dedup behavior.
- `--verify` — ensure Silver metadata/checksums still match the last generated run.

**Example**

```bash
python scripts/generate_silver_samples.py --pattern snapshot --entity-kind event --history-mode current_only
```

## `generate_pattern_test_data.py`

Builds bespoke pattern data plus optional assertion files for integration tests. Use it when you need a handful of batches (T0–Tn) for snapshot, incremental append, incremental merge, or current history scenarios.

**Notable flags**

- `--pattern` / `--all` — choose the pattern to materialize.
- `--batches`, `--rows` — tune how many batches and how many records per batch.
- `--update-rate`, `--insert-rate`, `--entities`, `--changes-per-entity` — tune CDC/SCD scenarios.
- `--format` — write Parquet, CSV, or JSON.
- `--generate-assertions` — emit YAML assertions alongside the data for automated checks.
- `--dry-run` — preview what would be generated without writing files.

**Example**

```bash
python scripts/generate_pattern_test_data.py --pattern incremental_merge --rows 1000 --batches 4 --generate-assertions
```

## Best practices

- Regenerate Bronze fixtures before running integration tests so `_metadata.json` values match what the validators expect.
- Run `generate_silver_samples.py` after the Bronze script when you need curated Silver assets for demos or docs.
- Keep `sampledata/bronze_samples` and `sampledata/silver_samples` checked into source control for reproducibility, or regenerate them as part of your CI pipeline when builds start from scratch.
