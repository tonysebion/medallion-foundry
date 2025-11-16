# Upgrading Guide

## 1.0.0 -> Unreleased (toward 1.1.0)

### New Capabilities
- Typed config models (Pydantic) embedded in loaded config dicts under `__typed_model__`.
- `SilverArtifactWriter` protocol (`core.silver.writer`) and `DefaultSilverArtifactWriter` implementation.
- Partition abstractions: `BronzePartition`, `SilverPartition` (centralized path logic).
- Explicit Bronze sample bootstrap command.
- Structured deprecation & compatibility warnings.

### Deprecations (Removal Target 1.3.0)
| Code   | Description | Action Required |
|--------|-------------|-----------------|
| CFG001 | Implicit fallback using `platform.bronze.output_dir` as `local_path` | Add explicit `platform.bronze.local_path` |
| CFG002 | Legacy `source.api.url` key | Rename to `source.api.base_url` |
| CFG003 | Missing `source.api.endpoint` defaulting to `/` | Add explicit `endpoint` |
| API001 | Positional `write_silver_outputs` wrapper | Switch to `DefaultSilverArtifactWriter().write()` |

### Migration Steps
1. Search for `write_silver_outputs(` and refactor calls to use:
   ```python
   from core.silver.writer import DefaultSilverArtifactWriter
   writer = DefaultSilverArtifactWriter()
   outputs = writer.write(
       df,
       primary_keys=..., order_column=..., write_parquet=True, write_csv=False,
       parquet_compression="snappy", artifact_names={...}, partition_columns=[],
       error_cfg={"enabled": False, "max_bad_records": 0, "max_bad_percent": 0.0},
       silver_model=model, output_dir=target_dir,
   )
   ```
2. Update configs: add `platform.bronze.local_path` where missing.
3. Replace any `source.api.url` with `base_url` and ensure `endpoint` present.
4. (Optional) Begin consuming typed models:
   ```python
   cfg = load_config(path)
   typed = cfg.get("__typed_model__")  # RootConfig instance
   ```
5. Use bootstrap command (to be exposed via CLI in a future release) to generate Bronze samples instead of relying on implicit synthesis.

### Future (Post 1.1.0)
- Wrapper removal warnings will escalate to errors in 1.2.0 before deletion in 1.3.0.
- Expect stricter type enforcement and possible removal of dict fallbacks after 1.3.0.

### Troubleshooting
- If you see `BronzeFoundryCompatibilityWarning`, implement the required explicit config change.
- For `BronzeFoundryDeprecationWarning`, plan to remediate before the stated removal version.

