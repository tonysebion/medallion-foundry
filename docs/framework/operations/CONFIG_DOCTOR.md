# Config Doctor

The Config Doctor lints and migrates medallion-foundry YAML configs to the latest schema.

## Why use it?
- Ensure `config_version` is present (schema versioning).
- Migrate deprecated fields safely:
  - `platform.bronze.local_path` → `platform.bronze.output_dir` (keeps old key for visibility)
  - `source.api.url` → `source.api.base_url`
  - Ensure `source.api.endpoint` exists (defaults to `/`)
- Preview a unified diff or write changes in place.

## Installation
The CLI is installed with the package as `bronze-config-doctor`.

## Usage
Preview changes:

```bash
bronze-config-doctor configs/my_source.yaml
```

Write changes back in place:

```bash
bronze-config-doctor -i configs/my_source.yaml
```

Migrate multiple files:

```bash
bronze-config-doctor -i configs/*.yaml
```

Select target schema version (default: 1):

```bash
bronze-config-doctor --target-version 1 configs/my_source.yaml
```

## Exit codes
- `0`: All files processed; no fatal errors
- `1`: Failed to process one or more files
- `2`: One or more files not found

## Notes
- The tool attempts to preserve structure and comments where feasible, but YAML comments may be lost.
- For multi-source configs (`sources:`), the tool migrates each entry and keeps top-level defaults.
