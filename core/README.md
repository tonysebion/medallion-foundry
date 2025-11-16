# Core architecture overview

The `core` package mirrors the medallion layers and shared infrastructure so contributors can reason about each layer in isolation.

## Package map

```
core/
├── config/          # YAML loading, validation, and typed Silver config models
├── bronze/          # Bronze helpers (chunking, metadata, chunk writer planning)
├── silver/          # Silver helpers (artifacts, models, streaming)
├── storage/          # Plugin-driven storage backends + policy helpers
├── runner/           # Bronze orchestration (chunk/metadata runners)
└── extractors/       # Reusable connection-specific extractors (API/DB/File)
```

Each package includes its own `README.md` describing the responsibilities, so updates stay localized.

### Bronze ↔ Silver symmetry

The Bronze and Silver packages now follow similar idioms:

- Bronze: `core/bronze/base.py` and `core/bronze/plan.py` handle schema inference, metadata emission, and chunk writer construction.
- Silver: `core/silver/artifacts.py`, `models.py`, and `stream.py` implement model planning, normalization, and streaming promotion.

With this layout, every time you add a new Bronze helper (e.g., a new metadata guard), ask whether a Silver counterpart needs to live alongside it.

## Next refactors to keep symmetry

- Consider moving `core/runner` to expose a clearer Bronze lifecycle (e.g., `run_extract` plus context builder hooks), while the Silver CLI reuses the same flow through `core/silver`.
- Any new shared utilities (logging hooks, metadata vendors, quality guards) should live under `core/common/` or a focused package so Bronze and Silver can each import them without circularity.

Refer back to this file when you add new helper modules so the `core` packages stay balanced and easy to understand.
