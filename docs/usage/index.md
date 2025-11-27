# Usage Documentation

This section collates the owner-focused guides, checklists, and pattern references that help you explore the Bronze→Silver workflow without touching the engine internals.

## Beginner track
- [`QUICKSTART`](beginner/QUICKSTART.md) – run your first Bronze and Silver jobs with only YAML configuration.
- [`FIRST_RUN_CHECKLIST`](beginner/FIRST_RUN_CHECKLIST.md) – verify dependencies, storage access, and smoke tests before your first production run.

## Onboarding & intent
- [`intent-owner-guide`](onboarding/intent-owner-guide.md) – story-driven intent templates, safe-first runs, and metadata expectations.
- [`new_dataset_checklist`](onboarding/new_dataset_checklist.md) – capture ownership, entity intent, and load controls before running Bronze.
- [`bronze_readiness_checklist`](onboarding/bronze_readiness_checklist.md) – post-run audit to confirm Bronze files, metadata, and ownership align with intent.
- [`intent-lifecycle`](../framework/reference/intent-lifecycle.md) – command/path/map reference showing how Bronze, Silver, and metadata files move through the tiered pipeline.

## Patterns & advanced controls
- [`QUICK_REFERENCE`](patterns/QUICK_REFERENCE.md) – cheat sheet for common data scenarios and pattern selection.
- [`pattern_matrix`](patterns/pattern_matrix.md) – decision router and config recipes for every load model.
- [`EXTRACTION_GUIDANCE`](patterns/EXTRACTION_GUIDANCE.md) – when to choose `full`, `cdc`, or `current_history`.
- [`ENHANCED_FEATURES`](patterns/ENHANCED_FEATURES.md) – tuning knobs, partitioning, and observability options.
- [`docs/examples/configs/*.yaml`](../examples/configs) – copy-ready configs that reflect each pattern and feed the sample data.

> **Usage tests** – `tests/test_usage_flow.py` exercises the quickstart/config patterns, silver flows, and owner intent expansion so this walk-through stays accurate.
