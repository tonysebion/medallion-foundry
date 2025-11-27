# medallion-foundry

Welcome to the documentation hub. The docs now live inside three
focused subdirectories:

- `setup/` – installation, dependency checks, and first-production-run prerequisites that both maintainer and owner audiences share.
- `usage/` – beginner walkthroughs, onboarding guides, and pattern references for source/semantic owners.
- `framework/` – design docs, architecture notes, and operations playbooks for core contributors.

Use this page as a single navigation map if you are deciding where to go next. Each section below lists the key docs per track, plus the quick links you need from setup to usage to maintainer level detail.

## 1. Setup (shared prep)

Before running Bronze/Silver, complete the setup guide so cloning, virtualenv, and dependencies are verified:

| Doc | Focus |
| --- | ----- |
| `docs/setup/INSTALLATION.md` | Platform setup, dependency installation, security checks, and post-install verifications that keep both owners and maintainers runnable from day one. |

## 2. Usage (owner-facing)

Use these guides to move from “what is Bronze/Silver?” to selecting a pattern and executing your first extract:

| Doc | Focus |
| --- | ----- |
| `docs/usage/index.md` | Overview of beginner, onboarding, and pattern docs so every user knows what to read next. |
| `docs/usage/beginner/QUICKSTART.md` | Step-by-step walkthrough that gets you from zero to running `bronze_extract.py` and `silver_extract.py`. |
| `docs/usage/beginner/FIRST_RUN_CHECKLIST.md` | Checklist that ensures logging, metadata, and run controls are in place before a production run. |
| `docs/usage/onboarding/intent-owner-guide.md` | Story-driven intent templates, safe-first runs, and metadata expectations. |
| `docs/framework/reference/intent-lifecycle.md` | Command/path reference showing how Bronze, Silver, and metadata files move through the tiered flow. |
| `docs/usage/onboarding/new_dataset_checklist.md` | Owner questionnaire capturing metadata, load controls, and observability flags before running Bronze. |
| `docs/usage/onboarding/bronze_readiness_checklist.md` | Post-run audit to confirm Bronze files, `_metadata.json`, `_checksums.json`, and ownership align with your intent. |
| `docs/usage/patterns/QUICK_REFERENCE.md` | Cheat sheet for common data scenarios and quick pattern selection. |
| `docs/usage/patterns/pattern_matrix.md` | Decision router + per-pattern recipes that map load behaviors to config fields. |
| `docs/usage/patterns/EXTRACTION_GUIDANCE.md` | When to choose `full`, `cdc`, `current_history`, or hybrid loads for your source. |
| `docs/usage/patterns/ENHANCED_FEATURES.md` | Tunable features (partitioning, chunking, observability) to keep Bronze/Silver efficient. |
| `docs/examples/configs/*.yaml` | Ready-to-copy configs for each pattern that align with the sample data and tests. |

## 3. Framework Maintainers

When you need to change the engine, add patterns, or understand the architecture, these references are the source of truth:

| Doc | Focus |
| --- | ----- |
| `docs/framework/silver_patterns.md` | Canonical intent model: `entity_kind`, `history_mode`, `input_mode`, deletes, partitioning, metadata, etc. |
| `docs/framework/pipeline_engine.md` | Engine behaviors, Bronze/Silver flow, orchestration contracts, and metadata reporting. |
| `docs/framework/architecture.md` | Design intent for the services, layering, and data observability. |
| `docs/framework/reference/DOCUMENTATION.md` | Holistic guide that summarizes the framework patterns, config surfaces, and architecture FAQs. |
| `docs/framework/reference/CONFIG_REFERENCE.md` / `docs/framework/reference/QUICK_REFERENCE.md` | Exhaustive YAML options plus quick CLI patterns for tuning and automation. |
| `docs/framework/operations/OPERATIONS.md` / `docs/framework/operations/OPS_PLAYBOOK.md` | Operations guidance, monitoring, and metadata visibility for Bronze/Silver. |
| `docs/framework/operations/PERFORMANCE_TUNING.md` | Profiling, chunking, and write tuning notes to keep throughput and file size manageable. |
| `docs/framework/operations/CONFIG_DOCTOR.md` / `docs/framework/operations/ERROR_CODES.md` | Troubleshooting tools, error keys, and diagnostics. |
| `docs/framework/operations/SILVER_JOIN.md` | Silver-level joins and curated dataset guidance. |
| `docs/framework/operations/CONTRIBUTING.md` / `docs/framework/operations/TESTING.md` | Contribution process and automation/test expectations. |
| `docs/framework/manifesto-playbook.md` + `docs/framework/UPGRADING.md` | Experience goals and migration notes that keep the manifesto’s promise consistent. |

## Keeping the tracks aligned

1. Update the **framework docs** (maintainer track) when you add patterns or change behaviors.  
2. Refresh the **usage docs** so owners still find the right onboarding path (intent guide, pattern matrix, checklist).  
3. Review the **setup guide** when prerequisites, dependencies, or install steps change.  
4. Re-run the sample generator (`python scripts/generate_sample_data.py`) and automation tests (`pytest tests/test_integration_samples.py`) so examples stay executable and aligned with new paths.
