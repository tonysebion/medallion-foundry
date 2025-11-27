# Source Owner Intent Guide

This guide distills the “source owner experience” into a purpose-built path. Follow the numbered steps, copy the linked templates, and let the framework infer the rest—no Python hacking required.

1. **Describe your system & entities in plain language**
   - Start from `docs/examples/configs/templates/owner_intent_template.yaml`. Replace the `system`, `entity`, and high-level `bronze`/`silver` fields with your own objects.
   - Run `python scripts/expand_owner_intent.py --config <your-intent>.yaml` to print the inferred Bronze/Silver folders and save a resolved intent file (the generated `owner_intent_template.resolved.yaml` shows the pattern).

2. **Lean on the pattern matrix**
   - Consult `docs/usage/patterns/pattern_matrix.md` to see how every load pattern (full, CDC, current_history, hybrid reference/delta, incremental) maps to the sample configs we ship.
   - Copy the matching `docs/examples/configs/patterns/pattern_*.yaml` to create your own pattern variation and point it at your source feed.

3. **Run a safe-first Bronze extract**
   - Use the intent file with `python bronze_extract.py --config <your-intent>.yaml --date YYYY-MM-DD --dry-run` to verify the target folders and metadata without writing data.
   - When you’re ready, run the same command without `--dry-run` but with a narrow `--date` (or note `run_mode: dev_test` / `test_range_days: 1` in the intent file) so only one partition lands.

4. **Inspect Bronze via the readiness checklist**
   - Follow `docs/usage/onboarding/bronze_readiness_checklist.md` to confirm `_metadata.json`, `_checksums.json`, ownership metadata, and partition structure match your intent.
   - After you’re confident, mark the dataset (or metadata) with `bronze_status: ready` so downstream teams can see it in dashboards or OpenMetadata.

5. **Promote to Silver and iterate safely**
   - Run `python silver_extract.py --config <your-intent>.yaml --date YYYY-MM-DD` once Bronze checks out. The configuration already drives partitioning, normalization, metadata, and Silver chunking.
   - Keep iterating by editing the same intent file; reruns keep the aliases, owners, and metadata consistent.

Keep this guide next to your onboarding documents so every owner can see the complete journey (intent → samples → readiness) without hunting through multiple reference files. All the deep-dive docs (`reference_intent.yaml`, `docs/usage/patterns/pattern_matrix.md`, `docs/usage/onboarding/new_dataset_checklist.md`, `docs/usage/onboarding/bronze_readiness_checklist.md`) are linked above.
