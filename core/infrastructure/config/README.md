Core config helpers
====================

This package holds the shared configuration plumbing:

* `loader.py` handles reading YAML files and exposing `load_config/load_configs`.
* `validation.py` enforces schema rules, derives defaults, and normalizes the embedded `silver` block before the extractors run.
* `models.py` defines the typed `SilverConfig` primitives that back the normalization logic.

Add new validation helpers here when you need more structure in configs (e.g., cross-field constraints or schema versions).
