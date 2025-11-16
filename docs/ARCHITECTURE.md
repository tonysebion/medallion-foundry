# Medallion Foundry Architecture

This page is the visual anchor for how Bronze → Silver → Gold (and storage backends) fit together. Use the referenced documents for runbooks or configuration details.

## Platform layers

- **Bronze** captures raw extracts from sources. `bronze_extract.py` validates `platform` + `source`, slices data into chunks, applies schema/normalization, and writes metadata/checksum files. Bronze can target S3, Azure, GCS, or local storage, chooseable in `platform.bronze.storage_backend`.
- **Silver** promotes Bronze partitions into curated artifacts. It supports multiple Silver models (`SilverModel`) and output formats (Parquet/CSV), plus streaming mode for large Bronze partitions. Silver metadata includes runtime, model, and storage metrics to help governance decisions.
- **Storage layer** is now fully plugin-based (`core/storage_registry`). The Azure backend (`core/azure_storage.py`) and S3/GCS/local backends register themselves via `core/storage_plugins.py`, so you can drop in new providers without editing `core/storage.py`.

## Config paths

- Quick start & configs: `README.md` outlines how to run Bronze/Silver. `docs/CONFIG_REFERENCE.md` lists every configurable option (including storage metadata, `storage_scope`, and Silver model/profile presets).  
- Architecture deep dive: This page plus `docs/EXTRACTION_GUIDANCE.md` explain when to pick each Bronze/Silver model and highlight the newly generated sample datasets under `docs/examples/data`.
- Operational procedures: See `docs/OPERATIONS.md` for governance, testing, and sample regeneration playbooks.

## Samples & Tests

- Bronze sample data resides in `docs/examples/data/bronze_samples/` (full/CDC/current_history).  
- Silver samples mirror each Bronze pattern + Silver model under `docs/examples/data/silver_samples/<pattern>/<model>/`; each folder now includes a `README.md` describing the artifacts and how to regenerate them (`scripts/generate_silver_samples.py --formats both`).  
- Tests like `tests/test_silver_formats.py`, the integration suite, and `tests/test_silver_samples_generation.py` confirm both writers and the sample generator stay healthy.

## Next steps

- For more detail on storage backends see `docs/STORAGE_BACKEND_ARCHITECTURE.md`.  
- To plug in new storage providers, follow `core/storage_registry.py` + `core/storage_plugins.py` and register your factory.
