# Medallion Foundry Architecture

This page is the visual anchor for how Bronze → Silver (and storage backends) fit together. Use the referenced documents for runbooks or configuration details.

## Platform layers

- **Bronze** captures raw extracts from sources. `bronze_extract.py` validates `platform` + `source`, slices data into chunks, applies schema/normalization, and writes metadata/checksum files. Bronze can target S3, Azure, or local storage, chooseable in `platform.bronze.storage_backend`. Supports async HTTP extraction with prefetch pagination, unified retry + circuit breaker resilience, rate limiting, and optional OpenTelemetry tracing.
- **Silver** promotes Bronze partitions into curated artifacts. It supports multiple Silver models (`SilverModel`) and output formats (Parquet/CSV), plus streaming mode for large Bronze partitions with resume capability via checkpoints. Silver metadata includes runtime, model, and storage metrics to help governance decisions. Streaming resume skips already processed chunks safely.
- **Storage layer** is now fully plugin-based (`core/storage/registry.py`). The Azure backend (`core/storage/plugins/azure_storage.py`) and S3/local backends register themselves via `core/storage/plugin_factories.py`, so you can drop in new providers without editing `core/storage/backend.py`. Resilience features (retry/breaker) integrate seamlessly across all backends.

## Config paths

- Quick start & configs: `README.md` outlines how to run Bronze/Silver. `docs/CONFIG_REFERENCE.md` lists every configurable option (including storage metadata, `storage_scope`, Silver model/profile presets, async flags, rate limiting, tracing env vars, and resume options).  
- Architecture deep dive: This page plus `docs/EXTRACTION_GUIDANCE.md` explain when to pick each Bronze/Silver model and highlight the newly generated sample datasets under `docs/examples/data`.
- Operational procedures: See `docs/OPERATIONS.md` for governance, testing, and sample regeneration playbooks. Error taxonomy in `docs/ERROR_CODES.md` aids routing; performance tuning in `docs/PERFORMANCE_TUNING.md` covers benchmarks and optimization.

## Samples & Tests

- Bronze sample data resides in `docs/examples/data/bronze_samples/` (full/CDC/current_history).  
- Silver samples mirror each Bronze pattern + Silver model under `docs/examples/data/silver_samples/<pattern>/<model>/`; each folder now includes a `README.md` describing the artifacts and how to regenerate them (`scripts/generate_silver_samples.py --formats both`).  
- Tests like `tests/test_silver_formats.py`, the integration suite (Azurite/LocalStack), async HTTP smoke tests, streaming resume tests, and `tests/test_silver_samples_generation.py` confirm both writers and the sample generator stay healthy. Benchmark harness (`scripts/benchmark.py`) measures sync vs async, rate limit impact, and streaming throughput.

## Next steps

- For more detail on storage backends see `docs/STORAGE_BACKEND_ARCHITECTURE.md`.  
- To plug in new storage providers, follow `core/storage/registry.py` + `core/storage/plugin_factories.py` and register your factory.
- Resilience & async: See `docs/ENHANCED_FEATURES.md` for retry/breaker, async HTTP, rate limiting, tracing, and resume details.
- Performance: Run `scripts/benchmark.py` and consult `docs/PERFORMANCE_TUNING.md`.
