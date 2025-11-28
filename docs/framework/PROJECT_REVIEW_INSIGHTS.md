# Project Review - Key Insights & Recommendations

**Review Date**: November 28, 2025
**Scope**: Full project audit from architecture to testing
**Target Audience**: Current & future contributors, maintainers, users

---

## Summary for Future Developers

This document complements `PROJECT_REVIEW.md` with actionable insights and recommendations for working with medallion-foundry.

---

## Section 1: Architecture Decision Record (ADR)

### Decision: Plugin-Based Storage Backends

**Status**: Implemented (v1.0+)
**Rationale**: Allows adding S3, Azure, local backends without modifying core logic
**Implementation**: `core/storage/plugin_manager.py` + factory registration

**For Future Extensions**:
- To add a new backend (e.g., GCS), create `core/storage/plugins/gcs_storage.py`
- Implement `StorageBackend` interface (upload_file, download_file, list_files, delete_file)
- Register factory in `core/storage/plugin_factories.py`
- Add tests in `tests/test_gcs_integration.py`

### Decision: Pydantic Models for Configuration

**Status**: In Progress (v1.0 → v1.3)
**Rationale**: Type safety, validation errors upfront, IDE support
**Current State**: `core/config/typed_models.py` mirrors all config sections

**For Future Work**:
- Migrate remaining legacy models to Pydantic dataclasses
- Add `example()` method to each model for documentation
- Export JSON schema for external validators

### Decision: Chunking Strategy (Part Files)

**Status**: Implemented, evolved
**Rationale**: Allows parallel processing, keeps memory usage bounded

**Behavior**:
- Files split by `max_rows_per_file` (default 100K)
- Output: `part-0001.parquet`, `part-0002.parquet`, etc.
- Part count tracked in `_metadata.json`

**For Optimization**:
- Consider dynamic chunk sizing based on row width
- Add monitoring for "wide" vs "narrow" datasets
- Implement spill-to-disk for streaming merges in Silver

---

## Section 2: Design Patterns Used

### 1. **Extractor Factory Pattern**
```python
# Location: core/runner/job.py::build_extractor()
def build_extractor(cfg: Dict[str, Any]) -> BaseExtractor:
    source_type = cfg["source"].get("type", "api")
    if source_type == "api":
        return ApiExtractor()
    elif source_type == "db":
        return DbExtractor()
    # ... etc
```

**Use When**: Adding new extractors → add elif clause + test

### 2. **Context Pattern (RunContext)**
```python
# Location: core/context.py
class RunContext:
    cfg: Dict[str, Any]
    run_date: date
    bronze_path: Path
    # ... shared state across entire run
```

**Use When**: Passing shared state across modules → add to RunContext instead of threading params

### 3. **Plugin Registry Pattern**
```python
# Location: core/storage/plugin_manager.py
BACKEND_FACTORIES = {}
def register_backend(name: str, factory):
    BACKEND_FACTORIES[name] = factory
```

**Use When**: Adding extensible subsystems → use registry + factory

### 4. **Deprecation Wrapper Pattern**
```python
# Location: core/deprecation.py
def warn_if_using_deprecated_key(cfg, old_key, new_key):
    if old_key in cfg:
        warnings.warn(f"Use {new_key} instead", DeprecationWarning)
```

**Use When**: Changing config keys → wrap old key with deprecation warning

### 5. **Metadata for Idempotency**
```python
# Bronze/Silver both emit:
# - _metadata.json (run summary, record counts, timing)
# - _checksums.json (SHA256 per chunk)
# Enables: re-running same config/date = identical output
```

**Use When**: Implementing new features → add metadata, validate checksums

---

## Section 3: Testing Deep Dive

### Test Organization Philosophy

```
Unit Tests (mark=unit)
├─ Focus: Individual function/class logic
├─ Scope: No network, no I/O (mocked)
├─ Speed: < 1s per test
├─ Count: ~20+ files

Integration Tests (mark=integration)
├─ Focus: End-to-end flows (Bronze → Silver)
├─ Scope: Real I/O (S3 moto, Azure Azurite, local fs)
├─ Speed: 5-30s per test
├─ Count: ~10 files

Smoke Tests (quick validation)
├─ Focus: Happy path sanity checks
├─ Run: Before every commit
└─ Example: test_integration_smoke.py
```

### Key Test Files & What to Modify

| File | When to Modify | What to Check |
|------|---|---|
| `tests/test_config.py` | Config schema changes | Typed models, validation, deprecation warnings |
| `tests/test_api_extractor.py` | API extractor logic | Auth, pagination, retry, rate limit |
| `tests/conftest.py` | Adding new fixtures | Sample data availability, fixture scope |
| `tests/test_integration_samples.py` | End-to-end flow changes | Bronze→Silver round-trips, checksums |
| `tests/test_silver_processor.py` | Silver logic changes | Chunking, SCD1/2/CDC dedup, partitioning |

### Sample Data Strategy

**Bronze Sample Data** (`sampledata/source_samples/`):
- Represents 3 extraction patterns: full, CDC, current_history
- Used by integration tests to validate extraction
- Regenerate: `python scripts/generate_sample_data.py`
- **Important**: Checked into git; don't delete

**Silver Sample Data** (`sampledata/silver_samples/`):
- Shows expected Silver output shapes for each pattern + model combo
- Regenerate: `python scripts/generate_silver_samples.py --formats both`
- **Recommendation**: Keep in sync with Bronze samples via integration tests

### Coverage Goals

**Current**: ~75% (core modules)
**Target**: 85% (by v1.1)

**Low Coverage Areas**:
- Edge case error paths (network timeouts, malformed responses)
- Optional features (async HTTP, rate limiting when not stressed)
- Cloud storage specific (Azure managed identity flows)

**To Improve**:
- Add property-based tests (hypothesis) for config validation
- Stress tests for rate limiting, retry logic
- Chaos tests for cloud storage transience

---

## Section 4: Configuration Governance

### Config Ownership Model

```
┌─────────────────────────────────────────────────────┐
│ PLATFORM TEAM (Owns: platform: section)             │
├─────────────────────────────────────────────────────┤
│ Decisions:                                           │
│  ├─ Storage backend (S3, Azure, local)              │
│  ├─ Partitioning strategy (dt, pattern, etc.)       │
│  ├─ Output defaults (Parquet compression, etc.)     │
│  ├─ Rate limits, retry thresholds                   │
│  └─ Logging, monitoring, webhooks                   │
└─────────────────────────────────────────────────────┘
                        ↑
                   Shared in
                   platform:
                     section
                        ↑
┌─────────────────────────────────────────────────────┐
│ DOMAIN TEAMS (Owns: sources: section per dataset)   │
├─────────────────────────────────────────────────────┤
│ Decisions:                                           │
│  ├─ Source system details (API, DB, file path)      │
│  ├─ Extraction query/endpoint                       │
│  ├─ Load pattern (full, CDC, current_history)       │
│  ├─ Silver domain/entity/version naming             │
│  ├─ Primary keys, order columns                     │
│  └─ Normalization rules, error handling             │
└─────────────────────────────────────────────────────┘
```

### Config Version Management

- **Version 1** (current): Initial stable schema
- **Deprecation Path**:
  1. Feature added in new key
  2. Old key still works (generates CFG### warning)
  3. After N releases, old key removed
  4. Example: `source.api.url` → `source.api.base_url` (CFG002)

**For Contributors**:
- New config options → add to Pydantic model, increment config_version minor
- Removing options → implement deprecation warning first, remove 1-2 releases later
- Breaking changes → document in UPGRADING.md

---

## Section 5: Performance Tuning Checklist

### For Dataset Owners

```
□ Is your dataset large (>1GB)?
  └─ Tune max_rows_per_file (default 100K)
  └─ Consider Parquet compression (snappy, gzip)

□ API rate limits causing failures?
  └─ Set rate_limit_hz to sustainable level (e.g., 5 req/s)
  └─ Consider prefetch_pages if using async

□ Running many configs in parallel?
  └─ Set --parallel-workers to (CPU count / 2)
  └─ Monitor memory usage (each worker loads chunks)

□ Incremental load (CDC) is slow?
  └─ Verify cursor tracking in state file
  └─ Check Silver partition strategy (avoid too many partitions)
```

### For Infrastructure Teams

```
□ S3 performance: Use correct region, consider S3 Select for filtering
□ Azure performance: ADLS Gen2 faster than Blob Storage for analytics
□ Local NAS: Network latency? Consider caching intermediate results
□ Monitoring: Set up CloudWatch/Azure Monitor alerts for storage ops
□ Cost: Review file split size (smaller = more files = higher API costs)
```

---

## Section 6: Common Pitfalls & Solutions

### Pitfall 1: Config Validation Passes But Extraction Fails

**Symptoms**:
- `--validate-only` succeeds
- `python bronze_extract.py` fails with "connection refused"

**Root Cause**:
- `--validate-only` checks syntax only; `--dry-run` also tests connectivity

**Solution**:
```bash
# Test before running
python bronze_extract.py --config config.yaml --dry-run
```

### Pitfall 2: Silver Output Has Wrong Schema

**Symptoms**:
- Column names don't match config
- Data types are object instead of int/float

**Root Cause**:
- Missing `silver.schema.column_order` or type hints in source
- Normalization rules (trim_strings, empty_strings_as_null) not applied

**Solution**:
```yaml
silver:
  schema:
    column_order: [id, name, amount]  # Enforce order
  normalization:
    trim_strings: true
    empty_strings_as_null: true
```

### Pitfall 3: Rerun Creates Duplicates

**Symptoms**:
- Running same config twice doubles record count
- Checksums differ

**Root Cause**:
- Bronze metadata not being respected
- Old partition not being overwritten

**Solution**:
- Verify `--date` is the same for reruns
- Check for filesystem race conditions
- Ensure output directory is writable

### Pitfall 4: Async HTTP Extraction Hangs

**Symptoms**:
- Process hangs at 50% completion
- No logs

**Root Cause**:
- Prefetch buffer full (backpressure not working)
- Rate limiter too aggressive

**Solution**:
```yaml
source:
  run:
    use_async: true
    rate_limit_hz: 5        # Lower rate
    prefetch_pages: 2       # Lower prefetch
```

### Pitfall 5: Storage Backend Not Found

**Symptoms**:
- `FileNotFoundError: No backend factory for 's3'`

**Root Cause**:
- `core.storage.plugin_factories` not imported
- Plugin not registered

**Solution**:
```python
# In bronze_extract.py or silver_extract.py
import core.storage.plugin_factories  # noqa: F401 - registers backends
```

---

## Section 7: Debugging Guide

### Enable Debug Logging

```bash
# Option 1: Environment variable
export BRONZE_LOG_LEVEL=DEBUG
python bronze_extract.py --config config.yaml

# Option 2: CLI flag
python bronze_extract.py --config config.yaml --verbose
```

### Inspect Intermediate Outputs

```bash
# Bronze metadata
cat output/system=foo/table=bar/dt=2025-11-28/_metadata.json | python -m json.tool

# Bronze checksums
cat output/system=foo/table=bar/dt=2025-11-28/_checksums.json | python -m json.tool

# Silver metadata
cat silver_output/domain=foo/entity=bar/v1/load_date=2025-11-28/_metadata.json

# Check for error rows
ls -la output/system=foo/table=bar/dt=2025-11-28/_errors/
```

### Run Single Test in Isolation

```bash
# Run one test
pytest tests/test_api_extractor.py::test_bearer_token_auth -v

# Run tests matching pattern
pytest tests/ -k "scd_type_2" -v

# Run with print output
pytest tests/test_config.py::test_deprecation_warning -v -s
```

### Dry Run vs Validate Only

```bash
# Validate syntax + schema only
python bronze_extract.py --config config.yaml --validate-only

# Validate + test connectivity (recommended)
python bronze_extract.py --config config.yaml --dry-run

# Actually extract
python bronze_extract.py --config config.yaml --date 2025-11-28
```

---

## Section 8: Roadmap for Contributors

### High Priority (Next 2 Releases)

- [ ] Complete migration to Pydantic for all config models
- [ ] Add JSON schema export for config validation
- [ ] Implement retry telemetry (failures per extractor type)
- [ ] Enhanced error context in exception handling
- [ ] Documentation: "How to add a custom extractor" tutorial

### Medium Priority (Next 4 Releases)

- [ ] Multi-config job submission (batch job API)
- [ ] Advanced scheduling hooks (pre/post callbacks)
- [ ] Schema evolution tracking (version migrations)
- [ ] Cost optimization recommendations (file size, partition strategy)
- [ ] Gold layer template library (starter Parquet → Iceberg)

### Nice-to-Have (Backlog)

- [ ] Web UI for config validation + dry run
- [ ] Custom extractor marketplace/registry
- [ ] Terraform modules for cloud deployments
- [ ] dbt integration layer (Silver → dbt models)
- [ ] PySpark support for large extractions

---

## Section 9: Documentation Improvement Opportunities

### Areas Well-Documented

✅ Quick start (QUICKSTART.md)
✅ Configuration reference (CONFIG_REFERENCE.md)
✅ Architecture overview (architecture.md)
✅ Silver patterns (silver_patterns.md)
✅ Examples and sample configs

### Areas Needing Improvement

⚠️ Custom extractor development (template/example needed)
⚠️ Storage backend plugin development
⚠️ Tracing/observability integration
⚠️ Migration from legacy systems
⚠️ Kubernetes/containerization best practices

**Recommendations**:
1. Add `docs/CUSTOM_EXTRACTOR_DEVELOPMENT.md` with step-by-step tutorial
2. Add `docs/ADDING_STORAGE_BACKENDS.md` with GCS example
3. Expand `docs/framework/operations/OPERATIONS.md` with monitoring queries
4. Create `docs/examples/MIGRATION_PATTERNS.md` for legacy system users

---

## Section 10: Maintenance & Support

### Who Does What

| Role | Responsibilities |
|------|---|
| **Maintainer** | Merge PRs, version bumps, release notes, security patches |
| **Contributor** | Bug fixes, feature PRs, tests, docs updates |
| **User** | Config definition, orchestration, monitoring |

### SLA for Issues

- **Critical** (data loss, security): 24 hours
- **High** (extraction failures): 3 days
- **Medium** (config validation): 1 week
- **Low** (documentation, feature requests): backlog

### Release Process

1. Bump version in `core/__init__.py` (if manual) or via `setuptools_scm`
2. Update `CHANGELOG.md` with summary
3. Tag `v1.x.y` in git
4. GitHub Actions auto-builds and publishes
5. Announce in documentation

---

## Final Recommendations

### For Next Project Review (Quarterly)

1. **Metrics**:
   - Test coverage trend
   - Issue resolution SLA compliance
   - Release cadence (target: monthly minor, quarterly major)

2. **Backlog Health**:
   - Are deprecations being removed on schedule?
   - Are contributed features well-maintained?
   - Is documentation staying current?

3. **User Feedback**:
   - Common pain points in issues/discussions
   - Feature requests volume
   - Adoption metrics (GitHub stars, downloads)

### For Maintainers

- **Security**: Audit dependencies quarterly (pip audit, dependabot)
- **Performance**: Run benchmark suite before each release
- **Compatibility**: Test against supported Python versions (3.8-3.13)
- **Documentation**: Ensure every new feature has docs before merging

### For Users

- **Upgrade Regularly**: Stay within 1-2 releases of latest
- **Monitor Deprecations**: Watch CHANGELOG for CFG### warnings
- **Provide Feedback**: Report bugs, suggest features via GitHub issues
- **Share Learnings**: Document your configs, contribute examples

---

**Document Version**: 1.0
**Next Review**: Q1 2026
**Maintainer**: Anthony Sebion (@tonysebion)
**License**: MIT (project is open source)
