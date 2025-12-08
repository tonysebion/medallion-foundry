# medallion-foundry

**Extract data from APIs, databases, or files → Bronze layer → Silver curated datasets**

---

## 🚀 **Quick Start (5 minutes)**

**New to data pipelines?** Get running in 5 minutes:

```bash
# 1. Install
pip install -r requirements.txt

# 2. Try the demo
python scripts/run_demo.py

# 3. Copy a minimal config for your data
cp docs/examples/configs/minimal/minimal_api_example.yaml config/my_api.yaml
# Edit config/my_api.yaml with your API details

# 4. Run
python bronze_extract.py --config config/my_api.yaml --date 2025-11-27
```

**Prefer other sources?** Use minimal configs for your source type:
- REST API: `docs/examples/configs/minimal/minimal_api_example.yaml`
- Database: `docs/examples/configs/minimal/minimal_db_example.yaml`
- CSV/JSON Files: `docs/examples/configs/minimal/minimal_file_example.yaml`

**That's it!** Check `output/` for your Bronze data, then run Silver for curated datasets.

---

## 🎯 **What Do You Want To Do?**

### **I want to extract data from...**

| Data Source | Quick Config | Example |
|-------------|--------------|---------|
| **REST API** | `docs/examples/configs/examples/api_example.yaml` | GitHub API, Shopify, Stripe |
| **Database** | `docs/examples/configs/examples/db_example.yaml` | SQL Server, PostgreSQL |
| **CSV/JSON Files** | `docs/examples/configs/examples/file_example.yaml` | Local files, S3 buckets |
| **Custom Source** | `docs/examples/configs/examples/custom_example.yaml` | Build your own extractor |

### **I want to...**

- **Learn the basics** → [README Quick Start](../README.md)
- **Customize configs** → [Copy & Customize Guide](usage/beginner/COPY_AND_CUSTOMIZE.md)
- **Set up production** → [Intent Owner Guide](usage/onboarding/intent-owner-guide.md)
- **Choose the right pattern** → [Pattern Picker](usage/patterns/QUICK_REFERENCE.md) - Bronze patterns + Silver models
- **Build resilient pipelines** → [Error Handling Guide](guides/error_handling.md) - Retry, rate limiting, circuit breakers  
  → [Retry Configuration Guide](guides/retry_configuration.md)  
  → [API Rate Limiting Guide](guides/api_rate_limiting.md)
- **Troubleshoot** → [Config Doctor](framework/operations/CONFIG_DOCTOR.md)
- **Set up S3 storage** → [S3 Setup Guide](framework/operations/s3-setup-guide.md)
- **Understand Silver models** → [Silver Models Explained](usage/patterns/silver-models-explained.md)

---

## 📚 **Learning Paths**

### **Path 1: Just Get Data Moving (Beginner)**
1. [README Quick Start](../README.md) - Run sample data
2. [Copy & Customize](usage/beginner/COPY_AND_CUSTOMIZE.md) - Adapt for your source
3. [Intent Owner Guide](usage/onboarding/intent-owner-guide.md) - Define your dataset

### **Path 2: Production Data Pipeline (Advanced)**
1. [Intent Owner Guide](usage/onboarding/intent-owner-guide.md) - Define your dataset
2. [Pattern Picker](usage/patterns/QUICK_REFERENCE.md) - Choose Bronze pattern + Silver model
3. [Config Doctor](framework/operations/CONFIG_DOCTOR.md) - Pre-flight checks

### **Path 3: Extend the Framework (Developer)**
1. [Architecture](framework/architecture.md) - System design
2. [Custom Extractor Examples](framework/extending/custom-extractor-examples.md) - Build your own extractors
3. [Testing](framework/operations/TESTING.md) - Quality assurance

---

## 🔧 **Common Tasks**

| Task | Command | Notes |
|------|---------|-------|
| **Extract data** | `python bronze_extract.py --config config/my.yaml --date YYYY-MM-DD` | Creates Bronze layer |
| **Create Silver** | `python silver_extract.py --config config/my.yaml --date YYYY-MM-DD` | Curates Bronze data |
| **Validate config only** | `python bronze_extract.py --config config/my.yaml --validate-only` | Check YAML syntax and schema |
| **Dry run (test connections)** | `python bronze_extract.py --config config/my.yaml --dry-run` | Test connections without extraction |
| **Run demo** | `python scripts/run_demo.py` | Safe experimentation with sample data |

---

## 📖 **Reference**

- [Configuration Reference](framework/reference/CONFIG_REFERENCE.md) - All config options
- [Glossary](framework/reference/glossary.md) - Key terms and concepts
- [API Documentation](api/core.md) - Code reference
- [Operations Playbook](framework/operations/OPS_PLAYBOOK.md) - Production runbooks
- [Troubleshooting Guide](framework/operations/troubleshooting-guide.md) - Common issues & solutions
- [Error Handling & Resilience](guides/error_handling.md) - Retry, circuit breaker, rate limiting
- [Scripts Overview](scripts/README.md) - Utility scripts reference

---

## ⚙️ **System Requirements & Compatibility**

### Python Version Matrix

| Python | Status | Notes |
|--------|--------|-------|
| 3.9 | ✅ Recommended | Minimum supported version; best compatibility |
| 3.10 | ✅ Supported | Recommended for production |
| 3.11 | ✅ Supported | Recommended for production |
| 3.12 | ✅ Supported | Tested in CI/CD |
| 3.13 | ✅ Supported | Latest version supported |
| 3.8 | ❌ Not Supported | Uses Python 3.9+ features (f-strings, type hints) |

**Action:** Use `python3.9` or later. Run `python --version` to verify.

### Storage Backends

| Backend | Status | Min Requirements |
|---------|--------|------------------|
| **Local Filesystem** | ✅ | Python 3.9+, read/write permissions |
| **AWS S3** | ✅ | boto3, AWS credentials, IAM permissions |
| **Azure Blob/ADLS** | ✅ | Azure SDK, connection string or managed identity |

---

## 📋 **Intent Configs vs Legacy Configs**

Modern medallion-foundry uses **Intent Configs** – single YAML files containing both Bronze extraction and Silver promotion definitions.

### Intent Config (Recommended)
```yaml
# Single file: config/my_dataset.yaml
bronze:
  source_type: api
  system: my_system
  # ... Bronze config ...

silver:
  entity_kind: event
  # ... Silver config ...
```

**Usage:**
```bash
python bronze_extract.py --config config/my_dataset.yaml
python silver_extract.py --config config/my_dataset.yaml
```

### Legacy Approach (Not Recommended)
Separate files for Bronze and Silver – leads to sync issues and duplication.

```
config/
  ├── my_dataset_bronze.yaml   # Bronze only
  └── my_dataset_silver.yaml   # Silver only
```

**Migration:** Move to intent configs by consolidating separate files into a single file with both `bronze:` and `silver:` sections. See the example configs in `docs/examples/configs/examples/` for reference patterns.

---

## 🆘 **Need Help?**

**Stuck?** Try these in order:
1. Run `python scripts/run_demo.py` - See working examples
2. Check [Copy & Customize Guide](usage/beginner/COPY_AND_CUSTOMIZE.md) - Step-by-step config adaptation
3. Use [Config Doctor](framework/operations/CONFIG_DOCTOR.md) - Automated troubleshooting
4. Search [Pattern Reference](usage/patterns/QUICK_REFERENCE.md) - Find Bronze + Silver combinations
5. Check [Troubleshooting Guide](framework/operations/troubleshooting-guide.md) - Detailed problem solving
6. Review [S3 Setup Guide](framework/operations/s3-setup-guide.md) - Storage configuration

**Still stuck?** Check the [examples](../examples/) directory for working configs.
