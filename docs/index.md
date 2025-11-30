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

# 3. Copy a config for your data
cp docs/examples/configs/examples/api_example.yaml config/my_api.yaml
# Edit config/my_api.yaml with your API details

# 4. Run
python bronze_extract.py --config config/my_api.yaml --date 2025-11-27
```

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
- **Customize configs** → [`Copy & Customize Guide`](usage/beginner/COPY_AND_CUSTOMIZE.md)
- **Set up production** → [`Intent Owner Guide`](usage/onboarding/intent-owner-guide.md)
- **Choose the right pattern** → [`Pattern Picker`](usage/patterns/QUICK_REFERENCE.md)
- **Troubleshoot** → [`Config Doctor`](framework/operations/CONFIG_DOCTOR.md)

---

## 📚 **Learning Paths**

### **Path 1: Just Get Data Moving (Beginner)**
1. [README Quick Start](../README.md) - Run sample data
2. [Copy & Customize](usage/beginner/COPY_AND_CUSTOMIZE.md) - Adapt for your source
3. [Intent Owner Guide](usage/onboarding/intent-owner-guide.md) - Define your dataset

### **Path 2: Production Data Pipeline (Advanced)**
1. [Intent Owner Guide](usage/onboarding/intent-owner-guide.md) - Define your dataset
2. [Pattern Picker](usage/patterns/QUICK_REFERENCE.md) - Choose load strategy
3. [Config Doctor](framework/operations/CONFIG_DOCTOR.md) - Pre-flight checks

### **Path 3: Extend the Framework (Developer)**
1. [Architecture](framework/architecture.md) - System design
2. [Contributing](framework/operations/CONTRIBUTING.md) - Add features
3. [Testing](framework/operations/TESTING.md) - Quality assurance

---

## 🔧 **Common Tasks**

| Task | Command | Notes |
|------|---------|-------|
| **Extract data** | `python bronze_extract.py --config config/my.yaml --date YYYY-MM-DD` | Creates Bronze layer |
| **Create Silver** | `python silver_extract.py --config config/my.yaml --date YYYY-MM-DD` | Curates Bronze data |
| **Validate config** | `python bronze_extract.py --config config/my.yaml --validate-only` | Check before running |
| **Dry run** | `python bronze_extract.py --config config/my.yaml --dry-run` | Test connections |
| **Run demo** | `python scripts/run_demo.py` | Safe experimentation |

---

## 📖 **Reference**

- [Configuration Reference](framework/reference/CONFIG_REFERENCE.md) - All config options
- [API Documentation](api/core.md) - Code reference
- [Operations Playbook](framework/operations/OPS_PLAYBOOK.md) - Production runbooks
- [Troubleshooting](framework/operations/ERROR_CODES.md) - Common issues

---

## 🆘 **Need Help?**

**Stuck?** Try these in order:
1. Run `python scripts/run_demo.py` - See working examples
2. Check [Copy & Customize Guide](usage/beginner/COPY_AND_CUSTOMIZE.md) - Step-by-step config adaptation
3. Use [Config Doctor](framework/operations/CONFIG_DOCTOR.md) - Automated troubleshooting
4. Search [Pattern Reference](usage/patterns/QUICK_REFERENCE.md) - Find your use case

**Still stuck?** Check the [examples](../examples/) directory for working configs.
