# Safe Experimentation Guide

**Test configurations safely** - Learn by doing without breaking production.

---

## 🧪 **Experimentation Levels**

### **Level 1: No Risk (Recommended for Beginners)**

**Demo Mode** - See everything working with sample data:
```bash
python scripts/run_demo.py
```
- ✅ No setup required
- ✅ No API keys needed
- ✅ Shows complete Bronze → Silver flow
- ✅ Safe to run multiple times

### **Level 2: Low Risk (Test Your Configs)**

**Validation Only** - Check configs without extracting data:
```bash
python bronze_extract.py --config config/my.yaml --validate-only
```
- ✅ Validates YAML syntax
- ✅ Checks required fields
- ✅ Verifies data types
- ✅ No network calls or file writes

**Dry Run** - Test connections without saving data:
```bash
python bronze_extract.py --config config/my.yaml --dry-run
```
- ✅ Tests API/database connections
- ✅ Validates authentication
- ✅ Shows what would be extracted
- ✅ No data persistence

### **Level 3: Medium Risk (Extract Small Amounts)**

**Limit Rows** - Extract just a few records:
```yaml
# In your config
source:
  run:
    limit_rows: 10  # Only extract 10 rows
```

**Date Range** - Extract specific dates:
```bash
python bronze_extract.py --config config/my.yaml --date 2025-11-27
```
- ✅ Small, manageable data sets
- ✅ Easy to inspect results
- ✅ Quick feedback

---

## 🛡️ **Safety Features**

### **Automatic Cleanup**
- Failed runs automatically clean up partial files
- Metadata tracks successful vs failed extractions
- Checksums ensure data integrity

### **Idempotent Operations**
- Running the same command twice is safe
- Metadata prevents duplicate processing
- Bronze layer supports reruns

### **Isolated Testing**
- Use separate `config/test/` directory
- Different output paths for testing
- Environment variables for test credentials

---

## 🔬 **Experimentation Workflow**

### **Step 1: Start Small**
```bash
# 1. Copy a working config
cp docs/examples/configs/examples/api_example.yaml config/test_api.yaml

# 2. Validate it
python bronze_extract.py --config config/test_api.yaml --validate-only

# 3. Test connection (dry run)
python bronze_extract.py --config config/test_api.yaml --dry-run
```

### **Step 2: Extract Tiny Amount**
```bash
# Extract just 5 rows for testing
# Add to your config temporarily:
# source:
#   run:
#     limit_rows: 5

python bronze_extract.py --config config/test_api.yaml --date 2025-11-27
```

### **Step 3: Inspect Results**
```bash
# Check what was created
ls output/system=*/table=*/dt=2025-11-27/

# View metadata
cat output/system=*/table=*/dt=2025-11-27/_metadata.json

# Sample the data
python -c "import pandas as pd; print(pd.read_parquet('output/.../part-0001.parquet').head())"
```

### **Step 4: Try Silver**
```bash
# Promote to Silver layer
python silver_extract.py --config config/test_api.yaml --date 2025-11-27

# Check Silver output
ls silver_output/domain=*/entity=*/v1/load_date=2025-11-27/
```

---

## 🧹 **Cleanup After Testing**

### **Remove Test Data**
```bash
# Delete test outputs
rm -rf output/ silver_output/

# Or keep for inspection but move aside
mv output/ output_test_backup/
mv silver_output/ silver_output_test_backup/
```

### **Reset Configs**
```bash
# Remove test configs
rm config/test_*.yaml

# Or move to archive
mkdir config/archive/
mv config/test_*.yaml config/archive/
```

---

## 🚨 **What to Avoid in Production**

### **Don't Skip These Steps**
- ❌ Don't run unvalidated configs in production
- ❌ Don't skip dry runs for new data sources
- ❌ Don't test with production credentials initially

### **Common Mistakes**
- ❌ Running extractions without date filters (extracts all history)
- ❌ Not checking file sizes before full runs
- ❌ Using production endpoints for initial testing

---

## 📊 **Testing Checklist**

**Before production runs:**
- [ ] Config validates without errors (`--validate-only`)
- [ ] Dry run succeeds (`--dry-run`)
- [ ] Test extraction works with `limit_rows: 10`
- [ ] Output directories have sufficient space
- [ ] Credentials are correct (test with small API call)
- [ ] Date ranges are reasonable (not extracting years of data)

**During testing:**
- [ ] Monitor log output for warnings/errors
- [ ] Check file sizes are reasonable
- [ ] Verify data quality (sample records)
- [ ] Test Silver promotion works

**After testing:**
- [ ] Clean up test data
- [ ] Archive test configs
- [ ] Document any issues found

---

## 🆘 **When Things Go Wrong**

### **Quick Recovery**
```bash
# Stop any running processes
# Check logs for error details
# Clean up partial files if needed
rm -rf output/system=*/table=*/dt=2025-11-27/

# Try again with fixes
python bronze_extract.py --config config/my.yaml --date 2025-11-27
```

### **Get Help**
1. **Config issues**: Run [Config Doctor](../../framework/operations/CONFIG_DOCTOR.md)
2. **Error codes**: Check [Error Reference](../../framework/operations/ERROR_CODES.md)
3. **Examples**: Browse [working configs](../../examples/configs/README.md)

---

## 🎯 **Pro Tips**

- **Start with file sources** - No API keys, fastest feedback
- **Use environment variables** - Keep secrets out of configs
- **Test incrementally** - Small datasets → medium → full
- **Version control configs** - Track changes over time
- **Document your tests** - Note what worked/didn't work
