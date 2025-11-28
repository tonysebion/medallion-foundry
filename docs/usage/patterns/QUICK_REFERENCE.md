# Pattern Picker: Choose Your Extraction Strategy

**Don't know which pattern to use?** Answer 3 simple questions:

---

## ❓ **Question 1: What's Your Data Like?**

| Data Type | Pattern | Use Case | Example |
|-----------|---------|----------|---------|
| **Complete snapshots**<br>(full refresh each time) | `full` | Daily exports, complete API responses | Product catalog, user list |
| **Change events only**<br>(inserts/updates/deletes) | `cdc` | Transaction logs, audit trails | Order updates, user activity |
| **Current + history**<br>(SCD Type 2 style) | `current_history` | Slowly changing dimensions | Customer addresses, product status |

---

## ❓ **Question 2: What's Your Update Frequency?**

| Frequency | Recommendation | Config Flag |
|-----------|----------------|-------------|
| **Daily/Weekly** | Standard patterns | `load_pattern: full` |
| **Real-time/Hourly** | CDC patterns | `load_pattern: cdc` |
| **On-demand** | Any pattern | Based on data type above |

---

## ❓ **Question 3: What's Your Storage?**

| Storage | Config Example | Notes |
|---------|----------------|-------|
| **Local files** | `docs/examples/configs/examples/file_example.yaml` | Fastest for testing |
| **S3/Azure** | `docs/examples/configs/examples/api_example.yaml` | Production ready |
| **Database** | `docs/examples/configs/examples/db_example.yaml` | Direct table access |

---

## 🎯 **Quick Recommendations**

### **For Beginners**
```yaml
# Start with this - works for most cases
source:
  type: api  # or 'db' or 'file'
  run:
    load_pattern: full  # Change to 'cdc' for change data
```

### **For APIs**
- **GitHub Issues**: `load_pattern: full`
- **Shopify Orders**: `load_pattern: cdc`
- **User Profiles**: `load_pattern: current_history`

### **For Databases**
- **Transaction Tables**: `load_pattern: cdc`
- **Reference Tables**: `load_pattern: full`
- **Customer Data**: `load_pattern: current_history`

### **For Files**
- **Daily CSVs**: `load_pattern: full`
- **Change Logs**: `load_pattern: cdc`
- **Master Data**: `load_pattern: current_history`

---

## 📋 **Complete Config Examples**

| Pattern | Config File | What It Does |
|---------|-------------|--------------|
| **Full** | `docs/examples/configs/examples/file_example.yaml` | Complete snapshots |
| **CDC** | `docs/examples/configs/examples/api_example.yaml` | Change events |
| **Current+History** | `docs/examples/configs/examples/db_example.yaml` | SCD Type 2 |

---

## 🔄 **How to Change Patterns**

1. **Edit your config**:
   ```yaml
   source:
     run:
       load_pattern: cdc  # Change this
   ```

2. **Test safely**:
   ```bash
   python bronze_extract.py --config config/my.yaml --dry-run
   ```

3. **Run extraction**:
   ```bash
   python bronze_extract.py --config config/my.yaml --date YYYY-MM-DD
   ```

---

## 🆘 **Still Unsure?**

**Try the demo first:**
```bash
python scripts/run_demo.py
```

**Check real examples:**
- Browse `docs/examples/configs/examples/`
- Each file shows a working pattern
- Copy and modify for your needs

**Get detailed guidance:**
- [Pattern Matrix](pattern_matrix.md) - Complete reference
- [Enhanced Features](ENHANCED_FEATURES.md) - Advanced options
