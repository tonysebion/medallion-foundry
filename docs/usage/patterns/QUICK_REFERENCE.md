# Pattern Picker: Choose Your Extraction Strategy

**Don't know which pattern to use?** Answer 3 simple questions:

---

## ❓ **Question 1: What's Your Data Like?**

| Data Type | Bronze Pattern | Silver Models | Use Case | Example |
|-----------|----------------|---------------|----------|---------|
| **Complete snapshots** | `full` | `periodic_snapshot`<br>`full_merge_dedupe`<br>`scd_type_1` | Daily exports, complete API responses | Product catalog, user list |
| **Change events only** | `cdc` | `incremental_merge`<br>`full_merge_dedupe` | Transaction logs, audit trails | Order updates, user activity |
| **Current + history** | `current_history` | `scd_type_2`<br>`incremental_merge` | Slowly changing dimensions | Customer addresses, product status |
| **CDC + reference** | `cdc` (hybrid point) | `incremental_merge` | Reference data + delta changes | Customer master + updates |
| **CDC cumulative** | `cdc` (hybrid cumulative) | `incremental_merge` | Layered cumulative changes | Audit trails with history |
| **Incremental point** | `cdc` (incremental point) | `full_merge_dedupe` | Latest value wins | Status updates, counters |
| **Incremental SCD1** | `cdc` (incremental cumulative) | `scd_type_1` | Current state overwrites | Profile updates, settings |

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
    load_pattern: full

silver:
  model: periodic_snapshot  # Simple pass-through
```

### **For APIs**
| API Type | Bronze Pattern | Silver Model | Example |
|----------|----------------|--------------|---------|
| **GitHub Issues** | `full` | `periodic_snapshot` | Complete issue list |
| **Shopify Orders** | `cdc` | `incremental_merge` | Order updates |
| **User Profiles** | `current_history` | `scd_type_2` | Profile changes |
| **Reference + Updates** | `cdc` (hybrid point) | `incremental_merge` | Master data + changes |
| **Audit Logs** | `cdc` (hybrid cumulative) | `incremental_merge` | Complete change history |

### **For Databases**
| Table Type | Bronze Pattern | Silver Model | Example |
|------------|----------------|--------------|---------|
| **Transactions** | `cdc` | `incremental_merge` | Order history |
| **Reference Data** | `full` | `full_merge_dedupe` | Product catalog |
| **Customer Data** | `current_history` | `scd_type_2` | Address changes |
| **Status Tables** | `cdc` (incremental point) | `full_merge_dedupe` | Current status |
| **Settings/Config** | `cdc` (incremental cumulative) | `scd_type_1` | Latest settings |

### **For Files**
| File Type | Bronze Pattern | Silver Model | Example |
|-----------|----------------|--------------|---------|
| **Daily CSVs** | `full` | `periodic_snapshot` | Complete exports |
| **Change Logs** | `cdc` | `incremental_merge` | Audit trails |
| **Master Data** | `current_history` | `scd_type_2` | Customer master |
| **Delta Files** | `cdc` (hybrid point) | `incremental_merge` | Reference + deltas |
| **Cumulative Updates** | `cdc` (hybrid cumulative) | `incremental_merge` | Change accumulation |

---

## 📋 **Complete Config Examples**

| Pattern | Bronze Config | Silver Model | What It Does |
|---------|---------------|--------------|--------------|
| **Full** | `docs/examples/configs/patterns/pattern_full.yaml` | `periodic_snapshot` | Complete snapshots |
| **CDC** | `docs/examples/configs/patterns/pattern_cdc.yaml` | `incremental_merge` | Change events |
| **Current+History** | `docs/examples/configs/patterns/pattern_current_history.yaml` | `scd_type_2` | SCD Type 2 |
| **CDC Point** | `docs/examples/configs/patterns/pattern_hybrid_cdc_point.yaml` | `incremental_merge` | Reference + deltas |
| **CDC Cumulative** | `docs/examples/configs/patterns/pattern_hybrid_cdc_cumulative.yaml` | `incremental_merge` | Cumulative changes |
| **Incremental Point** | `docs/examples/configs/patterns/pattern_hybrid_incremental_point.yaml` | `full_merge_dedupe` | Latest values |
| **Incremental SCD1** | `docs/examples/configs/patterns/pattern_hybrid_incremental_cumulative.yaml` | `scd_type_1` | Current overwrites |

### **Pattern + Model Combinations**

```yaml
# Full snapshot → Simple periodic refresh
source:
  run:
    load_pattern: full
silver:
  model: periodic_snapshot

# CDC events → Incremental merge
source:
  run:
    load_pattern: cdc
silver:
  model: incremental_merge
  natural_keys: ["order_id"]
  event_ts_column: "changed_at"

# Current + history → SCD Type 2
source:
  run:
    load_pattern: current_history
silver:
  model: scd_type_2
  natural_keys: ["customer_id"]
  change_ts_column: "updated_at"

# CDC with reference → Incremental merge
source:
  run:
    load_pattern: cdc
    reference_mode:
      role: reference
      cadence_days: 7
silver:
  model: incremental_merge
  natural_keys: ["customer_id"]
  event_ts_column: "changed_at"

# Incremental SCD1 → Current state only
source:
  run:
    load_pattern: cdc
    delta_mode: cumulative
silver:
  model: scd_type_1
  natural_keys: ["user_id"]
  order_column: "updated_at"
```

---

## 🔄 **How to Change Patterns**

1. **Choose your Bronze pattern**:
   ```yaml
   source:
     run:
       load_pattern: cdc  # full, cdc, or current_history
   ```

2. **Select compatible Silver model**:
   ```yaml
   silver:
     model: incremental_merge  # Choose based on pattern above
     natural_keys: ["id"]      # Required for most models
     order_column: "updated_at"  # For deduplication
   ```

3. **Test safely**:
   ```bash
   python bronze_extract.py --config config/my.yaml --dry-run
   python silver_extract.py --config config/my.yaml --dry-run
   ```

4. **Run extraction**:
   ```bash
   python bronze_extract.py --config config/my.yaml --date YYYY-MM-DD
   python silver_extract.py --config config/my.yaml --date YYYY-MM-DD
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
- [Configuration Reference](../../framework/reference/CONFIG_REFERENCE.md) - Complete config options
- [Enhanced Features](ENHANCED_FEATURES.md) - Advanced options
