# Load Pattern Config Examples

These configurations demonstrate different data loading patterns used in the medallion architecture. Each pattern handles different types of data changes and historical tracking.

## 🔗 **How Patterns Connect to Sample Data**

### **Source Samples (Input)**
The framework includes **7 pre-generated source datasets** that simulate different data source behaviors:

```
sampledata/source_samples/
├── pattern1_full_events/          # Full daily snapshots
├── pattern2_cdc_events/           # CDC change logs
├── pattern3_scd_state/            # Current + history state
├── pattern4_hybrid_cdc_point/     # CDC + reference metadata
├── pattern5_hybrid_cdc_cumulative/# Cumulative CDC deltas
├── pattern6_hybrid_incremental_point/  # Incremental point-in-time
└── pattern7_hybrid_incremental_cumulative/ # Incremental cumulative
```

### **Pattern Configs (Processing)**
Each config below reads from the corresponding source sample and demonstrates how that pattern transforms data through Bronze → Silver layers.

### **Bronze Outputs (Results)**
When you run these configs, they create Bronze layer outputs. Some configs specify `local_output_dir` to show where outputs land.

---

## 📋 **Pattern Files & Their Source Data**

### **Basic Patterns**
- **`pattern_full.yaml`** → Reads `pattern1_full_events/`
  - **Purpose**: Full snapshot loading (complete dataset refresh)
  - **Source**: Daily full exports from a system that rewrites everything
  - **Run**: `python bronze_extract.py --config pattern_full.yaml --date 2025-11-13`

- **`pattern_cdc.yaml`** → Reads `pattern2_cdc_events/`
  - **Purpose**: Change Data Capture (incremental changes only)
  - **Source**: Transaction logs with inserts/updates/deletes
  - **Run**: `python bronze_extract.py --config pattern_cdc.yaml --date 2025-11-13`

- **`pattern_current_history.yaml`** → Reads `pattern3_scd_state/`
  - **Purpose**: Slowly Changing Dimensions (current + history)
  - **Source**: State snapshots with effective date ranges
  - **Run**: `python bronze_extract.py --config pattern_current_history.yaml --date 2025-11-13`

### **Hybrid Patterns**
- **`pattern_hybrid_cdc_point.yaml`** → Reads `pattern4_hybrid_cdc_point/`
  - **Purpose**: CDC with point-in-time views
  - **Source**: Reference data + delta changes
  - **Run**: `python bronze_extract.py --config pattern_hybrid_cdc_point.yaml --date 2025-11-13`

- **`pattern_hybrid_cdc_cumulative.yaml`** → Reads `pattern5_hybrid_cdc_cumulative/`
  - **Purpose**: CDC with cumulative updates
  - **Source**: Layered cumulative change files
  - **Run**: `python bronze_extract.py --config pattern_hybrid_cdc_cumulative.yaml --date 2025-11-13`

- **`pattern_hybrid_incremental_point.yaml`** → Reads `pattern6_hybrid_incremental_point/`
  - **Purpose**: Incremental loading with point-in-time
  - **Source**: Incremental snapshots, latest value wins
  - **Run**: `python bronze_extract.py --config pattern_hybrid_incremental_point.yaml --date 2025-11-13`

- **`pattern_hybrid_incremental_cumulative.yaml`** → Reads `pattern7_hybrid_incremental_cumulative/`
  - **Purpose**: Incremental SCD1 state refreshes (latest overwrite)
  - **Source**: Cumulative incremental merges
  - **Run**: `python bronze_extract.py --config pattern_hybrid_incremental_cumulative.yaml --date 2025-11-13`

### **File-Based Examples**
- **`file_cdc_example.yaml`** → Alternative CDC implementation
- **`file_current_history_example.yaml`** → Alternative SCD implementation

---

## 🚀 **Quick Test: Run a Pattern**

```bash
# 1. Verify source data exists
ls sampledata/source_samples/pattern1_full_events/

# 2. Run Bronze extraction
python bronze_extract.py --config docs/examples/configs/patterns/pattern_full.yaml --date 2025-11-13

# 3. Check Bronze output (goes to ./output/ by default)
ls output/system=retail_demo/table=orders/

# 4. Run Silver promotion (if silver.enabled: true)
python silver_extract.py --config docs/examples/configs/patterns/pattern_full.yaml --date 2025-11-13

# 5. Check Silver output
ls silver/domain=retail_demo/entity=orders/
```

---

## 📖 **Understanding Pattern Numbers**

The pattern numbers (1-7) correspond to the **Pattern Matrix** in `docs/usage/patterns/pattern_matrix.md`:

| Pattern | Matrix Name | Config File | Source Data |
|---------|-------------|-------------|-------------|
| 1 | Full Events | `pattern_full.yaml` | `pattern1_full_events/` |
| 2 | CDC Events | `pattern_cdc.yaml` | `pattern2_cdc_events/` |
| 3 | SCD State | `pattern_current_history.yaml` | `pattern3_scd_state/` |
| 4 | Derived Events (CDC) | `pattern_hybrid_cdc_point.yaml` | `pattern4_hybrid_cdc_point/` |
| 5 | Derived State (Cumulative) | `pattern_hybrid_cdc_cumulative.yaml` | `pattern5_hybrid_cdc_cumulative/` |
| 6 | Derived State (Latest) | `pattern_hybrid_incremental_point.yaml` | `pattern6_hybrid_incremental_point/` |
| 7 | State (SCD1) | `pattern_hybrid_incremental_cumulative.yaml` | `pattern7_hybrid_incremental_cumulative/` |

---

## 🎯 **Purpose**

These configs illustrate:
- Different load pattern implementations
- How patterns affect Bronze and Silver processing
- Historical data management strategies
- Incremental vs full loading trade-offs

## 💡 **Pro Tips**

- **Start with Pattern 1**: `pattern_full.yaml` is the simplest to understand
- **Check source data first**: Run `ls sampledata/source_samples/pattern1_full_events/` to verify data exists
- **Outputs go to `./output/`**: Unless `local_output_dir` is specified in the config
- **All configs work immediately**: No additional setup needed beyond the bundled sample data
- **Compare patterns**: Run multiple patterns with the same date to see different Bronze structures
