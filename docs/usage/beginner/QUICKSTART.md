# Quick Start Guide

**Get data flowing in 10 minutes** - No API keys required!

## 🎯 **Goal**

Run sample data through Bronze → Silver pipeline to understand the flow.

## 📋 **Steps**

### 1. Install & Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Generate sample data (creates realistic test datasets)
python scripts/generate_sample_data.py
```

### 2. Run Bronze Extraction

```bash
# Extract sample data to Bronze layer
python bronze_extract.py --config docs/examples/configs/examples/file_example.yaml --date 2025-11-27
```

**What happens:** Reads CSV files → Creates Parquet files in `output/` with metadata.

### 3. Run Silver Promotion

```bash
# Promote Bronze data to Silver layer
python silver_extract.py --config docs/examples/configs/examples/file_example.yaml --date 2025-11-27
```

**What happens:** Reads Bronze Parquet → Creates curated datasets in `silver_output/`.

### 4. Check Results

```bash
# View Bronze output
ls output/system=retail_demo/table=orders/dt=2025-11-27/

# View Silver output
ls silver_output/domain=retail_demo/entity=orders/v1/load_date=2025-11-27/
```

## 🎉 **Success!**

You now understand:
- **Bronze**: Raw extracted data with metadata
- **Silver**: Curated, transformed datasets
- **Pipeline**: Config-driven data movement

## 🚀 **Next Steps**

### **Experiment Safely**
- [Safe Experimentation Guide](SAFE_EXPERIMENTATION.md) - Test configs without breaking things
- Run `python scripts/run_demo.py` for interactive demo
- Try different dates: `--date 2025-11-26`, `--date 2025-11-28`

### **Customize for Your Data**
1. [Copy & Customize Guide](COPY_AND_CUSTOMIZE.md) - Adapt configs for your sources
2. [Pattern Picker](../patterns/QUICK_REFERENCE.md) - Choose the right extraction strategy
3. [Production Setup](../onboarding/intent-owner-guide.md) - Go live with real data

### **Common Issues**
- **"No data found"**: Check date matches sample data range
- **Permission errors**: Ensure write access to output directories
- **Config errors**: Run `--validate-only` first

## 🆘 **Stuck?**

1. Run the demo: `python scripts/run_demo.py`
2. Check [Copy & Customize Guide](COPY_AND_CUSTOMIZE.md)
3. Browse [examples](../../examples/README.md) for working configs
