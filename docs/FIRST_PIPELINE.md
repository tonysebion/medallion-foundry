# Your First Pipeline (5 Minutes)

Your colleague already set up Python and the framework. Now it's your turn to create a pipeline.

## Prerequisites

- Python is installed (your colleague set this up)
- You have a CSV file you want to process
- You've opened a terminal/command prompt in the project folder

## Step 1: Find Out About Your Data

**Don't know which columns are unique or when rows were updated?** Run this command to analyze your file:

```bash
python -m pipelines inspect-source --file ./your_data.csv
```

This will tell you:
- Which column(s) might uniquely identify each row (like `order_id` or `customer_id`)
- Which column looks like a "last updated" timestamp
- What columns exist in your data

**Save these suggestions** - you'll need them for Step 3.

## Step 2: Copy a Template

Copy the simple CSV template:

**Windows:**
```cmd
copy pipelines\examples\csv_snapshot.yaml my_pipeline.yaml
```

**Mac/Linux:**
```bash
cp pipelines/examples/csv_snapshot.yaml my_pipeline.yaml
```

## Step 3: Edit 4 Fields

Open `my_pipeline.yaml` in any text editor (Notepad, VSCode, etc.) and change these fields:

```yaml
bronze:
  system: your_system_name      # Change: e.g., "salesforce", "excel_export", "legacy_db"
  entity: your_table_name       # Change: e.g., "customers", "orders", "products"
  source_path: ./your_data.csv  # Change: path to your CSV file

silver:
  unique_columns: [id]           # Change: from Step 1 suggestions
  last_updated_column: updated_at # Change: from Step 1 suggestions
```

**Example:**
```yaml
bronze:
  system: excel_exports
  entity: customer_list
  source_path: ./data/customers_jan2025.csv

silver:
  unique_columns: [customer_id]
  last_updated_column: modified_date
```

## Step 4: Test It (Dry Run)

Before running for real, validate your configuration:

```bash
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --dry-run
```

This checks:
- Your YAML syntax is valid
- Your file exists
- Your column names match what's in the file

If you see errors, check your spelling and file paths.

## Step 5: Run It

```bash
python -m pipelines ./my_pipeline.yaml --date 2025-01-15
```

That's it! Your data is now in the Bronze and Silver folders.

## Where Did My Data Go?

After running, you'll have:

```
bronze/
  system=excel_exports/
    entity=customer_list/
      dt=2025-01-15/
        customer_list.parquet    # Your raw data
        _metadata.json           # Processing info

silver/
  domain=excel_exports/
    subject=customer_list/
      dt=2025-01-15/
        customer_list.parquet    # Your curated data
        _metadata.json           # Processing info
```

## Common Questions

### "I don't have an updated_at column"

If your data doesn't track when rows were modified, you have two options:

1. **Use any date column** you have (like `created_at` or `order_date`)
2. **Use periodic_snapshot model** which doesn't need a timestamp:
   ```yaml
   silver:
     unique_columns: [id]
     model: periodic_snapshot
   ```

### "How do I run this daily?"

Use Windows Task Scheduler or cron to run the command with today's date:

```bash
# PowerShell (today's date)
python -m pipelines ./my_pipeline.yaml --date (Get-Date -Format "yyyy-MM-dd")

# Linux/Mac
python -m pipelines ./my_pipeline.yaml --date $(date +%Y-%m-%d)
```

### "My file has a different date each day"

Use `{run_date}` in the path:

```yaml
source_path: ./data/customers_{run_date}.csv
```

This becomes `./data/customers_2025-01-15.csv` when you run with `--date 2025-01-15`.

### "I need help with database connections"

See the [Getting Started Guide](GETTING_STARTED.md) for database examples, or copy `pipelines/examples/mssql_dimension.yaml`.

## Quick Glossary

| Term | Plain English |
|------|---------------|
| **Bronze layer** | Raw data exactly as extracted from your source (CSV, database, API) |
| **Silver layer** | Cleaned data with duplicates removed, ready for analysis |
| **unique_columns** | The column(s) that uniquely identify each row (like a primary key) |
| **last_updated_column** | The column that shows when a row was last changed |
| **load_pattern** | How data is loaded: `full_snapshot` (everything), `incremental` (only new), `cdc` (changes only) |
| **model** | How Silver processes data: `periodic_snapshot` (simple), `full_merge_dedupe` (deduplicated), `scd_type_2` (full history), `cdc` (change stream) |
| **SCD Type 1** | Keep only the latest version of each record |
| **SCD Type 2** | Keep all versions with effective dates (history tracking) |
| **CDC** | Change Data Capture - source sends Insert/Update/Delete markers |

## Next Steps

- [Concepts Guide](CONCEPTS.md) - Understand Bronze/Silver layers
- [Getting Started Guide](GETTING_STARTED.md) - More examples and templates
- [Model Selection Guide](MODEL_SELECTION.md) - Choose the right processing pattern
