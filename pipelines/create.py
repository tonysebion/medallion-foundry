"""Interactive pipeline creator.

Guides users through creating a new pipeline with prompts.
Generates YAML configuration files by default (recommended for non-Python users).

Usage:
    python -m pipelines.create
    python -m pipelines.create --output pipelines/my_pipeline.yaml
    python -m pipelines.create --format python --output pipelines/my_pipeline.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Optional


def prompt(message: str, default: Optional[str] = None) -> str:
    """Prompt user for input with optional default."""
    if default:
        result = input(f"{message} [{default}]: ").strip()
        return result if result else default
    else:
        while True:
            result = input(f"{message}: ").strip()
            if result:
                return result
            print("  This field is required.")


def prompt_choice(
    message: str, choices: list[tuple[str, str]], default: int = 1
) -> str:
    """Prompt user to choose from options."""
    print(f"\n{message}")
    for i, (value, description) in enumerate(choices, 1):
        marker = "*" if i == default else " "
        print(f"  {marker}{i}. {description}")

    while True:
        result = input(f"Enter choice [1-{len(choices)}] ({default}): ").strip()
        if not result:
            return choices[default - 1][0]
        try:
            idx = int(result)
            if 1 <= idx <= len(choices):
                return choices[idx - 1][0]
        except ValueError:
            pass
        print(f"  Please enter a number between 1 and {len(choices)}")


def prompt_list(message: str, example: str = "") -> list[str]:
    """Prompt for comma-separated list."""
    hint = f" (e.g., {example})" if example else ""
    result = input(f"{message}{hint}: ").strip()
    if not result:
        return []
    return [item.strip() for item in result.split(",")]


def prompt_yes_no(message: str, default: bool = False) -> bool:
    """Prompt for yes/no answer."""
    default_str = "Y/n" if default else "y/N"
    result = input(f"{message} [{default_str}]: ").strip().lower()
    if not result:
        return default
    return result in ("y", "yes", "true", "1")


def guided_pattern_selection() -> dict:
    """Ask questions to determine optimal load_pattern and model.

    Returns a dict with load_pattern, model, and optional CDC options.
    """
    print("\n--- Let's figure out the best approach for your data ---\n")

    # Question 1: Data source behavior
    source_behavior = prompt_choice(
        "How does your source provide data?",
        [
            ("snapshot", "Complete file/table each time (daily export, full dump)"),
            ("incremental", "Only new/changed records (has an updated_at timestamp)"),
            ("cdc", "Change stream with I/U/D operation codes (CDC, Debezium, etc.)"),
        ],
    )

    # Question 2: History needs
    history_need = prompt_choice(
        "Do you need to track how records change over time?",
        [
            ("current", "No - I only need the current version of each record"),
            ("history", "Yes - I need to see all historical versions"),
        ],
    )

    result: dict = {}

    if source_behavior == "snapshot":
        result["load_pattern"] = "full_snapshot"
        if history_need == "current":
            result["model"] = "periodic_snapshot"
        else:
            result["model"] = "scd_type_2"

    elif source_behavior == "incremental":
        result["load_pattern"] = "incremental"
        if history_need == "current":
            result["model"] = "full_merge_dedupe"
        else:
            result["model"] = "scd_type_2"

    elif source_behavior == "cdc":
        result["load_pattern"] = "cdc"
        result["model"] = "cdc"
        result["keep_history"] = history_need == "history"

        # Ask about delete handling for CDC
        delete_handling = prompt_choice(
            "How should deleted records be handled?",
            [
                ("ignore", "Ignore deletes (filter them out)"),
                ("flag", "Soft delete (add _deleted=true column)"),
                ("remove", "Hard delete (remove from Silver)"),
            ],
        )
        result["handle_deletes"] = delete_handling

    print(
        f"\n  Recommended: load_pattern={result['load_pattern']}, model={result['model']}"
    )
    return result


def create_pipeline() -> Dict[str, Any]:
    """Interactive pipeline creation wizard.

    Returns a dictionary with all pipeline configuration.
    """
    print("=" * 60)
    print("  Pipeline Creator")
    print("=" * 60)
    print("\nThis wizard will help you create a new pipeline.")
    print("YAML format is recommended for non-Python users.\n")

    config: Dict[str, Any] = {}

    # Step 1: Basic info
    print("STEP 1: Basic Information")
    print("-" * 40)
    config["system"] = prompt("Source system name", "my_system")
    config["entity"] = prompt("Entity/table name", "my_table")
    config["name"] = f"{config['system']}_{config['entity']}"
    config["description"] = prompt(
        "Brief description",
        f"Pipeline for {config['entity']} from {config['system']}",
    )

    # Step 2: Source type
    source_choices = [
        ("database_mssql", "SQL Server database"),
        ("database_postgres", "PostgreSQL database"),
        ("database_mysql", "MySQL/MariaDB database"),
        ("file_csv", "CSV file"),
        ("file_parquet", "Parquet file"),
        ("file_json", "JSON file"),
        ("file_jsonl", "JSON Lines file"),
        ("file_excel", "Excel file"),
        ("file_fixed_width", "Fixed-width file"),
        ("api_rest", "REST API"),
    ]
    config["source_type"] = prompt_choice("What type of source?", source_choices)

    # Step 3: Source-specific options
    print("\nSTEP 2: Source Configuration")
    print("-" * 40)

    config["options"] = {}

    if config["source_type"].startswith("database_"):
        config["host"] = prompt("Database host", "${DB_HOST}")
        config["database"] = prompt("Database name")
        use_query = prompt_yes_no("Use custom SQL query?", False)

        if use_query:
            print("  Enter your SQL query (end with a blank line):")
            query_lines = []
            while True:
                line = input("    ")
                if not line:
                    break
                query_lines.append(line)
            config["query"] = "\n".join(query_lines)

    elif config["source_type"].startswith("file_"):
        ext_map = {
            "file_csv": ".csv",
            "file_parquet": ".parquet",
            "file_json": ".json",
            "file_jsonl": ".jsonl",
            "file_excel": ".xlsx",
            "file_fixed_width": ".txt",
        }
        ext = ext_map.get(config["source_type"], ".csv")
        default_path = f"./data/{config['system']}/{config['entity']}_{{run_date}}{ext}"
        config["source_path"] = prompt("File path pattern", default_path)

        if config["source_type"] == "file_fixed_width":
            cols = prompt_list("Column names", "id, name, value")
            if cols:
                config["options"]["columns"] = cols
            widths = prompt_list("Column widths (characters)", "10, 20, 15")
            if widths:
                config["options"]["widths"] = [int(w) for w in widths]

        elif config["source_type"] == "file_json":
            data_path = prompt("Path to data in JSON (leave blank for root)", "")
            if data_path:
                config["options"]["data_path"] = data_path

        elif config["source_type"] == "file_excel":
            sheet = prompt("Sheet name or index", "0")
            if sheet != "0":
                config["options"]["sheet"] = sheet

    elif config["source_type"] == "api_rest":
        config["source_path"] = prompt(
            "API URL",
            f"https://api.example.com/{config['entity']}",
        )

    # Step 4: Load pattern and model (guided decision tree)
    print("\nSTEP 3: Data Pattern")
    print("-" * 40)

    pattern_config = guided_pattern_selection()
    config["load_pattern"] = pattern_config["load_pattern"]
    config["model"] = pattern_config["model"]

    # CDC-specific options
    if config["model"] == "cdc":
        config["keep_history"] = pattern_config.get("keep_history", False)
        config["handle_deletes"] = pattern_config.get("handle_deletes", "ignore")

    # Watermark column for incremental/CDC
    if config["load_pattern"] in ("incremental", "cdc"):
        config["watermark_column"] = prompt(
            "Watermark column (e.g., LastUpdated, modified_at)"
        )

    # Step 5: Silver configuration
    print("\nSTEP 4: Silver Configuration")
    print("-" * 40)

    config["unique_columns"] = prompt_list("Primary key column(s)", "id")
    if not config["unique_columns"]:
        config["unique_columns"] = ["id"]

    config["last_updated_column"] = prompt(
        "Last updated timestamp column", "updated_at"
    )

    # Optional: specify attributes
    if prompt_yes_no("Specify which columns to include?", False):
        config["attributes"] = prompt_list("Columns to include", "name, email, status")

    return config


def generate_yaml_config(config: dict) -> str:
    """Generate YAML configuration from collected inputs."""
    lines = [
        "# yaml-language-server: $schema=../schema/pipeline.schema.json",
        "#",
        f"# Pipeline: {config['name']}",
        f"# {config.get('description', '')}",
        "#",
        "# To run:",
        f"#   python -m pipelines ./{config['name']}.yaml --date 2025-01-15",
        "",
        f"name: {config['name']}",
        f"description: {config.get('description', '')}",
        "",
        "# Bronze layer - raw data extraction",
        "bronze:",
        f"  system: {config['system']}",
        f"  entity: {config['entity']}",
        f"  source_type: {config['source_type']}",
    ]

    # Source path
    if config.get("source_path"):
        lines.append(f'  source_path: "{config["source_path"]}"')

    # Database connection
    if config.get("host"):
        lines.append(f"  host: {config['host']}")
    if config.get("database"):
        lines.append(f"  database: {config['database']}")
    if config.get("query"):
        lines.append("  query: |")
        for line in config["query"].split("\n"):
            lines.append(f"    {line}")

    # Load pattern
    if config.get("load_pattern") != "full_snapshot":
        lines.append(f"  load_pattern: {config['load_pattern']}")

    # Watermark
    if config.get("watermark_column"):
        lines.append(f"  watermark_column: {config['watermark_column']}")

    # Full refresh
    if config.get("full_refresh_days"):
        lines.append(f"  full_refresh_days: {config['full_refresh_days']}")

    # Chunk size
    if config.get("chunk_size"):
        lines.append(f"  chunk_size: {config['chunk_size']}")

    # Options
    if config.get("options"):
        lines.append("  options:")
        for key, value in config["options"].items():
            if isinstance(value, list):
                lines.append(f"    {key}:")
                for item in value:
                    lines.append(f"      - {item}")
            else:
                lines.append(f"    {key}: {value}")

    # Silver layer
    lines.extend(
        [
            "",
            "# Silver layer - data curation",
            "silver:",
        ]
    )

    # Model
    if config.get("model"):
        lines.append(f"  model: {config['model']}")

    # CDC-specific options
    if config.get("model") == "cdc":
        if config.get("keep_history"):
            lines.append(f"  keep_history: {str(config['keep_history']).lower()}")
        if config.get("handle_deletes") and config["handle_deletes"] != "ignore":
            lines.append(f"  handle_deletes: {config['handle_deletes']}")

    # Unique columns (primary keys)
    if len(config["unique_columns"]) == 1:
        lines.append(f"  unique_columns: [{config['unique_columns'][0]}]")
    else:
        lines.append("  unique_columns:")
        for key in config["unique_columns"]:
            lines.append(f"    - {key}")

    lines.append(f"  last_updated_column: {config['last_updated_column']}")

    # Attributes
    if config.get("attributes"):
        lines.append("  attributes:")
        for attr in config["attributes"]:
            lines.append(f"    - {attr}")

    lines.append("")

    return "\n".join(lines)


def generate_python_code(config: dict) -> str:
    """Generate Python pipeline code from collected inputs."""
    # Map YAML values to Python enum names
    source_type_map = {
        "file_csv": "FILE_CSV",
        "file_parquet": "FILE_PARQUET",
        "file_json": "FILE_JSON",
        "file_jsonl": "FILE_JSONL",
        "file_excel": "FILE_EXCEL",
        "file_fixed_width": "FILE_FIXED_WIDTH",
        "database_mssql": "DATABASE_MSSQL",
        "database_postgres": "DATABASE_POSTGRES",
        "database_mysql": "DATABASE_MYSQL",
        "database_db2": "DATABASE_DB2",
        "api_rest": "API_REST",
    }
    load_pattern_map = {
        "full_snapshot": "FULL_SNAPSHOT",
        "incremental": "INCREMENTAL_APPEND",
        "cdc": "CDC",
    }
    model_map = {
        "periodic_snapshot": "PERIODIC_SNAPSHOT",
        "full_merge_dedupe": "FULL_MERGE_DEDUPE",
        "scd_type_2": "SCD_TYPE_2",
        "event_log": "EVENT_LOG",
        "cdc": "CDC",
    }

    source_type = source_type_map.get(config["source_type"], "FILE_CSV")
    load_pattern = load_pattern_map.get(
        config.get("load_pattern", "full_snapshot"), "FULL_SNAPSHOT"
    )
    model = model_map.get(config.get("model", "full_merge_dedupe"), "FULL_MERGE_DEDUPE")

    # Build options dict
    options_parts = []
    if config.get("options"):
        for key, value in config["options"].items():
            options_parts.append(f'        "{key}": {repr(value)},')

    options_code = (
        "options={},\n"
        if not options_parts
        else "options={\n" + "\n".join(options_parts) + "\n    },\n"
    )

    # Build source path line
    source_path_line = ""
    if config.get("source_path"):
        source_path_line = f'    source_path="{config["source_path"]}",\n'

    # Build database lines
    db_lines = ""
    if config.get("host"):
        db_lines += f'    host="{config["host"]}",\n'
    if config.get("database"):
        db_lines += f'    database="{config["database"]}",\n'
    if config.get("query"):
        db_lines += f'    query="""\n{config["query"]}\n    """,\n'

    # Build watermark line
    watermark_line = ""
    if config.get("watermark_column"):
        watermark_line = f'    watermark_column="{config["watermark_column"]}",\n'

    # Build attributes line
    attributes_line = ""
    if config.get("attributes"):
        attributes_line = f"    attributes={config['attributes']},\n"

    # Build model options for CDC
    model_options_line = ""
    if config.get("model") == "cdc":
        if config.get("keep_history"):
            model_options_line += f"    keep_history={config['keep_history']},\n"
        if config.get("handle_deletes") and config["handle_deletes"] != "ignore":
            model_options_line += f'    handle_deletes="{config["handle_deletes"]}",\n'

    return f'''"""
Pipeline: {config["name"]}
{config.get("description", "")}

Generated by pipeline creator.

To run:
    python -m pipelines {config["name"]} --date 2025-01-15
"""

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import SilverEntity, SilverModel

# ============================================
# BRONZE
# ============================================

bronze = BronzeSource(
    system="{config["system"]}",
    entity="{config["entity"]}",
    source_type=SourceType.{source_type},
{source_path_line}{db_lines}    {options_code}    load_pattern=LoadPattern.{load_pattern},
{watermark_line})

# ============================================
# SILVER
# ============================================

silver = SilverEntity(
    model=SilverModel.{model},
    unique_columns={config["unique_columns"]},
    last_updated_column="{config["last_updated_column"]}",
{model_options_line}{attributes_line})

# ============================================
# PIPELINE
# ============================================

pipeline = Pipeline(bronze=bronze, silver=silver)
run = pipeline.run
run_bronze = pipeline.run_bronze
run_silver = pipeline.run_silver
'''


def main():
    parser = argparse.ArgumentParser(
        description="Create a new pipeline interactively",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Create a YAML pipeline (recommended)
    python -m pipelines.create --output ./pipelines/my_pipeline.yaml

    # Create a Python pipeline
    python -m pipelines.create --format python --output ./pipelines/my_pipeline.py

    # Print to stdout (for testing)
    python -m pipelines.create
        """,
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file path (default: print to stdout)",
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=["yaml", "python"],
        default="yaml",
        help="Output format: yaml (default) or python",
    )
    args = parser.parse_args()

    try:
        config = create_pipeline()

        print("\n" + "=" * 60)
        print("  Generated Pipeline")
        print("=" * 60)

        # Generate code in requested format
        if args.format == "yaml":
            code = generate_yaml_config(config)
            default_ext = ".yaml"
        else:
            code = generate_python_code(config)
            default_ext = ".py"

        if args.output:
            output_path = Path(args.output)
            # Add extension if not provided
            if not output_path.suffix:
                output_path = output_path.with_suffix(default_ext)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(code)
            print(f"\nPipeline saved to: {output_path}")

            if args.format == "yaml":
                print("\nTo run:")
                print(f"  python -m pipelines {output_path} --date 2025-01-15")
            else:
                print("\nTo run:")
                print(f"  python -m pipelines {output_path.stem} --date 2025-01-15")
        else:
            print(f"\n{code}")
            print("\nTo save this pipeline, run with --output:")
            if args.format == "yaml":
                print(
                    f"  python -m pipelines.create --output ./pipelines/{config['name']}.yaml"
                )
            else:
                print(
                    f"  python -m pipelines.create --format python --output ./pipelines/{config['name']}.py"
                )

    except KeyboardInterrupt:
        print("\n\nCancelled.")
        sys.exit(1)


if __name__ == "__main__":
    main()
