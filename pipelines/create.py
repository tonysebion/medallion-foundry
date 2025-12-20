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
from typing import Optional


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


def create_pipeline() -> dict:
    """Interactive pipeline creation wizard.

    Returns a dictionary with all pipeline configuration.
    """
    print("=" * 60)
    print("  Pipeline Creator")
    print("=" * 60)
    print("\nThis wizard will help you create a new pipeline.")
    print("YAML format is recommended for non-Python users.\n")

    config = {}

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

    # Step 4: Load pattern
    load_choices = [
        ("full_snapshot", "Full snapshot (replace all each run)"),
        ("incremental", "Incremental (only new records via watermark)"),
        ("cdc", "CDC (change data capture with I/U/D flags)"),
    ]
    config["load_pattern"] = prompt_choice("How should data be loaded?", load_choices)

    if config["load_pattern"] in ("incremental", "cdc"):
        config["watermark_column"] = prompt(
            "Watermark column (e.g., LastUpdated, modified_at)"
        )

    # Ask about periodic full refresh for incremental loads
    if config["load_pattern"] == "incremental":
        if prompt_yes_no("Enable periodic full refresh?", False):
            days = prompt("Full refresh every N days", "7")
            config["full_refresh_days"] = int(days)

    # Ask about chunking for large datasets
    if prompt_yes_no("Enable chunking for large data?", False):
        chunk_size = prompt("Rows per chunk", "100000")
        config["chunk_size"] = int(chunk_size)

    # Step 5: Silver configuration
    print("\nSTEP 3: Silver Configuration")
    print("-" * 40)

    config["natural_keys"] = prompt_list("Primary key column(s)", "id")
    if not config["natural_keys"]:
        config["natural_keys"] = ["id"]

    config["change_timestamp"] = prompt("Change timestamp column", "updated_at")

    entity_choices = [
        ("state", "Dimension (customer, product) - slowly changing"),
        ("event", "Fact/Event (orders, clicks) - immutable"),
    ]
    config["entity_kind"] = prompt_choice("What kind of entity is this?", entity_choices)

    history_choices = [
        ("current_only", "SCD Type 1 - Keep only latest version"),
        ("full_history", "SCD Type 2 - Keep all versions with effective dates"),
    ]
    config["history_mode"] = prompt_choice("How to handle history?", history_choices)

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
        lines.append(f"  source_path: \"{config['source_path']}\"")

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
    lines.extend([
        "",
        "# Silver layer - data curation",
        "silver:",
    ])

    # Natural keys
    if len(config["natural_keys"]) == 1:
        lines.append(f"  natural_keys: [{config['natural_keys'][0]}]")
    else:
        lines.append("  natural_keys:")
        for key in config["natural_keys"]:
            lines.append(f"    - {key}")

    lines.append(f"  change_timestamp: {config['change_timestamp']}")

    # Entity kind (only if not default)
    if config.get("entity_kind") != "state":
        lines.append(f"  entity_kind: {config['entity_kind']}")

    # History mode (only if not default)
    if config.get("history_mode") != "current_only":
        lines.append(f"  history_mode: {config['history_mode']}")

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
    entity_kind_map = {"state": "STATE", "event": "EVENT"}
    history_mode_map = {"current_only": "CURRENT_ONLY", "full_history": "FULL_HISTORY"}

    source_type = source_type_map.get(config["source_type"], "FILE_CSV")
    load_pattern = load_pattern_map.get(config.get("load_pattern", "full_snapshot"), "FULL_SNAPSHOT")
    entity_kind = entity_kind_map.get(config.get("entity_kind", "state"), "STATE")
    history_mode = history_mode_map.get(config.get("history_mode", "current_only"), "CURRENT_ONLY")

    # Build options dict
    options_parts = []
    if config.get("options"):
        for key, value in config["options"].items():
            options_parts.append(f'        "{key}": {repr(value)},')

    options_code = "options={},\n" if not options_parts else "options={\n" + "\n".join(options_parts) + "\n    },\n"

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

    # Build full refresh line
    full_refresh_line = ""
    if config.get("full_refresh_days"):
        full_refresh_line = f"    full_refresh_days={config['full_refresh_days']},\n"

    # Build chunk size line
    chunk_size_line = ""
    if config.get("chunk_size"):
        chunk_size_line = f"    chunk_size={config['chunk_size']},\n"

    # Build attributes line
    attributes_line = ""
    if config.get("attributes"):
        attributes_line = f"    attributes={config['attributes']},\n"

    return f'''"""
Pipeline: {config['name']}
{config.get('description', '')}

Generated by pipeline creator.

To run:
    python -m pipelines {config['name']} --date 2025-01-15
"""

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity

# ============================================
# BRONZE
# ============================================

bronze = BronzeSource(
    system="{config['system']}",
    entity="{config['entity']}",
    source_type=SourceType.{source_type},
{source_path_line}{db_lines}    {options_code}    load_pattern=LoadPattern.{load_pattern},
{watermark_line}{full_refresh_line}{chunk_size_line})

# ============================================
# SILVER
# ============================================

silver = SilverEntity(
    natural_keys={config['natural_keys']},
    change_timestamp="{config['change_timestamp']}",
    entity_kind=EntityKind.{entity_kind},
    history_mode=HistoryMode.{history_mode},
{attributes_line})

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
                print(f"  python -m pipelines.create --output ./pipelines/{config['name']}.yaml")
            else:
                print(f"  python -m pipelines.create --format python --output ./pipelines/{config['name']}.py")

    except KeyboardInterrupt:
        print("\n\nCancelled.")
        sys.exit(1)


if __name__ == "__main__":
    main()
