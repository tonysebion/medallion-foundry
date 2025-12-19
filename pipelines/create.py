"""Interactive pipeline creator.

Guides users through creating a new pipeline with prompts.

Usage:
    python -m pipelines.create
    python -m pipelines.create --output pipelines/claims/header.py
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


def create_pipeline() -> str:
    """Interactive pipeline creation wizard."""
    print("=" * 60)
    print("  Pipeline Creator")
    print("=" * 60)
    print("\nThis wizard will help you create a new pipeline.\n")

    # Step 1: Basic info
    print("STEP 1: Basic Information")
    print("-" * 40)
    system = prompt("Source system name", "my_system")
    entity = prompt("Entity/table name", "my_table")

    # Step 2: Source type
    source_choices = [
        ("DATABASE_MSSQL", "SQL Server database"),
        ("DATABASE_POSTGRES", "PostgreSQL database"),
        ("FILE_CSV", "CSV file"),
        ("FILE_PARQUET", "Parquet file"),
        ("FILE_SPACE_DELIMITED", "Space-delimited file"),
        ("API_REST", "REST API"),
    ]
    source_type = prompt_choice("What type of source?", source_choices)

    # Step 3: Source-specific options
    print("\nSTEP 2: Source Configuration")
    print("-" * 40)

    options_code = ""
    source_path = ""

    if source_type.startswith("DATABASE_"):
        host = prompt("Database host", "${DB_HOST}")
        database = prompt("Database name")
        use_query = input("Use custom SQL query? [y/N]: ").strip().lower() == "y"

        if use_query:
            print("  Enter your SQL query (end with a blank line):")
            query_lines = []
            while True:
                line = input("    ")
                if not line:
                    break
                query_lines.append(line)
            query = "\n".join(query_lines)
            options_code = f'''options={{
        "connection_name": "{system}_db",
        "host": "{host}",
        "database": "{database}",
        "query": """
{query}
        """,
    }},'''
        else:
            options_code = f'''options={{
        "connection_name": "{system}_db",
        "host": "{host}",
        "database": "{database}",
    }},'''

    elif source_type.startswith("FILE_"):
        default_path = f"/data/{system}/{entity}_{{run_date}}.csv"
        source_path = prompt("File path pattern", default_path)
        if source_type == "FILE_SPACE_DELIMITED":
            cols = prompt_list("Column names", "id, name, value")
            if cols:
                options_code = f"""options={{
        "csv_options": {{
            "columns": {cols},
        }}
    }},"""

    elif source_type == "API_REST":
        source_path = prompt("API URL", f"https://api.example.com/{entity}")

    # Step 4: Load pattern
    load_choices = [
        ("FULL_SNAPSHOT", "Full snapshot (replace all each run)"),
        ("INCREMENTAL_APPEND", "Incremental (only new records)"),
    ]
    load_pattern = prompt_choice("How should data be loaded?", load_choices)

    watermark_col = ""
    if load_pattern == "INCREMENTAL_APPEND":
        watermark_col = prompt("Watermark column (e.g., LastUpdated)")

    # Step 5: Silver configuration
    print("\nSTEP 3: Silver Configuration")
    print("-" * 40)

    natural_keys = prompt_list("Primary key column(s)", "id")
    if not natural_keys:
        natural_keys = ["id"]

    change_ts = prompt("Change timestamp column", "updated_at")

    entity_choices = [
        ("STATE", "Dimension (customer, product) - slowly changing"),
        ("EVENT", "Fact/Event (orders, clicks) - immutable"),
    ]
    entity_kind = prompt_choice("What kind of entity is this?", entity_choices)

    history_choices = [
        ("CURRENT_ONLY", "SCD Type 1 - Keep only latest version"),
        ("FULL_HISTORY", "SCD Type 2 - Keep all versions with effective dates"),
    ]
    history_mode = prompt_choice("How to handle history?", history_choices)

    # Step 6: Output path
    print("\nSTEP 4: Output Configuration")
    print("-" * 40)
    target_path = prompt("Silver output path", f"s3://silver/{system}/{entity}/")

    # Generate the pipeline code
    code = generate_pipeline_code(
        system=system,
        entity=entity,
        source_type=source_type,
        source_path=source_path,
        options_code=options_code,
        load_pattern=load_pattern,
        watermark_col=watermark_col,
        natural_keys=natural_keys,
        change_ts=change_ts,
        entity_kind=entity_kind,
        history_mode=history_mode,
        target_path=target_path,
    )

    return code


def generate_pipeline_code(
    system: str,
    entity: str,
    source_type: str,
    source_path: str,
    options_code: str,
    load_pattern: str,
    watermark_col: str,
    natural_keys: list[str],
    change_ts: str,
    entity_kind: str,
    history_mode: str,
    target_path: str,
) -> str:
    """Generate pipeline Python code from inputs."""
    if watermark_col:
        watermark_line = f'    watermark_column="{watermark_col}",'
    else:
        watermark_line = ""

    if source_path:
        source_path_line = f'    source_path="{source_path}",'
    else:
        source_path_line = '    source_path="",'
    options_section = f"    {options_code}" if options_code else "    options={},"

    return f'''"""
Pipeline: {system}.{entity}
Generated by pipeline creator
"""

from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

# ============================================
# BRONZE
# ============================================

bronze = BronzeSource(
    system="{system}",
    entity="{entity}",
    source_type=SourceType.{source_type},
{source_path_line}
{options_section}
    target_path="s3://bronze/system={{system}}/entity={{entity}}/dt={{run_date}}/",
    load_pattern=LoadPattern.{load_pattern},
{watermark_line}
)

# ============================================
# SILVER
# ============================================

silver = SilverEntity(
    source_path="s3://bronze/system={system}/entity={entity}/dt={{run_date}}/*.parquet",
    target_path="{target_path}",
    natural_keys={natural_keys},
    change_timestamp="{change_ts}",
    entity_kind=EntityKind.{entity_kind},
    history_mode=HistoryMode.{history_mode},
)


def run_bronze(run_date: str, **kwargs):
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    return silver.run(run_date, **kwargs)


def run(run_date: str, **kwargs):
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {{"bronze": bronze_result, "silver": silver_result}}
'''


def main():
    parser = argparse.ArgumentParser(description="Create a new pipeline interactively")
    parser.add_argument(
        "--output",
        "-o",
        help="Output file path (default: print to stdout)",
    )
    args = parser.parse_args()

    try:
        code = create_pipeline()

        print("\n" + "=" * 60)
        print("  Generated Pipeline")
        print("=" * 60)

        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(code)
            print(f"\nPipeline saved to: {output_path}")
            print(f"\nTo run: python -m pipelines {output_path.stem} --date 2025-01-15")
        else:
            print("\n" + code)
            print("\nTo save this pipeline, run with --output:")
            print("  python -m pipelines.create --output pipelines/your/pipeline.py")

    except KeyboardInterrupt:
        print("\n\nCancelled.")
        sys.exit(1)


if __name__ == "__main__":
    main()
