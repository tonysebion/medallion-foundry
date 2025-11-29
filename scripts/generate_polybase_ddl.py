#!/usr/bin/env python3
"""Generate SQL DDL for Polybase external tables from DatasetConfig YAML files."""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

import yaml

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config.dataset import DatasetConfig
from core.polybase import generate_polybase_setup, generate_temporal_functions_sql


def load_dataset_config(yaml_path: Path) -> DatasetConfig:
    """Load a dataset configuration from a YAML file."""
    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)
    return DatasetConfig.from_dict(data)


def generate_create_external_data_source_sql(polybase_setup) -> str:
    """Generate SQL for CREATE EXTERNAL DATA SOURCE."""
    if not polybase_setup.external_data_source:
        return ""

    eds = polybase_setup.external_data_source
    credential_clause = ""
    if eds.credential_name:
        credential_clause = f",\n    CREDENTIAL = {eds.credential_name}"

    sql = (
        f"-- Create External Data Source\n"
        f"CREATE EXTERNAL DATA SOURCE [{eds.name}]\n"
        f"WITH (\n"
        f"    TYPE = {eds.data_source_type},\n"
        f"    LOCATION = '{eds.location}'{credential_clause}\n"
        f");\n"
    )
    return sql


def generate_create_external_file_format_sql(polybase_setup) -> str:
    """Generate SQL for CREATE EXTERNAL FILE FORMAT."""
    if not polybase_setup.external_file_format:
        return ""

    eff = polybase_setup.external_file_format
    compression_clause = ""
    if eff.compression:
        compression_clause = f",\n  COMPRESSION = '{eff.compression}'"

    sql = (
        f"-- Create External File Format\n"
        f"CREATE EXTERNAL FILE FORMAT [{eff.name}]\n"
        f"WITH (\n"
        f"  FORMAT_TYPE = {eff.format_type}{compression_clause}\n"
        f");\n"
    )
    return sql


def generate_create_external_table_sql(
    polybase_setup, dataset_config: DatasetConfig
) -> str:
    """Generate SQL for CREATE EXTERNAL TABLE."""
    if not polybase_setup.external_tables:
        return ""

    sqls = []
    for ext_table in polybase_setup.external_tables:
        # Get attributes from dataset config
        attributes = dataset_config.silver.attributes or []

        # Build column list: all attributes + partition columns
        column_defs = []
        for attr in attributes:
            column_defs.append(f"    [{attr}] VARCHAR(255)")

        # Add partition columns
        for partition_col in ext_table.partition_columns:
            column_defs.append(f"    [{partition_col}] VARCHAR(255)")

        columns_sql = ",\n".join(column_defs)

        sql = (
            f"-- Create External Table: {ext_table.table_name}\n"
            f"CREATE EXTERNAL TABLE [{ext_table.schema_name}].[{ext_table.table_name}] (\n"
            f"{columns_sql}\n"
            f")\n"
            f"WITH (\n"
            f"    LOCATION = '{ext_table.artifact_name}/',\n"
            f"    DATA_SOURCE = [{polybase_setup.external_data_source.name}],\n"
            f"    FILE_FORMAT = [{polybase_setup.external_file_format.name}],\n"
            f"    REJECT_TYPE = {ext_table.reject_type},\n"
            f"    REJECT_VALUE = {ext_table.reject_value}\n"
            f");\n"
        )
        sqls.append(sql)

    return "\n".join(sqls)


def generate_sample_queries_comment(ext_table) -> str:
    """Generate SQL comments with sample queries for point-in-time analysis."""
    if not ext_table.sample_queries:
        return ""

    comment = f"-- Sample Queries for {ext_table.table_name}\n"
    comment += "-- Point-in-time queries to analyze temporal data:\n"
    for i, query in enumerate(ext_table.sample_queries, 1):
        comment += f"-- Query {i}:\n"
        comment += f"-- {query}\n"
    comment += "\n"
    return comment


def generate_full_ddl(dataset_config: DatasetConfig) -> str:
    """Generate complete DDL for a dataset's Polybase configuration."""
    polybase_setup = generate_polybase_setup(dataset_config)

    if not polybase_setup.enabled:
        return f"-- Polybase setup disabled for {dataset_config.dataset_id}\n"

    ddl_parts = [
        f"-- Polybase External Tables for {dataset_config.dataset_id}",
        f"-- Entity Kind: {dataset_config.silver.entity_kind.value}",
        f"-- Pattern Folder: {dataset_config.bronze.options.get('pattern_folder', 'unknown')}",
        "",
        "-- ============================================================",
        "",
        generate_create_external_data_source_sql(polybase_setup),
        "",
        generate_create_external_file_format_sql(polybase_setup),
        "",
        generate_create_external_table_sql(polybase_setup, dataset_config),
    ]

    # Add sample queries for each table
    for ext_table in polybase_setup.external_tables:
        ddl_parts.append("")
        ddl_parts.append(generate_sample_queries_comment(ext_table))

    # Add temporal functions for point-in-time queries
    temporal_functions = generate_temporal_functions_sql(dataset_config)
    if temporal_functions:
        ddl_parts.append("")
        ddl_parts.append("-- ============================================================")
        ddl_parts.append("-- Temporal Functions for Point-in-Time Queries")
        ddl_parts.append("-- ============================================================")
        ddl_parts.append("")
        ddl_parts.append(temporal_functions)

    ddl = "\n".join([part for part in ddl_parts if part is not None])
    return ddl


def process_yaml_files(
    yaml_paths: List[Path], output_dir: Optional[Path] = None
) -> None:
    """Process multiple YAML files and generate DDL for each."""
    for yaml_path in yaml_paths:
        if not yaml_path.exists():
            print(f"Error: {yaml_path} not found", file=sys.stderr)
            continue

        try:
            dataset_config = load_dataset_config(yaml_path)
            ddl = generate_full_ddl(dataset_config)

            # Output DDL
            if output_dir:
                output_file = output_dir / f"{dataset_config.dataset_id}_polybase.sql"
                output_file.write_text(ddl)
                print(f"Generated: {output_file}")
            else:
                print(ddl)
                print("\n" + "=" * 80 + "\n")

        except Exception as e:
            print(f"Error processing {yaml_path}: {e}", file=sys.stderr)


def main() -> None:
    """Main entry point for DDL generation."""
    parser = argparse.ArgumentParser(
        description="Generate SQL DDL for Polybase external tables from dataset YAML configs"
    )
    parser.add_argument(
        "yaml_files",
        nargs="+",
        type=Path,
        help="Path(s) to dataset YAML configuration file(s)",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        help="Output directory for generated SQL files (default: stdout)",
    )

    args = parser.parse_args()

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)

    process_yaml_files(args.yaml_files, args.output_dir)


if __name__ == "__main__":
    main()
