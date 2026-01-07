"""Template loader for YAML pipeline configurations.

Loads static YAML templates and substitutes placeholders with runtime values.
"""

import re
from pathlib import Path
from typing import Dict, Optional


def load_yaml_with_substitutions(
    template_path: Path,
    substitutions: Dict[str, str],
    output_dir: Optional[Path] = None,
) -> str:
    """Load a YAML template and substitute placeholders.

    Args:
        template_path: Path to the YAML template file
        substitutions: Dict of placeholder names to values
            e.g., {"bucket": "mdf", "prefix": "test123", "source_path": "/tmp/data.csv"}
        output_dir: Optional directory to write the substituted YAML to.
            If provided, writes to {output_dir}/{template_name} and returns the path.
            If not provided, returns the substituted YAML content as a string.

    Returns:
        If output_dir is None: The substituted YAML content as a string
        If output_dir is provided: The path to the written file as a string

    Placeholder format:
        Use {placeholder_name} in YAML files.
        Example: source_path: "{source_path}"
                 target_path: "s3://{bucket}/{prefix}/bronze/"
    """
    # Read the template
    template_content = template_path.read_text()

    # Substitute all placeholders
    result = template_content
    for key, value in substitutions.items():
        # Handle both {key} and {{key}} patterns
        # Single braces are placeholders, double braces are escaped
        pattern = r"\{" + re.escape(key) + r"\}"
        result = re.sub(pattern, str(value), result)

    if output_dir is not None:
        # Write to output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / template_path.name
        output_path.write_text(result)
        return str(output_path)

    return result


def get_pattern_template(pattern_name: str) -> Path:
    """Get the path to a pattern template file.

    Args:
        pattern_name: Name of the pattern (without .yaml extension)

    Returns:
        Path to the template file

    Raises:
        FileNotFoundError: If the template doesn't exist
    """
    patterns_dir = Path(__file__).parent / "patterns"
    template_path = patterns_dir / f"{pattern_name}.yaml"

    if not template_path.exists():
        available = [p.stem for p in patterns_dir.glob("*.yaml")]
        raise FileNotFoundError(
            f"Pattern template '{pattern_name}' not found. "
            f"Available patterns: {available}"
        )

    return template_path


def build_substitutions(
    bucket: str,
    prefix: str,
    source_path: str,
    run_date: str,
    system: str,
    entity: str,
    natural_key: str = "",
    change_timestamp: str = "",
    attributes: Optional[list] = None,
    entity_kind: str = "state",
    history_mode: str = "current_only",
    load_pattern: str = "full_snapshot",
    watermark_column: str = "",
) -> Dict[str, str]:
    """Build a substitutions dictionary with common placeholders.

    This helper ensures consistent placeholder naming across tests.
    """
    subs = {
        "bucket": bucket,
        "prefix": prefix,
        "source_path": source_path.replace("\\", "/"),  # Normalize Windows paths
        "run_date": run_date,
        "system": system,
        "entity": entity,
        "entity_kind": entity_kind,
        "history_mode": history_mode,
        "load_pattern": load_pattern,
    }

    if natural_key:
        subs["natural_key"] = natural_key
    if change_timestamp:
        subs["change_timestamp"] = change_timestamp
    if attributes:
        # Format as YAML list
        subs["attributes"] = "\n".join(f"    - {attr}" for attr in attributes)
    if watermark_column:
        subs["watermark_column"] = watermark_column

    return subs
