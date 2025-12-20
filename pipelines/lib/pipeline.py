"""Pipeline helper for simplified pipeline definitions.

The Pipeline class eliminates boilerplate by combining Bronze and Silver
configurations and auto-generating the run functions that the CLI expects.

Example:
    from pipelines.lib import Pipeline
    from pipelines.lib.bronze import BronzeSource, SourceType
    from pipelines.lib.silver import SilverEntity

    bronze = BronzeSource(
        system="retail",
        entity="orders",
        source_path="./data/orders_{run_date}.csv",
        source_type=SourceType.FILE_CSV,
    )

    silver = SilverEntity(
        natural_keys=["order_id"],
        change_timestamp="updated_at",
    )

    # Pipeline auto-wires Bronze → Silver and generates run functions
    pipeline = Pipeline(bronze=bronze, silver=silver)

    # Export for CLI discovery
    run = pipeline.run
    run_bronze = pipeline.run_bronze
    run_silver = pipeline.run_silver
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

if TYPE_CHECKING:
    from pipelines.lib.bronze import BronzeSource
    from pipelines.lib.silver import SilverEntity

logger = logging.getLogger(__name__)

__all__ = ["Pipeline"]


@dataclass
class Pipeline:
    """Combines Bronze and Silver into a runnable pipeline.

    The Pipeline class serves two purposes:
    1. Eliminates boilerplate run functions from every pipeline file
    2. Auto-wires Silver's source_path from Bronze's target_path (when not specified)

    Args:
        bronze: Optional BronzeSource configuration
        silver: Optional SilverEntity configuration
        name: Optional pipeline name for logging

    Usage:
        # Minimal setup
        pipeline = Pipeline(bronze=bronze, silver=silver)
        run = pipeline.run
        run_bronze = pipeline.run_bronze
        run_silver = pipeline.run_silver

        # Or destructure in one line
        run, run_bronze, run_silver = pipeline.run, pipeline.run_bronze, pipeline.run_silver

        # Bronze-only pipeline
        pipeline = Pipeline(bronze=bronze)

        # Silver-only pipeline (must have explicit source_path)
        pipeline = Pipeline(silver=silver)
    """

    bronze: Optional[BronzeSource] = None
    silver: Optional[SilverEntity] = None
    name: Optional[str] = None

    # Callbacks for custom behavior (advanced usage)
    before_bronze: Optional[Callable[[str, Dict[str, Any]], None]] = field(
        default=None, repr=False
    )
    after_bronze: Optional[Callable[[str, Dict[str, Any], Dict[str, Any]], None]] = field(
        default=None, repr=False
    )
    before_silver: Optional[Callable[[str, Dict[str, Any]], None]] = field(
        default=None, repr=False
    )
    after_silver: Optional[Callable[[str, Dict[str, Any], Dict[str, Any]], None]] = field(
        default=None, repr=False
    )

    def __post_init__(self) -> None:
        """Validate and auto-wire configuration."""
        if self.bronze is None and self.silver is None:
            raise ValueError(
                "Pipeline requires at least one of: bronze, silver"
            )

        # Auto-wire Silver's source_path from Bronze's target_path if not specified
        if self.bronze is not None and self.silver is not None:
            self._auto_wire_silver_source()

        # Infer name from Bronze/Silver if not specified
        if self.name is None:
            if self.bronze is not None:
                self.name = f"{self.bronze.system}.{self.bronze.entity}"
            elif self.silver is not None:
                self.name = "silver_pipeline"

    def _auto_wire_silver_source(self) -> None:
        """Auto-wire Silver's source_path from Bronze's target_path.

        Only applies when:
        1. Both Bronze and Silver are configured
        2. Silver's source_path is empty or uses the default pattern
        """
        if self.bronze is None or self.silver is None:
            return

        # Check if Silver needs auto-wiring (empty or placeholder source_path)
        silver_source = getattr(self.silver, "source_path", "")
        if not silver_source or silver_source == "":
            # Construct source_path from Bronze's target_path
            bronze_target = self.bronze.target_path
            # Append *.parquet glob pattern if not already a glob
            if "*" not in bronze_target:
                if bronze_target.endswith("/"):
                    auto_source = f"{bronze_target}*.parquet"
                else:
                    auto_source = f"{bronze_target}/*.parquet"
            else:
                auto_source = bronze_target

            # Update Silver's source_path using object.__setattr__ for frozen dataclass compatibility
            object.__setattr__(self.silver, "source_path", auto_source)
            logger.debug(
                "Auto-wired Silver source_path from Bronze: %s", auto_source
            )

    def run(self, run_date: str, **kwargs: Any) -> Dict[str, Any]:
        """Run full pipeline: Bronze → Silver.

        Args:
            run_date: The date for this pipeline run (YYYY-MM-DD format)
            **kwargs: Additional arguments passed to both layers
                - dry_run: Validate without executing
                - target_override: Override target paths

        Returns:
            Dictionary with results from both layers:
            {
                "bronze": {...},  # Bronze extraction result
                "silver": {...},  # Silver curation result
            }
        """
        result: Dict[str, Any] = {}

        if self.bronze is not None:
            if self.before_bronze:
                self.before_bronze(run_date, kwargs)

            bronze_result = self.bronze.run(run_date, **kwargs)
            result["bronze"] = bronze_result

            if self.after_bronze:
                self.after_bronze(run_date, kwargs, bronze_result)

        if self.silver is not None:
            if self.before_silver:
                self.before_silver(run_date, kwargs)

            silver_result = self.silver.run(run_date, **kwargs)
            result["silver"] = silver_result

            if self.after_silver:
                self.after_silver(run_date, kwargs, silver_result)

        return result

    def run_bronze(self, run_date: str, **kwargs: Any) -> Dict[str, Any]:
        """Run Bronze layer only.

        Args:
            run_date: The date for this extraction run (YYYY-MM-DD format)
            **kwargs: Additional arguments (dry_run, target_override, etc.)

        Returns:
            Bronze extraction result dictionary

        Raises:
            ValueError: If no Bronze source is configured
        """
        if self.bronze is None:
            raise ValueError(
                "No Bronze source configured. "
                "Use Pipeline(bronze=...) to configure Bronze extraction."
            )

        if self.before_bronze:
            self.before_bronze(run_date, kwargs)

        result = self.bronze.run(run_date, **kwargs)

        if self.after_bronze:
            self.after_bronze(run_date, kwargs, result)

        return result

    def run_silver(self, run_date: str, **kwargs: Any) -> Dict[str, Any]:
        """Run Silver layer only.

        Args:
            run_date: The date for this curation run (YYYY-MM-DD format)
            **kwargs: Additional arguments (dry_run, target_override, etc.)

        Returns:
            Silver curation result dictionary

        Raises:
            ValueError: If no Silver entity is configured
        """
        if self.silver is None:
            raise ValueError(
                "No Silver entity configured. "
                "Use Pipeline(silver=...) to configure Silver curation."
            )

        if self.before_silver:
            self.before_silver(run_date, kwargs)

        result = self.silver.run(run_date, **kwargs)

        if self.after_silver:
            self.after_silver(run_date, kwargs, result)

        return result

    def validate(self, run_date: Optional[str] = None) -> Dict[str, Any]:
        """Validate pipeline configuration.

        Performs pre-flight checks on both Bronze and Silver configurations.

        Args:
            run_date: Optional run date for path validation

        Returns:
            Dictionary with validation results:
            {
                "valid": bool,
                "bronze_issues": [...],
                "silver_issues": [...],
            }
        """
        result: Dict[str, Any] = {
            "valid": True,
            "bronze_issues": [],
            "silver_issues": [],
        }

        if self.bronze is not None:
            bronze_issues = self.bronze.validate(run_date)
            result["bronze_issues"] = bronze_issues
            if bronze_issues:
                result["valid"] = False

        if self.silver is not None:
            silver_issues = self.silver.validate(run_date)
            result["silver_issues"] = silver_issues
            if silver_issues:
                result["valid"] = False

        return result

    def explain(self) -> str:
        """Return a human-readable explanation of the pipeline configuration.

        Returns:
            Multi-line string describing what the pipeline does
        """
        lines = [
            f"Pipeline: {self.name or 'unnamed'}",
            "=" * 50,
        ]

        if self.bronze is not None:
            lines.extend([
                "",
                "BRONZE LAYER:",
                f"  System:       {self.bronze.system}",
                f"  Entity:       {self.bronze.entity}",
                f"  Source Type:  {self.bronze.source_type.value}",
                f"  Source:       {self.bronze.source_path or '(database query)'}",
                f"  Target:       {self.bronze.target_path}",
                f"  Load Pattern: {self.bronze.load_pattern.value}",
            ])
            if self.bronze.watermark_column:
                lines.append(f"  Watermark:    {self.bronze.watermark_column}")

        if self.silver is not None:
            lines.extend([
                "",
                "SILVER LAYER:",
                f"  Source:       {self.silver.source_path}",
                f"  Target:       {self.silver.target_path}",
                f"  Natural Keys: {', '.join(self.silver.natural_keys)}",
                f"  Change Col:   {self.silver.change_timestamp}",
                f"  Entity Kind:  {self.silver.entity_kind.value}",
                f"  History Mode: {self.silver.history_mode.value}",
            ])
            if self.silver.attributes:
                lines.append(f"  Attributes:   {', '.join(self.silver.attributes)}")

        return "\n".join(lines)

    def __repr__(self) -> str:
        """Return a concise representation."""
        parts = []
        if self.bronze:
            parts.append(f"bronze={self.bronze.system}.{self.bronze.entity}")
        if self.silver:
            parts.append("silver=configured")
        return f"Pipeline({', '.join(parts)})"
