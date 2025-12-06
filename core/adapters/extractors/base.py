"""Base extractor interface per spec Section 3.

All extractors must implement the BaseExtractor interface. The interface
provides a unified approach for:
- Fetching records from various sources
- Managing watermarks for incremental loads
- Handling extraction configuration

Extractor Registration:
    Use the @register_extractor decorator to register new extractor types:

    @register_extractor("api")
    class ApiExtractor(BaseExtractor):
        ...

    Registered extractors can be retrieved via get_extractor_class("api").
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Dict, Any, List, Optional, Tuple, Type
from datetime import date

from core.primitives.state.watermark import Watermark, WatermarkStore, WatermarkType


# =============================================================================
# Extractor Registry
# =============================================================================

# Global registry mapping source type names to extractor classes
EXTRACTOR_REGISTRY: Dict[str, Type["BaseExtractor"]] = {}


def register_extractor(source_type: str) -> Callable[[Type["BaseExtractor"]], Type["BaseExtractor"]]:
    """Decorator to register an extractor class for a source type.

    Args:
        source_type: The source type name (e.g., "api", "db", "file", "custom")

    Returns:
        Decorator function that registers the class

    Example:
        @register_extractor("api")
        class ApiExtractor(BaseExtractor):
            ...
    """
    def decorator(cls: Type["BaseExtractor"]) -> Type["BaseExtractor"]:
        EXTRACTOR_REGISTRY[source_type] = cls
        return cls
    return decorator


def get_extractor_class(source_type: str) -> Optional[Type["BaseExtractor"]]:
    """Get the extractor class for a source type.

    Args:
        source_type: The source type name

    Returns:
        The extractor class, or None if not registered
    """
    return EXTRACTOR_REGISTRY.get(source_type)


def list_extractor_types() -> List[str]:
    """List all registered extractor types.

    Returns:
        List of registered source type names
    """
    return list(EXTRACTOR_REGISTRY.keys())


@dataclass
class ExtractionResult:
    """Result of an extraction operation."""

    records: List[Dict[str, Any]] = field(default_factory=list)
    new_watermark: Optional[str] = None
    record_count: int = 0
    watermark_column: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.record_count == 0 and self.records:
            self.record_count = len(self.records)


class BaseExtractor(ABC):
    """Abstract base class for all extractors.

    Implementations must return:

      - a list of dict-like records.
      - an optional new_cursor string representing incremental state.

    Subclasses may override get_watermark_config() to specify how watermarks
    are managed for their source type.
    """

    @abstractmethod
    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch records from the source.

        Args:
            cfg: Full pipeline configuration dictionary
            run_date: The date of the extraction run

        Returns:
            Tuple of (records list, new watermark value or None)
        """
        raise NotImplementedError()

    def get_watermark_config(
        self, cfg: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Get watermark configuration for this extractor.

        Override in subclasses to specify watermark handling.

        Args:
            cfg: Full pipeline configuration dictionary

        Returns:
            Dictionary with watermark config, or None if not supported.
            Expected keys:
            - column: watermark column name
            - type: watermark type (timestamp, date, integer, string)
            - enabled: whether watermark is enabled
        """
        return None

    def fetch_with_watermark(
        self,
        cfg: Dict[str, Any],
        run_date: date,
        watermark_store: Optional[WatermarkStore] = None,
        run_id: Optional[str] = None,
    ) -> ExtractionResult:
        """Fetch records with unified watermark handling.

        This method provides a standard way to handle watermarks across
        all extractor types. It:
        1. Loads existing watermark from store
        2. Calls fetch_records() to get data
        3. Updates watermark in store after successful extraction

        Args:
            cfg: Full pipeline configuration dictionary
            run_date: The date of the extraction run
            watermark_store: Optional watermark store for persistence
            run_id: Optional run ID for tracking

        Returns:
            ExtractionResult with records and metadata
        """
        source = cfg.get("source", {})
        system = source.get("system", "unknown")
        table = source.get("table", "unknown")

        wm_config = self.get_watermark_config(cfg)
        watermark: Optional[Watermark] = None

        # Load existing watermark if configured
        if wm_config and wm_config.get("enabled") and watermark_store:
            wm_column = wm_config.get("column", "")
            wm_type_str = wm_config.get("type", "timestamp")
            try:
                wm_type = WatermarkType(wm_type_str)
            except ValueError:
                wm_type = WatermarkType.TIMESTAMP

            watermark = watermark_store.get(system, table, wm_column, wm_type)

        # Fetch records
        records, new_cursor = self.fetch_records(cfg, run_date)

        result = ExtractionResult(
            records=records,
            new_watermark=new_cursor,
            record_count=len(records),
            watermark_column=wm_config.get("column") if wm_config else None,
        )

        # Update watermark if we have new data
        if watermark and new_cursor and watermark_store:
            watermark.update(
                new_value=new_cursor,
                run_id=run_id or "unknown",
                run_date=run_date,
                record_count=len(records),
            )
            watermark_store.save(watermark)
            result.metadata["watermark_updated"] = True
            result.metadata["previous_watermark"] = watermark.watermark_value

        return result
