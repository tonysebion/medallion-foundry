"""
Run option primitives shared between Bronze and Silver CLIs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from core.primitives.foundations.patterns import LoadPattern


@dataclass
class RunOptions:
    load_pattern: LoadPattern
    require_checksum: bool
    write_parquet: bool
    write_csv: bool
    parquet_compression: str
    primary_keys: List[str] = field(default_factory=list)
    order_column: Optional[str] = None
    partition_columns: List[str] = field(default_factory=list)
    artifact_names: Dict[str, str] = field(default_factory=dict)
    on_success_webhooks: List[str] = field(default_factory=list)
    on_failure_webhooks: List[str] = field(default_factory=list)
    artifact_writer_kind: str = "default"  # or "transactional"

    @staticmethod
    def default_artifacts() -> Dict[str, str]:
        return {
            "full_snapshot": "full_snapshot",
            "cdc": "cdc_changes",
            "history": "history",
            "current": "current",
        }
