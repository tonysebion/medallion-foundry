"""Backward compatibility shim for core.primitives.

DEPRECATED: Use core.foundation instead.
"""
from core.foundation import primitives as foundations
from core.foundation import state
from core.foundation import catalog
from core.foundation.time_utils import utc_isoformat, utc_now

__all__ = ["foundations", "state", "catalog", "utc_isoformat", "utc_now"]
