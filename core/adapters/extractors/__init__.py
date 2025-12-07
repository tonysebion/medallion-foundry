"""Backward compatibility shim for core.adapters.extractors."""
from core.domain.adapters.extractors import *
from core.domain.adapters.extractors import (
    api_extractor,
    db_extractor,
    db_multi_extractor,
    file_extractor,
    pagination,
    cursor_state,
)
