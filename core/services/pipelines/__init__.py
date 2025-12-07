"""Backward compatibility shim."""
from core.domain.services.pipelines import *
from core.domain.services import pipelines as _pipelines
bronze = _pipelines.bronze
silver = _pipelines.silver
