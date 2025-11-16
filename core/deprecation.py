"""Deprecation and compatibility warning utilities.

Provides structured warning categories and helper to standardize messaging.
"""
from __future__ import annotations
import warnings
from dataclasses import dataclass
from typing import Optional

class BronzeFoundryDeprecationWarning(DeprecationWarning):
    """Base category for deprecation notices."""

class BronzeFoundryCompatibilityWarning(UserWarning):
    """Non-breaking compatibility fallback triggered."""

warnings.simplefilter("default", BronzeFoundryDeprecationWarning)
warnings.simplefilter("default", BronzeFoundryCompatibilityWarning)

@dataclass(frozen=True)
class DeprecationSpec:
    code: str
    message: str
    since: str
    remove_in: Optional[str] = None

    def format(self) -> str:
        suffix = f" (scheduled removal: {self.remove_in})" if self.remove_in else ""
        return f"[{self.code}] {self.message} (since {self.since}){suffix}"


def emit_deprecation(spec: DeprecationSpec) -> None:
    warnings.warn(spec.format(), BronzeFoundryDeprecationWarning, stacklevel=2)


def emit_compat(message: str, code: str) -> None:
    warnings.warn(f"[{code}] {message}", BronzeFoundryCompatibilityWarning, stacklevel=2)
