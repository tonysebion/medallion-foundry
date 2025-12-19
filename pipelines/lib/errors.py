"""Structured exception hierarchy for pipelines.

Provides specific exception types for common failure modes,
with rich context for debugging and troubleshooting.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pipelines.lib.bronze import BronzeSource
    from pipelines.lib.silver import SilverEntity

__all__ = [
    "PipelineError",
    "BronzeExtractionError",
    "SilverCurationError",
    "ConnectionError",
    "ConfigurationError",
    "ChecksumError",
    "SourceNotFoundError",
    "ValidationError",
]


class PipelineError(Exception):
    """Base exception for all pipeline errors.

    Provides structured error information for debugging.
    """

    def __init__(
        self,
        message: str,
        *,
        system: Optional[str] = None,
        entity: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        suggestion: Optional[str] = None,
    ) -> None:
        self.system = system
        self.entity = entity
        self.details = details or {}
        self.suggestion = suggestion

        # Build full message
        parts = [message]

        if system or entity:
            context = f"{system or '?'}.{entity or '?'}"
            parts.insert(0, f"[{context}]")

        if details:
            detail_lines = [f"  {k}: {v}" for k, v in details.items()]
            parts.append("\nDetails:")
            parts.extend(detail_lines)

        if suggestion:
            parts.append(f"\nSuggestion: {suggestion}")

        super().__init__("\n".join(parts) if len(parts) > 1 else message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for structured logging."""
        return {
            "error_type": self.__class__.__name__,
            "message": str(self.args[0]) if self.args else "",
            "system": self.system,
            "entity": self.entity,
            "details": self.details,
            "suggestion": self.suggestion,
        }


class BronzeExtractionError(PipelineError):
    """Error during Bronze layer extraction.

    Raised when data extraction from a source fails.
    """

    def __init__(
        self,
        message: str,
        *,
        source: Optional["BronzeSource"] = None,
        cause: Optional[Exception] = None,
        **kwargs: Any,
    ) -> None:
        self.source = source
        self.cause = cause

        details = kwargs.pop("details", {})
        if source:
            details.update({
                "source_type": source.source_type.value,
                "source_path": source.source_path,
                "load_pattern": source.load_pattern.value,
            })
        if cause:
            details["cause"] = str(cause)
            details["cause_type"] = type(cause).__name__

        super().__init__(
            message,
            system=source.system if source else kwargs.get("system"),
            entity=source.entity if source else kwargs.get("entity"),
            details=details,
            **kwargs,
        )


class SilverCurationError(PipelineError):
    """Error during Silver layer curation.

    Raised when data curation (dedup, history, etc.) fails.
    """

    def __init__(
        self,
        message: str,
        *,
        entity: Optional["SilverEntity"] = None,
        cause: Optional[Exception] = None,
        **kwargs: Any,
    ) -> None:
        self.silver_entity = entity
        self.cause = cause

        details = kwargs.pop("details", {})
        if entity:
            details.update({
                "source_path": entity.source_path,
                "target_path": entity.target_path,
                "natural_keys": entity.natural_keys,
                "history_mode": entity.history_mode.value,
            })
        if cause:
            details["cause"] = str(cause)
            details["cause_type"] = type(cause).__name__

        super().__init__(
            message,
            entity=kwargs.get("entity"),
            details=details,
            **kwargs,
        )


class ConnectionError(PipelineError):
    """Error connecting to a data source.

    Raised when database or API connection fails.
    """

    def __init__(
        self,
        message: str,
        *,
        connection_name: Optional[str] = None,
        host: Optional[str] = None,
        cause: Optional[Exception] = None,
        **kwargs: Any,
    ) -> None:
        self.connection_name = connection_name
        self.host = host
        self.cause = cause

        details = kwargs.pop("details", {})
        if connection_name:
            details["connection_name"] = connection_name
        if host:
            details["host"] = host
        if cause:
            details["cause"] = str(cause)
            details["cause_type"] = type(cause).__name__

        suggestion = kwargs.pop("suggestion", None)
        if not suggestion:
            suggestion = (
                "Check that the host is reachable and credentials are correct. "
                "Verify environment variables are set."
            )

        super().__init__(message, details=details, suggestion=suggestion, **kwargs)


class ConfigurationError(PipelineError):
    """Error in pipeline configuration.

    Raised when configuration is invalid or incomplete.
    """

    def __init__(
        self,
        message: str,
        *,
        field: Optional[str] = None,
        value: Any = None,
        **kwargs: Any,
    ) -> None:
        self.field = field
        self.value = value

        details = kwargs.pop("details", {})
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = str(value)

        super().__init__(message, details=details, **kwargs)


class ChecksumError(PipelineError):
    """Error during checksum validation.

    Raised when data integrity checks fail.
    """

    def __init__(
        self,
        message: str,
        *,
        file_path: Optional[str] = None,
        expected: Optional[str] = None,
        actual: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.file_path = file_path
        self.expected = expected
        self.actual = actual

        details = kwargs.pop("details", {})
        if file_path:
            details["file_path"] = file_path
        if expected:
            details["expected_checksum"] = expected
        if actual:
            details["actual_checksum"] = actual

        suggestion = kwargs.pop("suggestion", None)
        if not suggestion:
            suggestion = (
                "Data may be corrupted. Re-run the Bronze extraction "
                "or check the source data."
            )

        super().__init__(message, details=details, suggestion=suggestion, **kwargs)


class SourceNotFoundError(PipelineError):
    """Source data not found.

    Raised when expected source files or tables don't exist.
    """

    def __init__(
        self,
        message: str,
        *,
        path: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.path = path

        details = kwargs.pop("details", {})
        if path:
            details["path"] = path

        suggestion = kwargs.pop("suggestion", None)
        if not suggestion:
            suggestion = (
                "Ensure the source data exists. For Silver, run Bronze first."
            )

        super().__init__(message, details=details, suggestion=suggestion, **kwargs)


class ValidationError(PipelineError):
    """Validation errors in pipeline configuration.

    Raised when validate() finds issues that prevent execution.
    """

    def __init__(
        self,
        message: str,
        *,
        issues: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:
        self.issues = issues or []

        details = kwargs.pop("details", {})
        if issues:
            details["issue_count"] = len(issues)

        # Build message with issues
        if issues:
            issue_lines = "\n".join(f"  - {issue}" for issue in issues)
            message = f"{message}\n\nIssues found:\n{issue_lines}"

        super().__init__(message, details=details, **kwargs)
