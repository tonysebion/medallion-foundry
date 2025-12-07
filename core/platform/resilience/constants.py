"""Resilience-related constants used across the framework.

This module centralizes default values for retry policies, circuit breakers,
rate limiting, and other resilience patterns to ensure consistency and easy
configuration updates.
"""

# =============================================================================
# Retry Policy Defaults
# =============================================================================

# Maximum number of retry attempts before giving up
DEFAULT_MAX_ATTEMPTS: int = 5

# Initial delay between retries (in seconds)
DEFAULT_BASE_DELAY: float = 0.5

# Maximum delay between retries (in seconds), regardless of backoff
DEFAULT_MAX_DELAY: float = 8.0

# Multiplier for exponential backoff (delay * multiplier^attempt)
DEFAULT_BACKOFF_MULTIPLIER: float = 2.0

# Fraction of delay to add as jitter (0.0 to 1.0)
DEFAULT_JITTER: float = 0.2


# =============================================================================
# Circuit Breaker Defaults
# =============================================================================

# Number of failures before the circuit opens
DEFAULT_FAILURE_THRESHOLD: int = 5

# Time to wait before attempting recovery (in seconds)
DEFAULT_COOLDOWN_SECONDS: float = 30.0

# Number of calls allowed during half-open state
DEFAULT_HALF_OPEN_MAX_CALLS: int = 1


# =============================================================================
# Late Data Handling Defaults
# =============================================================================

# Default threshold for considering data "late" (in days)
DEFAULT_LATE_DATA_THRESHOLD_DAYS: int = 7

# Default quarantine subdirectory name
DEFAULT_QUARANTINE_PATH: str = "_quarantine"


# =============================================================================
# Chunk Processing Defaults
# =============================================================================

# Default chunk size for streaming large files (1 MB)
DEFAULT_CHUNK_SIZE_BYTES: int = 1024 * 1024


# =============================================================================
# Storage Backend Defaults
# =============================================================================

# Default timeout for storage operations (in seconds)
DEFAULT_STORAGE_TIMEOUT_SECONDS: int = 30


__all__ = [
    # Retry Policy
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_BASE_DELAY",
    "DEFAULT_MAX_DELAY",
    "DEFAULT_BACKOFF_MULTIPLIER",
    "DEFAULT_JITTER",
    # Circuit Breaker
    "DEFAULT_FAILURE_THRESHOLD",
    "DEFAULT_COOLDOWN_SECONDS",
    "DEFAULT_HALF_OPEN_MAX_CALLS",
    # Late Data
    "DEFAULT_LATE_DATA_THRESHOLD_DAYS",
    "DEFAULT_QUARANTINE_PATH",
    # Chunk Processing
    "DEFAULT_CHUNK_SIZE_BYTES",
    # Storage
    "DEFAULT_STORAGE_TIMEOUT_SECONDS",
]
