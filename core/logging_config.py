"""Logging configuration for bronze-foundry.

This module provides flexible logging configuration with support for:
- Environment variable-based log level control
- JSON formatting for production monitoring
- Custom handlers and formatters
- Structured logging with context
"""

import logging
import logging.handlers
import json
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional, Union
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """Format log records as JSON for structured logging."""
    
    def __init__(self, include_context: bool = True):
        """
        Initialize JSON formatter.
        
        Args:
            include_context: Whether to include additional context fields
        """
        super().__init__()
        self.include_context = include_context
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON string."""
        log_data: Dict[str, Any] = {
            'timestamp': datetime.utcfromtimestamp(record.created).isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        
        # Add optional context
        if self.include_context:
            log_data['module'] = record.module
            log_data['function'] = record.funcName
            log_data['line'] = record.lineno
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'created', 'filename', 'funcName',
                          'levelname', 'levelno', 'lineno', 'module', 'msecs',
                          'message', 'pathname', 'process', 'processName', 'relativeCreated',
                          'thread', 'threadName', 'exc_info', 'exc_text', 'stack_info']:
                log_data[key] = value
        
        return json.dumps(log_data)


class HumanReadableFormatter(logging.Formatter):
    """Format log records in human-readable format with colors (optional)."""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'
    }
    
    def __init__(self, use_colors: bool = False, include_context: bool = False):
        """
        Initialize human-readable formatter.
        
        Args:
            use_colors: Whether to use ANSI colors (for terminals)
            include_context: Whether to include module/function info
        """
        if include_context:
            fmt = '[%(levelname)s] %(asctime)s - %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
        else:
            fmt = '[%(levelname)s] %(asctime)s - %(name)s - %(message)s'
        
        super().__init__(fmt=fmt, datefmt='%Y-%m-%d %H:%M:%S')
        self.use_colors = use_colors and sys.stdout.isatty()
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with optional colors."""
        formatted = super().format(record)
        
        if self.use_colors:
            color = self.COLORS.get(record.levelname, '')
            reset = self.COLORS['RESET']
            return f"{color}{formatted}{reset}"
        
        return formatted


def get_log_level_from_env() -> int:
    """
    Get log level from environment variable.
    
    Environment variables checked (in order):
    1. BRONZE_LOG_LEVEL - Bronze-foundry specific
    2. LOG_LEVEL - Generic
    
    Returns:
        Logging level (default: INFO)
    """
    level_name = os.environ.get('BRONZE_LOG_LEVEL') or os.environ.get('LOG_LEVEL', 'INFO')
    level_name = level_name.upper()
    
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'WARN': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL,
    }
    
    return level_map.get(level_name, logging.INFO)


def get_log_format_from_env() -> str:
    """
    Get log format from environment variable.
    
    Environment variable: BRONZE_LOG_FORMAT
    Values: 'json', 'human', 'simple'
    
    Returns:
        Log format name (default: 'human')
    """
    return os.environ.get('BRONZE_LOG_FORMAT', 'human').lower()


def setup_logging(
    level: Optional[int] = None,
    format_type: Optional[str] = None,
    log_file: Optional[Path] = None,
    use_colors: bool = False,
    include_context: bool = False
) -> None:
    """
    Configure logging for bronze-foundry.
    
    Args:
        level: Logging level (defaults to BRONZE_LOG_LEVEL or INFO)
        format_type: Format type ('json', 'human', 'simple')
        log_file: Optional path to log file
        use_colors: Use ANSI colors in console output
        include_context: Include module/function context in logs
    
    Environment Variables:
        BRONZE_LOG_LEVEL: Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        BRONZE_LOG_FORMAT: Set format (json, human, simple)
        BRONZE_LOG_FILE: Path to log file
        LOG_LEVEL: Fallback for log level
    
    Examples:
        >>> # Use defaults (INFO level, human format)
        >>> setup_logging()
        
        >>> # Debug level with JSON format
        >>> setup_logging(level=logging.DEBUG, format_type='json')
        
        >>> # Production: JSON to file, INFO to console
        >>> setup_logging(
        ...     format_type='json',
        ...     log_file=Path('logs/bronze.log'),
        ...     include_context=True
        ... )
    """
    # Get configuration from environment if not specified
    if level is None:
        level = get_log_level_from_env()
    
    if format_type is None:
        format_type = get_log_format_from_env()
    
    if log_file is None:
        log_file_env = os.environ.get('BRONZE_LOG_FILE')
        if log_file_env:
            log_file = Path(log_file_env)
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers
    root_logger.handlers.clear()
    
    # Create formatter
    if format_type == 'json':
        formatter = JSONFormatter(include_context=include_context)
    elif format_type == 'simple':
        formatter = logging.Formatter('%(levelname)s: %(message)s')
    else:  # human
        formatter = HumanReadableFormatter(use_colors=use_colors, include_context=include_context)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Use rotating file handler (10MB max, 5 backups)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(level)
        
        # Always use JSON for file logs
        file_formatter = JSONFormatter(include_context=True)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)


def get_logger(name: str, extra: Optional[Dict[str, Any]] = None) -> Union[logging.Logger, logging.LoggerAdapter]:
    """
    Get a logger with optional extra context.
    
    Args:
        name: Logger name (typically __name__)
        extra: Extra context to include in all log messages
    
    Returns:
        Configured logger or logger adapter with extra fields
    
    Example:
        >>> logger = get_logger(__name__, extra={'system': 'salesforce', 'table': 'accounts'})
        >>> logger.info("Starting extraction")
        # Output includes system and table in every message
    """
    logger = logging.getLogger(name)
    
    if extra:
        # Create adapter that adds extra fields to all messages
        return logging.LoggerAdapter(logger, extra)
    
    return logger


def log_exception(logger: logging.Logger, message: str, exc: Exception) -> None:
    """
    Log an exception with full context.
    
    Args:
        logger: Logger instance
        message: Additional context message
        exc: Exception to log
    
    Example:
        >>> try:
        ...     risky_operation()
        ... except Exception as e:
        ...     log_exception(logger, "Failed to process data", e)
    """
    logger.error(
        f"{message}: {str(exc)}",
        exc_info=True,
        extra={
            'exception_type': type(exc).__name__,
            'exception_message': str(exc)
        }
    )


def log_performance(logger: logging.Logger, operation: str, duration_seconds: float, **metrics: Any) -> None:
    """
    Log performance metrics.
    
    Args:
        logger: Logger instance
        operation: Operation name
        duration_seconds: Operation duration
        **metrics: Additional metrics to log
    
    Example:
        >>> log_performance(
        ...     logger,
        ...     "data_extraction",
        ...     duration_seconds=45.2,
        ...     records_processed=10000,
        ...     records_per_second=221
        ... )
    """
    logger.info(
        f"Performance: {operation} completed in {duration_seconds:.2f}s",
        extra={
            'operation': operation,
            'duration_seconds': duration_seconds,
            **metrics
        }
    )


# Default setup for importing modules
# This provides basic logging even if setup_logging() isn't called explicitly
if not logging.getLogger().handlers:
    setup_logging()
