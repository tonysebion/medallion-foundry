"""Custom exception classes for medallion-foundry.

This module provides specific exception types for better error handling and debugging.
"""

from typing import Optional, Dict, Any


class BronzeFoundryError(Exception):
    """Base exception for all medallion-foundry errors."""
    
    error_code: str = "ERR000"  # Override in subclasses
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, error_code: Optional[str] = None):
        """
        Initialize medallion-foundry exception.
        
        Args:
            message: Human-readable error message
            details: Optional dictionary with additional context
            error_code: Optional error code override
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
        if error_code:
            self.error_code = error_code
    
    def __str__(self) -> str:
        """Return string representation with error code and details."""
        parts = [f"[{self.error_code}] {self.message}"]
        if self.details:
            detail_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            parts.append(f"({detail_str})")
        return " ".join(parts)


class ConfigValidationError(BronzeFoundryError):
    """Raised when configuration validation fails.
    
    Examples:
        - Missing required configuration keys
        - Invalid configuration values
        - Type mismatches in configuration
        - Backend-specific validation failures
    """
    
    error_code = "CFG001"
    
    def __init__(self, message: str, config_path: Optional[str] = None, key: Optional[str] = None):
        """
        Initialize configuration validation error.
        
        Args:
            message: Description of validation failure
            config_path: Path to config file that failed validation
            key: Specific configuration key that caused the error
        """
        details = {}
        if config_path:
            details['config_path'] = config_path
        if key:
            details['config_key'] = key
        super().__init__(message, details)


class ExtractionError(BronzeFoundryError):
    """Raised when data extraction fails.
    
    Examples:
        - API request failures
        - Database query errors
        - Pagination failures
        - Authentication errors
        - Data parsing errors
    """
    
    error_code = "EXT001"
    
    def __init__(
        self, 
        message: str, 
        extractor_type: Optional[str] = None,
        system: Optional[str] = None,
        table: Optional[str] = None,
        original_error: Optional[Exception] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            message: Description of extraction failure
            extractor_type: Type of extractor (api, db, custom)
            system: System name from configuration
            table: Table name from configuration
            original_error: Original exception that caused this error
        """
        details = {}
        if extractor_type:
            details['extractor_type'] = extractor_type
        if system:
            details['system'] = system
        if table:
            details['table'] = table
        if original_error:
            details['original_error'] = str(original_error)
            details['error_type'] = type(original_error).__name__
        
        super().__init__(message, details)
        self.original_error = original_error


class StorageError(BronzeFoundryError):
    """Raised when storage operations fail.
    
    Examples:
        - S3 upload/download failures
        - Azure blob operations failures
        - Local filesystem errors
        - Permission errors
        - Network connectivity issues
    """
    
    error_code = "STG001"
    
    def __init__(
        self,
        message: str,
        backend_type: Optional[str] = None,
        operation: Optional[str] = None,
        file_path: Optional[str] = None,
        remote_path: Optional[str] = None,
        original_error: Optional[Exception] = None
    ):
        """
        Initialize storage error.
        
        Args:
            message: Description of storage failure
        backend_type: Storage backend type (s3, azure, local)
            operation: Operation that failed (upload, download, delete, list)
            file_path: Local file path involved in the operation
            remote_path: Remote path involved in the operation
            original_error: Original exception that caused this error
        """
        details = {}
        if backend_type:
            details['backend_type'] = backend_type
        if operation:
            details['operation'] = operation
        if file_path:
            details['file_path'] = file_path
        if remote_path:
            details['remote_path'] = remote_path
        if original_error:
            details['original_error'] = str(original_error)
            details['error_type'] = type(original_error).__name__
        
        super().__init__(message, details)
        self.original_error = original_error


class AuthenticationError(ExtractionError):
    """Raised when authentication fails.
    
    Examples:
        - Invalid API token
        - Expired credentials
        - Missing environment variables for auth
        - OAuth token refresh failures
    """
    
    error_code = "AUTH001"
    
    def __init__(self, message: str, auth_type: Optional[str] = None, env_var: Optional[str] = None):
        """
        Initialize authentication error.
        
        Args:
            message: Description of authentication failure
            auth_type: Type of authentication (bearer, api_key, basic)
            env_var: Environment variable name for credentials
        """
        details = {}
        if auth_type:
            details['auth_type'] = auth_type
        if env_var:
            details['env_var'] = env_var
        
        super().__init__(message)
        self.details.update(details)


class PaginationError(ExtractionError):
    """Raised when pagination logic fails.
    
    Examples:
        - Missing pagination parameters in response
        - Infinite pagination loop detected
        - Invalid cursor value
        - Page size exceeds API limits
    """
    
    error_code = "PAGE001"
    
    def __init__(
        self,
        message: str,
        pagination_type: Optional[str] = None,
        page: Optional[int] = None,
        cursor: Optional[str] = None
    ):
        """
        Initialize pagination error.
        
        Args:
            message: Description of pagination failure
            pagination_type: Type of pagination (offset, page, cursor)
            page: Current page number
            cursor: Current cursor value
        """
        details = {}
        if pagination_type:
            details['pagination_type'] = pagination_type
        if page is not None:
            details['page'] = page
        if cursor:
            details['cursor'] = cursor
        
        super().__init__(message)
        self.details.update(details)


class StateManagementError(BronzeFoundryError):
    """Raised when state file operations fail.
    
    Examples:
        - Cannot read/write state file
        - Corrupt state file
        - State file lock conflicts
        - Invalid cursor format
    """
    
    error_code = "STATE001"
    
    def __init__(
        self,
        message: str,
        state_file: Optional[str] = None,
        cursor_column: Optional[str] = None,
        original_error: Optional[Exception] = None
    ):
        """
        Initialize state management error.
        
        Args:
            message: Description of state management failure
            state_file: Path to state file
            cursor_column: Database column used for cursor
            original_error: Original exception that caused this error
        """
        details = {}
        if state_file:
            details['state_file'] = state_file
        if cursor_column:
            details['cursor_column'] = cursor_column
        if original_error:
            details['original_error'] = str(original_error)
            details['error_type'] = type(original_error).__name__
        
        super().__init__(message, details)
        self.original_error = original_error


class DataQualityError(BronzeFoundryError):
    """Raised when data quality checks fail.
    
    Examples:
        - Schema validation failures
        - Record count below minimum threshold
        - Duplicate records detected
        - Excessive null values
        - Data type mismatches
    """
    
    error_code = "QUAL001"
    
    def __init__(
        self,
        message: str,
        check_type: Optional[str] = None,
        expected: Optional[Any] = None,
        actual: Optional[Any] = None,
        failed_records: Optional[int] = None
    ):
        """
        Initialize data quality error.
        
        Args:
            message: Description of data quality issue
            check_type: Type of quality check (schema, count, duplicates, nulls)
            expected: Expected value or threshold
            actual: Actual value found
            failed_records: Number of records that failed validation
        """
        details = {}
        if check_type:
            details['check_type'] = check_type
        if expected is not None:
            details['expected'] = expected
        if actual is not None:
            details['actual'] = actual
        if failed_records is not None:
            details['failed_records'] = failed_records
        
        super().__init__(message, details)


class RetryExhaustedError(BronzeFoundryError):
    """Raised when all retry attempts are exhausted.
    
    Examples:
        - Network request failed after max retries
        - Temporary service outage
        - Rate limit exceeded
    """
    
    error_code = "RETRY001"
    
    def __init__(
        self,
        message: str,
        attempts: Optional[int] = None,
        operation: Optional[str] = None,
        last_error: Optional[Exception] = None
    ):
        """
        Initialize retry exhausted error.
        
        Args:
            message: Description of failure
            attempts: Number of attempts made
            operation: Operation that was retried
            last_error: Last exception before giving up
        """
        details = {}
        if attempts is not None:
            details['attempts'] = attempts
        if operation:
            details['operation'] = operation
        if last_error:
            details['last_error'] = str(last_error)
            details['error_type'] = type(last_error).__name__
        
        super().__init__(message, details)
        self.last_error = last_error
