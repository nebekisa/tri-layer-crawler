"""
Centralized exception handling.
"""

import logging
from typing import Optional, Any
from functools import wraps
import traceback

logger = logging.getLogger(__name__)


class CrawlerError(Exception):
    """Base exception for crawler errors."""
    pass


class ExtractionError(CrawlerError):
    """Raised when data extraction fails."""
    pass


class NetworkError(CrawlerError):
    """Raised when network request fails."""
    pass


class DatabaseError(CrawlerError):
    """Raised when database operation fails."""
    pass


class ConfigurationError(CrawlerError):
    """Raised when configuration is invalid."""
    pass


def handle_errors(default_return: Any = None, reraise: bool = False):
    """
    Decorator for graceful error handling.
    
    Args:
        default_return: Value to return on error
        reraise: Whether to reraise the exception after logging
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"Error in {func.__name__}: {e}",
                    extra={"extra_data": {
                        "function": func.__name__,
                        "error_type": type(e).__name__,
                        "traceback": traceback.format_exc()
                    }}
                )
                if reraise:
                    raise
                return default_return
        return wrapper
    return decorator


class ErrorTracker:
    """
    Track and report errors for monitoring.
    """
    
    _errors = []
    
    @classmethod
    def track(cls, error: Exception, context: Optional[dict] = None):
        """Track an error with context."""
        error_data = {
            "type": type(error).__name__,
            "message": str(error),
            "context": context or {},
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        cls._errors.append(error_data)
        logger.error(f"Tracked error: {error_data}")
    
    @classmethod
    def get_errors(cls, limit: int = 100) -> list:
        """Get recent errors."""
        return cls._errors[-limit:]
    
    @classmethod
    def clear(cls):
        """Clear error history."""
        cls._errors.clear()
    
    @classmethod
    def count(cls) -> int:
        """Get error count."""
        return len(cls._errors)