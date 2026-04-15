"""
Production logging configuration with JSON formatting.
"""

import logging
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    Compatible with ELK stack, Datadog, etc.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields if present
        if hasattr(record, "extra_data"):
            log_data["extra"] = record.extra_data
        
        return json.dumps(log_data)


class ConsoleFormatter(logging.Formatter):
    """Human-readable console formatter with colors."""
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'       # Reset
    }
    
    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        return (
            f"{color}[{record.levelname}]{reset} "
            f"{datetime.utcnow().strftime('%H:%M:%S')} - "
            f"{record.name} - "
            f"{record.getMessage()}"
        )


def setup_logging(
    log_level: str = "INFO",
    log_file: str = "logs/app.log",
    json_format: bool = False
) -> None:
    """
    Configure application logging.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Path to log file
        json_format: Use JSON format for file logs (production)
    """
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    root_logger.handlers.clear()
    
    # Console handler (always human-readable)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ConsoleFormatter())
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    
    # File handler (JSON in production)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    if json_format:
        file_handler.setFormatter(JSONFormatter())
    else:
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
    file_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    
    # Quiet down noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


class LoggerMixin:
    """
    Mixin class to add logging capability to any class.
    
    Usage:
        class MyClass(LoggerMixin):
            def do_something(self):
                self.logger.info("Doing something")
    """
    
    @property
    def logger(self) -> logging.Logger:
        if not hasattr(self, '_logger'):
            self._logger = logging.getLogger(self.__class__.__name__)
        return self._logger