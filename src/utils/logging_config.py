"""
Production logging configuration with rotation and JSON formatting.
"""

import logging
import json
import sys
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from typing import Dict, Any


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
        
        return json.dumps(log_data, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    """Human-readable console formatter with colors."""
    
    COLORS = {
        "DEBUG": "\033[36m",    # Cyan
        "INFO": "\033[32m",     # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",    # Red
        "CRITICAL": "\033[35m", # Magenta
        "RESET": "\033[0m",     # Reset
    }
    
    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.COLORS["RESET"])
        reset = self.COLORS["RESET"]
        
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        return (
            f"{color}[{timestamp}] {record.levelname:8}{reset} "
            f"{record.name}: {record.getMessage()}"
        )


def setup_logging(
    log_level: str = "INFO",
    log_dir: str = "logs",
    json_logs: bool = True,
    max_bytes: int = 10_485_760,  # 10MB
    backup_count: int = 5,
) -> None:
    """
    Configure application logging.
    
    Features:
        - Console output (human-readable)
        - File output (JSON format for machine parsing)
        - Log rotation (size-based)
        - Exception tracking
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory for log files
        json_logs: Enable JSON formatted file logs
        max_bytes: Max size per log file before rotation
        backup_count: Number of backup files to keep
    """
    # Create log directory
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    root_logger.handlers.clear()
    
    # Console handler (human-readable)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(ConsoleFormatter())
    root_logger.addHandler(console_handler)
    
    # File handler (JSON format with rotation)
    if json_logs:
        file_handler = RotatingFileHandler(
            filename=log_path / "application.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JSONFormatter())
        root_logger.addHandler(file_handler)
    
    # Error file handler (only errors and above)
    error_handler = RotatingFileHandler(
        filename=log_path / "errors.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(JSONFormatter())
    root_logger.addHandler(error_handler)
    
    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


class LoggerAdapter(logging.LoggerAdapter):
    """
    Adapter to add extra context to log messages.
    
    Usage:
        logger = LoggerAdapter(logging.getLogger(__name__), {"component": "crawler"})
        logger.info("Starting crawl")  # Extra fields included in JSON
    """
    
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})
    
    def process(self, msg, kwargs):
        kwargs["extra"] = {"extra_data": self.extra}
        return msg, kwargs