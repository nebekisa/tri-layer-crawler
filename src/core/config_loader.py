"""
Production logging configuration with JSON formatting and log levels.
"""

import logging
import json
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    Compatible with ELK stack, Datadog, Loki.
    """
    
    def __init__(self, service_name: str = "tri-layer-crawler"):
        super().__init__()
        self.service_name = service_name
    
    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": self.service_name,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "pid": os.getpid(),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info)
            }
        
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
        
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        return (
            f"{color}[{record.levelname}]{reset} "
            f"{timestamp} - "
            f"{record.name} - "
            f"{record.getMessage()}"
        )


def setup_logging(
    log_level: str = "INFO",
    log_file: str = "logs/app.log",
    json_format: bool = False,
    service_name: str = "tri-layer-crawler"
) -> None:
    """
    Configure application logging.
    
    Args:
        log_level: Logging level
        log_file: Path to log file
        json_format: Use JSON format for file logs
        service_name: Service name for logs
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
        file_handler.setFormatter(JSONFormatter(service_name))
    else:
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
    file_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    
    # Error file handler (separate file for errors)
    error_handler = logging.FileHandler("logs/error.log", encoding='utf-8')
    error_handler.setFormatter(JSONFormatter(service_name) if json_format else logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    error_handler.setLevel(logging.ERROR)
    root_logger.addHandler(error_handler)
    
    # Quiet down noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)


class LoggerMixin:
    """Mixin to add logging capability to any class."""
    
    @property
    def logger(self) -> logging.Logger:
        if not hasattr(self, '_logger'):
            self._logger = logging.getLogger(self.__class__.__name__)
        return self._logger


class MetricsLogger:
    """Log metrics in structured format for Prometheus/Loki."""
    
    @staticmethod
    def log_crawl_metrics(
        url: str,
        duration_ms: float,
        bytes_downloaded: int,
        status_code: int,
        success: bool
    ) -> None:
        """Log crawl metrics."""
        logger = logging.getLogger("metrics.crawler")
        logger.info(
            "Crawl completed",
            extra={
                "extra_data": {
                    "url": url,
                    "duration_ms": duration_ms,
                    "bytes_downloaded": bytes_downloaded,
                    "status_code": status_code,
                    "success": success,
                    "metric_type": "crawl"
                }
            }
        )
    
    @staticmethod
    def log_analysis_metrics(
        item_id: int,
        entities_found: int,
        sentiment_score: float,
        processing_time_ms: float
    ) -> None:
        """Log AI analysis metrics."""
        logger = logging.getLogger("metrics.analytics")
        logger.info(
            "Analysis completed",
            extra={
                "extra_data": {
                    "item_id": item_id,
                    "entities_found": entities_found,
                    "sentiment_score": sentiment_score,
                    "processing_time_ms": processing_time_ms,
                    "metric_type": "analysis"
                }
            }
        )