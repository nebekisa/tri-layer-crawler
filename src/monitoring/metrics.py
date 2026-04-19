"""
Prometheus metrics for Tri-Layer Intelligence Crawler.

Exposes:
    - Request counts by endpoint, method, status
    - Request latency histograms
    - Crawler-specific metrics (URLs crawled, success rate)
    - Queue metrics (pending, processing, DLQ)
    - Database connection pool stats
"""

from prometheus_client import Counter, Histogram, Gauge, Info
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi import FastAPI
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class CrawlerMetrics:
    """Custom metrics for crawler operations."""
    
    def __init__(self):
        # Crawler metrics
        self.urls_crawled_total = Counter(
            'crawler_urls_total',
            'Total URLs crawled',
            ['domain', 'status']
        )
        
        self.urls_skipped_total = Counter(
            'crawler_urls_skipped_total',
            'URLs skipped (robots.txt, duplicates)',
            ['domain', 'reason']
        )
        
        self.crawl_duration_seconds = Histogram(
            'crawler_duration_seconds',
            'Time spent crawling URLs',
            ['domain'],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
        )
        
        self.bytes_downloaded_total = Counter(
            'crawler_bytes_downloaded_total',
            'Total bytes downloaded',
            ['domain']
        )
        
        # Queue metrics
        self.queue_pending = Gauge(
            'crawler_queue_pending',
            'URLs pending in queue'
        )
        
        self.queue_processing = Gauge(
            'crawler_queue_processing',
            'URLs currently being processed'
        )
        
        self.queue_dlq = Gauge(
            'crawler_queue_dlq',
            'URLs in Dead Letter Queue'
        )
        
        # Database metrics
        self.db_pool_size = Gauge(
            'crawler_db_pool_size',
            'Database connection pool size'
        )
        
        self.db_pool_available = Gauge(
            'crawler_db_pool_available',
            'Available database connections'
        )
        
        # System info
        self.build_info = Info(
            'crawler_build',
            'Build information'
        )
        
        logger.info("CrawlerMetrics initialized")
    
    def record_crawl_success(self, domain: str, duration: float, bytes_downloaded: int):
        """Record a successful crawl."""
        self.urls_crawled_total.labels(domain=domain, status='success').inc()
        self.crawl_duration_seconds.labels(domain=domain).observe(duration)
        self.bytes_downloaded_total.labels(domain=domain).inc(bytes_downloaded)
    
    def record_crawl_failure(self, domain: str, error: str):
        """Record a failed crawl."""
        self.urls_crawled_total.labels(domain=domain, status='failure').inc()
    
    def record_skip(self, domain: str, reason: str):
        """Record a skipped URL."""
        self.urls_skipped_total.labels(domain=domain, reason=reason).inc()
    
    def update_queue_metrics(self, pending: int, processing: int, dlq: int):
        """Update queue gauge metrics."""
        self.queue_pending.set(pending)
        self.queue_processing.set(processing)
        self.queue_dlq.set(dlq)
    
    def update_db_metrics(self, pool_size: int, available: int):
        """Update database pool metrics."""
        self.db_pool_size.set(pool_size)
        self.db_pool_available.set(available)
    
    def set_build_info(self, version: str, environment: str):
        """Set build information."""
        self.build_info.info({
            'version': version,
            'environment': environment
        })


# Global metrics instance
metrics = CrawlerMetrics()
metrics.set_build_info('4.0.0', 'production')


def setup_metrics(app: FastAPI) -> None:
    """
    Configure Prometheus metrics for FastAPI application.
    
    Args:
        app: FastAPI application instance.
    """
    # Setup default FastAPI instrumentation
    instrumentator = Instrumentator(
        should_group_status_codes=True,
        should_ignore_untemplated=True,
        should_respect_env_var=True,
        should_instrument_requests_inprogress=True,
        excluded_handlers=["/health", "/metrics"],
        env_var_name="ENABLE_METRICS",
        inprogress_name="http_requests_inprogress",
        inprogress_labels=True,
    )
    
    # Add custom metrics
    instrumentator.add(
        metrics.urls_crawled_total
    ).add(
        metrics.crawl_duration_seconds
    )
    
    # Instrument the app
    instrumentator.instrument(app).expose(
        app,
        endpoint="/metrics",
        include_in_schema=True
    )
    
    logger.info("? Prometheus metrics configured at /metrics")
    
    return instrumentator


def get_metrics() -> CrawlerMetrics:
    """Get the global metrics instance."""
    return metrics
