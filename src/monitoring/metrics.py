"""
Comprehensive Prometheus metrics for Tri-Layer Crawler.
"""

from prometheus_client import Counter, Gauge, Histogram, Summary, Info, CollectorRegistry
import logging
import psycopg2
from typing import Dict, Any
import time

logger = logging.getLogger(__name__)

registry = CollectorRegistry()

# ============ CRAWLER METRICS ============
crawled_items_total = Gauge('crawler_items_total', 'Total crawled items in database', registry=registry)
crawl_requests_total = Counter('crawler_requests_total', 'Total crawl requests', ['domain', 'status'], registry=registry)
crawl_duration_seconds = Histogram('crawler_duration_seconds', 'Crawl request duration', ['domain'], 
                                   buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60), registry=registry)
crawl_success_rate = Gauge('crawler_success_rate', 'Crawl success rate percentage', registry=registry)
crawl_bytes_total = Counter('crawler_bytes_total', 'Total bytes downloaded', ['domain'], registry=registry)

# ============ QUEUE METRICS ============
queue_pending = Gauge('crawler_queue_pending', 'URLs pending in queue', registry=registry)
queue_processing = Gauge('crawler_queue_processing', 'URLs currently processing', registry=registry)
queue_dlq = Gauge('crawler_queue_dlq', 'URLs in Dead Letter Queue', registry=registry)
queue_wait_seconds = Histogram('crawler_queue_wait_seconds', 'Time URLs spend in queue', registry=registry)

# ============ DATABASE METRICS ============
db_insert_rate = Gauge('crawler_db_insert_rate', 'Database inserts per second', registry=registry)
db_query_duration = Histogram('crawler_db_query_duration', 'Database query duration', 
                              ['query_type'], buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1), registry=registry)
db_total_records = Gauge('crawler_db_total_records', 'Total records in database', registry=registry)

# ============ AI ANALYTICS METRICS ============
sentiment_positive = Gauge('crawler_sentiment_positive', 'Items with positive sentiment', registry=registry)
sentiment_negative = Gauge('crawler_sentiment_negative', 'Items with negative sentiment', registry=registry)
sentiment_neutral = Gauge('crawler_sentiment_neutral', 'Items with neutral sentiment', registry=registry)
sentiment_avg_polarity = Gauge('crawler_sentiment_avg_polarity', 'Average sentiment polarity', registry=registry)

entities_total = Gauge('crawler_entities_total', 'Total entities extracted', registry=registry)
entities_by_type = Gauge('crawler_entities_by_type', 'Entities by type', ['entity_type'], registry=registry)
keywords_total = Gauge('crawler_keywords_total', 'Total keywords extracted', registry=registry)

readability_avg = Gauge('crawler_readability_avg', 'Average readability score', registry=registry)
processing_time_ms = Histogram('crawler_processing_time_ms', 'AI processing time', 
                               buckets=(10, 50, 100, 250, 500, 1000, 2000), registry=registry)

# ============ DOMAIN METRICS ============
domains_crawled = Gauge('crawler_domains_total', 'Total unique domains crawled', registry=registry)
items_per_domain = Gauge('crawler_items_per_domain', 'Items crawled per domain', ['domain'], registry=registry)

# ============ SYSTEM METRICS ============
system_uptime_seconds = Gauge('crawler_uptime_seconds', 'System uptime in seconds', registry=registry)
api_requests_total = Counter('api_requests_total', 'API requests', ['endpoint', 'method', 'status'], registry=registry)
api_latency_seconds = Histogram('api_latency_seconds', 'API request latency', 
                                ['endpoint'], buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5), registry=registry)

build_info = Info('crawler_build', 'Build information', registry=registry)
build_info.info({'version': '5.0.0', 'environment': 'production'})


class MetricsCollector:
    """Collect and update all metrics from database."""
    
    _start_time = time.time()
    _last_insert_count = 0
    _last_insert_time = time.time()
    
    @classmethod
    def get_db_connection(cls):
        return psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
    
    @classmethod
    def update_all(cls):
        """Update all metrics."""
        start = time.time()
        
        try:
            cls._update_database_metrics()
            cls._update_crawler_metrics()
            cls._update_queue_metrics()
            cls._update_ai_metrics()
            cls._update_domain_metrics()
            cls._update_system_metrics()
        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
        
        processing_time_ms.labels().observe((time.time() - start) * 1000)
    
    @classmethod
    def _update_database_metrics(cls):
        """Update database-related metrics."""
        conn = cls.get_db_connection()
        cur = conn.cursor()
        
        # Total records
        cur.execute('SELECT COUNT(*) FROM crawled_items')
        count = cur.fetchone()[0]
        crawled_items_total.set(count)
        db_total_records.set(count)
        
        # Calculate insert rate
        now = time.time()
        elapsed = now - cls._last_insert_time
        if elapsed > 0:
            rate = (count - cls._last_insert_count) / elapsed
            db_insert_rate.set(max(0, rate))
        
        cls._last_insert_count = count
        cls._last_insert_time = now
        
        cur.close()
        conn.close()
    
    @classmethod
    def _update_crawler_metrics(cls):
        """Update crawler performance metrics."""
        conn = cls.get_db_connection()
        cur = conn.cursor()
        
        # Success rate
        cur.execute('''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) as success
            FROM crawled_items
        ''')
        total, success = cur.fetchone()
        if total > 0:
            crawl_success_rate.set((success / total) * 100)
        
        # Request counts by domain
        cur.execute('''
            SELECT domain, COUNT(*) 
            FROM crawled_items 
            GROUP BY domain
        ''')
        for domain, cnt in cur.fetchall():
            crawl_requests_total.labels(domain=domain, status='success').inc(cnt)
        
        cur.close()
        conn.close()
    
    @classmethod
    def _update_queue_metrics(cls):
        """Update queue metrics from Redis."""
        try:
            from src.queue.redis_queue import RedisQueue
            queue = RedisQueue()
            stats = queue.get_stats()
            queue_pending.set(stats.get('pending', 0))
            queue_processing.set(stats.get('processing', 0))
            queue_dlq.set(stats.get('dlq', 0))
            queue.close()
        except Exception as e:
            logger.debug(f"Queue metrics unavailable: {e}")
    
    @classmethod
    def _update_ai_metrics(cls):
        """Update AI analytics metrics."""
        conn = cls.get_db_connection()
        cur = conn.cursor()
        
        # Sentiment distribution
        cur.execute('''
            SELECT 
                COUNT(CASE WHEN sentiment_label = 'positive' THEN 1 END) as pos,
                COUNT(CASE WHEN sentiment_label = 'negative' THEN 1 END) as neg,
                COUNT(CASE WHEN sentiment_label = 'neutral' THEN 1 END) as neu,
                AVG(sentiment_polarity) as avg_pol
            FROM analysis_results
        ''')
        pos, neg, neu, avg_pol = cur.fetchone()
        sentiment_positive.set(pos or 0)
        sentiment_negative.set(neg or 0)
        sentiment_neutral.set(neu or 0)
        sentiment_avg_polarity.set(avg_pol or 0)
        
        # Entity counts
        cur.execute('SELECT COUNT(*) FROM extracted_entities')
        entities_total.set(cur.fetchone()[0] or 0)
        
        cur.execute('''
            SELECT entity_type, COUNT(*) 
            FROM extracted_entities 
            GROUP BY entity_type
        ''')
        for etype, cnt in cur.fetchall():
            entities_by_type.labels(entity_type=etype).set(cnt)
        
        # Keywords
        cur.execute('SELECT COUNT(*) FROM extracted_keywords')
        keywords_total.set(cur.fetchone()[0] or 0)
        
        # Readability
        cur.execute('SELECT AVG(flesch_kincaid_grade) FROM analysis_results')
        avg_read = cur.fetchone()[0]
        readability_avg.set(avg_read or 0)
        
        cur.close()
        conn.close()
    
    @classmethod
    def _update_domain_metrics(cls):
        """Update domain statistics."""
        conn = cls.get_db_connection()
        cur = conn.cursor()
        
        cur.execute('SELECT COUNT(DISTINCT domain) FROM crawled_items')
        domains_crawled.set(cur.fetchone()[0] or 0)
        
        cur.execute('SELECT domain, COUNT(*) FROM crawled_items GROUP BY domain')
        for domain, cnt in cur.fetchall():
            items_per_domain.labels(domain=domain).set(cnt)
        
        cur.close()
        conn.close()
    
    @classmethod
    def _update_system_metrics(cls):
        """Update system metrics."""
        system_uptime_seconds.set(time.time() - cls._start_time)


def get_registry():
    return registry
