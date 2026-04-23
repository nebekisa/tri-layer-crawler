"""
Periodic tasks for scheduled crawling.
"""

import logging
from datetime import datetime
from src.workers import app
from src.workers.crawl_tasks import crawl_url, process_queue

logger = logging.getLogger(__name__)


@app.task(name='health_check')
def health_check():
    """Periodic health check task."""
    from src.queue.redis_queue import RedisQueue
    
    try:
        queue = RedisQueue()
        stats = queue.get_stats()
        queue.close()
        
        logger.info(f"Health check: Queue pending={stats['pending']}, DLQ={stats['dlq']}")
        
        return {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'queue_stats': stats
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {'status': 'unhealthy', 'error': str(e)}


@app.task(name='scheduled_crawl')
def scheduled_crawl(priority: str = 'normal', max_depth: int = 1):
    """
    Scheduled crawl task.
    
    Args:
        priority: 'high', 'normal', or 'deep'
        max_depth: Maximum crawl depth
    """
    from src.core.config_loader import get_settings
    
    settings = get_settings()
    urls = settings.crawler.start_urls
    
    logger.info(f"Scheduled crawl: priority={priority}, urls={len(urls)}")
    
    results = []
    for url in urls:
        if priority == 'high':
            task = crawl_url.apply_async(args=[url], priority=9)
        else:
            task = crawl_url.delay(url)
        results.append({'url': url, 'task_id': task.id})
    
    return {
        'status': 'scheduled',
        'priority': priority,
        'urls': len(urls),
        'tasks': results,
        'timestamp': datetime.utcnow().isoformat()
    }


@app.task(name='crawl_seed_list')
def crawl_seed_list(seed_file: str = 'config/seeds.txt'):
    """
    Crawl URLs from a seed list file.
    
    Args:
        seed_file: Path to file with URLs (one per line)
    """
    from pathlib import Path
    
    seed_path = Path(seed_file)
    if not seed_path.exists():
        logger.warning(f"Seed file not found: {seed_file}")
        return {'status': 'error', 'reason': 'seed_file_not_found'}
    
    with open(seed_path) as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    logger.info(f"Crawling {len(urls)} URLs from {seed_file}")
    
    from src.workers.crawl_tasks import crawl_batch
    task = crawl_batch.delay(urls)
    
    return {
        'status': 'scheduled',
        'urls': len(urls),
        'batch_task_id': task.id,
        'timestamp': datetime.utcnow().isoformat()
    }


@app.task(name='retry_failed_urls')
def retry_failed_urls():
    """Retry URLs in Dead Letter Queue."""
    from src.queue.redis_queue import RedisQueue
    
    queue = RedisQueue()
    dlq_items = queue.get_dlq_items(limit=50)
    
    retried = 0
    for item in dlq_items:
        if queue.requeue_from_dlq(item['url']):
            retried += 1
    
    queue.close()
    
    logger.info(f"Retried {retried} URLs from DLQ")
    
    return {
        'status': 'complete',
        'retried': retried,
        'timestamp': datetime.utcnow().isoformat()
    }


# Add retry failed URLs to beat schedule (every 4 hours)
app.conf.beat_schedule['retry-failed'] = {
    'task': 'retry_failed_urls',
    'schedule': 14400.0,  # 4 hours in seconds
    'options': {'queue': 'low_priority'},
}
