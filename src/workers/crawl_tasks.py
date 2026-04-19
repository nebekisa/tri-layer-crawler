"""
Celery tasks for distributed crawling.
"""

import logging
from src.workers import app

logger = logging.getLogger(__name__)


@app.task(bind=True, name='crawl_url', max_retries=3)
def crawl_url(self, url: str):
    """Crawl a single URL."""
    logger.info(f'[{self.request.hostname}] Crawling: {url}')
    
    try:
        from src.crawlers.concurrent_crawler import ConcurrentCrawler
        
        crawler = ConcurrentCrawler()
        results = crawler.crawl_surface_only([url])
        crawler.close()
        
        if results:
            return {
                'status': 'success',
                'url': url,
                'worker': self.request.hostname,
                'title': results[0].get('title', '')[:100]
            }
        
        return {'status': 'failed', 'url': url}
        
    except Exception as e:
        logger.error(f'Failed {url}: {e}')
        countdown = 2 ** self.request.retries
        raise self.retry(exc=e, countdown=countdown)


@app.task(bind=True, name='crawl_batch')
def crawl_batch(self, urls: list):
    """Crawl multiple URLs."""
    logger.info(f'[{self.request.hostname}] Crawling {len(urls)} URLs')
    
    from src.crawlers.concurrent_crawler import ConcurrentCrawler
    
    crawler = ConcurrentCrawler()
    results = crawler.crawl_surface_only(urls)
    crawler.close()
    
    return {
        'worker': self.request.hostname,
        'total': len(urls),
        'successful': len(results)
    }


@app.task(bind=True, name='process_queue')
def process_queue(self, max_tasks: int = 10):
    """Process URLs from Redis queue."""
    from src.queue.redis_queue import RedisQueue
    
    queue = RedisQueue()
    processed = 0
    
    for _ in range(max_tasks):
        item = queue.pop(timeout=1)
        if item is None:
            break
        
        result = crawl_url.delay(item['url'])
        processed += 1
    
    queue.close()
    
    return {
        'worker': self.request.hostname,
        'processed': processed
    }
