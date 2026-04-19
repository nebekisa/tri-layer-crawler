"""
Queue-based crawler that consumes URLs from Redis.
"""

import logging
from typing import Optional, List, Dict, Any
from src.queue import RedisQueue
from src.crawlers.concurrent_crawler import ConcurrentCrawler

logger = logging.getLogger(__name__)


class QueueCrawler:
    """
    Crawler that pulls URLs from Redis queue and processes them.
    """
    
    def __init__(self, max_workers: int = 4):
        self.queue = RedisQueue()
        self.max_workers = max_workers
        self.running = False
        self.results: List[Dict[str, Any]] = []
    
    def add_seed_urls(self, urls: list) -> int:
        """Add initial seed URLs to queue."""
        added = self.queue.push_batch(urls)
        logger.info(f"Added {added} seed URLs to queue")
        return added
    
    def crawl_from_queue(self, max_urls: Optional[int] = None):
        """
        Continuously crawl URLs from queue.
        
        Args:
            max_urls: Maximum URLs to crawl (None = unlimited).
        """
        self.running = True
        crawled = 0
        
        logger.info("Starting queue-based crawler...")
        
        # Create a single crawler instance for all URLs
        crawler = ConcurrentCrawler()
        
        while self.running:
            # Check limit
            if max_urls and crawled >= max_urls:
                logger.info(f"Reached max URLs limit: {max_urls}")
                break
            
            # Pop URL with 5 second timeout
            item = self.queue.pop(timeout=5)
            
            if item is None:
                logger.debug("Queue empty, waiting...")
                continue
            
            url = item['url']
            
            try:
                # Use the public crawl method with a single URL
                # Temporarily override start_urls
                original_urls = crawler.start_urls
                crawler.start_urls = [url]
                
                # Crawl the single URL
                results = crawler.crawl()
                
                # Restore original URLs
                crawler.start_urls = original_urls
                
                if results:
                    self.results.extend(results)
                    self.queue.mark_complete(url, success=True)
                    crawled += 1
                    logger.info(f"[{crawled}] Crawled: {url} - {results[0].get('title', 'No title')[:50]}")
                else:
                    self.queue.mark_complete(url, success=False)
                    logger.warning(f"Failed: {url}")
                    
            except Exception as e:
                logger.error(f"Error crawling {url}: {e}")
                self.queue.mark_complete(url, success=False)
        
        crawler.close()
        logger.info(f"Crawl complete. Processed {crawled} URLs, {len(self.results)} successful")
        stats = self.queue.get_stats()
        logger.info(f"Queue stats: Pending={stats['pending']}, DLQ={stats['dlq']}")
        
        return self.results
    
    def stop(self):
        """Stop the crawler."""
        self.running = False
        self.queue.close()
    
    def get_results(self) -> List[Dict[str, Any]]:
        """Get all successfully crawled results."""
        return self.results
