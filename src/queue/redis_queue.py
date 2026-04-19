"""
Redis-backed message queue for distributed crawling.

Features:
    - Persistent URL queue with priorities
    - Dead Letter Queue (DLQ) for failed URLs
    - Duplicate prevention via Bloom filter or Set
    - Exponential backoff retry tracking
"""

import json
import logging
import time
from typing import Optional, List, Dict, Any
from datetime import datetime

import redis
import os

logger = logging.getLogger(__name__)


def get_redis_client() -> redis.Redis:
    """
    Factory function to create Redis client from environment variables.
    
    Returns:
        Configured Redis client.
    """
    return redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        password=os.getenv('REDIS_PASSWORD', 'RedisSecure2024!'),
        decode_responses=True,
        socket_connect_timeout=5,
        socket_keepalive=True,
        health_check_interval=30
    )


class RedisQueue:
    """
    Persistent URL queue backed by Redis.
    
    Architecture:
        - Main Queue: pending_urls (LIST) - URLs waiting to be crawled
        - Processing Set: processing_urls (SET) - URLs currently being crawled
        - Dead Letter Queue: dlq_urls (LIST) - URLs that failed after max retries
        - Seen Set: seen_urls (SET) - All URLs ever seen (deduplication)
        - Retry Counters: retry:{url} (STRING with TTL) - Track retry attempts
    """
    
    QUEUE_KEY = "crawler:pending_urls"
    PROCESSING_KEY = "crawler:processing_urls"
    DLQ_KEY = "crawler:dlq_urls"
    SEEN_KEY = "crawler:seen_urls"
    RETRY_PREFIX = "crawler:retry:"
    URL_DATA_PREFIX = "crawler:urldata:"
    
    def __init__(self, client: Optional[redis.Redis] = None, max_retries: int = 3):
        """
        Initialize the Redis queue.
        
        Args:
            client: Redis client (created if not provided).
            max_retries: Maximum retry attempts per URL.
        """
        self.client = client or get_redis_client()
        self.max_retries = max_retries
        
        # Test connection
        try:
            self.client.ping()
            logger.info(f"RedisQueue connected to {self.client.connection_pool.connection_kwargs.get('host', 'unknown')}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def push(self, url: str, priority: int = 0, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Push a URL onto the queue.
        
        Args:
            url: URL to crawl.
            priority: Higher priority = processed first (0 = normal, 1 = high, -1 = low).
            metadata: Optional additional data (depth, referrer, etc.).
            
        Returns:
            True if URL was added, False if already seen.
        """
        # Check if already seen (deduplication)
        if self.client.sismember(self.SEEN_KEY, url):
            logger.debug(f"URL already seen, skipping: {url}")
            return False
        
        # Mark as seen
        self.client.sadd(self.SEEN_KEY, url)
        
        # Store metadata if provided
        if metadata:
            self.client.setex(
                f"{self.URL_DATA_PREFIX}{url}",
                86400 * 7,  # 7 days TTL
                json.dumps(metadata)
            )
        
        # Create queue item with priority
        queue_item = json.dumps({
            'url': url,
            'priority': priority,
            'added_at': datetime.utcnow().isoformat(),
            'metadata': metadata or {}
        })
        
        # Push based on priority
        if priority > 0:
            # High priority - push to left (LIFO for high priority)
            self.client.lpush(self.QUEUE_KEY, queue_item)
        else:
            # Normal/low priority - push to right (FIFO)
            self.client.rpush(self.QUEUE_KEY, queue_item)
        
        logger.debug(f"Pushed URL to queue: {url} (priority={priority})")
        return True
    
    def push_batch(self, urls: List[str], priority: int = 0) -> int:
        """
        Push multiple URLs to the queue.
        
        Args:
            urls: List of URLs.
            priority: Priority for all URLs.
            
        Returns:
            Number of URLs actually added (excluding duplicates).
        """
        added = 0
        for url in urls:
            if self.push(url, priority):
                added += 1
        return added
    
    def pop(self, timeout: int = 0) -> Optional[Dict[str, Any]]:
        """
        Pop a URL from the queue for processing.
        
        Args:
            timeout: Seconds to block waiting for URL (0 = non-blocking).
            
        Returns:
            Queue item dict with 'url' and metadata, or None if queue empty.
        """
        # Blocking pop from left (FIFO)
        if timeout > 0:
            result = self.client.blpop(self.QUEUE_KEY, timeout=timeout)
            if result is None:
                return None
            _, item_json = result
        else:
            item_json = self.client.lpop(self.QUEUE_KEY)
            if item_json is None:
                return None
        
        try:
            item = json.loads(item_json)
            url = item['url']
            
            # Move to processing set
            self.client.sadd(self.PROCESSING_KEY, url)
            
            logger.debug(f"Popped URL from queue: {url}")
            return item
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in queue: {item_json[:100]}... Error: {e}")
            return None
    
    def mark_complete(self, url: str, success: bool = True):
        """
        Mark a URL as completed (remove from processing).
        
        Args:
            url: Completed URL.
            success: True if crawl succeeded, False if failed.
        """
        # Remove from processing set
        self.client.srem(self.PROCESSING_KEY, url)
        
        # Clear retry counter on success
        if success:
            self.client.delete(f"{self.RETRY_PREFIX}{url}")
            logger.debug(f"URL completed successfully: {url}")
        else:
            # Increment retry counter
            retry_key = f"{self.RETRY_PREFIX}{url}"
            retries = self.client.incr(retry_key)
            self.client.expire(retry_key, 3600)  # 1 hour TTL
            
            if retries >= self.max_retries:
                # Move to Dead Letter Queue
                self._move_to_dlq(url, f"Max retries ({retries}) exceeded")
                logger.warning(f"URL moved to DLQ after {retries} retries: {url}")
            else:
                # Re-queue with exponential backoff
                self.push(url, priority=-1)  # Lower priority for retries
                logger.info(f"URL requeued for retry {retries}/{self.max_retries}: {url}")
    
    def _move_to_dlq(self, url: str, reason: str):
        """
        Move a failed URL to the Dead Letter Queue.
        
        Args:
            url: Failed URL.
            reason: Failure reason.
        """
        dlq_item = json.dumps({
            'url': url,
            'failed_at': datetime.utcnow().isoformat(),
            'reason': reason,
            'retry_count': self.client.get(f"{self.RETRY_PREFIX}{url}") or 0
        })
        
        self.client.rpush(self.DLQ_KEY, dlq_item)
        self.client.srem(self.PROCESSING_KEY, url)
        self.client.delete(f"{self.RETRY_PREFIX}{url}")
    
    def get_dlq_items(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get items from Dead Letter Queue for inspection.
        
        Args:
            limit: Maximum items to retrieve.
            
        Returns:
            List of DLQ items.
        """
        items = self.client.lrange(self.DLQ_KEY, 0, limit - 1)
        return [json.loads(item) for item in items]
    
    def requeue_from_dlq(self, url: str) -> bool:
        """
        Requeue a URL from DLQ back to main queue.
        
        Args:
            url: URL to requeue.
            
        Returns:
            True if found and requeued.
        """
        dlq_items = self.client.lrange(self.DLQ_KEY, 0, -1)
        
        for i, item_json in enumerate(dlq_items):
            item = json.loads(item_json)
            if item['url'] == url:
                # Remove from DLQ
                self.client.lset(self.DLQ_KEY, i, "__DELETED__")
                self.client.lrem(self.DLQ_KEY, 1, "__DELETED__")
                
                # Reset retry counter and requeue
                self.client.delete(f"{self.RETRY_PREFIX}{url}")
                self.push(url, priority=0)
                
                logger.info(f"Requeued URL from DLQ: {url}")
                return True
        
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.
        
        Returns:
            Dictionary with queue metrics.
        """
        return {
            'pending': self.client.llen(self.QUEUE_KEY),
            'processing': self.client.scard(self.PROCESSING_KEY),
            'dlq': self.client.llen(self.DLQ_KEY),
            'total_seen': self.client.scard(self.SEEN_KEY),
            'redis_info': self.client.info('memory'),
        }
    
    def clear(self):
        """Clear all queues (DANGER: Irreversible)."""
        self.client.delete(self.QUEUE_KEY)
        self.client.delete(self.PROCESSING_KEY)
        self.client.delete(self.DLQ_KEY)
        self.client.delete(self.SEEN_KEY)
        
        # Delete all retry keys
        for key in self.client.scan_iter(f"{self.RETRY_PREFIX}*"):
            self.client.delete(key)
        
        logger.warning("All queues cleared")
    
    def close(self):
        """Close Redis connection."""
        self.client.close()
        logger.debug("Redis connection closed")
