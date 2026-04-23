"""
Redis-backed message queue for distributed crawling.
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
    """Factory function to create Redis client."""
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
        - Visited Set: visited_urls (SET) - URLs successfully crawled
        - Retry Counters: retry:{url} (STRING with TTL) - Track retry attempts
    """
    
    QUEUE_KEY = "crawler:pending_urls"
    PROCESSING_KEY = "crawler:processing_urls"
    DLQ_KEY = "crawler:dlq_urls"
    SEEN_KEY = "crawler:seen_urls"
    VISITED_KEY = "crawler:visited_urls"
    RETRY_PREFIX = "crawler:retry:"
    URL_DATA_PREFIX = "crawler:urldata:"
    
    def __init__(self, client: Optional[redis.Redis] = None, max_retries: int = 3):
        self.client = client or get_redis_client()
        self.max_retries = max_retries
        
        try:
            self.client.ping()
            logger.info(f"RedisQueue connected")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def push(self, url: str, priority: int = 0, metadata: Optional[Dict] = None) -> bool:
        """Push URL onto queue."""
        if self.client.sismember(self.SEEN_KEY, url):
            return False
        
        self.client.sadd(self.SEEN_KEY, url)
        
        if metadata:
            self.client.setex(
                f"{self.URL_DATA_PREFIX}{url}",
                86400 * 7,
                json.dumps(metadata)
            )
        
        queue_item = json.dumps({
            'url': url,
            'priority': priority,
            'added_at': datetime.utcnow().isoformat(),
            'metadata': metadata or {}
        })
        
        if priority > 0:
            self.client.lpush(self.QUEUE_KEY, queue_item)
        else:
            self.client.rpush(self.QUEUE_KEY, queue_item)
        
        return True
    
    def push_batch(self, urls: List[str], priority: int = 0) -> int:
        """Push multiple URLs."""
        added = 0
        for url in urls:
            if self.push(url, priority):
                added += 1
        return added
    
    def pop(self, timeout: int = 0) -> Optional[Dict[str, Any]]:
        """Pop URL from queue."""
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
            self.client.sadd(self.PROCESSING_KEY, url)
            return item
        except json.JSONDecodeError:
            return None
    
    def mark_complete(self, url: str, success: bool = True):
        """Mark URL as completed."""
        self.client.srem(self.PROCESSING_KEY, url)
        
        if success:
            self.client.sadd(self.VISITED_KEY, url)
            self.client.delete(f"{self.RETRY_PREFIX}{url}")
        else:
            retry_key = f"{self.RETRY_PREFIX}{url}"
            retries = self.client.incr(retry_key)
            self.client.expire(retry_key, 3600)
            
            if retries >= self.max_retries:
                self._move_to_dlq(url, f"Max retries ({retries}) exceeded")
            else:
                self.push(url, priority=-1)
    
    def is_visited(self, url: str) -> bool:
        """Check if URL has been successfully crawled."""
        return self.client.sismember(self.VISITED_KEY, url)
    
    def mark_visited(self, url: str) -> None:
        """Mark URL as successfully crawled."""
        self.client.sadd(self.VISITED_KEY, url)
    
    def get_visited_count(self) -> int:
        """Get count of successfully crawled URLs."""
        return self.client.scard(self.VISITED_KEY)
    
    def _move_to_dlq(self, url: str, reason: str):
        """Move failed URL to Dead Letter Queue."""
        dlq_item = json.dumps({
            'url': url,
            'failed_at': datetime.utcnow().isoformat(),
            'reason': reason
        })
        self.client.rpush(self.DLQ_KEY, dlq_item)
        self.client.srem(self.PROCESSING_KEY, url)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            'pending': self.client.llen(self.QUEUE_KEY),
            'processing': self.client.scard(self.PROCESSING_KEY),
            'dlq': self.client.llen(self.DLQ_KEY),
            'total_seen': self.client.scard(self.SEEN_KEY),
            'visited': self.client.scard(self.VISITED_KEY)
        }
    
    def clear(self):
        """Clear all queues."""
        self.client.delete(self.QUEUE_KEY)
        self.client.delete(self.PROCESSING_KEY)
        self.client.delete(self.DLQ_KEY)
        self.client.delete(self.SEEN_KEY)
        self.client.delete(self.VISITED_KEY)
        logger.warning("All queues cleared")
    
    def close(self):
        """Close Redis connection."""
        self.client.close()
