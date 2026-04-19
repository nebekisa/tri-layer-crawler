"""
Redis-based message queue for URL crawling.
"""

from .redis_queue import RedisQueue, get_redis_client

__all__ = ['RedisQueue', 'get_redis_client']
