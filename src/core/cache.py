"""
Simple caching layer for frequently accessed data.
"""

import time
from typing import Any, Optional, Dict, Callable
from functools import wraps
from threading import Lock

logger = logging.getLogger(__name__)


class TTLCache:
    """
    Thread-safe TTL cache.
    
    Features:
        - Time-based expiration
        - Max size limit
        - Thread-safe operations
    """
    
    def __init__(self, max_size: int = 100, default_ttl: int = 300):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, tuple] = {}  # key: (value, expiry_time)
        self._lock = Lock()
        self._hits = 0
        self._misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if time.time() < expiry:
                    self._hits += 1
                    return value
                else:
                    del self._cache[key]
            
            self._misses += 1
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with TTL."""
        with self._lock:
            # Evict oldest if at max size
            if len(self._cache) >= self.max_size:
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
            
            expiry = time.time() + (ttl or self.default_ttl)
            self._cache[key] = (value, expiry)
    
    def delete(self, key: str) -> None:
        """Remove key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
    
    def clear(self) -> None:
        """Clear all cached items."""
        with self._lock:
            self._cache.clear()
    
    @property
    def hit_rate(self) -> float:
        """Cache hit rate."""
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            'size': len(self._cache),
            'max_size': self.max_size,
            'hits': self._hits,
            'misses': self._misses,
            'hit_rate': self.hit_rate
        }


# Global cache instances
analytics_cache = TTLCache(max_size=50, default_ttl=3600)  # 1 hour
entity_cache = TTLCache(max_size=200, default_ttl=86400)   # 24 hours


def cached(cache: TTLCache, ttl: Optional[int] = None):
    """Decorator for caching function results."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # Try cache
            result = cache.get(key)
            if result is not None:
                return result
            
            # Call function and cache
            result = func(*args, **kwargs)
            cache.set(key, result, ttl)
            return result
        
        return wrapper
    return decorator