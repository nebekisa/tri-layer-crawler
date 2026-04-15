"""
Thread-safe singleton cache for ML models.
Eliminates redundant model loading.
"""

from threading import Lock
from typing import Dict, Any, Callable
import logging

logger = logging.getLogger(__name__)


class ModelCache:
    """
    Thread-safe singleton cache for ML models.
    
    Usage:
        cache = ModelCache()
        nlp = cache.get_or_load('spacy', lambda: spacy.load('en_core_web_sm'))
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._models = {}
                    cls._instance._load_times = {}
        return cls._instance
    
    def get_or_load(self, key: str, loader: Callable) -> Any:
        """
        Get cached model or load it if not present.
        
        Args:
            key: Unique identifier for the model
            loader: Function that loads the model
            
        Returns:
            The loaded/cached model
        """
        if key not in self._models:
            with self._lock:
                if key not in self._models:
                    import time
                    logger.info(f"Loading model: {key}")
                    start = time.perf_counter()
                    self._models[key] = loader()
                    self._load_times[key] = (time.perf_counter() - start) * 1000
                    logger.info(f"Model {key} loaded in {self._load_times[key]:.0f}ms")
        return self._models[key]
    
    def preload(self, models: Dict[str, Callable]) -> None:
        """
        Preload multiple models.
        
        Args:
            models: Dict of {key: loader_function}
        """
        for key, loader in models.items():
            self.get_or_load(key, loader)
    
    def is_loaded(self, key: str) -> bool:
        """Check if a model is cached."""
        return key in self._models
    
    def get_load_time(self, key: str) -> float:
        """Get model load time in milliseconds."""
        return self._load_times.get(key, 0.0)
    
    def clear(self, key: str = None) -> None:
        """
        Clear cached models.
        
        Args:
            key: Specific model to clear, or None to clear all
        """
        with self._lock:
            if key:
                self._models.pop(key, None)
                self._load_times.pop(key, None)
            else:
                self._models.clear()
                self._load_times.clear()
    
    def stats(self) -> Dict:
        """Get cache statistics."""
        return {
            'cached_models': list(self._models.keys()),
            'load_times_ms': self._load_times,
            'total_models': len(self._models)
        }