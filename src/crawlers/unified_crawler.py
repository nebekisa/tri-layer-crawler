"""
Unified crawler that intelligently selects surface or deep crawling.
"""

import logging
import asyncio
from typing import List, Dict, Optional
from enum import Enum

from src.crawlers.concurrent_crawler import ConcurrentCrawler
from src.crawlers.deep_crawler import DeepCrawler
from src.core.config_loader import get_settings

logger = logging.getLogger(__name__)


class CrawlMethod(str, Enum):
    """Crawling method selection."""
    AUTO = "auto"           # Auto-detect based on URL patterns
    SURFACE = "surface"     # Force requests/BeautifulSoup
    DEEP = "deep"           # Force Playwright


class UnifiedCrawler:
    """
    Intelligent crawler that routes to appropriate method.
    """
    
    JS_PATTERNS = [
        '/js/', '/javascript/', '/spa/', 
        '/react/', '/vue/', '/angular/',
        '?_escaped_fragment_=', '#!',
        'toscrape.com/js/',
    ]
    
    def __init__(self, method: CrawlMethod = CrawlMethod.AUTO):
        self.method = method
        self.surface_crawler = ConcurrentCrawler()
        self.deep_crawler = DeepCrawler()
        logger.info(f"UnifiedCrawler ready (method={method})")
    
    async def crawl_async(
        self,
        urls: List[str],
        take_screenshots: bool = True,
        extract_selectors: Optional[Dict[str, str]] = None
    ) -> List[Dict]:
        """
        Async crawl using appropriate method.
        """
        surface_urls = []
        deep_urls = []
        
        for url in urls:
            if self._should_use_deep(url):
                deep_urls.append(url)
            else:
                surface_urls.append(url)
        
        logger.info(f"Classified {len(surface_urls)} surface, {len(deep_urls)} deep URLs")
        
        results = []
        
        # Surface crawl (synchronous, run in thread pool)
        if surface_urls:
            logger.info(f"Starting surface crawl of {len(surface_urls)} URLs")
            loop = asyncio.get_event_loop()
            surface_results = await loop.run_in_executor(
                None, 
                self._crawl_surface_sync, 
                surface_urls
            )
            for r in surface_results:
                r['crawl_method'] = 'surface'
            results.extend(surface_results)
        
        # Deep crawl (async)
        if deep_urls:
            logger.info(f"Starting deep crawl of {len(deep_urls)} URLs")
            deep_results = await self.deep_crawler.crawl(
                urls=deep_urls,
                take_screenshot=take_screenshots,
                extract_selectors=extract_selectors
            )
            for r in deep_results:
                r['crawl_method'] = 'deep'
            results.extend(deep_results)
        
        logger.info(f"Unified crawl complete: {len(results)} total items")
        return results
    
    def _crawl_surface_sync(self, urls: List[str]) -> List[Dict]:
        """Synchronous surface crawl for executor."""
        return self.surface_crawler.crawl_surface_only(urls)
    
    def crawl(
        self,
        urls: List[str],
        take_screenshots: bool = True,
        extract_selectors: Optional[Dict[str, str]] = None
    ) -> List[Dict]:
        """
        Synchronous wrapper for crawl_async.
        """
        try:
            # Check if we're already in an event loop
            loop = asyncio.get_running_loop()
            # We're in async context - can't run sync
            raise RuntimeError(
                "crawl() called from async context. Use crawl_async() instead."
            )
        except RuntimeError:
            # No running loop - safe to run
            return asyncio.run(
                self.crawl_async(urls, take_screenshots, extract_selectors)
            )
    
    def _should_use_deep(self, url: str) -> bool:
        """Determine if URL requires deep crawling."""
        if self.method == CrawlMethod.DEEP:
            return True
        if self.method == CrawlMethod.SURFACE:
            return False
        
        url_lower = url.lower()
        return any(pattern.lower() in url_lower for pattern in self.JS_PATTERNS)