"""
Expanding web crawler with seed management and link discovery.
"""

import logging
import time
import requests
from typing import Dict, List, Optional, Set, Any
from datetime import datetime
from urllib.parse import urlparse

from src.core.config_loader import get_settings
from src.crawlers.concurrent_crawler import ConcurrentCrawler
from src.crawlers.link_extractor import LinkExtractor
from src.crawlers.seed_manager import SeedManager
from src.queue.redis_queue import RedisQueue

logger = logging.getLogger(__name__)


class ExpandingCrawler:
    """Real web crawler with dynamic seed management."""
    
    def __init__(self, max_depth: int = 3, max_urls: int = 100, same_domain_only: bool = False):
        """
        Initialize expanding crawler.
        
        Args:
            max_depth: Maximum crawl depth
            max_urls: Maximum URLs to crawl
            same_domain_only: Only crawl same domain as seeds (False for multi-domain)
        """
        self.max_depth = max_depth
        self.max_urls = max_urls
        self.same_domain_only = same_domain_only
        self.extract_links_enabled = True
        
        # Initialize seed manager
        settings = get_settings()
        seed_config = getattr(settings.crawler, 'seed_config', {})
        
        self.seed_manager = SeedManager(
            seed_file=getattr(seed_config, 'seed_file', 'config/seeds.txt') if hasattr(seed_config, 'seed_file') else 'config/seeds.txt',
            max_seeds=getattr(seed_config, 'max_seeds', 50) if hasattr(seed_config, 'max_seeds') else 50,
            allowed_domains=getattr(seed_config, 'allowed_domains', None) if hasattr(seed_config, 'allowed_domains') else None,
            exclude_patterns=getattr(seed_config, 'exclude_patterns', []) if hasattr(seed_config, 'exclude_patterns') else []
        )
        
        self.crawler = ConcurrentCrawler()
        self.queue = RedisQueue()
        self.link_extractor = LinkExtractor(
            same_domain_only=False,  # Allow cross-domain when seeds are multi-domain
            respect_nofollow=False,
            allowed_extensions=None,
            excluded_patterns=['mailto:', 'javascript:', 'tel:']
        )
        
        self.visited_urls: Set[str] = set()
        self.crawled_count = 0
        self.discovered_count = 0
        self.results: List[Dict] = []
        
        logger.info(f"ExpandingCrawler ready: max_depth={self.max_depth}, max_urls={self.max_urls}")
    
    def _fetch_html(self, url: str) -> Optional[str]:
        """Fetch raw HTML directly."""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Failed to fetch HTML for {url}: {e}")
            return None
    
    def crawl(self, seed_urls: Optional[List[str]] = None) -> List[Dict]:
        """
        Start expanding crawl with dynamic seed loading.
        
        Args:
            seed_urls: Optional override seed URLs
            
        Returns:
            List of crawled items
        """
        # Load seeds from seed manager
        if seed_urls:
            seeds = seed_urls
        else:
            sources = ['file', 'config']
            settings = get_settings()
            if hasattr(settings.crawler, 'seed_config'):
                if getattr(settings.crawler.seed_config, 'load_from_db', False):
                    sources.append('db')
            
            seeds = self.seed_manager.load_seeds(sources)
        
        if not seeds:
            logger.error("No seeds loaded!")
            return []
        
        logger.info(f"Starting crawl with {len(seeds)} seed URLs")
        logger.info(f"Domain distribution: {self.seed_manager.get_domain_distribution()}")
        
        # Clear queue and add seeds
        self.queue.clear()
        for url in seeds:
            normalized = self.link_extractor.normalize_url(url)
            if normalized:
                self.queue.push(normalized, priority=10, metadata={'depth': 1, 'is_seed': True})
                self.discovered_count += 1
        
        # Track domains being crawled
        crawled_domains: Dict[str, int] = {}
        
        while self.crawled_count < self.max_urls:
            item = self.queue.pop(timeout=5)
            if item is None:
                logger.info("Queue empty - crawl complete")
                break
            
            url = item['url']
            metadata = item.get('metadata', {})
            depth = metadata.get('depth', 1) if isinstance(metadata, dict) else 1
            domain = urlparse(url).netloc
            
            if url in self.visited_urls:
                continue
            
            if depth > self.max_depth:
                continue
            
            # Domain balancing - log distribution
            crawled_domains[domain] = crawled_domains.get(domain, 0) + 1
            
            logger.info(f"[{self.crawled_count + 1}/{self.max_urls}] Depth {depth}: {url}")
            
            try:
                results = self.crawler.crawl_surface_only([url])
                result = results[0] if results else None
                raw_html = self._fetch_html(url)
                
                if result:
                    result['crawl_depth'] = depth
                    self.results.append(result)
                    self.visited_urls.add(url)
                    self.crawled_count += 1
                    
                    if self.extract_links_enabled and depth < self.max_depth and raw_html:
                        links = self.link_extractor.extract_links(raw_html, url)
                        new_depth = depth + 1
                        queued = 0
                        for link in links:
                            if link not in self.visited_urls:
                                self.queue.push(link, priority=10 - new_depth, 
                                               metadata={'depth': new_depth})
                                self.discovered_count += 1
                                queued += 1
                        if queued > 0:
                            logger.info(f"  ?? Discovered {queued} new links at depth {new_depth}")
                    
                    self.queue.mark_complete(url, success=True)
                    title = result.get('title', 'No title')[:50]
                    logger.info(f"  ? Crawled: {title}")
                else:
                    self.queue.mark_complete(url, success=False)
                    self.seed_manager.mark_seed_failed(url)
                    logger.warning(f"  ? Failed: {url}")
                    
            except Exception as e:
                logger.error(f"Error crawling {url}: {e}")
                self.queue.mark_complete(url, success=False)
                self.seed_manager.mark_seed_failed(url)
            
            time.sleep(self.crawler.download_delay)
        
        logger.info(f"Crawl complete: {self.crawled_count} crawled, {self.discovered_count} discovered")
        logger.info(f"Domain crawl distribution: {crawled_domains}")
        
        return self.results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get crawl statistics including seed info."""
        return {
            'crawled_count': self.crawled_count,
            'discovered_count': self.discovered_count,
            'visited_count': len(self.visited_urls),
            'max_depth': self.max_depth,
            'max_urls': self.max_urls,
            'seed_stats': self.seed_manager.get_seed_stats(),
            'queue_stats': self.queue.get_stats()
        }
    
    def close(self):
        """Clean up resources."""
        self.crawler.close()
        self.queue.close()
