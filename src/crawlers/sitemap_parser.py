"""
Sitemap.xml parser for URL discovery.

Supports:
    - Standard sitemap.xml
    - Sitemap index files
    - Gzipped sitemaps
    - lastmod filtering
    - Priority-based ordering
"""

import logging
import gzip
import time
from typing import List, Dict, Optional, Iterator
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.monitoring.metrics import get_metrics

logger = logging.getLogger(__name__)


class SitemapParser:
    """
    Parse sitemap.xml files and extract URLs for crawling.
    """
    
    NAMESPACES = {
        'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
        'image': 'http://www.google.com/schemas/sitemap-image/1.1',
        'news': 'http://www.google.com/schemas/sitemap-news/0.9',
    }
    
    def __init__(self, user_agent: str = "CerberusCrawler/1.0"):
        self.user_agent = user_agent
        self.session = self._create_session()
        self.metrics = get_metrics()
    
    def _create_session(self) -> requests.Session:
        """Create a session with retry logic."""
        session = requests.Session()
        session.headers.update({'User-Agent': self.user_agent})
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def discover_sitemap_url(self, domain: str) -> Optional[str]:
        """
        Try to discover sitemap location from common paths.
        
        Args:
            domain: Base domain (e.g., https://example.com)
            
        Returns:
            Sitemap URL if found, None otherwise.
        """
        common_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap-index.xml',
            '/sitemap/sitemap.xml',
            '/sitemap.php',
            '/sitemap.txt',
        ]
        
        for path in common_paths:
            url = urljoin(domain, path)
            try:
                response = self.session.head(url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"Discovered sitemap at: {url}")
                    return url
            except Exception:
                continue
        
        logger.warning(f"No sitemap discovered for {domain}")
        return None
    
    def parse_sitemap(self, sitemap_url: str, 
                      max_age_days: Optional[int] = None,
                      min_priority: Optional[float] = None) -> List[Dict]:
        """
        Parse a sitemap and extract URLs.
        
        Args:
            sitemap_url: URL to sitemap.xml
            max_age_days: Only include URLs updated within N days
            min_priority: Only include URLs with priority >= this value
            
        Returns:
            List of dicts with 'url', 'lastmod', 'priority', 'changefreq'
        """
        urls = []
        start_time = time.time()
        
        try:
            content = self._fetch_sitemap(sitemap_url)
            root = ET.fromstring(content)
            
            # Check if it's a sitemap index
            sitemap_tags = root.findall('.//sm:sitemap', self.NAMESPACES)
            if sitemap_tags:
                logger.info(f"Found sitemap index with {len(sitemap_tags)} sitemaps")
                for sitemap in sitemap_tags:
                    loc = sitemap.find('sm:loc', self.NAMESPACES)
                    if loc is not None and loc.text:
                        # Parse nested sitemap
                        urls.extend(self.parse_sitemap(
                            loc.text, max_age_days, min_priority
                        ))
            else:
                # Parse URL entries
                urls = self._parse_url_entries(
                    root, sitemap_url, max_age_days, min_priority
                )
                
        except Exception as e:
            logger.error(f"Failed to parse sitemap {sitemap_url}: {e}")
            self.metrics.record_crawl_failure(
                domain=urlparse(sitemap_url).netloc,
                error=str(e)
            )
        
        duration = time.time() - start_time
        logger.info(f"Extracted {len(urls)} URLs from {sitemap_url} in {duration:.2f}s")
        
        return urls
    
    def _fetch_sitemap(self, url: str) -> bytes:
        """Fetch sitemap content, handling gzip."""
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        
        content = response.content
        
        # Handle gzipped sitemaps
        if url.endswith('.gz') or response.headers.get('Content-Encoding') == 'gzip':
            content = gzip.decompress(content)
        
        return content
    
    def _parse_url_entries(self, root: ET.Element, base_url: str,
                           max_age_days: Optional[int],
                           min_priority: Optional[float]) -> List[Dict]:
        """Parse URL entries from sitemap."""
        urls = []
        cutoff_date = None
        
        if max_age_days:
            cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        for url_elem in root.findall('.//sm:url', self.NAMESPACES):
            loc = url_elem.find('sm:loc', self.NAMESPACES)
            if loc is None or not loc.text:
                continue
            
            url = loc.text.strip()
            
            # Extract lastmod
            lastmod_elem = url_elem.find('sm:lastmod', self.NAMESPACES)
            lastmod = None
            if lastmod_elem is not None and lastmod_elem.text:
                try:
                    lastmod = datetime.fromisoformat(
                        lastmod_elem.text.replace('Z', '+00:00')
                    )
                except ValueError:
                    pass
            
            # Filter by age
            if cutoff_date and lastmod and lastmod < cutoff_date:
                continue
            
            # Extract priority
            priority_elem = url_elem.find('sm:priority', self.NAMESPACES)
            priority = None
            if priority_elem is not None and priority_elem.text:
                try:
                    priority = float(priority_elem.text)
                except ValueError:
                    pass
            
            # Filter by priority
            if min_priority and priority and priority < min_priority:
                continue
            
            # Extract changefreq
            changefreq_elem = url_elem.find('sm:changefreq', self.NAMESPACES)
            changefreq = changefreq_elem.text if changefreq_elem is not None else None
            
            urls.append({
                'url': url,
                'lastmod': lastmod.isoformat() if lastmod else None,
                'priority': priority,
                'changefreq': changefreq,
                'source': base_url,
                'discovered_at': datetime.utcnow().isoformat() + 'Z'
            })
            
            self.metrics.record_crawl_success(
                domain=urlparse(url).netloc,
                duration=0,
                bytes_downloaded=0
            )
        
        # Sort by priority (highest first) then by lastmod (newest first)
        urls.sort(
            key=lambda x: (
                -(x['priority'] or 0.5),
                x['lastmod'] or '1970-01-01'
            ),
            reverse=True
        )
        
        return urls
    
    def discover_and_parse(self, domain: str, **kwargs) -> List[Dict]:
        """
        Discover sitemap for a domain and parse it.
        
        Args:
            domain: Domain to discover sitemap for
            **kwargs: Additional arguments for parse_sitemap
            
        Returns:
            List of extracted URLs
        """
        sitemap_url = self.discover_sitemap_url(domain)
        if sitemap_url:
            return self.parse_sitemap(sitemap_url, **kwargs)
        return []
    
    def push_to_queue(self, urls: List[Dict], queue) -> int:
        """
        Push discovered URLs to Redis queue.
        
        Args:
            urls: List of URL dicts from parse_sitemap
            queue: RedisQueue instance
            
        Returns:
            Number of URLs pushed
        """
        pushed = 0
        for item in urls:
            priority = int((item.get('priority') or 0.5) * 10)
            metadata = {
                'lastmod': item.get('lastmod'),
                'changefreq': item.get('changefreq'),
                'source': 'sitemap'
            }
            
            if queue.push(item['url'], priority=priority, metadata=metadata):
                pushed += 1
        
        logger.info(f"Pushed {pushed} URLs to queue from sitemap")
        return pushed
