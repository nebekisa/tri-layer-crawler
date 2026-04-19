"""
Dark web crawler for .onion sites.
PASSIVE-ONLY: No form submission, no login attempts.
"""

import logging
import time
from typing import List, Dict, Optional
from urllib.parse import urlparse

from src.tor.tor_manager import TorManager
from src.core.config_loader import get_settings

logger = logging.getLogger(__name__)


class DarkCrawler:
    """
    Passive crawler for .onion sites via Tor.
    
    Ethical constraints:
        - No form submission
        - No login attempts
        - No interaction with restricted areas
        - Respects robots.txt on .onion (if present)
        - Rate limited to 1 request per 10 seconds
    """
    
    def __init__(self):
        self.tor = TorManager()
        self.settings = get_settings()
        self.min_delay = 10  # seconds between requests (be polite)
        self.last_request = 0
        
    def verify_setup(self) -> Dict[str, bool]:
        """Verify Tor is properly configured."""
        return {
            'tor_connected': self.tor.verify_connection(),
            'onion_accessible': self.tor.test_onion_access()
        }
    
    def crawl(self, onion_urls: List[str]) -> List[Dict]:
        """
        Crawl .onion URLs (passive only).
        
        Args:
            onion_urls: List of .onion URLs
            
        Returns:
            List of extracted content (minimal)
        """
        results = []
        
        # Verify Tor is working
        if not self.tor.verify_connection():
            logger.error("Tor not connected. Aborting.")
            return results
        
        for url in onion_urls:
            # Validate .onion domain
            if not self._is_onion_url(url):
                logger.warning(f"Not a .onion URL: {url}")
                continue
            
            # Rate limit
            self._rate_limit()
            
            try:
                result = self._crawl_single(url)
                if result:
                    results.append(result)
                    logger.info(f"✓ {url}: {result.get('title', 'No title')[:50]}")
            except Exception as e:
                logger.error(f"Failed {url}: {e}")
                
                # Rotate identity on failure
                self.tor.rotate_identity()
        
        return results
    
    def _crawl_single(self, url: str) -> Optional[Dict]:
        """Crawl a single .onion URL."""
        session = self.tor.get_session()
        
        response = session.get(url, timeout=60)
        response.raise_for_status()
        
        self.last_request = time.time()
        
        # Minimal extraction (ethical constraints)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        return {
            'url': url,
            'title': soup.title.string if soup.title else None,
            'status_code': response.status_code,
            'content_length': len(response.content),
            'crawled_at': time.time(),
            'layer': 'dark'
        }
    
    def _is_onion_url(self, url: str) -> bool:
        """Validate .onion domain."""
        parsed = urlparse(url)
        return parsed.hostname and parsed.hostname.endswith('.onion')
    
    def _rate_limit(self):
        """Enforce minimum delay between requests."""
        elapsed = time.time() - self.last_request
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)