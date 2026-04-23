"""
Link extraction and normalization for expanding crawler.
Bulletproof version with extensive debugging.
"""

import re
import logging
from urllib.parse import urljoin, urlparse, urlunparse
from typing import List, Set, Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class LinkExtractor:
    """Extract and normalize links from HTML pages."""
    
    def __init__(self, 
                 same_domain_only: bool = True,
                 respect_nofollow: bool = False,
                 allowed_extensions: Optional[List[str]] = None,
                 excluded_patterns: Optional[List[str]] = None):
        
        self.same_domain_only = same_domain_only
        self.respect_nofollow = respect_nofollow
        self.allowed_extensions = allowed_extensions
        self.excluded_patterns = excluded_patterns or ['mailto:', 'javascript:', 'tel:']
    
    def extract_links(self, html: str, base_url: str) -> List[str]:
        """Extract all valid links from HTML."""
        
        if not html:
            logger.warning(f"Empty HTML for {base_url}")
            return []
        
        # Parse HTML
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find all links
        links: Set[str] = set()
        base_domain = urlparse(base_url).netloc
        
        # Method 1: Find all <a> tags
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '').strip()
            if href:
                absolute = urljoin(base_url, href)
                normalized = self._validate_and_normalize(absolute, base_domain)
                if normalized:
                    links.add(normalized)
        
        # Method 2: Also check for links in other elements
        for elem in soup.find_all(['link', 'area'], href=True):
            href = elem.get('href', '').strip()
            if href:
                absolute = urljoin(base_url, href)
                normalized = self._validate_and_normalize(absolute, base_domain)
                if normalized:
                    links.add(normalized)
        
        logger.info(f"Extracted {len(links)} valid links from {base_url}")
        
        # If no links found, log a sample of HTML for debugging
        if len(links) == 0:
            logger.warning(f"No links found in {base_url}. HTML sample: {html[:200]}...")
        
        return list(links)
    
    def _validate_and_normalize(self, url: str, base_domain: str) -> Optional[str]:
        """Validate and normalize a single URL."""
        
        # Skip empty
        if not url:
            return None
        
        # Skip non-http schemes
        parsed = urlparse(url)
        if parsed.scheme not in ('http', 'https'):
            return None
        
        # Skip excluded patterns
        url_lower = url.lower()
        for pattern in self.excluded_patterns:
            if pattern in url_lower:
                return None
        
        # Domain restriction
        if self.same_domain_only:
            if parsed.netloc != base_domain:
                return None
        
        # Build normalized URL (remove fragment)
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip('/') if parsed.path not in ('', '/') else '/',
            parsed.params,
            parsed.query,
            ''  # No fragment
        ))
        
        return normalized
    
    def normalize_url(self, url: str, base_domain: Optional[str] = None) -> Optional[str]:
        """Public method to normalize a URL."""
        domain = base_domain or urlparse(url).netloc
        return self._validate_and_normalize(url, domain)
