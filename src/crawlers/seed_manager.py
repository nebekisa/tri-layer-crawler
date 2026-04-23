"""
Seed URL management for scalable crawling.
"""

import logging
import re
from typing import List, Dict, Set, Optional, Tuple
from urllib.parse import urlparse, urlunparse
from pathlib import Path
from collections import Counter

logger = logging.getLogger(__name__)


class SeedManager:
    """
    Manage seed URLs from multiple sources.
    
    Features:
        - Load from text file, config, or database
        - Validate and normalize URLs
        - Deduplicate seeds
        - Domain distribution tracking
        - Seed health monitoring
    """
    
    def __init__(self, 
                 seed_file: Optional[str] = "config/seeds.txt",
                 max_seeds: Optional[int] = None,
                 allowed_domains: Optional[List[str]] = None,
                 exclude_patterns: Optional[List[str]] = None):
        """
        Initialize seed manager.
        
        Args:
            seed_file: Path to seeds.txt file
            max_seeds: Maximum seeds to load
            allowed_domains: Only load seeds from these domains
            exclude_patterns: Skip URLs matching these patterns
        """
        self.seed_file = Path(seed_file) if seed_file else None
        self.max_seeds = max_seeds
        self.allowed_domains = set(allowed_domains) if allowed_domains else None
        self.exclude_patterns = exclude_patterns or []
        
        self.seeds: List[str] = []
        self.seed_metadata: Dict[str, Dict] = {}
        self.domain_counts: Counter = Counter()
        self.failed_seeds: Set[str] = set()
        
        logger.info(f"SeedManager initialized with max_seeds={max_seeds}")
    
    def load_seeds(self, sources: Optional[List[str]] = None) -> List[str]:
        """
        Load seeds from all configured sources.
        
        Args:
            sources: Optional list of source types ['file', 'config', 'db']
            
        Returns:
            List of validated seed URLs
        """
        sources = sources or ['file', 'config']
        all_seeds = []
        
        for source in sources:
            if source == 'file' and self.seed_file and self.seed_file.exists():
                seeds = self._load_from_file()
                logger.info(f"Loaded {len(seeds)} seeds from {self.seed_file}")
                all_seeds.extend(seeds)
            
            elif source == 'config':
                seeds = self._load_from_config()
                logger.info(f"Loaded {len(seeds)} seeds from config")
                all_seeds.extend(seeds)
            
            elif source == 'db':
                seeds = self._load_from_database()
                logger.info(f"Loaded {len(seeds)} seeds from database")
                all_seeds.extend(seeds)
        
        # Process seeds
        self.seeds = self._process_seeds(all_seeds)
        
        # Track domain distribution
        self._update_domain_counts()
        
        logger.info(f"Total seeds loaded: {len(self.seeds)}")
        logger.info(f"Domain distribution: {dict(self.domain_counts)}")
        
        return self.seeds
    
    def _load_from_file(self) -> List[str]:
        """Load seeds from text file."""
        if not self.seed_file or not self.seed_file.exists():
            return []
        
        seeds = []
        with open(self.seed_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#'):
                    seeds.append(line)
        
        return seeds
    
    def _load_from_config(self) -> List[str]:
        """Load seeds from settings.yaml."""
        try:
            from src.core.config_loader import get_settings
            settings = get_settings()
            return settings.crawler.start_urls
        except Exception as e:
            logger.warning(f"Could not load seeds from config: {e}")
            return []
    
    def _load_from_database(self) -> List[str]:
        """Load seeds from database (previously successful URLs)."""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host='localhost', port=5433,
                database='tri_layer_crawler',
                user='crawler_user', password='CrawlerPass2024!'
            )
            cur = conn.cursor()
            
            # Get top domains with successful crawls
            cur.execute('''
                SELECT DISTINCT url 
                FROM crawled_items 
                WHERE status_code = 200 
                ORDER BY crawled_at DESC 
                LIMIT 50
            ''')
            
            seeds = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            
            return seeds
            
        except Exception as e:
            logger.warning(f"Could not load seeds from database: {e}")
            return []
    
    def _process_seeds(self, urls: List[str]) -> List[str]:
        """Validate, normalize, and deduplicate seeds."""
        processed: Set[str] = set()
        
        for url in urls:
            # Validate and normalize
            normalized = self._normalize_url(url)
            if not normalized:
                continue
            
            # Check domain restrictions
            if self.allowed_domains:
                domain = urlparse(normalized).netloc
                if domain not in self.allowed_domains:
                    logger.debug(f"Skipping {normalized}: domain not allowed")
                    continue
            
            # Check exclude patterns
            skip = False
            for pattern in self.exclude_patterns:
                if pattern in normalized:
                    logger.debug(f"Skipping {normalized}: matches exclude pattern '{pattern}'")
                    skip = True
                    break
            if skip:
                continue
            
            processed.add(normalized)
            
            # Respect max_seeds limit
            if self.max_seeds and len(processed) >= self.max_seeds:
                logger.info(f"Reached max_seeds limit ({self.max_seeds})")
                break
        
        return list(processed)
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """Normalize and validate URL."""
        url = url.strip()
        
        # Basic validation
        if not url:
            return None
        
        # Add scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        
        # Validate scheme
        if parsed.scheme not in ('http', 'https'):
            return None
        
        # Validate domain
        if not parsed.netloc:
            return None
        
        # Normalize: remove fragment, trailing slash
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip('/') or '/',
            parsed.params,
            parsed.query,
            ''  # No fragment
        ))
        
        return normalized
    
    def _update_domain_counts(self):
        """Update domain distribution counts."""
        self.domain_counts.clear()
        for url in self.seeds:
            domain = urlparse(url).netloc
            self.domain_counts[domain] += 1
    
    def get_seeds_by_domain(self, domain: Optional[str] = None) -> List[str]:
        """
        Get seeds filtered by domain.
        
        Args:
            domain: Filter by specific domain, or None for all
            
        Returns:
            List of seed URLs
        """
        if domain:
            return [s for s in self.seeds if urlparse(s).netloc == domain]
        return self.seeds.copy()
    
    def get_domain_distribution(self) -> Dict[str, int]:
        """Get count of seeds per domain."""
        return dict(self.domain_counts)
    
    def get_seed_stats(self) -> Dict:
        """Get seed statistics."""
        return {
            'total_seeds': len(self.seeds),
            'domains': len(self.domain_counts),
            'domain_distribution': dict(self.domain_counts),
            'failed_seeds': len(self.failed_seeds),
            'max_seeds': self.max_seeds,
            'source_file': str(self.seed_file) if self.seed_file else None
        }
    
    def mark_seed_failed(self, url: str):
        """Mark a seed as failed."""
        self.failed_seeds.add(url)
        logger.warning(f"Marked seed as failed: {url}")
    
    def is_seed_healthy(self, url: str) -> bool:
        """Check if seed is still healthy."""
        return url not in self.failed_seeds
    
    def add_seed(self, url: str) -> bool:
        """Add a new seed dynamically."""
        normalized = self._normalize_url(url)
        if normalized and normalized not in self.seeds:
            self.seeds.append(normalized)
            domain = urlparse(normalized).netloc
            self.domain_counts[domain] += 1
            return True
        return False
    
    def remove_seed(self, url: str):
        """Remove a seed."""
        normalized = self._normalize_url(url)
        if normalized and normalized in self.seeds:
            self.seeds.remove(normalized)
            domain = urlparse(normalized).netloc
            self.domain_counts[domain] -= 1
            if self.domain_counts[domain] <= 0:
                del self.domain_counts[domain]
