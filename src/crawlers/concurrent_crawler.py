"""
Production-grade concurrent web crawler.
"""

import logging
import time
import csv
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from src.core.config_loader import get_settings
from src.utils.text_cleaner import clean_text
from src.crawlers.stats import CrawlStats
from src.utils.encoding_handler import decode_response_content

logger = logging.getLogger(__name__)


class ConcurrentCrawler:
    """
    Production crawler with concurrent requests and error handling.
    
    Features:
        - ThreadPoolExecutor for parallel crawling
        - Exponential backoff retry
        - Domain-based rate limiting
        - Comprehensive error handling
    """
    
    def __init__(self):
        settings = get_settings()
        self.start_urls = settings.crawler.start_urls
        self.max_workers = settings.crawler.concurrent_requests
        self.download_delay = settings.crawler.download_delay
        self.timeout = settings.crawler.request_timeout
        self.max_retries = settings.crawler.max_retries
        self.user_agent = settings.crawler.user_agent
        self.stats = CrawlStats()
        
        # Track last request time per domain for rate limiting
        self._domain_last_request: Dict[str, float] = {}
        
        # Results storage
        self.results: List[Dict] = []
        
        logger.info(f"ConcurrentCrawler ready: {self.max_workers} workers")
    
    def crawl(self) -> List[Dict]:
        logger.info(f"Starting concurrent crawl of {len(self.start_urls)} URLs")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {
                executor.submit(self._crawl_with_retry, url): url 
                for url in self.start_urls
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                self.stats.record_attempt(url)
                
                try:
                    result = future.result()
                    if result:
                        self.results.append(result)
                        self.stats.record_success(
                            url, 
                            len(result.get('content', '')), 
                            result.get('domain', 'unknown')
                        )
                        logger.info(f"[OK] {url}: {result['title'][:50]}")
                    else:
                        self.stats.record_failure(url, "No result returned")
                        logger.warning(f"[FAIL] {url}")
                except Exception as e:
                    self.stats.record_failure(url, str(e))
                    logger.error(f"[FAIL] {url}: {e}")
        
        self.stats.finish()
        logger.info(f"Crawl complete: {self.stats.urls_succeeded}/{self.stats.urls_attempted} URLs in {self.stats.elapsed_seconds:.2f}s")
        
        # Print stats summary
        print("\n" + self.stats.summary())
        
        return self.results
    
    def _crawl_with_retry(self, url: str) -> Optional[Dict]:
        """
        Crawl a URL with exponential backoff retry.
        """
        for attempt in range(self.max_retries):
            try:
                return self._crawl_single_url(url)
            except requests.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # 1s, 2s, 4s
                    logger.debug(f"Retry {attempt + 1}/{self.max_retries} for {url} in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed {url} after {self.max_retries} attempts: {e}")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error for {url}: {e}")
                return None
        
        return None
    
    def _crawl_single_url(self, url: str) -> Optional[Dict]:
        """
        Crawl a single URL with rate limiting.
        """
        # Apply domain rate limiting
        domain = urlparse(url).netloc
        self._rate_limit_domain(domain)
        
        # Prepare session with headers
        session = requests.Session()
        session.headers.update({'User-Agent': self.user_agent})
        
        # Make request
        response = session.get(url, timeout=self.timeout)
        response.raise_for_status()
        
        # Update last request time
        self._domain_last_request[domain] = time.time()
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract data
        return self._extract_data(soup, url, response)
    
    def _rate_limit_domain(self, domain: str):
        """
        Ensure minimum delay between requests to same domain.
        """
        if domain in self._domain_last_request:
            elapsed = time.time() - self._domain_last_request[domain]
            if elapsed < self.download_delay:
                time.sleep(self.download_delay - elapsed)
    
    def _extract_data(self, soup: BeautifulSoup, url: str, response: requests.Response) -> Dict:
        """
        Extract structured data from page.
        """
        # Title
        title = self._extract_title(soup, url)
        
        # Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        meta_description = meta_desc.get('content', '') if meta_desc else ''
        
        # Content
        body = soup.find('body')
        content = body.get_text(separator=' ', strip=True) if body else ''
        
        # Clean
        title = clean_text(title)
        content = clean_text(content)
        meta_description = clean_text(meta_description)
        
        return {
            'url': url,
            'title': title,
            'content': content,
            'meta_description': meta_description,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'status_code': response.status_code,
            'domain': urlparse(url).netloc,
        }
    
    def _extract_title(self, soup: BeautifulSoup, url: str) -> str:
        """Extract title with fallbacks."""
        # OpenGraph
        og = soup.find('meta', property='og:title')
        if og and og.get('content'):
            return og['content']
        
        # HTML title
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            return str(title_tag.string)
        
        # H1
        h1 = soup.find('h1')
        if h1 and h1.string:
            return str(h1.string)
        
        # Fallback
        return urlparse(url).path.split('/')[-1] or url
            # In src/crawlers/concurrent_crawler.py
    def crawl_surface_only(self, urls: List[str]) -> List[Dict]:
        """
        Crawl URLs without saving to database.
        
        Args:
            urls: List of URLs to crawl
            
        Returns:
            List of extracted items
        """
        original_urls = self.start_urls
        self.start_urls = urls
        
        # Temporarily override results storage
        temp_results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {
                executor.submit(self._crawl_with_retry, url): url 
                for url in urls
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                self.stats.record_attempt(url)
                
                try:
                    result = future.result()
                    if result:
                        temp_results.append(result)
                        self.stats.record_success(
                            url,
                            len(result.get('content', '')),
                            result.get('domain', 'unknown')
                        )
                    else:
                        self.stats.record_failure(url, "No result")
                except Exception as e:
                    self.stats.record_failure(url, str(e))
                    logger.error(f"Failed {url}: {e}")
        
        self.start_urls = original_urls
        return temp_results
    
    def save_results(self, output_path: Optional[Path] = None):
        """
        Save results to CSV and JSON.
        """
        if not self.results:
            logger.warning("No results to save")
            return
        try:
            from src.repositories.db_repository import DatabaseRepository
            repo = DatabaseRepository()
            saved_count = repo.save_batch(self.results)
            repo.close()
            logger.info(f"[OK] Saved {saved_count} items to database")
        except Exception as e:
            logger.error(f"[FAIL] Database save failed: {e}")
        if output_path is None:
            settings = get_settings()
            project_root = Path(__file__).parent.parent.parent
            output_path = project_root / settings.storage.csv_output_path
        
        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save CSV
        if self.results:
            fieldnames = list(self.results[0].keys())
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.results)
            logger.info(f"[OK] Saved {len(self.results)} items to {output_path}")
        
        # Save JSON
        json_path = output_path.with_suffix('.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[OK] JSON backup saved to {json_path}")
    def close(self):
        """Clean up resources."""
        # Close database session if any
        if hasattr(self, 'db_session') and self.db_session:
            self.db_session.close()
        logger.debug("Crawler resources cleaned up")