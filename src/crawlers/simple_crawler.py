"""
Simple, reliable web crawler - USING PROVEN WORKING CODE.
"""

import logging
import json
import csv
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from src.core.config_loader import get_settings
from src.utils.text_cleaner import clean_text

logger = logging.getLogger(__name__)


class SimpleCrawler:
    """A crawler that actually works."""
    
    def __init__(self):
        settings = get_settings()
        self.start_urls = settings.crawler.start_urls
        self.csv_path = Path(settings.storage.csv_output_path)
        logger.info(f"Crawler ready for {len(self.start_urls)} URLs")
    
    def crawl(self) -> List[Dict]:
        """Execute crawling using proven working approach."""
        results = []
        
        for url in self.start_urls:
            logger.info(f"Crawling: {url}")
            
            # EXACT CODE FROM test_direct.py THAT WORKED
            try:
                response = requests.get(url)
                
                # This is the exact working approach
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract title - EXACT working code
                title_tag = soup.find('title')
                title = str(title_tag.string) if title_tag else url
                
                # Extract meta description
                meta_tag = soup.find('meta', attrs={'name': 'description'})
                meta_description = meta_tag.get('content', '') if meta_tag else ''
                
                # Extract content - EXACT working code
                body = soup.find('body')
                content = body.get_text(separator=' ', strip=True) if body else ""
                
                # Clean text
                title = clean_text(title)
                content = clean_text(content)
                meta_description = clean_text(meta_description)
                
                # Build item
                item = {
                    'url': url,
                    'title': title,
                    'content': content,
                    'meta_description': meta_description,
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'status_code': response.status_code,
                }
                
                results.append(item)
                logger.info(f"✓ Success: {title[:50]}")
                
            except Exception as e:
                logger.error(f"Failed {url}: {e}")
        
        # Save results
        if results:
            self._save_results(results)
        
        return results
    
    def _save_results(self, results: List[Dict]):
        """Save results to CSV and JSON."""
        # Ensure directory exists
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save CSV
        if results:
            fieldnames = list(results[0].keys())
            with open(self.csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            logger.info(f"✓ Saved {len(results)} items to {self.csv_path}")
        
        # Save JSON backup
        json_path = self.csv_path.with_suffix('.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ JSON backup saved to {json_path}")