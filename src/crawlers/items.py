"""
Data Contract for the Tri-Layer Intelligence Crawler.

This module defines the standard schema for all scraped data.
Every spider MUST yield items that conform to this structure.

Design Decision:
    We use Scrapy's Item class (not plain dicts) because:
    1. Field names are explicit and documented.
    2. Scrapy provides built-in serialization (dict, JSON).
    3. IDEs provide autocomplete for field access.
"""

import scrapy
from datetime import datetime
from typing import Optional


class CrawlerItem(scrapy.Item):
    """
    Standardized data structure for crawled web pages.
    
    Fields:
        url (str): The canonical URL of the crawled page.
        title (str): The page title (from <title> tag or OpenGraph).
        content (str): The main textual content extracted from the page body.
        timestamp (str): ISO 8601 formatted timestamp of when the crawl occurred.
        meta_description (Optional[str]): Meta description tag content.
        status_code (Optional[int]): HTTP status code of the response.
        content_type (Optional[str]): MIME type of the response (e.g., 'text/html').
    """
    
    # Required Fields - Every crawl MUST populate these
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    timestamp = scrapy.Field()
    
    # Optional Fields - Provide additional context when available
    meta_description = scrapy.Field()
    status_code = scrapy.Field()
    content_type = scrapy.Field()
    
    def __repr__(self):
        """
        Professional string representation for logging.
        Prevents accidentally logging entire page content (which is huge).
        """
        content_preview = self.get('content', '')[:50] + '...' if self.get('content') else 'None'
        return (
            f"CrawlerItem(url={self.get('url')!r}, "
            f"title={self.get('title')!r}, "
            f"content_preview={content_preview!r})"
        )
    
    @classmethod
    def create_from_response(cls, response, extracted_title: str, extracted_content: str, 
                            meta_description: Optional[str] = None) -> 'CrawlerItem':
        """
        Factory method to create a CrawlerItem from a Scrapy Response object.
        
        This ensures consistent timestamp generation and reduces duplicate code
        across different spiders.
        
        Args:
            response: Scrapy Response object.
            extracted_title: Cleaned title string.
            extracted_content: Cleaned main content string.
            meta_description: Optional meta description.
            
        Returns:
            CrawlerItem: Fully populated item ready for pipeline processing.
        """
        return cls(
            url=response.url,
            title=extracted_title,
            content=extracted_content,
            timestamp=datetime.utcnow().isoformat() + 'Z',  # UTC timestamp with Zulu indicator
            meta_description=meta_description,
            status_code=response.status,
            content_type=response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore')
        )