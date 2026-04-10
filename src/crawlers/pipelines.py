"""
Data Processing Pipelines for Tri-Layer Intelligence Crawler.

Pipelines process items sequentially after they are yielded by a Spider.
Order matters: Validation MUST happen before writing to storage.

Architecture Pattern: Chain of Responsibility
    Each pipeline receives an item, processes it, and passes it to the next.
"""

import csv
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from scrapy import Spider
from scrapy.exceptions import DropItem

from src.core.config_loader import get_settings
from src.crawlers.items import CrawlerItem

logger = logging.getLogger(__name__)


class ValidationPipeline:
    """
    Validates CrawlerItem before it reaches storage.
    
    Rejects items that:
        - Have empty or whitespace-only titles
        - Have empty content (less than 50 characters)
        - Have invalid URL schemes (only http/https allowed)
    
    Why this exists:
        Garbage in, garbage out. We prevent bad data from entering our storage
        layer, saving disk space and ensuring API quality.
    """
    
    MIN_CONTENT_LENGTH = 50  # Reject pages with essentially no content
    
    def process_item(self, item: CrawlerItem, spider: Spider) -> CrawlerItem:
        """
        Validate the item. Raise DropItem if validation fails.
        
        Args:
            item: The scraped item to validate.
            spider: The spider that yielded this item (useful for spider-specific rules).
            
        Returns:
            CrawlerItem: The validated item, passed to next pipeline.
            
        Raises:
            DropItem: If validation fails.
        """
        
        # Validate Title
        title = item.get('title', '')
        if not title or not title.strip():
            raise DropItem(f"Item dropped: Empty title for URL {item.get('url')}")
        
        # Validate Content
        content = item.get('content', '')
        if not content or len(content.strip()) < self.MIN_CONTENT_LENGTH:
            raise DropItem(
                f"Item dropped: Content too short ({len(content)} chars) for URL {item.get('url')}"
            )
        
        # Validate URL Scheme
        url = item.get('url', '')
        parsed = urlparse(url)
        if parsed.scheme not in ('http', 'https'):
            raise DropItem(f"Item dropped: Invalid URL scheme '{parsed.scheme}' for {url}")
        
        # Log successful validation
        logger.debug(f"Item validated successfully: {item.get('url')}")
        
        return item


class CsvWriterPipeline:
    """
    Writes validated CrawlerItems to a CSV file.
    
    Features:
        - Creates output directory if it doesn't exist.
        - Writes headers automatically on first item.
        - Appends rows efficiently.
        - Handles Unicode encoding gracefully.
    
    Configuration:
        Reads output path from settings.yaml (storage.csv_output_path).
    """
    
    def __init__(self):
        """Initialize pipeline state."""
        self.file_handle = None
        self.csv_writer = None
        self.output_path: Optional[Path] = None
        self.headers_written = False
        
    def open_spider(self, spider: Spider):
        """
        Called when the spider is opened.
        
        Opens the CSV file for writing and ensures the directory exists.
        """
        settings = get_settings()
        csv_path = settings.storage.csv_output_path
        
        # Convert relative path to absolute (relative to project root)
        # Path(__file__).parent.parent.parent gives us tri-layer-crawler/
        project_root = Path(__file__).parent.parent.parent
        self.output_path = project_root / csv_path
        
        # Ensure parent directory exists
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Open file in append mode with UTF-8 encoding
        # newline='' prevents extra blank lines on Windows
        self.file_handle = open(self.output_path, 'a', newline='', encoding='utf-8')
        
        logger.info(f"CSV Writer Pipeline opened: {self.output_path}")
        
    def process_item(self, item: CrawlerItem, spider: Spider) -> CrawlerItem:
        """
        Write a single item to the CSV file.
        
        Args:
            item: Validated CrawlerItem to write.
            spider: The spider that yielded this item.
            
        Returns:
            CrawlerItem: Unmodified item, passed to next pipeline.
        """
        if not self.file_handle:
            raise RuntimeError("CSV file not opened. open_spider() must be called first.")
        
        # Convert Scrapy Item to plain dict
        item_dict = dict(item)
        
        # Get fieldnames from the Item's defined fields
        fieldnames = list(CrawlerItem.fields.keys())
        
        # Initialize CSV DictWriter if not already done
        if self.csv_writer is None:
            self.csv_writer = csv.DictWriter(
                self.file_handle, 
                fieldnames=fieldnames,
                quoting=csv.QUOTE_MINIMAL
            )
        
        # Write header only once (when file is empty)
        if not self.headers_written:
            # Check if file is empty (tell() returns position)
            self.file_handle.seek(0, 2)  # Seek to end
            if self.file_handle.tell() == 0:
                self.csv_writer.writeheader()
                self.file_handle.flush()  # Ensure header is written immediately
            self.headers_written = True
        
        # Write the row
        self.csv_writer.writerow(item_dict)
        self.file_handle.flush()  # Flush to disk immediately for data safety
        
        logger.debug(f"Item written to CSV: {item.get('url')}")
        
        return item
        
    def close_spider(self, spider: Spider):
        """
        Called when the spider is closed.
        
        Closes the CSV file handle cleanly.
        """
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
            logger.info(f"CSV Writer Pipeline closed. Data saved to {self.output_path}")