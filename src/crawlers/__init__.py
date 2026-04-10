"""
Crawler module for Tri-Layer Intelligence Crawler.

This package contains all Scrapy-related components:
- Items: Data contract definitions
- Spiders: Web crawling logic
- Middlewares: Request/response processing
- Pipelines: Post-processing and storage
"""

from .items import CrawlerItem

__all__ = ['CrawlerItem']