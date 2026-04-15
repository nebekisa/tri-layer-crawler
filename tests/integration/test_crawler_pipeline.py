"""Integration tests for crawler pipeline."""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from crawlers.concurrent_crawler import ConcurrentCrawler
from analytics.pipeline import AnalyticsPipeline
from database.manager import get_db_session
from database.models import CrawledItem


class TestCrawlerPipeline:
    """Test full crawler → database → analytics pipeline."""
    
    @pytest.mark.integration
    def test_end_to_end_flow(self):
        """Test complete data flow."""
        # 1. Crawl
        crawler = ConcurrentCrawler()
        crawler.start_urls = ["https://books.toscrape.com"]
        results = crawler.crawl_surface_only(crawler.start_urls)
        
        assert len(results) > 0
        assert 'title' in results[0]
        assert 'content' in results[0]
        
        # 2. Save to database
        session = get_db_session()
        item = CrawledItem(
            url=results[0]['url'],
            title=results[0]['title'],
            content=results[0]['content'],
            domain='books.toscrape.com',
            status_code=200,
            content_length=len(results[0]['content'])
        )
        session.add(item)
        session.commit()
        
        # 3. Analyze
        pipeline = AnalyticsPipeline()
        analysis = pipeline.analyze(
            item_id=item.id,
            url=item.url,
            content=item.content,
            title=item.title
        )
        
        assert analysis is not None
        assert len(analysis.entities) >= 0
        assert analysis.sentiment is not None
        
        # Cleanup
        session.delete(item)
        session.commit()
        session.close()