"""Performance benchmarks for crawler."""

import pytest
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from crawlers.concurrent_crawler import ConcurrentCrawler


class TestCrawlerPerformance:
    """Performance benchmarks."""
    
    @pytest.mark.slow
    @pytest.mark.performance
    def test_concurrent_crawl_speed(self):
        """Test that concurrent crawling is faster than sequential."""
        urls = [
            "https://books.toscrape.com",
            "https://quotes.toscrape.com",
        ]
        
        # Sequential (1 worker)
        crawler_seq = ConcurrentCrawler()
        crawler_seq.max_workers = 1
        
        start = time.perf_counter()
        seq_results = crawler_seq.crawl_surface_only(urls)
        seq_time = time.perf_counter() - start
        
        # Concurrent (2 workers)
        crawler_con = ConcurrentCrawler()
        crawler_con.max_workers = 2
        
        start = time.perf_counter()
        con_results = crawler_con.crawl_surface_only(urls)
        con_time = time.perf_counter() - start
        
        # Concurrent should be faster
        assert con_time < seq_time * 0.8  # At least 20% faster
        assert len(con_results) == len(seq_results)