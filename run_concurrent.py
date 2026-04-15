"""
Runner for concurrent crawler with structured logging.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logging_config import setup_logging, LoggerAdapter
from src.crawlers.concurrent_crawler import ConcurrentCrawler
import logging

# Setup structured logging
setup_logging(log_level="INFO", json_logs=True)

# Create logger with context
logger = LoggerAdapter(
    logging.getLogger("runner"),
    {"component": "main", "version": "2.0.0"}
)


def main():
    print("=" * 60)
    print("Tri-Layer Intelligence Crawler - Production Mode")
    print("=" * 60)
    
    logger.info("Starting crawler", extra={"extra_data": {"mode": "production"}})
    
    crawler = ConcurrentCrawler()
    
    try:
        results = crawler.crawl()
        
        if results:
            crawler.save_results()
            
            print("\n" + "=" * 60)
            print(f"[OK] Successfully crawled {len(results)} URLs")
            print("=" * 60)
            
            for item in results:
                print(f"  - {item['title'][:60]}... ({item['domain']})")
            
            logger.info(
                "Crawl completed successfully",
                extra={"extra_data": {"urls_crawled": len(results)}}
            )
        else:
            print("\n[FAIL] No results extracted")
            logger.warning("No results extracted")
            
    except Exception as e:
        logger.exception("Crawler failed with exception")
        raise
    finally:
        crawler.close()


if __name__ == "__main__":
    main()