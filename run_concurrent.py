"""
Runner for concurrent crawler.
"""

import logging
import sys
from pathlib import Path

from src.crawlers.concurrent_crawler import ConcurrentCrawler

# Fix Windows console encoding for logging
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Ensure logs directory exists
Path("logs").mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/crawler.log', encoding='utf-8'),  # File handles UTF-8 fine
        logging.StreamHandler(sys.stdout)  # Use fixed stdout
    ]
)

# Replace Unicode characters with ASCII equivalents in log messages
class AsciiLogger:
    """Wrapper to replace Unicode with ASCII for console compatibility."""
    def __init__(self, logger):
        self.logger = logger
    
    def _clean(self, msg):
        replacements = {
            '✓': '[OK]',
            '✗': '[FAIL]',
            '⚠': '[WARN]',
            '→': '->',
            '…': '...',
        }
        for uni, ascii_val in replacements.items():
            msg = msg.replace(uni, ascii_val)
        return msg
    
    def info(self, msg, *args, **kwargs):
        return self.logger.info(self._clean(msg), *args, **kwargs)
    
    def warning(self, msg, *args, **kwargs):
        return self.logger.warning(self._clean(msg), *args, **kwargs)
    
    def error(self, msg, *args, **kwargs):
        return self.logger.error(self._clean(msg), *args, **kwargs)
    
    def debug(self, msg, *args, **kwargs):
        return self.logger.debug(self._clean(msg), *args, **kwargs)


def main():
    print("=" * 60)
    print("Tri-Layer Intelligence Crawler - Production Mode")
    print("=" * 60)
    
    crawler = ConcurrentCrawler()
    results = crawler.crawl()
    
    if results:
        crawler.save_results()
        
        print("\n" + "=" * 60)
        print(f"[OK] Successfully crawled {len(results)} URLs")
        print("=" * 60)
        
        for item in results:
            print(f"  - {item['title'][:60]}... ({item['domain']})")
    else:
        print("\n[FAIL] No results extracted")


if __name__ == "__main__":
    main()