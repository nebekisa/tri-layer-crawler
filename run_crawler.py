"""
Simple runner script for the crawler.
"""

import json
import csv
from pathlib import Path

from src.crawlers.simple_crawler import SimpleCrawler
from src.core.config_loader import get_settings


def main():
    """Run the crawler and save results."""
    print("=" * 60)
    print("Tri-Layer Intelligence Crawler - MVP")
    print("=" * 60)
    
    # Initialize crawler
    crawler = SimpleCrawler()
    
    # Execute crawl
    results = crawler.crawl()
    
    if not results:
        print("No results extracted.")
        return
    
    # Get output path from config
    settings = get_settings()
    csv_path = Path(settings.storage.csv_output_path)
    
    # Preview first item
    print("\n" + "=" * 60)
    print("Preview of first item:")
    print("=" * 60)
    item = results[0]
    print(f"URL: {item['url']}")
    print(f"Title: {item['title']}")
    print(f"Content length: {len(item['content'])} chars")
    print(f"Content preview: {item['content'][:200]}...")
    print(f"\n✓ Results saved to {csv_path}")
    print(f"✓ JSON backup saved to {csv_path.with_suffix('.json')}")

if __name__ == "__main__":
    main()