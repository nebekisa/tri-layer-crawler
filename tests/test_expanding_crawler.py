"""
Test expanding crawler with 50-100 URLs.
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from src.crawlers.expanding_crawler import ExpandingCrawler


def main():
    print("\n" + "="*70)
    print("??? EXPANDING CRAWLER TEST")
    print("="*70)
    
    # Create crawler
    crawler = ExpandingCrawler()
    
    print(f"\nConfiguration:")
    print(f"  Max Depth: {crawler.max_depth}")
    print(f"  Max URLs: {crawler.max_urls}")
    print(f"  Same Domain Only: {crawler.same_domain_only}")
    print(f"  Seed Domains: {crawler.seed_domains}")
    
    print("\n" + "-"*70)
    print("?? STARTING CRAWL...")
    print("-"*70 + "\n")
    
    # Run crawl
    results = crawler.crawl()
    
    print("\n" + "="*70)
    print("?? CRAWL COMPLETE")
    print("="*70)
    
    stats = crawler.get_stats()
    
    print(f"""
Final Statistics:
  URLs Crawled:      {stats['crawled_count']}
  URLs Discovered:   {stats['discovered_count']}
  Queue Pending:     {stats['queue_stats']['pending']}
  Success Rate:      {len(results)}/{stats['crawled_count']} crawled
  
Seed Domains: {', '.join(stats['seed_domains'])}
    """)
    
    # Show sample of crawled URLs
    if results:
        print("\nSample of crawled URLs:")
        for i, item in enumerate(results[:10]):
            depth = item.get('crawl_depth', '?')
            print(f"  {i+1}. [Depth {depth}] {item['title'][:50]}...")
            print(f"      {item['url']}")
    
    crawler.close()
    
    print("\n? Test complete!")


if __name__ == "__main__":
    main()
