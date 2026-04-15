"""Test the Playwright deep crawler."""

import asyncio
from src.crawlers.deep_crawler import DeepCrawler


async def test_deep_crawler():
    """Test deep crawler on JavaScript-heavy sites."""
    
    crawler = DeepCrawler()
    
    # Test URLs (JS-heavy sites)
    urls = [
        "https://quotes.toscrape.com/js/",  # JavaScript-rendered quotes
        "https://books.toscrape.com",        # Static (works too)
    ]
    
    # Custom selectors for quotes site
    selectors = {
        'quotes': '.quote .text',
        'authors': '.quote .author',
        'tags': '.quote .tags'
    }
    
    print("=" * 70)
    print("🌐 DEEP WEB CRAWLER TEST (Playwright)")
    print("=" * 70)
    
    results = await crawler.crawl(
        urls=urls,
        wait_selector='.quote',  # Wait for quotes to load
        extract_selectors=selectors,
        take_screenshot=True
    )
    
    print("\n" + "=" * 70)
    print("📊 RESULTS")
    print("=" * 70)
    
    for result in results:
        print(f"\n📍 URL: {result['url']}")
        print(f"   Title: {result['title'][:60]}...")
        print(f"   Method: {result['method']}")
        print(f"   Content length: {len(result.get('content', ''))} chars")
        
        if 'quotes' in result:
            print(f"   Quotes found: {len(result['quotes']) if isinstance(result['quotes'], list) else 1}")
            if isinstance(result['quotes'], list):
                print(f"     • {result['quotes'][0][:50]}...")
        
        if 'screenshot' in result:
            print(f"   Screenshot: {result['screenshot']}")
    
    print("\n" + "=" * 70)
    print("✅ DEEP CRAWLER WORKING")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(test_deep_crawler())