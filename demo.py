"""
Demonstration script showcasing all features.
"""

import asyncio
from src.crawlers.unified_crawler import UnifiedCrawler
from src.analytics.pipeline import AnalyticsPipeline


async def run_demo():
    """Run complete system demonstration."""
    
    print("=" * 70)
    print("🚀 TRI-LAYER INTELLIGENCE CRAWLER - FULL DEMO")
    print("=" * 70)
    
    # Test URLs (mix of surface and deep)
    urls = [
        "https://books.toscrape.com",           # Surface
        "https://quotes.toscrape.com/js/",      # Deep (JS)
    ]
    
    # 1. Crawl
    print("\n📡 PHASE 1: INTELLIGENT CRAWLING")
    print("-" * 70)
    
    crawler = UnifiedCrawler()
    
    selectors = {
        'quotes': '.quote .text',
        'authors': '.quote .author',
    }
    
    # Use crawl_async since we're in async context
    results = await crawler.crawl_async(
        urls=urls,
        take_screenshots=True,
        extract_selectors=selectors
    )
    
    print(f"\n✅ Crawled {len(results)} pages")
    
    # 2. AI Analytics
    print("\n🧠 PHASE 2: AI ANALYTICS")
    print("-" * 70)
    
    pipeline = AnalyticsPipeline()
    
    for i, item in enumerate(results, 1):
        if not item:
            continue
            
        print(f"\n📄 Item {i}: {item.get('title', 'No title')[:60]}...")
        
        content = item.get('content', '')
        if not content:
            print("   ⚠️ No content extracted")
            continue
        
        analysis = pipeline.analyze(
            item_id=i,
            url=item['url'],
            content=content,
            title=item.get('title', '')
        )
        
        print(f"   • Entities: {len(analysis.entities)} found")
        if analysis.entities:
            entity_names = [e.text for e in analysis.entities[:3]]
            print(f"     → {', '.join(entity_names)}")
        
        print(f"   • Sentiment: {analysis.sentiment.label.value} ({analysis.sentiment.polarity:+.3f})")
        print(f"   • Keywords: {len(analysis.keywords)} extracted")
        if analysis.keywords:
            top_kw = [k.keyword for k in analysis.keywords[:3]]
            print(f"     → {', '.join(top_kw)}")
        
        print(f"   • Readability: Grade {analysis.readability.flesch_kincaid_grade:.1f}")
        print(f"   • Crawl Method: {item.get('crawl_method', 'unknown')}")
        
        if 'screenshot' in item:
            print(f"   • Screenshot: {item['screenshot']}")
    
    # 3. Summary
    print("\n" + "=" * 70)
    print("📊 FINAL SUMMARY")
    print("=" * 70)
    print(f"✅ Total pages crawled: {len(results)}")
    print(f"✅ Surface crawls: {sum(1 for r in results if r and r.get('crawl_method') == 'surface')}")
    print(f"✅ Deep crawls: {sum(1 for r in results if r and r.get('crawl_method') == 'deep')}")
    print(f"✅ Screenshots captured: {sum(1 for r in results if r and 'screenshot' in r)}")
    print("\n🎉 DEMO COMPLETE - SYSTEM READY FOR PRODUCTION")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_demo())