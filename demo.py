"""
Demonstration script with Prometheus metrics.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.crawlers.unified_crawler import UnifiedCrawler
from src.analytics.pipeline import AnalyticsPipeline
from prometheus_client import Counter, Histogram, Gauge, generate_latest
import requests


# Prometheus metrics
crawled_pages = Counter('crawler_pages_total', 'Total pages crawled', ['method'])
analysis_duration = Histogram('analysis_duration_seconds', 'Analysis processing time')
entities_found = Counter('entities_found_total', 'Total entities extracted')
sentiment_score = Gauge('sentiment_score', 'Sentiment polarity', ['url'])


async def run_demo():
    """Run complete system demonstration."""
    
    print("=" * 70)
    print("🚀 TRI-LAYER INTELLIGENCE CRAWLER - FULL DEMO")
    print("=" * 70)
    
    urls = [
        "https://books.toscrape.com",
        "https://quotes.toscrape.com/js/",
    ]
    
    # 1. Crawl
    print("\n📡 PHASE 1: INTELLIGENT CRAWLING")
    print("-" * 70)
    
    crawler = UnifiedCrawler()
    
    selectors = {
        'quotes': '.quote .text',
        'authors': '.quote .author',
    }
    
    results = await crawler.crawl_async(
        urls=urls,
        take_screenshots=True,
        extract_selectors=selectors
    )
    
    # Update metrics
    for r in results:
        if r:
            crawled_pages.labels(method=r.get('crawl_method', 'unknown')).inc()
    
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
            continue
        
        # Time the analysis
        import time
        start = time.perf_counter()
        analysis = pipeline.analyze(
            item_id=i,
            url=item['url'],
            content=content,
            title=item.get('title', '')
        )
        duration = time.perf_counter() - start
        analysis_duration.observe(duration)
        
        # Update metrics
        entities_found.inc(len(analysis.entities))
        sentiment_score.labels(url=item['url']).set(analysis.sentiment.polarity)
        
        print(f"   • Entities: {len(analysis.entities)} found")
        if analysis.entities:
            entity_names = [e.text for e in analysis.entities[:3]]
            print(f"     → {', '.join(entity_names)}")
        
        print(f"   • Sentiment: {analysis.sentiment.label.value} ({analysis.sentiment.polarity:+.3f})")
        print(f"   • Keywords: {len(analysis.keywords)} extracted")
        print(f"   • Crawl Method: {item.get('crawl_method', 'unknown')}")
    
    # 3. Push metrics to Prometheus Pushgateway (if available)
    try:
        requests.post('http://localhost:9091/metrics/job/crawler_demo', 
                      data=generate_latest(), timeout=2)
        print("\n📊 Metrics pushed to Prometheus")
    except:
        print("\n⚠️ Prometheus Pushgateway not available - metrics only in /metrics endpoint")
    
    print("\n" + "=" * 70)
    print("📊 FINAL SUMMARY")
    print("=" * 70)
    print(f"✅ Total pages crawled: {len(results)}")
    print(f"✅ Surface crawls: {sum(1 for r in results if r and r.get('crawl_method') == 'surface')}")
    print(f"✅ Deep crawls: {sum(1 for r in results if r and r.get('crawl_method') == 'deep')}")
    print("\n🎉 DEMO COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_demo())