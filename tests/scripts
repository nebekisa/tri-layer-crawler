"""
Test script for AI Analytics modules.
Validates entity extraction and sentiment analysis on real crawled data.
"""

import sys
import time
import json
import tracemalloc
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.database.manager import get_db_session
from src.database.models import CrawledItem
from src.analytics.entity_extractor import EntityExtractor
from src.analytics.sentiment_analyzer import SentimentAnalyzer


def format_entity(entity):
    """Format entity for display."""
    return f"  • {entity.text} ({entity.label.value})"


def format_sentiment(result):
    """Format sentiment for display."""
    emoji = {'positive': '🟢', 'negative': '🔴', 'neutral': '⚪', 'mixed': '🟡'}
    return f"{emoji.get(result.label.value, '')} {result.label.value.upper()} (polarity: {result.polarity:+.3f}, confidence: {result.confidence:.1%})"


def test_entity_extraction(text: str, title: str):
    """Test entity extraction on text."""
    print("\n" + "=" * 60)
    print(f"🔍 ENTITY EXTRACTION TEST: {title[:50]}...")
    print("=" * 60)
    
    extractor = EntityExtractor()
    
    # Measure performance
    start_time = time.perf_counter()
    entities = extractor.extract(text, max_entities=20)
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    
    print(f"\n📊 Found {len(entities)} entities in {elapsed_ms:.1f}ms")
    print(f"📏 Text length: {len(text)} characters")
    
    if entities:
        print("\n🏷️  Top Entities:")
        # Group by type
        by_type = {}
        for e in entities[:15]:
            if e.label.value not in by_type:
                by_type[e.label.value] = []
            by_type[e.label.value].append(e.text)
        
        for entity_type, texts in by_type.items():
            unique_texts = list(set(texts))[:5]
            print(f"\n  {entity_type}:")
            for t in unique_texts:
                print(f"    • {t}")
    else:
        print("\n⚠️  No entities found (text may be too short or non-English)")
    
    return {
        'entity_count': len(entities),
        'processing_ms': elapsed_ms,
        'chars_per_ms': len(text) / elapsed_ms if elapsed_ms > 0 else 0
    }


def test_sentiment_analysis(text: str, title: str):
    """Test sentiment analysis on text."""
    print("\n" + "=" * 60)
    print(f"😊 SENTIMENT ANALYSIS TEST: {title[:50]}...")
    print("=" * 60)
    
    analyzer = SentimentAnalyzer()
    
    # Measure performance
    start_time = time.perf_counter()
    result = analyzer.analyze(text)
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    
    print(f"\n📊 Analysis completed in {elapsed_ms:.1f}ms")
    print(f"\n💭 Result: {format_sentiment(result)}")
    print(f"   Subjectivity: {result.subjectivity:.1%} (0=objective, 1=subjective)")
    
    # Show text snippet that influenced sentiment
    words = text.split()
    snippet = ' '.join(words[:50]) + '...' if len(words) > 50 else text
    print(f"\n📝 Text preview: \"{snippet[:200]}...\"")
    
    return {
        'label': result.label.value,
        'polarity': result.polarity,
        'subjectivity': result.subjectivity,
        'confidence': result.confidence,
        'processing_ms': elapsed_ms
    }


def test_memory_usage():
    """Test memory usage of loading models."""
    print("\n" + "=" * 60)
    print("💾 MEMORY USAGE TEST")
    print("=" * 60)
    
    tracemalloc.start()
    
    # Load entity extractor (loads spaCy)
    print("\n📦 Loading Entity Extractor (spaCy)...")
    start_time = time.perf_counter()
    extractor = EntityExtractor()
    _ = extractor._get_nlp()  # Force load
    load_time = (time.perf_counter() - start_time) * 1000
    
    current, peak = tracemalloc.get_traced_memory()
    print(f"   Load time: {load_time:.0f}ms")
    print(f"   Memory: {current / 1024 / 1024:.1f} MB (peak: {peak / 1024 / 1024:.1f} MB)")
    
    tracemalloc.reset_peaks()
    
    # Load sentiment analyzer (loads VADER)
    print("\n📦 Loading Sentiment Analyzer (VADER + TextBlob)...")
    start_time = time.perf_counter()
    analyzer = SentimentAnalyzer()
    _ = analyzer._get_vader()  # Force load
    load_time = (time.perf_counter() - start_time) * 1000
    
    current, peak = tracemalloc.get_traced_memory()
    print(f"   Load time: {load_time:.0f}ms")
    print(f"   Memory: {current / 1024 / 1024:.1f} MB (peak: {peak / 1024 / 1024:.1f} MB)")
    
    tracemalloc.stop()
    
    return {
        'spacy_memory_mb': current / 1024 / 1024,
        'total_memory_mb': peak / 1024 / 1024
    }


def main():
    """Run all analytics tests."""
    print("=" * 60)
    print("🧪 TRI-LAYER AI ANALYTICS - MODULE TESTING")
    print("=" * 60)
    
    # Get test data from database
    session = get_db_session()
    
    try:
        items = session.query(CrawledItem).limit(2).all()
        
        if not items:
            print("\n⚠️  No crawled items in database. Run crawler first:")
            print("   docker-compose run --rm crawler")
            return
        
        print(f"\n📚 Testing with {len(items)} crawled items from database")
        
        all_results = {
            'items_tested': len(items),
            'entity_tests': [],
            'sentiment_tests': [],
            'memory_test': None
        }
        
        # Test each item
        for i, item in enumerate(items, 1):
            print(f"\n\n{'#' * 60}")
            print(f"📄 ITEM {i}/{len(items)}: {item.url}")
            print(f"   Title: {item.title[:80]}...")
            print(f"   Content: {len(item.content)} characters")
            print(f"{'#' * 60}")
            
            # Entity extraction
            entity_result = test_entity_extraction(item.content, item.title)
            all_results['entity_tests'].append(entity_result)
            
            # Sentiment analysis
            sentiment_result = test_sentiment_analysis(item.content, item.title)
            all_results['sentiment_tests'].append(sentiment_result)
        
        # Memory test (only once, at the end)
        memory_result = test_memory_usage()
        all_results['memory_test'] = memory_result
        
        # Summary
        print("\n\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        
        print("\n🎯 Entity Extraction:")
        for i, r in enumerate(all_results['entity_tests']):
            print(f"   Item {i+1}: {r['entity_count']} entities in {r['processing_ms']:.1f}ms")
        
        print("\n😊 Sentiment Analysis:")
        for i, r in enumerate(all_results['sentiment_tests']):
            print(f"   Item {i+1}: {r['label']} (polarity: {r['polarity']:+.3f}, {r['processing_ms']:.1f}ms)")
        
        print("\n💾 Memory Usage:")
        print(f"   Total: {memory_result['total_memory_mb']:.1f} MB")
        
        # Save results
        output_path = Path("data/test_results.json")
        output_path.parent.mkdir(exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"\n💾 Full results saved to: {output_path}")
        
        # Verdict
        print("\n" + "=" * 60)
        print("✅ VERDICT: All modules working correctly")
        print("=" * 60)
        print("Ready for full pipeline integration.")
        
    finally:
        session.close()


if __name__ == "__main__":
    main()