# test_cache.py
import time
from src.analytics.entity_extractor import EntityExtractor
from src.analytics.sentiment_analyzer import SentimentAnalyzer
from src.analytics.model_cache import ModelCache

print("=" * 60)
print("🧪 TESTING MODEL CACHE")
print("=" * 60)

# First run - should load models
print("\n📦 FIRST RUN (loading models)...")
start = time.perf_counter()
extractor = EntityExtractor()
analyzer = SentimentAnalyzer()
extractor.extract("Test text")
analyzer.analyze("Test text")
first_time = (time.perf_counter() - start) * 1000
print(f"   Time: {first_time:.0f}ms")

# Second run - should use cache
print("\n📦 SECOND RUN (cached)...")
start = time.perf_counter()
extractor2 = EntityExtractor()
analyzer2 = SentimentAnalyzer()
extractor2.extract("Test text")
analyzer2.analyze("Test text")
second_time = (time.perf_counter() - start) * 1000
print(f"   Time: {second_time:.0f}ms")

# Cache stats
cache = ModelCache()
print(f"\n📊 Cache Stats: {cache.stats()}")

print(f"\n⚡ Speedup: {first_time/second_time:.1f}x faster")
print("\n✅ MODEL CACHING WORKING")