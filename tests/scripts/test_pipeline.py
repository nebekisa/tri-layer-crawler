"""Test the complete analytics pipeline."""

from src.analytics.pipeline import AnalyticsPipeline

# Sample content
sample = {
    'id': 1,
    'url': 'https://books.toscrape.com',
    'title': 'All products | Books to Scrape - Sandbox',
    'content': """
    Books to Scrape is a demo website for web scraping practice.
    We offer thousands of books across multiple categories including
    Travel, Mystery, Historical Fiction, Sequential Art, Classics,
    Philosophy, Romance, Womens Fiction, Fiction, Childrens,
    Religion, Nonfiction, Music, Default, Science Fiction,
    Sports and Games, Fantasy, New Adult, Young Adult, Science,
    Poetry, Paranormal, Art, Psychology, Autobiography, Parenting,
    Adult Fiction, Humor, Horror, History, Food and Drink,
    Christian Fiction, Business, Biography, Thriller, Contemporary,
    Spirituality, Academic, Self Help, Historical, Political,
    Cultural, Travel, Mystery, Historical Fiction.
    """
}

print("=" * 70)
print("🧪 FULL ANALYTICS PIPELINE TEST")
print("=" * 70)

# Initialize pipeline
pipeline = AnalyticsPipeline()

# Run analysis
print(f"\n📊 Analyzing: {sample['title'][:50]}...")
result = pipeline.analyze(
    item_id=sample['id'],
    url=sample['url'],
    content=sample['content'],
    title=sample['title']
)

# Display results
print("\n" + "=" * 70)
print("📈 ANALYSIS RESULTS")
print("=" * 70)

print(f"\n🔍 Entities Found: {len(result.entities)}")
for entity in result.entities[:5]:
    print(f"   • {entity.text} ({entity.label.value})")

print(f"\n😊 Sentiment:")
print(f"   • Label: {result.sentiment.label.value}")
print(f"   • Polarity: {result.sentiment.polarity:+.3f}")
print(f"   • Confidence: {result.sentiment.confidence:.1%}")

print(f"\n🔑 Top Keywords:")
for kw in result.keywords[:5]:
    print(f"   • {kw.keyword} (score: {kw.score:.3f})")

print(f"\n📚 Readability:")
print(f"   • Flesch Reading Ease: {result.readability.flesch_reading_ease:.1f}")
print(f"   • Grade Level: {result.readability.flesch_kincaid_grade:.1f}")
print(f"   • Reading Time: {result.readability.reading_time_minutes:.2f} min")
print(f"   • Words: {result.readability.word_count}")

print(f"\n⚡ Performance:")
print(f"   • Processing Time: {result.processing_time_ms:.1f}ms")

print(f"\n🔐 Content Hash: {result.content_hash[:16]}...")

# Pipeline stats
print("\n" + "=" * 70)
print("📊 PIPELINE STATISTICS")
print("=" * 70)
stats = pipeline.get_stats()
print(f"\n   Cached Models: {stats['cached_models']['cached_models']}")
print(f"   Load Times: {stats['cached_models']['load_times_ms']}")

print("\n" + "=" * 70)
print("✅ FULL PIPELINE WORKING - ALL MODULES INTEGRATED")
print("=" * 70)