# test_readability.py
from src.analytics.readability_metrics import ReadabilityAnalyzer

# Test texts with different complexity
texts = {
    "Simple": "The cat sat on the mat. It was a nice day. The sun was warm.",
    "Standard": "Books to Scrape is a demo website for web scraping practice. We offer thousands of books across multiple categories.",
    "Complex": "The epistemological foundations of postmodernist discourse necessitate a paradigmatic shift in our conceptualization of linguistic signifiers."
}

analyzer = ReadabilityAnalyzer()

print("=" * 60)
print("📚 READABILITY METRICS TEST")
print("=" * 60)

for name, text in texts.items():
    print(f"\n{'─' * 60}")
    print(f"📄 {name} Text")
    print(f"{'─' * 60}")
    
    metrics = analyzer.analyze(text)
    
    print(f"   Flesch Reading Ease: {metrics.flesch_reading_ease}")
    print(f"   Grade Level: {metrics.flesch_kincaid_grade}")
    print(f"   Reading Time: {metrics.reading_time_minutes} min")
    print(f"   Words: {metrics.word_count}, Sentences: {metrics.sentence_count}")
    print(f"   Level: {analyzer.get_reading_level_summary(metrics)}")
    print(f"   Difficulty: {analyzer.get_difficulty_label(metrics)}")

print("\n" + "=" * 60)
print("✅ READABILITY METRICS WORKING")
print("=" * 60)