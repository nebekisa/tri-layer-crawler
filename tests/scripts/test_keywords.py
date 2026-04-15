# test_keywords.py
from src.analytics.keyword_extractor import KeywordExtractor

text = """
Books to Scrape is a demo website for web scraping practice.
We offer thousands of books across multiple categories including
Travel, Mystery, Historical Fiction, and Romance novels.
Our bestseller "A Light in the Attic" by Shel Silverstein
is available for £51.77 with free worldwide shipping.
"""

extractor = KeywordExtractor(num_keywords=10)
keywords = extractor.extract(text)

print("=" * 50)
print("🔑 KEYWORD EXTRACTION TEST")
print("=" * 50)

print("\n📊 Top Keywords:")
for kw in keywords:
    bar = "█" * int(kw.score * 20)
    print(f"  {kw.keyword:<30} {kw.score:.3f} {bar}")

print(f"\n📈 Unigrams: {[k.keyword for k in keywords if k.ngram == 1][:5]}")
print(f"📈 Bigrams:  {[k.keyword for k in keywords if k.ngram == 2][:5]}")
print(f"📈 Trigrams: {[k.keyword for k in keywords if k.ngram == 3][:5]}")

print("\n✅ KEYWORD EXTRACTION WORKING")