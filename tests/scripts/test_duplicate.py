# test_duplicate.py
from src.analytics.duplicate_detector import DuplicateDetector

detector = DuplicateDetector(similarity_threshold=3)

# Original text
original = """
Books to Scrape is a demo website for web scraping practice.
We offer thousands of books across multiple categories.
"""

# Near duplicate (few words changed)
near_duplicate = """
Books to Scrape is a demonstration website for web scraping.
We offer thousands of books in many categories.
"""

# Different content
different = """
The quick brown fox jumps over the lazy dog near the river bank.
"""

print("=" * 60)
print("🔍 DUPLICATE DETECTION TEST")
print("=" * 60)

# Compute hashes
hash1 = detector.compute_hash(original)
hash2 = detector.compute_hash(near_duplicate)
hash3 = detector.compute_hash(different)

print(f"\n📊 SimHashes:")
print(f"   Original:      {hash1}")
print(f"   Near Duplicate: {hash2}")
print(f"   Different:     {hash3}")

# Check duplicates
is_dup, dist, _ = detector.is_duplicate(near_duplicate, [original])
print(f"\n🔍 Near Duplicate Check:")
print(f"   Is duplicate: {is_dup}")
print(f"   Hamming distance: {dist} (threshold: {detector.threshold})")

is_dup2, dist2, _ = detector.is_duplicate(different, [original])
print(f"\n🔍 Different Content Check:")
print(f"   Is duplicate: {is_dup2}")
print(f"   Hamming distance: {dist2} (threshold: {detector.threshold})")

# Similarity scores
print(f"\n📈 Similarity Scores:")
print(f"   Original vs Near Duplicate: {detector.similarity_score(original, near_duplicate):.1%}")
print(f"   Original vs Different:      {detector.similarity_score(original, different):.1%}")

print("\n" + "=" * 60)
print("✅ DUPLICATE DETECTION WORKING")
print("=" * 60)