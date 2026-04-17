"""
Run AI analytics on all crawled items - FIXED VERSION.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.manager import get_db_session
from src.database.models import CrawledItem, AnalysisResultDB, ExtractedEntityDB, ExtractedKeywordDB
from src.analytics.pipeline import AnalyticsPipeline

print("=" * 60)
print("🧠 RUNNING AI ANALYTICS ON CRAWLED DATA")
print("=" * 60)

session = get_db_session()
pipeline = AnalyticsPipeline()

# Get all items without analysis
items = session.query(CrawledItem).all()
print(f"\n📊 Found {len(items)} items to analyze")

for item in items:
    print(f"\n📄 Analyzing: {item.title[:50]}...")
    
    # Check if already analyzed
    existing = session.query(AnalysisResultDB).filter(
        AnalysisResultDB.item_id == item.id
    ).first()
    
    if existing:
        print(f"   ⏭️ Already analyzed - deleting old analysis...")
        session.delete(existing)
        session.flush()
    
    # Run full analysis
    try:
        analysis = pipeline.analyze(
            item_id=item.id,
            url=item.url,
            content=item.content,
            title=item.title
        )
    except Exception as e:
        print(f"   ❌ Analysis failed: {e}")
        continue
    
    # Save to database - FIXED: Convert all numpy types to Python types
    db_analysis = AnalysisResultDB(
        item_id=item.id,
        sentiment_label=str(analysis.sentiment.label.value),
        sentiment_polarity=float(analysis.sentiment.polarity),
        sentiment_subjectivity=float(analysis.sentiment.subjectivity),
        sentiment_confidence=float(analysis.sentiment.confidence),
        primary_topic=str(analysis.topics.primary_topic) if analysis.topics else None,
        topic_confidence=float(analysis.topics.confidence) if analysis.topics and analysis.topics.confidence else None,
        flesch_reading_ease=float(analysis.readability.flesch_reading_ease),
        flesch_kincaid_grade=float(analysis.readability.flesch_kincaid_grade),
        reading_time_minutes=float(analysis.readability.reading_time_minutes),
        word_count=int(analysis.readability.word_count),
        sentence_count=int(analysis.readability.sentence_count),
        content_hash=str(analysis.content_hash),
        processing_time_ms=float(analysis.processing_time_ms)
    )
    
    session.add(db_analysis)
    session.flush()
    
    # Save entities - FIXED: Convert to Python types
    for entity in analysis.entities:
        db_entity = ExtractedEntityDB(
            analysis_id=int(db_analysis.id),
            entity_text=str(entity.text),
            entity_type=str(entity.label.value),
            confidence=float(entity.confidence)
        )
        session.add(db_entity)
    
    # Save keywords - FIXED: Convert numpy types to Python types
    for keyword in analysis.keywords:
        db_keyword = ExtractedKeywordDB(
            analysis_id=int(db_analysis.id),
            keyword=str(keyword.keyword),
            score=float(keyword.score),  # CRITICAL: Convert np.float64 to float
            ngram=int(keyword.ngram)      # CRITICAL: Convert np.int64 to int
        )
        session.add(db_keyword)
    
    try:
        session.commit()
        print(f"   ✅ Analyzed: {len(analysis.entities)} entities, "
              f"{analysis.sentiment.label.value} sentiment, "
              f"{len(analysis.keywords)} keywords")
    except Exception as e:
        session.rollback()
        print(f"   ❌ Save failed: {e}")

# Summary
total_analyzed = session.query(AnalysisResultDB).count()
total_entities = session.query(ExtractedEntityDB).count()
total_keywords = session.query(ExtractedKeywordDB).count()

print("\n" + "=" * 60)
print("📊 ANALYTICS COMPLETE")
print("=" * 60)
print(f"✅ Items analyzed: {total_analyzed}")
print(f"✅ Entities extracted: {total_entities}")
print(f"✅ Keywords extracted: {total_keywords}")

session.close()