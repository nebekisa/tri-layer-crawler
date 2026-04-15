"""
Unified analytics pipeline for processing crawled content.
Orchestrates all AI modules in a single efficient pass.
"""

import logging
import time
from typing import Optional, Dict, Any
from datetime import datetime

from src.analytics.entity_extractor import EntityExtractor
from src.analytics.sentiment_analyzer import SentimentAnalyzer
from src.analytics.keyword_extractor import KeywordExtractor
from src.analytics.readability_metrics import ReadabilityAnalyzer
from src.analytics.duplicate_detector import DuplicateDetector
from src.analytics.models import AnalysisResult

logger = logging.getLogger(__name__)


class AnalyticsPipeline:
    """
    Unified pipeline for all AI analytics.
    
    Features:
        - Single-pass processing
        - Performance timing per module
        - Graceful degradation (one module fails, others continue)
        - Cached models for efficiency
    """
    
    def __init__(self):
        """Initialize all analyzers with cached models."""
        logger.info("Initializing Analytics Pipeline...")
        
        self.entity_extractor = EntityExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.keyword_extractor = KeywordExtractor(num_keywords=15)
        self.readability_analyzer = ReadabilityAnalyzer()
        self.duplicate_detector = DuplicateDetector(similarity_threshold=3)
        
        logger.info("Analytics Pipeline ready")
    
    def analyze(self, item_id: int, url: str, content: str, title: str = "") -> AnalysisResult:
        """
        Run complete analysis on a single content item.
        
        Args:
            item_id: Database ID of crawled item
            url: Source URL
            content: Main text content
            title: Optional page title
            
        Returns:
            Complete AnalysisResult with all metrics
        """
        start_time = time.perf_counter()
        timings = {}
        
        # Combine title and content for better analysis
        full_text = f"{title}\n\n{content}" if title else content
        
        # 1. Entity Extraction
        try:
            t1 = time.perf_counter()
            entities = self.entity_extractor.extract(full_text, max_entities=30)
            timings['entities_ms'] = (time.perf_counter() - t1) * 1000
        except Exception as e:
            logger.error(f"Entity extraction failed: {e}")
            entities = []
            timings['entities_ms'] = 0
        
        # 2. Sentiment Analysis
        try:
            t2 = time.perf_counter()
            sentiment = self.sentiment_analyzer.analyze(full_text)
            timings['sentiment_ms'] = (time.perf_counter() - t2) * 1000
        except Exception as e:
            logger.error(f"Sentiment analysis failed: {e}")
            from src.analytics.models import SentimentResult, SentimentLabel
            sentiment = SentimentLabel.NEUTRAL
            timings['sentiment_ms'] = 0
        
        # 3. Keyword Extraction
        try:
            t3 = time.perf_counter()
            keywords = self.keyword_extractor.extract(full_text, max_keywords=15)
            timings['keywords_ms'] = (time.perf_counter() - t3) * 1000
        except Exception as e:
            logger.error(f"Keyword extraction failed: {e}")
            keywords = []
            timings['keywords_ms'] = 0
        
        # 4. Readability Metrics
        try:
            t4 = time.perf_counter()
            readability = self.readability_analyzer.analyze(full_text)
            timings['readability_ms'] = (time.perf_counter() - t4) * 1000
        except Exception as e:
            logger.error(f"Readability analysis failed: {e}")
            readability = self.readability_analyzer._empty_metrics()
            timings['readability_ms'] = 0
        
        # 5. Content Hash (for duplicate detection)
        try:
            t5 = time.perf_counter()
            content_hash = self.duplicate_detector.compute_hash(full_text)
            timings['hash_ms'] = (time.perf_counter() - t5) * 1000
        except Exception as e:
            logger.error(f"Hash computation failed: {e}")
            content_hash = ""
            timings['hash_ms'] = 0
        
        total_ms = (time.perf_counter() - start_time) * 1000
        
        logger.info(
            f"Analysis complete for item {item_id}: "
            f"{len(entities)} entities, "
            f"{sentiment.label.value} sentiment, "
            f"{len(keywords)} keywords, "
            f"grade {readability.flesch_kincaid_grade:.1f}, "
            f"{total_ms:.1f}ms total"
        )
        
        return AnalysisResult(
            item_id=item_id,
            url=url,
            entities=entities,
            sentiment=sentiment,
            topics=None,  # To be added later
            keywords=keywords,
            readability=readability,
            content_hash=content_hash,
            analyzed_at=datetime.utcnow(),
            processing_time_ms=total_ms
        )
    
    def analyze_batch(self, items: list) -> list:
        """
        Analyze multiple items efficiently.
        
        Args:
            items: List of (item_id, url, content, title) tuples
            
        Returns:
            List of AnalysisResult objects
        """
        results = []
        for item_id, url, content, title in items:
            try:
                result = self.analyze(item_id, url, content, title)
                results.append(result)
            except Exception as e:
                logger.error(f"Batch analysis failed for item {item_id}: {e}")
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        from src.analytics.model_cache import ModelCache
        cache = ModelCache()
        
        return {
            'cached_models': cache.stats(),
            'modules': {
                'entity_extraction': True,
                'sentiment_analysis': True,
                'keyword_extraction': True,
                'readability': True,
                'duplicate_detection': True
            }
        }