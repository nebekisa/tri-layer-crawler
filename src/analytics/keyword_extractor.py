"""
Keyword extraction using YAKE (Yet Another Keyword Extractor).
Lightweight, unsupervised, multilingual.
"""

import logging
from typing import List, Optional
import yake

from src.analytics.models import KeywordResult

logger = logging.getLogger(__name__)


class KeywordExtractor:
    """
    Extract keywords from text using YAKE.
    
    Features:
        - Unsupervised (no training required)
        - Multilingual support
        - Configurable n-gram size
        - Deduplication with stemming
    """
    
    def __init__(
        self,
        language: str = "en",
        max_ngram_size: int = 3,
        deduplication_threshold: float = 0.9,
        num_keywords: int = 20
    ):
        """
        Initialize YAKE keyword extractor.
        
        Args:
            language: ISO 639-1 language code
            max_ngram_size: Maximum n-gram length (1-3)
            deduplication_threshold: Similarity threshold for deduplication
            num_keywords: Number of keywords to extract
        """
        self.language = language
        self.max_ngram_size = max_ngram_size
        self.deduplication_threshold = deduplication_threshold
        self.num_keywords = num_keywords
        
        # Initialize YAKE
        self._kw_extractor = yake.KeywordExtractor(
            lan=language,
            n=max_ngram_size,
            dedupLim=deduplication_threshold,
            top=num_keywords,
            features=None
        )
        
        logger.debug(f"YAKE initialized: lang={language}, ngram={max_ngram_size}")
    
    def extract(self, text: str, max_keywords: Optional[int] = None) -> List[KeywordResult]:
        """
        Extract keywords from text.
        
        Args:
            text: Input text to analyze
            max_keywords: Override number of keywords to return
            
        Returns:
            List of KeywordResult objects sorted by relevance
        """
        if not text or not text.strip():
            return []
        
        # Truncate for performance (YAKE handles long text well)
        max_chars = 50000
        if len(text) > max_chars:
            text = text[:max_chars]
        
        try:
            # Extract keywords
            keywords = self._kw_extractor.extract_keywords(text)
            
            # Convert to our model
            results = []
            for kw, score in keywords[:max_keywords or self.num_keywords]:
                # Determine n-gram size
                ngram = len(kw.split())
                
                # Normalize score (YAKE returns lower=better, we want higher=better)
                normalized_score = 1.0 / (1.0 + score)
                
                results.append(KeywordResult(
                    keyword=kw.strip(),
                    score=round(normalized_score, 4),
                    ngram=ngram
                ))
            
            logger.debug(f"Extracted {len(results)} keywords from {len(text)} chars")
            return results
            
        except Exception as e:
            logger.error(f"Keyword extraction failed: {e}")
            return []
    
    def extract_ngrams(self, text: str, ngram_size: int = 1) -> List[KeywordResult]:
        """
        Extract keywords of specific n-gram size.
        
        Args:
            text: Input text
            ngram_size: 1=unigrams, 2=bigrams, 3=trigrams
            
        Returns:
            Filtered keywords
        """
        keywords = self.extract(text)
        return [k for k in keywords if k.ngram == ngram_size]
    
    def get_top_keywords(self, text: str, n: int = 5) -> List[str]:
        """Convenience method to get top keyword strings only."""
        keywords = self.extract(text, max_keywords=n)
        return [k.keyword for k in keywords]