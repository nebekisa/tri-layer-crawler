"""
Sentiment analysis with model caching.
"""

import logging
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.analytics.models import SentimentResult, SentimentLabel
from src.analytics.model_cache import ModelCache

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """Hybrid sentiment analyzer with cached VADER model."""
    
    @staticmethod
    def _load_vader() -> SentimentIntensityAnalyzer:
        """Load VADER analyzer (called once by cache)."""
        logger.info("Loading VADER sentiment analyzer")
        return SentimentIntensityAnalyzer()
    
    def _get_vader(self) -> SentimentIntensityAnalyzer:
        """Get cached VADER analyzer."""
        cache = ModelCache()
        return cache.get_or_load('vader_sentiment', self._load_vader)
    
    def analyze(self, text: str) -> SentimentResult:
        """Analyze sentiment of text."""
        if not text or not text.strip():
            return SentimentResult(
                label=SentimentLabel.NEUTRAL,
                polarity=0.0,
                subjectivity=0.0,
                confidence=0.0
            )
        
        # TextBlob analysis
        blob = TextBlob(text[:5000])
        tb_polarity = blob.sentiment.polarity
        tb_subjectivity = blob.sentiment.subjectivity
        
        # VADER analysis
        vader = self._get_vader()
        vader_scores = vader.polarity_scores(text[:5000])
        vader_compound = vader_scores['compound']
        
        # Combine scores
        polarity = (tb_polarity * 0.4) + (vader_compound * 0.6)
        subjectivity = tb_subjectivity
        
        # Determine label
        if polarity > 0.1:
            label = SentimentLabel.POSITIVE
            confidence = min(1.0, abs(polarity) * 1.5)
        elif polarity < -0.1:
            label = SentimentLabel.NEGATIVE
            confidence = min(1.0, abs(polarity) * 1.5)
        else:
            label = SentimentLabel.NEUTRAL
            confidence = 1.0 - abs(polarity)
        
        confidence = confidence * (0.5 + subjectivity * 0.5)
        
        return SentimentResult(
            label=label,
            polarity=round(polarity, 4),
            subjectivity=round(subjectivity, 4),
            confidence=round(confidence, 4)
        )