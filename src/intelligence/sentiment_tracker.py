"""
Sentiment trend analysis over time.
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np

from src.intelligence.models import SentimentTrend

logger = logging.getLogger(__name__)


class SentimentTracker:
    """
    Track sentiment changes over time for entities and topics.
    
    Features:
        - Time series sentiment analysis
        - Trend direction detection
        - Significant shift detection
        - Moving average smoothing
    """
    
    def __init__(self, window_hours: int = 24):
        """
        Initialize tracker.
        
        Args:
            window_hours: Time window for trend analysis
        """
        self.window_hours = window_hours
    
    def track_entity_sentiment(
        self,
        entity_name: str,
        sentiment_history: List[Dict]
    ) -> Optional[SentimentTrend]:
        """
        Track sentiment for a specific entity over time.
        
        Args:
            entity_name: Entity to track
            sentiment_history: List of {timestamp, sentiment_score}
            
        Returns:
            SentimentTrend or None if insufficient data
        """
        if len(sentiment_history) < 3:
            return None
        
        # Sort by timestamp
        history = sorted(sentiment_history, key=lambda x: x['timestamp'])
        
        # Extract time series
        time_series = [
            {
                'timestamp': h['timestamp'].isoformat(),
                'sentiment': h['sentiment_score']
            }
            for h in history
        ]
        
        # Calculate trend
        scores = [h['sentiment_score'] for h in history]
        
        # Linear regression for trend
        x = np.arange(len(scores))
        y = np.array(scores)
        
        if len(x) > 1:
            slope, intercept = np.polyfit(x, y, 1)
            change_magnitude = slope * len(scores)
        else:
            slope = 0
            change_magnitude = 0
        
        # Determine direction
        if change_magnitude > 0.1:
            direction = 'increasing'
        elif change_magnitude < -0.1:
            direction = 'decreasing'
        else:
            direction = 'stable'
        
        # Calculate confidence based on data points and variance
        variance = np.var(scores) if len(scores) > 1 else 0
        confidence = min(1.0, len(scores) / 10) * (1.0 - min(variance, 0.5))
        
        return SentimentTrend(
            entity_or_topic=entity_name,
            time_series=time_series,
            trend_direction=direction,
            change_magnitude=round(change_magnitude, 3),
            confidence=round(confidence, 3)
        )
    
    def detect_significant_shifts(
        self,
        trends: List[SentimentTrend],
        threshold: float = 0.3
    ) -> List[SentimentTrend]:
        """
        Filter trends with significant sentiment shifts.
        
        Args:
            trends: List of sentiment trends
            threshold: Minimum change magnitude to be significant
            
        Returns:
            Trends with significant shifts
        """
        significant = [
            t for t in trends
            if abs(t.change_magnitude) >= threshold and t.confidence >= 0.6
        ]
        
        # Sort by change magnitude
        significant.sort(key=lambda x: abs(x.change_magnitude), reverse=True)
        
        logger.info(f"Detected {len(significant)} significant sentiment shifts")
        
        return significant