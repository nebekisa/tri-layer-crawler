"""Unit tests for sentiment tracker."""

import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from intelligence.sentiment_tracker import SentimentTracker


class TestSentimentTracker:
    """Test sentiment tracking logic."""
    
    def test_track_entity_sentiment_insufficient_data(self):
        """Test tracking with insufficient data points."""
        tracker = SentimentTracker()
        
        history = [
            {'timestamp': datetime.utcnow(), 'sentiment_score': 0.5}
        ]
        
        result = tracker.track_entity_sentiment('TestEntity', history)
        assert result is None
    
    def test_track_entity_sentiment_stable(self):
        """Test tracking with stable sentiment."""
        tracker = SentimentTracker()
        
        now = datetime.utcnow()
        history = [
            {'timestamp': now - timedelta(hours=2), 'sentiment_score': 0.5},
            {'timestamp': now - timedelta(hours=1), 'sentiment_score': 0.52},
            {'timestamp': now, 'sentiment_score': 0.48},
        ]
        
        result = tracker.track_entity_sentiment('TestEntity', history)
        
        assert result is not None
        assert result.entity_or_topic == 'TestEntity'
        assert result.trend_direction in ['increasing', 'decreasing', 'stable']
        assert 0 <= result.confidence <= 1
    
    def test_track_entity_sentiment_increasing(self):
        """Test tracking with increasing sentiment."""
        tracker = SentimentTracker()
        
        now = datetime.utcnow()
        history = [
            {'timestamp': now - timedelta(hours=2), 'sentiment_score': -0.5},
            {'timestamp': now - timedelta(hours=1), 'sentiment_score': 0.0},
            {'timestamp': now, 'sentiment_score': 0.5},
        ]
        
        result = tracker.track_entity_sentiment('TestEntity', history)
        
        assert result is not None
        assert result.trend_direction == 'increasing'
        assert result.change_magnitude > 0
    
    def test_detect_significant_shifts(self):
        """Test detection of significant sentiment shifts."""
        tracker = SentimentTracker()
        
        # Create mock trends
        from intelligence.models import SentimentTrend
        
        trends = [
            SentimentTrend(
                entity_or_topic='Entity1',
                time_series=[],
                trend_direction='increasing',
                change_magnitude=0.5,
                confidence=0.8
            ),
            SentimentTrend(
                entity_or_topic='Entity2',
                time_series=[],
                trend_direction='stable',
                change_magnitude=0.05,
                confidence=0.9
            ),
        ]
        
        significant = tracker.detect_significant_shifts(trends, threshold=0.3)
        
        assert len(significant) == 1
        assert significant[0].entity_or_topic == 'Entity1'