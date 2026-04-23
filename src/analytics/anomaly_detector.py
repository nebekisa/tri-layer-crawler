"""
Multi-dimensional anomaly detection for crawled content.
Fixed with correct schema column names.
"""

import logging
import math
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from collections import defaultdict, deque
import statistics

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Statistical anomaly detection for crawled content."""
    
    def __init__(self, window_size: int = 100, threshold: float = 2.5):
        self.window_size = window_size
        self.threshold = threshold
        
        self.content_lengths: deque = deque(maxlen=window_size)
        self.sentiment_scores: deque = deque(maxlen=window_size)
        self.entity_counts: deque = deque(maxlen=window_size)
        
        self.entity_first_seen: Dict[str, datetime] = {}
        self.entity_frequency: Dict[str, int] = defaultdict(int)
    
    def calculate_z_score(self, value: float, values: deque) -> Optional[float]:
        """Calculate z-score for a value against baseline."""
        if len(values) < 3:
            return None
        
        mean = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 1.0
        
        if std == 0:
            return 0.0
        
        return abs(value - mean) / std
    
    def detect_content_anomaly(self, content_length: int, entity_count: int) -> Dict:
        """Detect anomalies in content features."""
        anomalies = []
        scores = {}
        
        z_score_len = self.calculate_z_score(content_length, self.content_lengths)
        if z_score_len is not None:
            scores['content_length_zscore'] = round(z_score_len, 3)
            if z_score_len > self.threshold:
                anomalies.append({
                    'type': 'content_length',
                    'severity': 'high' if z_score_len > 4 else 'medium',
                    'z_score': z_score_len,
                    'value': content_length
                })
        
        z_score_ent = self.calculate_z_score(entity_count, self.entity_counts)
        if z_score_ent is not None:
            scores['entity_count_zscore'] = round(z_score_ent, 3)
            if z_score_ent > self.threshold:
                anomalies.append({
                    'type': 'entity_count',
                    'severity': 'high' if z_score_ent > 4 else 'medium',
                    'z_score': z_score_ent,
                    'value': entity_count
                })
        
        return {
            'anomalies': anomalies,
            'scores': scores,
            'is_anomaly': len(anomalies) > 0
        }
    
    def detect_sentiment_anomaly(self, sentiment_score: float) -> Dict:
        """Detect anomalies in sentiment."""
        z_score = self.calculate_z_score(sentiment_score, self.sentiment_scores)
        
        if z_score is None:
            return {'is_anomaly': False, 'z_score': None}
        
        is_anomaly = z_score > self.threshold
        
        return {
            'is_anomaly': is_anomaly,
            'z_score': round(z_score, 3),
            'severity': 'high' if z_score > 4 else 'medium' if is_anomaly else 'none',
            'value': sentiment_score
        }
    
    def analyze_item(self, item: Dict) -> Dict:
        """Comprehensive anomaly analysis for a single item."""
        content_anomaly = self.detect_content_anomaly(
            item.get('content_length', 0),
            len(item.get('entities', []))
        )
        
        sentiment_anomaly = self.detect_sentiment_anomaly(
            item.get('sentiment_score', 0)
        )
        
        anomaly_score = 0
        if content_anomaly['is_anomaly']:
            anomaly_score += 0.5
        if sentiment_anomaly['is_anomaly']:
            anomaly_score += 0.5
        
        is_anomaly = anomaly_score >= 0.5
        
        severity = 'none'
        if anomaly_score >= 0.8:
            severity = 'high'
        elif anomaly_score >= 0.5:
            severity = 'medium'
        
        return {
            'is_anomaly': is_anomaly,
            'severity': severity,
            'anomaly_score': round(anomaly_score, 2),
            'content_anomaly': content_anomaly,
            'sentiment_anomaly': sentiment_anomaly,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def build_baseline_from_database(self, limit: int = 100) -> Dict:
        """Build baseline from historical data using correct schema."""
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        # Get content lengths from crawled_items
        cur.execute('''
            SELECT content_length
            FROM crawled_items
            WHERE content_length IS NOT NULL
            ORDER BY id DESC
            LIMIT %s
        ''', (limit,))
        
        for row in cur.fetchall():
            if row[0]:
                self.content_lengths.append(row[0])
        
        # Get sentiment scores from analysis_results
        cur.execute('''
            SELECT sentiment_polarity
            FROM analysis_results
            WHERE sentiment_polarity IS NOT NULL
            ORDER BY id DESC
            LIMIT %s
        ''', (limit,))
        
        for row in cur.fetchall():
            if row[0] is not None:
                self.sentiment_scores.append(row[0])
        
        # Get entity counts per analysis_id
        cur.execute('''
            SELECT analysis_id, COUNT(*) as entity_count
            FROM extracted_entities
            GROUP BY analysis_id
            ORDER BY analysis_id DESC
            LIMIT %s
        ''', (limit,))
        
        for row in cur.fetchall():
            self.entity_counts.append(row[1])
        
        cur.close()
        conn.close()
        
        return {
            'baseline_size': {
                'content_length': len(self.content_lengths),
                'sentiment': len(self.sentiment_scores),
                'entity_counts': len(self.entity_counts)
            },
            'ready': len(self.content_lengths) >= 3
        }
    
    def get_stats(self) -> Dict:
        """Get detector statistics."""
        return {
            'window_size': self.window_size,
            'threshold': self.threshold,
            'baseline_sizes': {
                'content_length': len(self.content_lengths),
                'sentiment': len(self.sentiment_scores),
                'entity_counts': len(self.entity_counts)
            },
            'content_mean': statistics.mean(self.content_lengths) if self.content_lengths else 0,
            'entities_tracked': len(self.entity_first_seen)
        }


_detector = AnomalyDetector()


def get_detector() -> AnomalyDetector:
    return _detector
