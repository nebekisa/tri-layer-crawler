"""
Statistical anomaly detection for crawled data.
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np

from src.intelligence.models import Anomaly, AlertSeverity

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    Detect anomalies in crawling patterns and analytics.
    
    Features:
        - Volume anomaly detection (Z-score)
        - Sentiment shift detection
        - New entity detection
        - Keyword spike detection
    """
    
    def __init__(self, sensitivity: float = 2.0):
        """
        Initialize detector.
        
        Args:
            sensitivity: Z-score threshold for anomalies (lower = more sensitive)
        """
        self.sensitivity = sensitivity
    
    def detect_volume_anomalies(
        self,
        hourly_volumes: List[Dict],
        lookback_hours: int = 24
    ) -> List[Anomaly]:
        """
        Detect anomalous changes in crawl volume.
        
        Args:
            hourly_volumes: List of {'hour': datetime, 'count': int}
            lookback_hours: Hours to analyze
            
        Returns:
            List of volume anomalies
        """
        if len(hourly_volumes) < 3:
            return []
        
        anomalies = []
        
        # Extract counts
        counts = np.array([v['count'] for v in hourly_volumes])
        
        # Calculate baseline (mean of all except current)
        for i in range(1, len(hourly_volumes)):
            baseline = counts[:i].mean()
            std = counts[:i].std() if counts[:i].std() > 0 else 1
            
            current = counts[i]
            z_score = (current - baseline) / std
            
            if abs(z_score) > self.sensitivity:
                severity = self._determine_severity(abs(z_score))
                
                anomaly = Anomaly(
                    anomaly_type='volume',
                    description=f"Crawl volume {'spike' if z_score > 0 else 'drop'}: {current} vs baseline {baseline:.1f}",
                    severity=severity,
                    detected_at=hourly_volumes[i]['hour'],
                    affected_entities=[],
                    expected_value=float(baseline),
                    actual_value=float(current),
                    deviation_percent=float((current - baseline) / baseline * 100)
                )
                
                anomalies.append(anomaly)
        
        logger.info(f"Detected {len(anomalies)} volume anomalies")
        
        return anomalies
    
    def detect_new_entities(
        self,
        current_entities: List[str],
        historical_entities: List[str],
        threshold: int = 3
    ) -> List[Anomaly]:
        """
        Detect new entities that haven't appeared before.
        
        Args:
            current_entities: Entities from current crawl
            historical_entities: Entities from historical data
            threshold: Min occurrences to consider significant
            
        Returns:
            List of new entity anomalies
        """
        historical_set = set(historical_entities)
        
        from collections import Counter
        current_counter = Counter(current_entities)
        
        anomalies = []
        
        for entity, count in current_counter.items():
            if entity not in historical_set and count >= threshold:
                severity = AlertSeverity.LOW if count < 5 else AlertSeverity.MEDIUM
                
                anomaly = Anomaly(
                    anomaly_type='new_entity',
                    description=f"New entity detected: {entity} ({count} occurrences)",
                    severity=severity,
                    detected_at=datetime.utcnow(),
                    affected_entities=[entity],
                    expected_value=0.0,
                    actual_value=float(count),
                    deviation_percent=100.0
                )
                
                anomalies.append(entity_anomaly)
        
        logger.info(f"Detected {len(anomalies)} new entities")
        
        return anomalies
    
    def detect_sentiment_anomalies(
        self,
        sentiment_history: Dict[str, List[float]],
        threshold: float = 0.4
    ) -> List[Anomaly]:
        """
        Detect anomalous sentiment shifts.
        
        Args:
            sentiment_history: {entity: [historical_scores]}
            threshold: Minimum change to be anomalous
            
        Returns:
            List of sentiment anomalies
        """
        anomalies = []
        
        for entity, scores in sentiment_history.items():
            if len(scores) < 3:
                continue
            
            # Calculate trend
            recent_avg = np.mean(scores[-3:])
            historical_avg = np.mean(scores[:-3]) if len(scores) > 3 else recent_avg
            
            change = recent_avg - historical_avg
            
            if abs(change) > threshold:
                direction = 'improved' if change > 0 else 'declined'
                severity = self._determine_severity(abs(change) * 2)
                
                anomaly = Anomaly(
                    anomaly_type='sentiment',
                    description=f"Sentiment for '{entity}' has {direction} significantly",
                    severity=severity,
                    detected_at=datetime.utcnow(),
                    affected_entities=[entity],
                    expected_value=float(historical_avg),
                    actual_value=float(recent_avg),
                    deviation_percent=float(change * 100)
                )
                
                anomalies.append(anomaly)
        
        return anomalies
    
    def _determine_severity(self, z_score: float) -> AlertSeverity:
        """Determine severity based on Z-score."""
        if z_score > 5:
            return AlertSeverity.CRITICAL
        elif z_score > 4:
            return AlertSeverity.HIGH
        elif z_score > 3:
            return AlertSeverity.MEDIUM
        elif z_score > 2:
            return AlertSeverity.LOW
        else:
            return AlertSeverity.INFO