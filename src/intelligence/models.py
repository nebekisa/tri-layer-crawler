"""
Intelligence layer data models.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Set
from datetime import datetime, timedelta
from enum import Enum


class InsightType(str, Enum):
    """Types of generated insights."""
    ENTITY_NETWORK = "entity_network"
    SENTIMENT_SHIFT = "sentiment_shift"
    TRENDING_TOPIC = "trending_topic"
    VOLUME_ANOMALY = "volume_anomaly"
    NEW_ENTITY = "new_entity"
    CORRELATION = "correlation"


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class EntityCorrelation:
    """Correlation between entities across sources."""
    entity_name: str
    entity_type: str
    sources: List[str]  # URLs where entity appears
    confidence: float
    first_seen: datetime
    last_seen: datetime
    occurrence_count: int
    related_entities: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'entity_name': self.entity_name,
            'entity_type': self.entity_type,
            'sources': self.sources,
            'confidence': self.confidence,
            'first_seen': self.first_seen.isoformat(),
            'last_seen': self.last_seen.isoformat(),
            'occurrence_count': self.occurrence_count,
            'related_entities': self.related_entities
        }


@dataclass
class SentimentTrend:
    """Sentiment trend over time."""
    entity_or_topic: str
    time_series: List[Dict[str, Any]]  # [{timestamp: ..., sentiment: ...}]
    trend_direction: str  # 'increasing', 'decreasing', 'stable'
    change_magnitude: float
    confidence: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'entity_or_topic': self.entity_or_topic,
            'time_series': self.time_series,
            'trend_direction': self.trend_direction,
            'change_magnitude': self.change_magnitude,
            'confidence': self.confidence
        }


@dataclass
class TopicCluster:
    """Cluster of related topics/keywords."""
    cluster_id: int
    primary_topic: str
    keywords: List[str]
    document_count: int
    representative_docs: List[str]  # URLs
    coherence_score: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'cluster_id': self.cluster_id,
            'primary_topic': self.primary_topic,
            'keywords': self.keywords,
            'document_count': self.document_count,
            'representative_docs': self.representative_docs,
            'coherence_score': self.coherence_score
        }


@dataclass
class Anomaly:
    """Detected anomaly in data patterns."""
    anomaly_type: str  # 'volume', 'sentiment', 'entity', 'keyword'
    description: str
    severity: AlertSeverity
    detected_at: datetime
    affected_entities: List[str]
    expected_value: float
    actual_value: float
    deviation_percent: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'anomaly_type': self.anomaly_type,
            'description': self.description,
            'severity': self.severity.value,
            'detected_at': self.detected_at.isoformat(),
            'affected_entities': self.affected_entities,
            'expected_value': self.expected_value,
            'actual_value': self.actual_value,
            'deviation_percent': self.deviation_percent
        }


@dataclass
class IntelligenceReport:
    """Complete intelligence report."""
    generated_at: datetime
    timeframe_hours: int
    total_documents_analyzed: int
    
    # Insights
    entity_correlations: List[EntityCorrelation]
    sentiment_trends: List[SentimentTrend]
    topic_clusters: List[TopicCluster]
    anomalies: List[Anomaly]
    
    # Summary
    key_findings: List[str]
    recommendations: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'generated_at': self.generated_at.isoformat(),
            'timeframe_hours': self.timeframe_hours,
            'total_documents_analyzed': self.total_documents_analyzed,
            'entity_correlations': [e.to_dict() for e in self.entity_correlations],
            'sentiment_trends': [s.to_dict() for s in self.sentiment_trends],
            'topic_clusters': [t.to_dict() for t in self.topic_clusters],
            'anomalies': [a.to_dict() for a in self.anomalies],
            'key_findings': self.key_findings,
            'recommendations': self.recommendations
        }