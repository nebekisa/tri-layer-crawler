"""
Main intelligence engine orchestrator.
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta

from src.intelligence.entity_correlator import EntityCorrelator
from src.intelligence.sentiment_tracker import SentimentTracker
from src.intelligence.models import IntelligenceReport
from src.database.manager import get_db_session
from src.database.models import CrawledItem, AnalysisResultDB, ExtractedEntityDB

logger = logging.getLogger(__name__)


class IntelligenceEngine:
    """
    Main intelligence engine orchestrating all analysis.
    
    Features:
        - Cross-source entity correlation
        - Sentiment trend analysis
        - Topic clustering
        - Anomaly detection
        - Insight generation
    """
    
    def __init__(self):
        """Initialize intelligence engine."""
        self.entity_correlator = EntityCorrelator()
        self.sentiment_tracker = SentimentTracker()
        
        logger.info("Intelligence Engine initialized")
    
    def generate_report(
        self,
        timeframe_hours: int = 24
    ) -> IntelligenceReport:
        """
        Generate complete intelligence report.
        
        Args:
            timeframe_hours: Analysis timeframe in hours
            
        Returns:
            Comprehensive intelligence report
        """
        logger.info(f"Generating intelligence report for last {timeframe_hours}h")
        
        # Fetch data from database
        session = get_db_session()
        
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=timeframe_hours)
            
            # Get crawled items with analysis
            items = session.query(CrawledItem).join(
                AnalysisResultDB
            ).filter(
                CrawledItem.crawled_at >= cutoff_time
            ).all()
            
            if not items:
                logger.warning("No data available for analysis")
                return self._empty_report(timeframe_hours)
            
            # Prepare data for correlation
            entities_by_source = []
            sentiment_history = {}
            
            for item in items:
                if not item.analysis:
                    continue
                
                # Collect entities
                entities = session.query(ExtractedEntityDB).filter(
                    ExtractedEntityDB.analysis_id == item.analysis.id
                ).all()
                
                entities_by_source.append({
                    'source_url': item.url,
                    'crawled_at': item.crawled_at,
                    'entities': entities
                })
                
                # Collect sentiment history
                for entity in entities:
                    if entity.entity_text not in sentiment_history:
                        sentiment_history[entity.entity_text] = []
                    
                    sentiment_history[entity.entity_text].append({
                        'timestamp': item.crawled_at,
                        'sentiment_score': item.analysis.sentiment_polarity
                    })
            
            # Run correlations
            entity_correlations = self.entity_correlator.correlate(entities_by_source)
            
            # Run sentiment trends
            sentiment_trends = []
            for entity_name, history in sentiment_history.items():
                trend = self.sentiment_tracker.track_entity_sentiment(
                    entity_name, history
                )
                if trend:
                    sentiment_trends.append(trend)
            
            # Detect significant shifts
            significant_shifts = self.sentiment_tracker.detect_significant_shifts(
                sentiment_trends
            )
            
            # Generate key findings
            key_findings = self._generate_findings(
                entity_correlations,
                significant_shifts,
                len(items)
            )
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                entity_correlations,
                significant_shifts
            )
            
            report = IntelligenceReport(
                generated_at=datetime.utcnow(),
                timeframe_hours=timeframe_hours,
                total_documents_analyzed=len(items),
                entity_correlations=entity_correlations[:20],  # Top 20
                sentiment_trends=significant_shifts[:10],      # Top 10 shifts
                topic_clusters=[],  # To be implemented
                anomalies=[],       # To be implemented
                key_findings=key_findings,
                recommendations=recommendations
            )
            
            logger.info(f"Report generated: {len(items)} docs, {len(entity_correlations)} correlations")
            
            return report
            
        finally:
            session.close()
    
    def _generate_findings(
        self,
        correlations: List,
        sentiment_shifts: List,
        total_docs: int
    ) -> List[str]:
        """Generate key findings from analysis."""
        findings = []
        
        # Overall stats
        findings.append(f"Analyzed {total_docs} documents in the reporting period")
        
        # Top correlations
        if correlations:
            top = correlations[0]
            findings.append(
                f"Entity '{top.entity_name}' appears across {len(top.sources)} sources "
                f"with {top.confidence:.0%} confidence"
            )
        
        # Sentiment shifts
        if sentiment_shifts:
            shift = sentiment_shifts[0]
            direction_word = 'improving' if shift.change_magnitude > 0 else 'declining'
            findings.append(
                f"Sentiment for '{shift.entity_or_topic}' is {direction_word} "
                f"(change: {shift.change_magnitude:+.3f})"
            )
        
        return findings
    
    def _generate_recommendations(
        self,
        correlations: List,
        sentiment_shifts: List
    ) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Recommendations based on sentiment
        negative_shifts = [s for s in sentiment_shifts if s.change_magnitude < -0.2]
        if negative_shifts:
            entities = [s.entity_or_topic for s in negative_shifts[:3]]
            recommendations.append(
                f"Monitor negative sentiment trends for: {', '.join(entities)}"
            )
        
        # Recommendations based on correlations
        if correlations:
            top_correlated = correlations[0]
            recommendations.append(
                f"Investigate cross-source presence of '{top_correlated.entity_name}'"
            )
        
        if not recommendations:
            recommendations.append("Continue regular monitoring cadence")
        
        return recommendations
    
    def _empty_report(self, timeframe_hours: int) -> IntelligenceReport:
        """Generate empty report when no data available."""
        return IntelligenceReport(
            generated_at=datetime.utcnow(),
            timeframe_hours=timeframe_hours,
            total_documents_analyzed=0,
            entity_correlations=[],
            sentiment_trends=[],
            topic_clusters=[],
            anomalies=[],
            key_findings=["No data available for analysis"],
            recommendations=["Run crawler to collect initial data"]
        )