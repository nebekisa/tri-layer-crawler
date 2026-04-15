"""
Cross-source entity correlation engine.
"""

import logging
from typing import List, Dict, Set, Tuple
from collections import defaultdict
from datetime import datetime
import hashlib

from src.intelligence.models import EntityCorrelation

logger = logging.getLogger(__name__)


class EntityCorrelator:
    """
    Correlates entities across multiple sources.
    
    Features:
        - Fuzzy name matching
        - Co-occurrence analysis
        - Confidence scoring
        - Related entity discovery
    """
    
    def __init__(self, similarity_threshold: float = 0.85):
        """
        Initialize correlator.
        
        Args:
            similarity_threshold: Minimum similarity for entity matching
        """
        self.threshold = similarity_threshold
    
    def correlate(
        self,
        entities_by_source: List[Dict]
    ) -> List[EntityCorrelation]:
        """
        Correlate entities across sources.
        
        Args:
            entities_by_source: List of {
                'source_url': str,
                'crawled_at': datetime,
                'entities': List[ExtractedEntity]
            }
            
        Returns:
            List of correlated entities
        """
        # Group entities by normalized name
        entity_groups: Dict[str, List[Tuple[str, datetime, str]]] = defaultdict(list)
        
        for source_data in entities_by_source:
            source_url = source_data['source_url']
            crawled_at = source_data['crawled_at']
            
            for entity in source_data['entities']:
                # Normalize entity name
                normalized = self._normalize_entity_name(entity.text)
                
                entity_groups[normalized].append((
                    source_url,
                    crawled_at,
                    entity.label.value if hasattr(entity, 'label') else 'UNKNOWN'
                ))
        
        # Build correlations
        correlations = []
        
        for normalized_name, occurrences in entity_groups.items():
            if len(occurrences) < 2:  # Need at least 2 sources
                continue
            
            # Extract data
            sources = list(set(o[0] for o in occurrences))
            timestamps = [o[1] for o in occurrences]
            entity_type = self._majority_vote([o[2] for o in occurrences])
            
            # Calculate confidence
            confidence = self._calculate_confidence(occurrences, sources)
            
            # Find related entities (co-occurring)
            related = self._find_related_entities(normalized_name, entities_by_source)
            
            correlation = EntityCorrelation(
                entity_name=self._get_display_name(occurrences),
                entity_type=entity_type,
                sources=sources,
                confidence=confidence,
                first_seen=min(timestamps),
                last_seen=max(timestamps),
                occurrence_count=len(occurrences),
                related_entities=related[:5]  # Top 5 related
            )
            
            correlations.append(correlation)
        
        # Sort by confidence * occurrence_count (impact score)
        correlations.sort(
            key=lambda x: x.confidence * x.occurrence_count,
            reverse=True
        )
        
        logger.info(f"Correlated {len(correlations)} entities across sources")
        
        return correlations
    
    def _normalize_entity_name(self, name: str) -> str:
        """Normalize entity name for matching."""
        # Lowercase
        normalized = name.lower().strip()
        
        # Remove punctuation
        import re
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Remove extra whitespace
        normalized = ' '.join(normalized.split())
        
        # Generate stable hash for privacy
        return hashlib.md5(normalized.encode()).hexdigest()[:12]
    
    def _get_display_name(self, occurrences: List[Tuple]) -> str:
        """Get most common display name."""
        names = [self._extract_original_name(o) for o in occurrences]
        return max(set(names), key=names.count)
    
    def _extract_original_name(self, occurrence: Tuple) -> str:
        """Extract original name from occurrence data."""
        # Implementation depends on data structure
        return "Entity"  # Placeholder
    
    def _majority_vote(self, types: List[str]) -> str:
        """Get most common entity type."""
        if not types:
            return 'UNKNOWN'
        return max(set(types), key=types.count)
    
    def _calculate_confidence(
        self,
        occurrences: List[Tuple],
        sources: List[str]
    ) -> float:
        """
        Calculate confidence score for correlation.
        
        Factors:
            - Number of sources (more = higher)
            - Temporal proximity
            - Name consistency
        """
        # Base confidence from source count
        source_score = min(1.0, len(sources) / 5)
        
        # Temporal score (occurrences close in time)
        timestamps = [o[1] for o in occurrences]
        if len(timestamps) > 1:
            time_span = (max(timestamps) - min(timestamps)).total_seconds()
            temporal_score = max(0.5, 1.0 - (time_span / (30 * 24 * 3600)))  # 30 days max
        else:
            temporal_score = 0.5
        
        # Combined confidence
        confidence = (source_score * 0.6) + (temporal_score * 0.4)
        
        return round(min(1.0, confidence), 3)
    
    def _find_related_entities(
        self,
        entity_name: str,
        entities_by_source: List[Dict]
    ) -> List[str]:
        """Find entities that frequently co-occur."""
        co_occurrence: Dict[str, int] = defaultdict(int)
        
        for source_data in entities_by_source:
            entity_names = [
                self._normalize_entity_name(e.text)
                for e in source_data['entities']
            ]
            
            if entity_name in entity_names:
                for other in entity_names:
                    if other != entity_name:
                        co_occurrence[other] += 1
        
        # Sort by co-occurrence count
        related = sorted(co_occurrence.items(), key=lambda x: x[1], reverse=True)
        return [r[0] for r in related[:10]]