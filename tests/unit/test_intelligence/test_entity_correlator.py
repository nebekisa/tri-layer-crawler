"""Unit tests for entity correlator."""

import pytest
import sys
from pathlib import Path
from datetime import datetime

# Ensure src is in path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from intelligence.entity_correlator import EntityCorrelator
from analytics.models import ExtractedEntity, EntityType


class TestEntityCorrelator:
    """Test entity correlation logic."""
    
    def test_normalize_entity_name(self):
        """Test entity name normalization."""
        correlator = EntityCorrelator()
        
        # Same entity, different formatting
        name1 = correlator._normalize_entity_name("Acme Corporation")
        name2 = correlator._normalize_entity_name("Acme  Corporation!")
        
        # Normalization should be consistent
        assert len(name1) == 12  # MD5 hash truncated to 12 chars
        assert len(name2) == 12
    
    def test_correlate_empty_input(self):
        """Test correlation with empty input."""
        correlator = EntityCorrelator()
        correlations = correlator.correlate([])
        assert correlations == []
    
    def test_correlate_single_source(self):
        """Test correlation with single source (should be empty)."""
        correlator = EntityCorrelator()
        
        entities_by_source = [{
            'source_url': 'https://site1.com',
            'crawled_at': datetime.utcnow(),
            'entities': [
                ExtractedEntity(
                    text='Acme Corp',
                    label=EntityType.ORGANIZATION,
                    start_char=0,
                    end_char=9,
                    confidence=1.0
                )
            ]
        }]
        
        correlations = correlator.correlate(entities_by_source)
        # Single source shouldn't produce correlations
        assert correlations == []
    
    def test_majority_vote(self):
        """Test majority voting for entity types."""
        correlator = EntityCorrelator()
        
        types = ['ORG', 'ORG', 'PERSON', 'ORG', 'PERSON']
        result = correlator._majority_vote(types)
        assert result == 'ORG'
        
        empty_result = correlator._majority_vote([])
        assert empty_result == 'UNKNOWN'
    
    def test_calculate_confidence(self):
        """Test confidence score calculation."""
        correlator = EntityCorrelator()
        
        occurrences = [
            ('url1', datetime.utcnow(), 'ORG'),
            ('url2', datetime.utcnow(), 'ORG'),
            ('url3', datetime.utcnow(), 'ORG'),
        ]
        sources = ['url1', 'url2', 'url3']
        
        confidence = correlator._calculate_confidence(occurrences, sources)
        assert 0 <= confidence <= 1