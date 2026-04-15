"""
Named Entity Recognition using spaCy with model caching.
"""

import logging
from typing import List, Optional
import spacy
from spacy.language import Language

from src.analytics.models import ExtractedEntity, EntityType
from src.analytics.model_cache import ModelCache

logger = logging.getLogger(__name__)


class EntityExtractor:
    """Extract named entities using cached spaCy model."""
    
    ENTITY_MAPPING = {
        'PERSON': EntityType.PERSON,
        'ORG': EntityType.ORGANIZATION,
        'LOC': EntityType.LOCATION,
        'GPE': EntityType.GPE,
        'PRODUCT': EntityType.PRODUCT,
        'DATE': EntityType.DATE,
        'MONEY': EntityType.MONEY,
        'PERCENT': EntityType.PERCENT,
        'EVENT': EntityType.EVENT,
        'WORK_OF_ART': EntityType.WORK_OF_ART,
        'LAW': EntityType.LAW,
        'LANGUAGE': EntityType.LANGUAGE,
        'FACILITY': EntityType.FACILITY,
    }
    
    @staticmethod
    def _load_spacy() -> Language:
        """Load spaCy model (called once by cache)."""
        try:
            return spacy.load('en_core_web_sm')
        except OSError:
            logger.error("spaCy model not found. Run: python -m spacy download en_core_web_sm")
            raise
    
    def _get_nlp(self) -> Language:
        """Get cached spaCy model."""
        cache = ModelCache()
        return cache.get_or_load('spacy_en_core_web_sm', self._load_spacy)
    
    def extract(
        self,
        text: str,
        min_confidence: float = 0.0,
        max_entities: int = 50,
        include_types: Optional[List[EntityType]] = None
    ) -> List[ExtractedEntity]:
        """Extract named entities from text."""
        if not text or not text.strip():
            return []
        
        nlp = self._get_nlp()
        
        # Truncate for performance
        max_length = 100000
        if len(text) > max_length:
            text = text[:max_length]
        
        doc = nlp(text)
        
        entities = []
        for ent in doc.ents:
            entity_type = self.ENTITY_MAPPING.get(ent.label_)
            if entity_type is None:
                continue
            
            if include_types and entity_type not in include_types:
                continue
            
            entity = ExtractedEntity(
                text=ent.text.strip(),
                label=entity_type,
                start_char=ent.start_char,
                end_char=ent.end_char,
                confidence=1.0
            )
            entities.append(entity)
            
            if len(entities) >= max_entities:
                break
        
        # Deduplicate
        seen = set()
        unique_entities = []
        for entity in entities:
            key = (entity.text.lower(), entity.label)
            if key not in seen:
                seen.add(key)
                unique_entities.append(entity)
        
        return unique_entities
    
    def extract_people(self, text: str) -> List[str]:
        """Extract person names only."""
        entities = self.extract(text, include_types=[EntityType.PERSON])
        return [e.text for e in entities]
    
    def extract_organizations(self, text: str) -> List[str]:
        """Extract organization names only."""
        entities = self.extract(text, include_types=[EntityType.ORGANIZATION])
        return [e.text for e in entities]